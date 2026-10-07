"""已提交批次 -> gzip NDJSON 的流式下载。

内存有界：逐页（键集分页，每页最多 DOWNLOAD_PAGE_SIZE 行）从主库读取，
逐行序列化为 NDJSON 并压入 gzip 压缩器，压缩块产出后立即下发；任何时刻
内存里只有一页记录、一行序列化字节和压缩器缓冲，不会攒齐整批 NDJSON、
完整压缩文件或完整解压结果，也不生成任何临时文件。

下载连接在发出 200 响应头之前就已进入一个显式只读事务（见
db.open_download_snapshot），固定 WAL 读快照：下载期间该批次即使被永久
删除，流内读到的仍是删除前的全部记录（顺序不变），gzip CRC32/ISIZE 尾部
照常写出；而 WAL 下读事务不阻塞写，慢下载不阻塞删除、另一批上传提交或
已完成批次的读取。

资源交接：快照连接在返回 StreamingResponse 之前就已打开并固定，因此不能
依赖异步生成器本身的终结（生成器若从未开始迭代，其 finally 不会执行，且
对象还可能处在引用环里只能等循环垃圾回收——这会拖延读事务结束与 WAL 回收）。
路由在响应的 BackgroundTask 里登记 Snapshot.release 作为最终兜底：只要
响应对象（无论正常完成、断连还是取消）走完 ASGI 生命周期都会执行，即使
正文一次都没开始迭代也一样。生成器自己的 finally 负责正常/中途异常路径的
即时释放；release 幂等，重复清理不报错。
"""
from __future__ import annotations

import json
import sqlite3
import zlib
from typing import AsyncIterator

from anyio import CancelScope, to_thread

from . import db

# 单次从库中取出的行数上限：单行正文上限 64KiB，
# 整页最坏约 64 * 64KiB = 4MiB，正常情况下远小于此。
DOWNLOAD_PAGE_SIZE = 64


class _GzipNdjsonStreamer:
    """逐页读行、逐行序列化、逐块 gzip 压缩的小状态机（在线程中运行）。"""

    def __init__(self, conn: sqlite3.Connection, batch_id: str) -> None:
        # conn 已由路由在响应头发出前置入只读事务，持有删除前快照。
        self._conn = conn
        # wbits=31：gzip 封装；结束时由 finish() 写出 CRC32/ISIZE 校验尾部
        self._comp = zlib.compressobj(level=6, wbits=31)
        self._batch_id = batch_id
        self._after_seq = 0
        self._rows_done = False
        self._gzip_closed = False

    def produce(self) -> tuple[bytes, bool]:
        """读取并压缩下一页。

        返回 (压缩块, finished)：finished 为真时压缩块含 gzip 结束尾部
        （CRC32 + ISIZE），整个流到此完整结束。压缩块可能为空（zlib 内部
        缓冲攒批时无产出），此时 finished 必为假，调用方继续取下一页。
        """
        if self._rows_done:
            return self._finish_gzip(), True

        rows = db.iter_batch_lines(
            self._conn, self._batch_id, DOWNLOAD_PAGE_SIZE, self._after_seq
        )
        payload = bytearray()
        for row in rows:
            # 只输出 id、message；ensure_ascii=False 保留原文 UTF-8 字节，
            # 引号、反斜杠、换行/制表等由 json.dumps 按 JSON 规则转义，
            # compact 分隔符；每行（含末行）都以 \n 结束。
            line = json.dumps(
                {"id": row["id"], "message": row["message"]},
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            payload.extend(line)
            payload.append(0x0A)
            self._after_seq = row["seq"]

        last_page = len(rows) < DOWNLOAD_PAGE_SIZE
        if last_page:
            self._rows_done = True

        chunk = self._comp.compress(bytes(payload)) if payload else b""
        if last_page:
            # 空批不可能出现：上传时空批返回 422，下载前还做了 404 预检
            return chunk + self._finish_gzip(), True
        return chunk, False

    def _finish_gzip(self) -> bytes:
        if self._gzip_closed:
            return b""
        self._gzip_closed = True
        # flush 输出 zlib 内部残留并追加 gzip CRC32/ISIZE 结束尾部
        return self._comp.flush()


class Snapshot:
    """下载快照资源：持有固定 WAL 读快照的连接，并负责其确定性释放。

    生命周期由两条路径共同覆盖且幂等：
    - 生成器正常结束 / 发送失败 / 开始迭代后被取消：生成器 finally 中
      在屏蔽取消的作用域内 release（即时释放）；
    - 正文在首次迭代之前就终止（断连/取消发生在发响应头前后、生成器从未
      进入）：生成器 finally 根本不会执行，由路由登记的响应 BackgroundTask
      在响应收尾时 release。

    无论被调用几次都只清理一次：COMMIT（异常状态下 ROLLBACK 兜底）结束
    只读事务，随后关闭连接释放游标与 WAL 快照。
    """

    def __init__(self, conn: sqlite3.Connection, batch_id: str) -> None:
        self._conn = conn
        self._batch_id = batch_id
        self._released = False

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    @property
    def batch_id(self) -> str:
        return self._batch_id

    @property
    def released(self) -> bool:
        """读事务与连接是否已经释放（供关闭路径核对，正常清理后为真）。"""
        return self._released

    def release(self) -> None:
        """结束只读快照事务并释放连接；正常完成、取消、断连或发送失败都调用。

        幂等：重复调用（生成器 finally 与 BackgroundTask 兜底各执行一次）
        直接返回，不报错。
        """
        if self._released:
            return
        self._released = True
        conn, self._conn = self._conn, None
        try:
            # 只读事务用 COMMIT 结束即可；若产流中途出错导致事务状态异常，
            # ROLLBACK 兜底，随后关闭连接释放游标与 WAL 快照。
            try:
                conn.execute("COMMIT")
            except sqlite3.Error:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
        finally:
            # sqlite3.Connection.close 幂等且自身不抛异常；显式兜底防止任何
            # 意外阻断连接释放（读快照回收不能依赖垃圾回收）。
            try:
                conn.close()
            except sqlite3.Error:
                pass


async def gzip_ndjson_stream(snapshot: Snapshot) -> AsyncIterator[bytes]:
    """逐块产出该批次的完整 gzip 流的异步生成器。

    snapshot 是路由在发出响应头前打开的快照连接（已在只读事务中）。无论
    正常结束、向客户端写入失败（中途断连）还是协程开始迭代后被取消，
    finally 都会在屏蔽取消的作用域内释放事务与连接。若协程从未被迭代
    （首次迭代前就终止），本 finally 不会执行——由路由的 BackgroundTask
    兜底释放，见 main.download_batch。
    """
    streamer = await to_thread.run_sync(
        _GzipNdjsonStreamer, snapshot.conn, snapshot.batch_id
    )
    try:
        while True:
            chunk, finished = await to_thread.run_sync(streamer.produce)
            if chunk:
                yield chunk
            if finished:
                break
    finally:
        # 屏蔽取消也要完成关闭：连接释放不应因任务已取消而被吞掉。
        # release 幂等；响应收尾时 BackgroundTask 再调一次也无副作用。
        with CancelScope(shield=True):
            await to_thread.run_sync(snapshot.release)
