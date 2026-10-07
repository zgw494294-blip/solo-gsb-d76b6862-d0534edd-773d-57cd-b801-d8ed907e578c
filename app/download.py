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

资源交接：快照连接由 GzipNdjsonDownload 持有，close 幂等。正常结束、
客户端断连、协程取消、发送失败都会及时结束读事务、释放游标与连接；
即使响应正文从未开始迭代（例如响应头尚未发出客户端就已断开），
SnapshotStreamingResponse.__call__ 的 finally 也会兜底关闭——清理不依赖
垃圾回收，快照释放后 WAL 即可被回收。
"""
from __future__ import annotations

import json
import sqlite3
import zlib
from typing import AsyncIterator

from anyio import CancelScope, to_thread
from starlette.responses import StreamingResponse
from starlette.types import Receive, Scope, Send

from . import db

# 单次从库中取出的行数上限：单行正文上限 64KiB，
# 整页最坏约 64 * 64KiB = 4MiB，正常情况下远小于此。
DOWNLOAD_PAGE_SIZE = 64


class _GzipNdjsonStreamer:
    """逐页读行、逐行序列化、逐块 gzip 压缩的小状态机（在线程中运行）。"""

    def __init__(self, conn: sqlite3.Connection, batch_id: str) -> None:
        # conn 已由路由在响应头发出前置入只读事务，持有删除前快照；
        # 本对象负责在结束时提交/回滚该事务并关闭连接（close 幂等）。
        self._conn = conn
        # wbits=31：gzip 封装；结束时由 flush() 写出 CRC32/ISIZE 校验尾部
        self._comp = zlib.compressobj(level=6, wbits=31)
        self._batch_id = batch_id
        self._after_seq = 0
        self._rows_done = False
        self._flushed = False
        self._released = False

    @property
    def released(self) -> bool:
        """读事务是否已结束且连接已关闭。"""
        return self._released

    def produce(self) -> tuple[bytes, bool]:
        """读取并压缩下一页。

        返回 (压缩块, finished)：finished 为真时压缩块含 gzip 结束尾部
        （CRC32 + ISIZE），整个流到此完整结束。压缩块可能为空（zlib 内部
        缓冲攒批时无产出），此时 finished 必为假，调用方继续取下一页。
        """
        if self._rows_done:
            return self._finish(), True

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
            return chunk + self._finish(), True
        return chunk, False

    def _finish(self) -> bytes:
        if self._flushed:
            return b""
        self._flushed = True
        # flush 输出 zlib 内部残留并追加 gzip CRC32/ISIZE 结束尾部
        return self._comp.flush()

    def close(self) -> None:
        """结束只读快照事务并释放游标/连接；幂等，重复调用不报错。"""
        if self._released:
            return
        self._released = True
        try:
            # 只读事务用 COMMIT 结束即可；若产流中途出错导致事务状态异常，
            # ROLLBACK 兜底，随后关闭连接释放 WAL 快照。
            try:
                self._conn.execute("COMMIT")
            except sqlite3.Error:
                try:
                    self._conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
        finally:
            self._conn.close()


class GzipNdjsonDownload:
    """一次整批下载：持有快照连接，保证它恰好被释放一次。

    路由在发出任何响应字节前创建（连接已在只读事务中）。无论响应正文
    是否开始迭代、正常结束、客户端断连、协程取消还是发送失败，
    aclose() 都会结束读事务并关闭连接；重复调用不报错。
    """

    def __init__(self, conn: sqlite3.Connection, batch_id: str) -> None:
        self._streamer = _GzipNdjsonStreamer(conn, batch_id)

    async def iterate(self) -> AsyncIterator[bytes]:
        """逐块产出该批次完整 gzip 流的异步生成器（响应正文迭代器）。"""
        streamer = self._streamer
        try:
            while True:
                chunk, finished = await to_thread.run_sync(streamer.produce)
                if chunk:
                    yield chunk
                if finished:
                    break
        finally:
            # 正常结束、写入失败或协程被取消都会走到这里；屏蔽取消完成
            # 关闭：连接释放不应因任务已取消而被吞掉
            if not streamer.released:
                await self.aclose()

    async def aclose(self) -> None:
        """幂等关闭快照连接；即使正文从未开始迭代也必须（且只需）调用。"""
        streamer = self._streamer
        if streamer.released:
            return
        with CancelScope(shield=True):
            await to_thread.run_sync(streamer.close)


class SnapshotStreamingResponse(StreamingResponse):
    """携带快照连接的流式响应：__call__ 结束时兜底释放快照。

    Starlette 的 StreamingResponse 不会关闭 body_iterator：若客户端在
    正文开始前断连、或发送响应头/首块即失败，异步生成器可能从未启动，
    其 finally 不会执行，快照只能等垃圾回收。这里在 __call__ 层再兜底
    关闭一次（幂等），保证读事务与连接在任何终止路径下都被及时释放。
    """

    def __init__(self, download: GzipNdjsonDownload, **kwargs) -> None:
        self._download = download
        super().__init__(download.iterate(), **kwargs)

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            # 正常完成、断连、取消、发送失败（含正文尚未开始）都会走到
            await self._download.aclose()
