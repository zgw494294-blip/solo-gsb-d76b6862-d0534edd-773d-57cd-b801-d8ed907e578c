"""下载快照资源交接的进程内核对（不依赖外部 uvicorn，直接驱动 ASGI 应用）。

为什么不用 HTTP 黑盒核对资源释放：GET /batches/{id}/download 的快照连接在
「正文首次迭代前就终止」时是否释放，只能在服务进程内观察。本测试因此直接
调用 ASGI 应用，并在整个过程中 **禁用自动循环垃圾回收（gc.disable）**：若
资源仍靠生成器对象的引用环被循环 GC 回收，禁用后连接会一直打开、读事务不
结束、WAL 检查点报 busy。修复后四种终止路径都由确定性的 release 收尾：

  1. 零次迭代终止：200 响应头已发出、正文一个字节都没产出时取消；
  2. 开始迭代后取消：已产出首个压缩块后取消；
  3. 正常结束：完整读完；
  4. 发送失败：send 分别在发响应头、发首块正文时抛异常。

每条路径都核对：读事务已结束、连接已关闭（重复清理不报错）；并在制造 WAL
增长后用 PRAGMA wal_checkpoint(TRUNCATE) 不返回 busy 来证明读快照已释放。
最后核对「删除中的下载内容完整」：200 已发出后删除批次，进行中的下载仍按
原顺序输出删除前全部记录并写出完整 gzip CRC/ISIZE 尾部。

直接运行：python tests/test_download_release.py
"""
from __future__ import annotations

import asyncio
import gc
import gzip
import json
import os
import sqlite3
import sys
import tempfile

# 允许从仓库根目录或 tests/ 目录直接运行
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 必须在导入 app.* 之前固定独立的临时数据库
_TMP = tempfile.mkdtemp(prefix="download_release_")
os.environ["DB_PATH"] = os.path.join(_TMP, "batches.db")

from app import db, download  # noqa: E402
from app.main import app  # noqa: E402

ROWS = 3000

passed = 0
failed: list[str] = []


def check(name, cond, extra=""):
    global passed
    if cond:
        passed += 1
        print(f"  PASS {name}")
    else:
        failed.append(name)
        print(f"  FAIL {name} {extra}")


def _scope(batch_id: str) -> dict:
    path = f"/batches/{batch_id}/download"
    return {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 1),
        "server": ("test", 80),
        "root_path": "",
        "state": {},
    }


def _closed(conn: sqlite3.Connection) -> bool:
    """连接已关闭：再执行语句会抛 ProgrammingError。"""
    try:
        conn.execute("SELECT 1")
    except sqlite3.ProgrammingError:
        return True
    except sqlite3.Error:
        return True
    return False


def _seed_batch(batch_id: str, rows: int = ROWS) -> bytes:
    """直接插入一个已提交批次，返回上传侧 NDJSON 原文（默认分隔符）。"""
    payload = b"".join(
        json.dumps({"id": f"dd{i:06d}", "message": f"message-{i}"}).encode()
        + b"\n"
        for i in range(rows)
    )
    conn = db.connect()
    conn.execute(
        "INSERT INTO batches (id, line_count, created_at) VALUES (?, ?, ?)",
        (batch_id, rows, "2026-10-07T00:00:00+00:00"),
    )
    conn.executemany(
        "INSERT INTO lines (batch_id, seq, id, message) VALUES (?, ?, ?, ?)",
        [
            (batch_id, i + 1, f"dd{i:06d}", f"message-{i}")
            for i in range(rows)
        ],
    )
    conn.close()
    return payload


class _SendFail(Exception):
    """模拟向客户端发送失败（连接已断开导致写失败等）。"""


async def _main() -> None:
    db.init_db()

    # 记录每个下载请求拿到的快照连接与 Snapshot 对象
    snap_conns: list[sqlite3.Connection] = []
    snapshots: list[download.Snapshot] = []
    orig_open = db.open_download_snapshot

    def tracked_open(batch_id):
        conn = orig_open(batch_id)
        if conn is not None:
            snap_conns.append(conn)
        return conn

    db.open_download_snapshot = tracked_open
    orig_snap_init = download.Snapshot.__init__

    def tracked_snap_init(self, conn, bid):
        orig_snap_init(self, conn, bid)
        snapshots.append(self)

    download.Snapshot.__init__ = tracked_snap_init

    async def rx_quiet():
        await asyncio.sleep(10_000)

    # ------------------------------------------------------------------ #
    print("== 禁用自动循环垃圾回收 ==")
    gc.disable()
    check("cyclic GC disabled during checks", not gc.isenabled())

    bid = "a" * 32
    _seed_batch(bid)

    # 1) 零次迭代终止：200 响应头已发出、正文零产出时取消整个请求 ----------
    print("== 1) 正文首次迭代前终止 ==")

    async def tx_hang_until_cancel(message):
        if message["type"] == "http.response.start":
            check("zero-iteration: 200 sent before body",
                  message["status"] == 200, message["status"])
            await asyncio.sleep(10_000)  # 永不返回；等任务被取消

    task = asyncio.ensure_future(app(_scope(bid), rx_quiet, tx_hang_until_cancel))
    await asyncio.sleep(0.2)  # 等 200 响应头发出、流停在首次迭代之前
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    s = snapshots[-1]
    conn = snap_conns[-1]
    check("zero-iteration: snapshot marked released", s.released)
    check("zero-iteration: connection closed", _closed(conn))

    # 2) 开始迭代后取消 -------------------------------------------------- #
    print("== 2) 开始产出后取消 ==")
    first_body = asyncio.Event()

    async def tx_cancel_after_first(message):
        if message["type"] == "http.response.body" and message.get("body"):
            first_body.set()
            await asyncio.sleep(10_000)

    task = asyncio.ensure_future(app(_scope(bid), rx_quiet, tx_cancel_after_first))
    await asyncio.wait_for(first_body.wait(), timeout=10)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    s = snapshots[-1]
    conn = snap_conns[-1]
    check("after-start cancel: snapshot released", s.released)
    check("after-start cancel: connection closed", _closed(conn))

    # 3) 正常结束 -------------------------------------------------------- #
    print("== 3) 正常结束 ==")
    parts: list[bytes] = []
    head: dict = {}

    async def rx3():
        await asyncio.sleep(10_000)

    async def tx_collect(message):
        if message["type"] == "http.response.start":
            head["status"] = message["status"]
            head["headers"] = {
                k.decode().lower(): v.decode() for k, v in message["headers"]
            }
        elif message["type"] == "http.response.body":
            parts.append(message.get("body", b""))

    await app(_scope(bid), rx3, tx_collect)
    body = b"".join(parts)
    s = snapshots[-1]
    conn = snap_conns[-1]
    check("normal: status 200", head.get("status") == 200, head)
    check("normal: content-type application/gzip",
          head.get("headers", {}).get("content-type") == "application/gzip",
          head.get("headers"))
    check("normal: no content-encoding",
          "content-encoding" not in head.get("headers", {}), head.get("headers"))
    check(
        "normal: content-disposition unchanged",
        head.get("headers", {}).get("content-disposition")
        == f'attachment; filename="batch-{bid}.json.gz"',
        head.get("headers"),
    )
    check("normal: snapshot released", s.released)
    check("normal: connection closed", _closed(conn))
    # gzip.decompress 会校验 CRC32/ISIZE 尾部；逐行 JSON 与顺序逐条核对
    raw = gzip.decompress(body)
    records = [json.loads(line) for line in raw.decode("utf-8").splitlines()]
    check("normal: all rows in order with LF + gzip trailer",
          raw.endswith(b"\n") and len(records) == ROWS
          and records[0]["id"] == "dd000000"
          and records[-1]["id"] == f"dd{ROWS - 1:06d}")

    # 4) 发送失败 -------------------------------------------------------- #
    print("== 4) 发送失败 ==")
    for tag, fail_on_body in (
        ("response.start", False),
        ("first body chunk", True),
    ):
        seen_body = asyncio.Event()

        async def tx_fail(message, fail_on_body=fail_on_body):
            if message["type"] == "http.response.start" and not fail_on_body:
                raise _SendFail("write failed sending status line")
            if (
                fail_on_body
                and message["type"] == "http.response.body"
                and message.get("body")
            ):
                seen_body.set()
                raise _SendFail("write failed sending body")

        try:
            await app(_scope(bid), rx_quiet, tx_fail)
            propagated = False
        except BaseException as exc:  # anyio 可能包成 ExceptionGroup
            propagated = isinstance(exc, _SendFail) or any(
                isinstance(e, _SendFail)
                for e in getattr(exc, "exceptions", [])
            )
        s = snapshots[-1]
        conn = snap_conns[-1]
        check(f"send-fail at {tag}: exception propagated", propagated)
        check(f"send-fail at {tag}: snapshot released", s.released)
        check(f"send-fail at {tag}: connection closed", _closed(conn))

    # 5) 重复清理幂等 ---------------------------------------------------- #
    print("== 5) 重复清理不报错 ==")
    for s in snapshots:
        s.release()
        s.release()
    check("repeated release() idempotent", True)

    # 6) 未知批次先 404，且不留快照连接 ---------------------------------- #
    print("== 6) 未知批次 404 ==")
    n_conns_before = len(snap_conns)
    head404 = {}

    async def tx404(message):
        if message["type"] == "http.response.start":
            head404["status"] = message["status"]

    await app(_scope("deadbeefdeadbeefdeadbeefdeadbeef"), rx_quiet, tx404)
    check("unknown batch -> 404", head404.get("status") == 404, head404)
    check("unknown batch opens no snapshot", len(snap_conns) == n_conns_before)

    # 7) WAL 快照确已释放：制造增长后 TRUNCATE 检查点不得 busy ------------- #
    print("== 7) WAL 读快照已释放（checkpoint 不 busy）==")
    w = db.connect()
    w.executemany(
        "INSERT INTO batches (id, line_count, created_at) VALUES (?, ?, ?)",
        [(f"{i:032x}", 0, "t") for i in range(1, 5000)],
    )
    busy, _log, _ckpt = w.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    w.close()
    check("wal_checkpoint(TRUNCATE) not busy", busy == 0, f"busy={busy}")

    # 8) 删除中的下载内容完整（200 已发后删除，仍输出全部旧记录+完整尾部）-- #
    print("== 8) 删除进行中的下载内容完整 ==")
    dbid = "b" * 32
    expected_payload = _seed_batch(dbid)
    expected_records = [
        json.loads(line) for line in expected_payload.decode("utf-8").splitlines()
    ]

    dl_parts: list[bytes] = []
    first = asyncio.Event()

    async def tx_slow_then_delete(message):
        if message["type"] == "http.response.body" and message.get("body"):
            if not first.is_set():
                first.set()
                # 首个压缩块（200 响应头与 gzip 字节）已发出，删除批次
                await asyncio.to_thread(
                    _delete_batch_blocking, dbid
                )
            await asyncio.sleep(0.002)  # 慢速下载，拉长与删除的重叠
            dl_parts.append(message["body"])

    async def rx8():
        await asyncio.sleep(10_000)

    await app(_scope(dbid), rx8, tx_slow_then_delete)
    dl_body = b"".join(dl_parts)
    # gzip.decompress 校验 CRC32/ISIZE；若尾部不完整会抛 BadGzipFile
    dl_raw = gzip.decompress(dl_body)
    dl_records = [json.loads(line) for line in dl_raw.decode("utf-8").splitlines()]
    check("download amid delete: full gzip trailer (CRC/ISIZE)",
          dl_body[:2] == b"\x1f\x8b" and dl_raw.endswith(b"\n")
          and len(dl_records) == ROWS)
    check("download amid delete: all old records in original order",
          dl_records == expected_records,
          f"got={len(dl_records)} expected={len(expected_records)}")

    # 删除完成后的新下载为 404
    head_after = {}

    async def tx_after(message):
        if message["type"] == "http.response.start":
            head_after["status"] = message["status"]
        if message["type"] == "http.response.body":
            # 404 是 JSON 错误体，绝不能是 gzip
            assert message.get("body", b"")[:2] != b"\x1f\x8b"

    await app(_scope(dbid), rx8, tx_after)
    check("new download after delete -> 404 (no gzip)",
          head_after.get("status") == 404, head_after)

    # 慢下载不拖延删除：上面删除在首块后已即时完成；删除连接也已关闭
    check("amid-delete download released its snapshot", snapshots[-1].released
          and _closed(snap_conns[-1]))

    gc.enable()


def _delete_batch_blocking(batch_id: str) -> None:
    conn = db.connect()
    try:
        count = db.delete_batch(conn, batch_id)
        if count != ROWS:
            raise AssertionError(f"deleted {count} rows, expected {ROWS}")
    finally:
        conn.close()


def main() -> int:
    asyncio.run(_main())
    print(f"\n{passed} passed, {len(failed)} failed: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
