"""GET /batches/{batch_id}/download 快照资源交接的确定性检查。

不启动 HTTP 服务：直接调用路由拿到响应对象，再以 ASGI 方式驱动它，
确定性地覆盖全部终止路径——

- 正常完整结束
- 正文开始后客户端断连（任务被取消）
- 正文零次迭代即终止（发送响应头即失败，正文异步生成器从未启动）
- 正文发送中途失败
- 响应已建但正文未开始时批次被删除 / 下载进行中批次被删除（快照完整性）

全程禁用循环垃圾回收（gc.disable()）：每个场景结束后立即核对读事务已
显式结束（COMMIT/ROLLBACK）、连接已关闭（测试自身仍持有连接对象，
排除被 GC 回收的可能）、WAL 可被完全回收（先写一笔产生新 WAL 帧，
wal_checkpoint(TRUNCATE) 成功才说明没有悬挂的读快照）——即资源释放
不依赖垃圾回收。另核对重复清理不报错、未知批次 404 且无泄漏。

运行：python3 tests/test_download_cleanup.py
"""
from __future__ import annotations

import gc
import gzip
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

# 独立临时库：必须在导入 app 之前设置（app.config 在导入时读取 DB_PATH）
_TMPDIR = tempfile.mkdtemp(prefix="ndjson_download_cleanup_")
os.environ["DB_PATH"] = os.path.join(_TMPDIR, "batches.db")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import anyio  # noqa: E402
from fastapi import HTTPException  # noqa: E402

from app import db, download, main as app_main  # noqa: E402

passed = 0
failed = []


def check(name, cond, extra=""):
    global passed
    if cond:
        passed += 1
        print(f"  PASS {name}")
    else:
        failed.append(name)
        print(f"  FAIL {name} {extra}")


# --------------------------------------------------------------------------- #
# 快照连接监视：记录事务语句与 close 调用，用于核对资源交接
# --------------------------------------------------------------------------- #
class ConnSpy:
    def __init__(self, real: sqlite3.Connection) -> None:
        self.real = real
        self.statements: list[str] = []
        self.closed = False

    def execute(self, sql, parameters=()):
        head = sql.strip().split(None, 1)[0].upper() if sql.strip() else ""
        self.statements.append(head)
        return self.real.execute(sql, parameters)

    def close(self):
        self.closed = True
        self.real.close()


spies: list[ConnSpy] = []
_real_open_snapshot = db.open_download_snapshot


def _spy_open_snapshot(batch_id):
    conn = _real_open_snapshot(batch_id)
    if conn is None:
        return None
    spy = ConnSpy(conn)
    spies.append(spy)
    return spy


db.open_download_snapshot = _spy_open_snapshot


# --------------------------------------------------------------------------- #
# 数据准备与断言工具
# --------------------------------------------------------------------------- #
def make_records(n, tag):
    # 每条 400 字符随机 hex：不可压缩，确保 gzip 流分多块产出
    return [
        {"id": f"{tag}{i:05d}", "message": os.urandom(200).hex()}
        for i in range(n)
    ]


def expected_ndjson(records):
    return b"".join(
        json.dumps(
            {"id": r["id"], "message": r["message"]},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
        for r in records
    )


def insert_batch(batch_id, records):
    conn = db.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO batches (id, line_count, created_at) VALUES (?, ?, ?)",
            (batch_id, len(records), "2026-01-01T00:00:00+00:00"),
        )
        conn.executemany(
            "INSERT INTO lines (batch_id, seq, id, message) VALUES (?, ?, ?, ?)",
            [
                (batch_id, i + 1, r["id"], r["message"])
                for i, r in enumerate(records)
            ],
        )
        conn.execute("COMMIT")
    finally:
        conn.close()


_probe = 0


def wal_reclaimable() -> bool:
    """写一笔产生新 WAL 帧后，wal_checkpoint(TRUNCATE) 能否完全回收。

    若有读快照悬挂（读事务未结束），其读标记会挡住检查点 -> busy=1。
    """
    global _probe
    _probe += 1
    conn = db.connect()
    try:
        conn.execute("PRAGMA busy_timeout = 1000")
        conn.execute(f"PRAGMA user_version = {_probe}")
        busy = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]
        return busy == 0
    finally:
        conn.close()


def check_released(name, spy):
    """核对快照资源已释放：事务显式结束、连接关闭、WAL 可完全回收。"""
    check(
        f"{name}: 读事务已显式结束(COMMIT/ROLLBACK)",
        "COMMIT" in spy.statements or "ROLLBACK" in spy.statements,
        str(spy.statements),
    )
    check(f"{name}: 连接已关闭", spy.closed)
    try:
        spy.real.execute("SELECT 1")
        check(f"{name}: 底层连接不可用(确已关闭)", False, "still usable")
    except sqlite3.ProgrammingError:
        check(f"{name}: 底层连接不可用(确已关闭)", True)
    check(f"{name}: WAL 可完全回收(快照已释放)", wal_reclaimable())


async def _never():
    """客户端永不断连的 receive。"""
    await anyio.sleep_forever()
    return {"type": "http.disconnect"}  # 不可达


def body_of(sent) -> bytes:
    return b"".join(
        m.get("body", b"") for m in sent if m["type"] == "http.response.body"
    )


def contains_error(err, exc_type) -> bool:
    """err（可能是 anyio 任务组包出的 ExceptionGroup）中是否含指定异常。"""
    if err is None:
        return False
    if isinstance(err, exc_type):
        return True
    return any(
        contains_error(e, exc_type) for e in getattr(err, "exceptions", [])
    )


# --------------------------------------------------------------------------- #
# 场景
# --------------------------------------------------------------------------- #
async def scenario_normal(bid, expected):
    print("== 正常完整结束 ==")
    response = app_main.download_batch(bid)
    spy = spies[-1]
    sent = []

    async def send(message):
        sent.append(message)

    await response({}, _never, send)
    start = sent[0]
    check("normal: 200 response start", start["status"] == 200, start)
    headers = dict(start["headers"])
    check(
        "normal: 响应头不变(content-type/disposition)",
        headers.get(b"content-type") == b"application/gzip"
        and headers.get(b"content-disposition")
        == f'attachment; filename="batch-{bid}.json.gz"'.encode(),
        str(headers),
    )
    check("normal: 无 content-encoding", b"content-encoding" not in headers)
    body = body_of(sent)
    check(
        "normal: gzip 完整且内容逐字节一致",
        gzip.decompress(body) == expected,
    )
    check_released("normal", spy)


async def scenario_disconnect(bid, expected):
    print("== 正文开始后客户端断连（取消） ==")
    response = app_main.download_batch(bid)
    spy = spies[-1]
    sent = []
    first_chunk = anyio.Event()

    async def send(message):
        if message["type"] == "http.response.body" and message.get("more_body"):
            if first_chunk.is_set():
                # 第二块起留出空档，让断连确定性地先送达（取消会打断它）
                await anyio.sleep(2)
            first_chunk.set()
        sent.append(message)

    async def receive():
        await first_chunk.wait()
        return {"type": "http.disconnect"}

    await response({}, receive, send)
    body = body_of(sent)
    check("disconnect: 已收到 gzip 头", body[:2] == b"\x1f\x8b", body[:4])
    check(
        "disconnect: 流被中途截断",
        0 < len(body) < len(expected),
        f"{len(body)} vs {len(expected)}",
    )
    check_released("disconnect", spy)


async def scenario_zero_iteration(bid):
    print("== 正文零次迭代即终止（发送响应头即失败） ==")
    response = app_main.download_batch(bid)
    spy = spies[-1]

    async def send(message):
        raise RuntimeError("client gone before headers")

    err = None
    try:
        await response({}, _never, send)
    except BaseException as exc:  # anyio 任务组会包成 ExceptionGroup
        err = exc
    check(
        "zero-iter: 发送失败向外传播",
        contains_error(err, RuntimeError),
        repr(err),
    )
    check(
        "zero-iter: 正文从未开始(未执行任何 SELECT)",
        "SELECT" not in spy.statements,
        str(spy.statements),
    )
    check_released("zero-iter", spy)


async def scenario_send_fails_midstream(bid):
    print("== 正文发送中途失败 ==")
    response = app_main.download_batch(bid)
    spy = spies[-1]
    sent = []

    async def send(message):
        if (
            message["type"] == "http.response.body"
            and message.get("more_body")
            and any(m["type"] == "http.response.body" for m in sent)
        ):
            raise RuntimeError("client gone mid-stream")
        sent.append(message)

    err = None
    try:
        await response({}, _never, send)
    except BaseException as exc:
        err = exc
    check(
        "midstream: 发送失败向外传播",
        contains_error(err, RuntimeError),
        repr(err),
    )
    check(
        "midstream: 正文已开始(已执行 SELECT)",
        "SELECT" in spy.statements,
        str(spy.statements),
    )
    check_released("midstream", spy)
    # 服务端兜底已释放快照；再显式关闭遗留的正文迭代器也不报错
    try:
        await response.body_iterator.aclose()
        check("midstream: 重复清理不报错", True)
    except Exception as exc:
        check("midstream: 重复清理不报错", False, repr(exc))


async def scenario_delete_before_first_byte(bid, records, expected):
    print("== 响应已建、正文未开始即删除批次（快照响应前已固定） ==")
    response = app_main.download_batch(bid)  # 快照已固定，正文尚未开始
    spy = spies[-1]
    t0 = time.monotonic()
    del_conn = db.connect()
    try:
        deleted = db.delete_batch(del_conn, bid)
    finally:
        del_conn.close()
    del_ms = (time.monotonic() - t0) * 1000
    check(
        "delete-before: 删除不被已建快照拖延",
        deleted == len(records) and del_ms < 3000,
        f"deleted={deleted} {del_ms:.0f}ms",
    )
    body = b"".join([c async for c in response.body_iterator])
    check(
        "delete-before: 仍输出删除前全部记录与完整 gzip 尾部",
        gzip.decompress(body) == expected,
    )
    check_released("delete-before", spy)


async def scenario_delete_during_download(bid, records, expected):
    print("== 下载进行中删除批次（慢下载不拖延删除/提交） ==")
    response = app_main.download_batch(bid)
    spy = spies[-1]
    chunks = []
    state = {}
    async for chunk in response.body_iterator:
        chunks.append(chunk)
        if "delete_ms" not in state:
            # 首块已出、下载进行中：先验证另一批提交不被拖延……
            t0 = time.monotonic()
            insert_batch("probe-batch", [{"id": "p", "message": "m"}])
            state["commit_ms"] = (time.monotonic() - t0) * 1000
            # ……再删除本批次，删除也不被慢下载拖延
            t0 = time.monotonic()
            del_conn = db.connect()
            try:
                state["deleted"] = db.delete_batch(del_conn, bid)
            finally:
                del_conn.close()
            state["delete_ms"] = (time.monotonic() - t0) * 1000
    body = b"".join(chunks)
    check(
        "delete-during: 他批提交不被慢下载拖延",
        state.get("commit_ms", 99999) < 3000,
        f"{state.get('commit_ms', -1):.0f}ms",
    )
    check(
        "delete-during: 删除快速完成",
        state.get("deleted") == len(records) and state["delete_ms"] < 3000,
        str(state),
    )
    check(
        "delete-during: 流内仍是删除前全部记录+完整 gzip 尾部",
        gzip.decompress(body) == expected,
    )
    check_released("delete-during", spy)
    # 删除完成后：新下载 404，且 404 路径不留泄漏
    try:
        app_main.download_batch(bid)
        check("delete-during: 删除后新下载 404", False)
    except HTTPException as exc:
        check("delete-during: 删除后新下载 404", exc.status_code == 404)
    check("delete-during: 404 路径无泄漏", wal_reclaimable())


async def scenario_component(bid, expected):
    print("== 组件级：零次迭代关闭 + 重复清理不报错 ==")
    # 正文从未开始：aclose 也必须释放快照
    conn = db.open_download_snapshot(bid)
    spy = spies[-1]
    stream = download.GzipNdjsonDownload(conn, bid)
    await stream.aclose()
    check_released("component-zero-iter", spy)
    try:
        await stream.aclose()  # 重复清理
        check("component: 重复 aclose 不报错", True)
    except Exception as exc:
        check("component: 重复 aclose 不报错", False, repr(exc))

    # 完整迭代后再 aclose：同样幂等
    conn2 = db.open_download_snapshot(bid)
    spy2 = spies[-1]
    stream2 = download.GzipNdjsonDownload(conn2, bid)
    body = b"".join([c async for c in stream2.iterate()])
    check("component: 完整迭代内容一致", gzip.decompress(body) == expected)
    try:
        await stream2.aclose()  # 已释放，幂等不报错
        check("component: 迭代后重复 aclose 不报错", True)
    except Exception as exc:
        check("component: 迭代后重复 aclose 不报错", False, repr(exc))
    check_released("component-full", spy2)


async def scenario_unknown():
    print("== 未知批次 404 且无泄漏 ==")
    before = len(spies)
    try:
        app_main.download_batch("0" * 32)
        check("unknown: 404", False)
    except HTTPException as exc:
        check("unknown: 404", exc.status_code == 404)
    check("unknown: 未遗留快照连接", len(spies) == before)
    check("unknown: WAL 可完全回收", wal_reclaimable())


async def run():
    check("循环垃圾回收已禁用(gc.disable)", not gc.isenabled())
    fx = {}
    for tag in (
        "normal", "disconnect", "zero", "mid",
        "delbefore", "delduring", "component",
    ):
        bid = db.new_batch_id()
        records = make_records(200, tag)
        insert_batch(bid, records)
        fx[tag] = (bid, records, expected_ndjson(records))

    await scenario_normal(fx["normal"][0], fx["normal"][2])
    await scenario_disconnect(fx["disconnect"][0], fx["disconnect"][2])
    await scenario_zero_iteration(fx["zero"][0])
    await scenario_send_fails_midstream(fx["mid"][0])
    await scenario_delete_before_first_byte(*fx["delbefore"])
    await scenario_delete_during_download(*fx["delduring"])
    await scenario_component(fx["component"][0], fx["component"][2])
    await scenario_unknown()


def main():
    # 全程禁用循环垃圾回收：资源释放不得依赖 GC
    gc.disable()
    gc.collect()
    db.init_db()
    anyio.run(run)
    print(f"\n{passed} passed, {len(failed)} failed: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
