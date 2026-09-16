"""损坏事件流的隔离测试。

一旦 SQLite 事件流与检查点不一致(重复/丢失/乱序), 损坏对局必须在任何
入口都被挡住, 不能重新进入结算:
- 服务重启恢复时隔离, 不进内存
- 玩家断线重连时收到 match_corrupt 而非 match_resume
- 运行期内存重建兜底发现损坏时隔离, 命令被 MATCH_CORRUPT 拒绝
- 隔离后 DB status=corrupt, 不再被 active_match_for 找回
- 只读 replay 仍可审计, 并带 verify_error
"""

from __future__ import annotations

import asyncio
import json

import websockets
import pytest

from server import storage
from server.replay_support import ReplayMismatch, verify_match
from server.server import GameServer

from .conftest import (
    VALID_DECK,
    login_pair,
    recv,
    recv_until,
    send,
    ws_connect,
)


async def _events(ws, timeout=0.4):
    out = []
    try:
        while True:
            out.append(await asyncio.wait_for(recv(ws), timeout=timeout))
    except asyncio.TimeoutError:
        pass
    return out


async def _make_server(db_path, fast_timing):
    gs = GameServer(db_path, **fast_timing)
    gs.forced_match_seed = 12345
    await gs.start()
    ws = await websockets.serve(gs.handle, "127.0.0.1", 0)
    port = ws.sockets[0].getsockname()[1]
    return gs, ws, port


async def _make_running_match(db_path, fast_timing):
    """开局、打一条带检查点的玩家命令, 然后关闭服务端, 返回 (gs, ws, mid)。"""
    gs, ws, port = await _make_server(db_path, fast_timing)
    w0 = await ws_connect(port)
    w1 = await ws_connect(port)
    await send(w0, type="login", name="alice")
    await send(w1, type="login", name="bob")
    await recv_until(w0, "login_ok")
    await recv_until(w1, "login_ok")
    await send(w0, type="queue", deck=VALID_DECK)
    await send(w1, type="queue", deck=VALID_DECK)
    m0 = await recv_until(w0, "match_begin")
    await recv_until(w1, "match_begin")
    await _events(w0)
    await _events(w1)
    mid = m0["match_id"]
    live = gs.matches[mid]
    uid = next(c["uid"] for c in live.state["seats"][0]["hand"]
               if c["id"] == "militia")
    await send(w0, type="command", op_id="seed-1",
               command={"cmd": "PLAY", "uid": uid, "target": 0})
    await recv_until(w0, "command_result", op_id="seed-1")
    await w0.close()
    await w1.close()
    await asyncio.sleep(0.1)
    await gs.shutdown()
    ws.close()
    await ws.wait_closed()
    return gs, mid


def _corrupt_event_stream(conn, match_id: str) -> None:
    """复制一条会改变状态的 CARD_PLAYED 事件, 制造哈希偏离。"""
    rows = conn.execute(
        "SELECT seq, after_cmd, evt FROM events WHERE match_id=? ORDER BY seq",
        (match_id,)).fetchall()
    played = next(r for r in rows
                  if json.loads(r["evt"])["type"] == "CARD_PLAYED")
    conn.execute(
        "INSERT INTO events(match_id,after_cmd,evt) VALUES(?,?,?)",
        (match_id, played["after_cmd"], played["evt"]))
    conn.commit()


# ---------------------------------------------------------------- 重启

async def test_restart_quarantines_corrupt_match(db_path, fast_timing,
                                                 restart_server):
    _, mid = await _make_running_match(db_path, fast_timing)
    conn = storage.connect(db_path)
    assert verify_match(conn, mid)["status"] == "running"
    _corrupt_event_stream(conn, mid)
    with pytest.raises(ReplayMismatch):
        verify_match(conn, mid)
    conn.close()

    gs2, ws_server, port = await restart_server()
    try:
        # 损坏对局绝不进入内存
        assert mid not in gs2.matches
        check = storage.connect(db_path)
        assert check.execute(
            "SELECT status FROM matches WHERE match_id=?", (mid,)
        ).fetchone()["status"] == "corrupt"
        assert check.execute(
            "SELECT COUNT(*) AS n FROM corruption WHERE match_id=?", (mid,)
        ).fetchone()["n"] == 1
        check.close()
    finally:
        ws_server.close()
        await ws_server.wait_closed()
        await gs2.shutdown()


# ---------------------------------------------------------------- 重连

async def test_reconnect_corrupt_match_notifies_and_blocks_resume(
        db_path, fast_timing, restart_server):
    _, mid = await _make_running_match(db_path, fast_timing)
    conn = storage.connect(db_path)
    _corrupt_event_stream(conn, mid)
    conn.close()

    gs2, ws_server, port = await restart_server()
    token = gs2.conn.execute(
        "SELECT token FROM players WHERE name='alice'").fetchone()[0]
    try:
        w = await ws_connect(port)
        await send(w, type="login", name="alice", token=token)
        await recv_until(w, "login_ok")
        notice = await recv_until(w, "match_corrupt", timeout=3)
        assert notice["match_id"] == mid
        # 绝不能同时下发 match_resume / snapshot
        try:
            extra = await asyncio.wait_for(recv(w), timeout=0.4)
            assert extra.get("type") not in ("match_resume", "snapshot"), extra
        except asyncio.TimeoutError:
            pass
        await w.close()
    finally:
        ws_server.close()
        await ws_server.wait_closed()
        await gs2.shutdown()


async def test_reconnect_does_not_reload_corrupt_into_memory(
        db_path, fast_timing, restart_server):
    """重连时内存缺失的损坏对局不能借 _get_or_load_live 复活。"""
    _, mid = await _make_running_match(db_path, fast_timing)
    conn = storage.connect(db_path)
    _corrupt_event_stream(conn, mid)
    conn.close()

    gs2, ws_server, port = await restart_server()
    token = gs2.conn.execute(
        "SELECT token FROM players WHERE name='bob'").fetchone()[0]
    try:
        w = await ws_connect(port)
        await send(w, type="login", name="bob", token=token)
        await recv_until(w, "login_ok")
        await recv_until(w, "match_corrupt")
        assert mid not in gs2.matches
        await w.close()
    finally:
        ws_server.close()
        await ws_server.wait_closed()
        await gs2.shutdown()


# -------------------------------------------------------- 内存对局+磁盘损坏

async def test_reconnect_while_live_match_db_corrupt_quarantines(server):
    """内存 LiveMatch 仍在, 但磁盘事件流被外部损坏: 重连不得用内存对象
    直接恢复, 必须校验磁盘并隔离(端到端复现过的真实漏洞)。"""
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0)
    await _events(w1)
    gs, port = server
    mid = m0["match_id"]
    live = gs.matches[mid]
    uid = next(c["uid"] for c in live.state["seats"][0]["hand"]
               if c["id"] == "militia")
    await send(w0, type="command", op_id="live-1",
               command={"cmd": "PLAY", "uid": uid, "target": 0})
    await recv_until(w0, "command_result", op_id="live-1")
    token = gs.conn.execute(
        "SELECT token FROM players WHERE name='alice'").fetchone()[0]

    # 旧连接仍在, 直接外部损坏 DB 事件流(不经过服务端)
    _corrupt_event_stream(gs.conn, mid)
    assert mid in gs.matches  # 损坏发生后内存对象尚未被摘除

    w2 = await ws_connect(port)
    await send(w2, type="login", name="alice", token=token)
    await recv_until(w2, "login_ok")
    notice = await recv_until(w2, "match_corrupt", timeout=3)
    assert notice["match_id"] == mid
    assert mid not in gs.matches
    assert storage.match_status(gs.conn, mid) == "corrupt"
    await w2.close()
    await w0.close()
    await w1.close()


# -------------------------------------------------------- 运行期内存重建

async def test_runtime_rebuild_corruption_quarantines_match(server):
    """构造"幂等竞争兜底重建时事件流损坏"的情形: apply 返回 corrupt,
    对局被摘除, 后续命令 NOT_IN_MATCH。"""
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0)
    await _events(w1)
    gs, _ = server
    mid = m0["match_id"]
    live = gs.matches[mid]
    uid = next(c["uid"] for c in live.state["seats"][0]["hand"]
               if c["id"] == "militia")
    await send(w0, type="command", op_id="rt-1",
               command={"cmd": "PLAY", "uid": uid, "target": 0})
    await recv_until(w0, "command_result", op_id="rt-1")

    async with live.lock:
        _corrupt_event_stream(gs.conn, mid)
        rebuilt = gs._rebuild_live_state(live)
        assert rebuilt is None

    assert mid not in gs.matches
    assert storage.match_status(gs.conn, mid) == "corrupt"

    # 定时器也被取消: 等待超过一个回合时长, 不会有任何 SYSTEM_TIMEOUT 落库
    await asyncio.sleep(0.4)
    n_timeout = gs.conn.execute(
        "SELECT COUNT(*) AS n FROM commands WHERE match_id=? AND cmd='SYSTEM_TIMEOUT'",
        (mid,)).fetchone()["n"]
    assert n_timeout == 0
    await w0.close()
    await w1.close()


async def test_command_after_quarantine_is_not_in_match(server):
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0)
    await _events(w1)
    gs, _ = server
    mid = m0["match_id"]
    live = gs.matches[mid]
    uid = next(c["uid"] for c in live.state["seats"][0]["hand"]
               if c["id"] == "militia")
    await send(w0, type="command", op_id="rt-2",
               command={"cmd": "PLAY", "uid": uid, "target": 0})
    await recv_until(w0, "command_result", op_id="rt-2")
    async with live.lock:
        _corrupt_event_stream(gs.conn, mid)
        assert gs._rebuild_live_state(live) is None

    await send(w0, type="command", op_id="after",
               command={"cmd": "END_TURN"})
    # 对局已不在 _live_for 中
    res = None
    try:
        res = await asyncio.wait_for(recv(w0), timeout=1.5)
    except asyncio.TimeoutError:
        pass
    assert res is not None and res.get("code") == "NOT_IN_MATCH"
    await w0.close()
    await w1.close()


# ---------------------------------------------------------------- 回放审计

async def test_replay_of_corrupt_match_is_readonly_with_error(
        db_path, fast_timing, restart_server):
    _, mid = await _make_running_match(db_path, fast_timing)
    conn = storage.connect(db_path)
    _corrupt_event_stream(conn, mid)
    conn.close()

    gs2, ws_server, port = await restart_server()
    try:
        w = await ws_connect(port)
        await send(w, type="login", name="auditor")
        await recv_until(w, "login_ok")
        await send(w, type="replay", match_id=mid)
        rep = await recv_until(w, "replay")
        # 审计仍可看到事件流, 但状态标注 corrupt 且带校验错误
        assert rep["status"] == "corrupt"
        assert rep["verify_error"] is not None
        assert len(rep["events"]) > 0
        await w.close()
    finally:
        ws_server.close()
        await ws_server.wait_closed()
        await gs2.shutdown()


# -------------------------------------------------------- 干净对局不受影响

async def test_clean_match_still_restores_and_resumes(
        db_path, fast_timing, restart_server):
    _, mid = await _make_running_match(db_path, fast_timing)
    gs2, ws_server, port = await restart_server()
    token = gs2.conn.execute(
        "SELECT token FROM players WHERE name='alice'").fetchone()[0]
    try:
        assert mid in gs2.matches
        w = await ws_connect(port)
        await send(w, type="login", name="alice", token=token)
        await recv_until(w, "login_ok")
        await recv_until(w, "match_resume")
        snap = await recv_until(w, "snapshot")
        assert isinstance(snap["state"]["seats"][0]["hand"], list)
        await w.close()
    finally:
        ws_server.close()
        await ws_server.wait_closed()
        await gs2.shutdown()
