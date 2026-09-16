"""隔离与排队操作的并发隔离测试。

复现漏洞: 命令/超时任务在取锁前持有 live 引用并排队等锁, 期间对局被隔离,
旧实现取锁后仍继续 decide/fold/落库, 覆盖隔离状态。修复后这些排队操作必须:
- 全部被闸门拒绝(corrupt/ended), 不产生命令/事件/检查点
- 不重排定时器、不复活 corrupt 对局
- DB 事务守卫是最后一道防线(直接绕过内存闸门时也写不进去)
"""

from __future__ import annotations

import asyncio
import json

import pytest

from server import storage
from server.engine.engine import decide

from .conftest import (
    login_pair,
    recv,
    recv_until,
    send,
)


async def _events(ws, timeout=0.4):
    out = []
    try:
        while True:
            out.append(await asyncio.wait_for(recv(ws), timeout=timeout))
    except asyncio.TimeoutError:
        pass
    return out


def _corrupt(conn, mid):
    rows = conn.execute(
        "SELECT after_cmd, evt FROM events WHERE match_id=? ORDER BY seq",
        (mid,)).fetchall()
    played = next(r for r in rows
                  if json.loads(r["evt"])["type"] == "CARD_PLAYED")
    conn.execute("INSERT INTO events(match_id,after_cmd,evt) VALUES(?,?,?)",
                 (mid, played["after_cmd"], played["evt"]))
    conn.commit()


# ------------------------------------------------ 排队命令撞上隔离

async def test_queued_commands_after_quarantine_all_rejected(server):
    """构造若干已经拿到 live 引用、即将进入结算的协程; 在它们取锁前隔离
    对局。所有排队命令必须返回 corrupt, DB 不新增任何命令/事件。"""
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0)
    await _events(w1)
    gs, _ = server
    mid = m0["match_id"]
    live = gs.matches[mid]
    uid = next(c["uid"] for c in live.state["seats"][0]["hand"]
               if c["id"] == "militia")
    await send(w0, type="command", op_id="seed-q",
               command={"cmd": "PLAY", "uid": uid, "target": 0})
    await recv_until(w0, "command_result", op_id="seed-q")

    # 先持有锁, 让排队操作阻塞; 持锁期间损坏并隔离对局后再释放
    async with live.lock:
        _corrupt(gs.conn, mid)  # 外部损坏(故意插入一条坏事件)
        storage.quarantine_match(gs.conn, mid, "test quarantine")
        n_commands_before = gs.conn.execute(
            "SELECT COUNT(*) n FROM commands WHERE match_id=?", (mid,)
        ).fetchone()["n"]
        n_events_before = gs.conn.execute(
            "SELECT COUNT(*) n FROM events WHERE match_id=?", (mid,)
        ).fetchone()["n"]
        live_ref = live
        # 直接在隔离后调用结算核心(模拟已排队、刚拿到锁的操作)
        results = []
        for cmd, op in (
            ({"cmd": "END_TURN", "seat": 0}, "q1"),
            ({"cmd": "PASS", "seat": 1}, "q2"),
            ({"cmd": "SYSTEM_TIMEOUT", "seat": 0, "reason": "deadline"}, None),
        ):
            r = await gs._apply_locked(live_ref, cmd, op)
            results.append(r["status"])

    assert set(results) == {"corrupt"}, results
    # 没有新增命令与事件(事务守卫 + 闸门双保险)
    assert gs.conn.execute(
        "SELECT COUNT(*) n FROM commands WHERE match_id=?", (mid,)
    ).fetchone()["n"] == n_commands_before
    assert gs.conn.execute(
        "SELECT COUNT(*) n FROM events WHERE match_id=?", (mid,)
    ).fetchone()["n"] == n_events_before
    assert mid not in gs.matches
    assert storage.match_status(gs.conn, mid) == "corrupt"
    await w0.close()
    await w1.close()


async def test_db_transaction_guard_blocks_write_to_corrupt_match(server):
    """绕过内存闸门直接调 storage.apply_command: 事务内 status 检查必须
    让整笔写入回滚, corrupt 对局不会被覆盖回 running。"""
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0)
    await _events(w1)
    gs, _ = server
    mid = m0["match_id"]
    live = gs.matches[mid]

    n_events_before = gs.conn.execute(
        "SELECT COUNT(*) n FROM events WHERE match_id=?", (mid,)
    ).fetchone()["n"]
    storage.quarantine_match(gs.conn, mid, "manual")
    gs.matches.pop(mid, None)

    events = decide(live.state, {"cmd": "END_TURN", "seat": 0})
    with pytest.raises(storage.MatchNotRunning):
        storage.apply_command(
            gs.conn, mid, "should-not-write", 0, {"cmd": "END_TURN"},
            events, {"accepted": True, "state_hash": "deadbeef"},
            None, 0.0)
    # 事件数不变, 状态仍是 corrupt(没有被 UPDATE deadline 复活)
    assert gs.conn.execute(
        "SELECT COUNT(*) n FROM events WHERE match_id=?", (mid,)
    ).fetchone()["n"] == n_events_before
    assert storage.match_status(gs.conn, mid) == "corrupt"
    assert gs.conn.execute(
        "SELECT deadline_at FROM matches WHERE match_id=?", (mid,)
    ).fetchone()["deadline_at"] is None
    await w0.close()
    await w1.close()


async def test_set_deadline_does_not_revive_corrupt_match(server):
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0)
    await _events(w1)
    gs, _ = server
    mid = m0["match_id"]
    storage.quarantine_match(gs.conn, mid, "manual")
    gs.matches.pop(mid, None)
    storage.set_deadline(gs.conn, mid, 9_999_999_999.0)
    row = gs.conn.execute(
        "SELECT status, deadline_at FROM matches WHERE match_id=?", (mid,)
    ).fetchone()
    assert row["status"] == "corrupt"
    assert row["deadline_at"] is None
    await w0.close()
    await w1.close()


# ------------------------------------------------ 排队操作撞上正常结束

async def test_queued_command_after_natural_end_is_rejected(server):
    """一条命令正在排队等锁, 持锁的另一条命令结束了对局(GAME_ENDED 摘除
    注册对象): 排队命令必须以 ended 拒绝, 不向 finished 对局追加事件。"""
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0)
    await _events(w1)
    gs, _ = server
    mid = m0["match_id"]
    live = gs.matches[mid]

    # 直接制造"已结束但 DB 已 finished、内存已摘除"的状态:
    # 用一条 CONCEDE 命令走真实结算
    outcome = await gs._apply_under_lock(
        live, {"cmd": "CONCEDE", "seat": 0}, "concede-1")
    mail = outcome.pop("_mail", None)
    assert outcome["status"] == "applied"
    if mail:
        await gs._deliver_mail(mail)
    assert mid not in gs.matches
    n_events = gs.conn.execute(
        "SELECT COUNT(*) n FROM events WHERE match_id=?", (mid,)
    ).fetchone()["n"]

    # 之后任何持有旧 live 引用的排队操作都必须被闸门拦截
    r = await gs._apply_locked(
        live, {"cmd": "END_TURN", "seat": 1}, "late-1")
    assert r["status"] == "ended"
    assert gs.conn.execute(
        "SELECT COUNT(*) n FROM events WHERE match_id=?", (mid,)
    ).fetchone()["n"] == n_events
    await w0.close()
    await w1.close()


# ------------------------------------------------ 超时任务撞上隔离

async def test_stale_timer_after_quarantine_does_nothing(server):
    """定时器任务越过 sleep 后发现对局已隔离: 不得产生 SYSTEM_TIMEOUT。"""
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0)
    await _events(w1)
    gs, _ = server
    mid = m0["match_id"]
    live = gs.matches[mid]
    epoch = live.timer_epoch

    storage.quarantine_match(gs.conn, mid, "manual")
    gs.matches.pop(mid, None)
    if live.timer_task:
        live.timer_task.cancel()

    # 直接调用定时器回调(它会 self.matches.get -> None 立即退出)
    await gs._fire_timer(mid, epoch, delay=0.0)
    n_timeouts = gs.conn.execute(
        "SELECT COUNT(*) n FROM commands WHERE match_id=? AND cmd='SYSTEM_TIMEOUT'",
        (mid,)).fetchone()["n"]
    assert n_timeouts == 0
    await w0.close()
    await w1.close()


async def test_websocket_command_after_quarantine_returns_not_in_match(server):
    """端到端: 隔离后通过真实 WebSocket 发命令, 收到拒绝而非继续结算。"""
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0)
    await _events(w1)
    gs, _ = server
    mid = m0["match_id"]
    storage.quarantine_match(gs.conn, mid, "manual")
    gs.matches.pop(mid, None)

    await send(w0, type="command", op_id="late-ws",
               command={"cmd": "END_TURN"})
    msg = await recv_until(w0, "error", timeout=2)
    assert msg["code"] == "NOT_IN_MATCH"
    await w0.close()
    await w1.close()


async def test_concurrent_apply_one_quarantines_others_noop(server):
    """真正并发: 多个结算协程竞争同一把锁, 持锁者隔离对局后释放,
    其余排队协程必须全部返回 corrupt 且 DB 无新增命令。"""
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0)
    await _events(w1)
    gs, _ = server
    mid = m0["match_id"]
    live = gs.matches[mid]
    uid = next(c["uid"] for c in live.state["seats"][0]["hand"]
               if c["id"] == "militia")
    await send(w0, type="command", op_id="seed-c",
               command={"cmd": "PLAY", "uid": uid, "target": 0})
    await recv_until(w0, "command_result", op_id="seed-c")

    n_before = gs.conn.execute(
        "SELECT COUNT(*) n FROM commands WHERE match_id=?", (mid,)
    ).fetchone()["n"]

    async def quarantiner():
        async with live.lock:
            await asyncio.sleep(0.05)
            _corrupt(gs.conn, mid)
            storage.quarantine_match(gs.conn, mid, "concurrent")
            # 摘除内存对象, 模拟隔离提交
            gs.matches.pop(mid, None)

    async def late_command(op, cmd):
        await asyncio.sleep(0.01)  # 确保 quarantiner 先拿锁
        return await gs._apply_under_lock(live, cmd, op)

    results = await asyncio.gather(
        quarantiner(),
        late_command("c1", {"cmd": "END_TURN", "seat": 0}),
        late_command("c2", {"cmd": "PASS", "seat": 1}),
        late_command(None, {"cmd": "SYSTEM_TIMEOUT", "seat": 0,
                            "reason": "deadline"}),
    )
    statuses = [r["status"] for r in results[1:]]
    assert statuses == ["corrupt", "corrupt", "corrupt"], statuses
    n_after = gs.conn.execute(
        "SELECT COUNT(*) n FROM commands WHERE match_id=?", (mid,)
    ).fetchone()["n"]
    assert n_after == n_before
    assert storage.match_status(gs.conn, mid) == "corrupt"
    await w0.close()
    await w1.close()
