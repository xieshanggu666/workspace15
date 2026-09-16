"""服务端集成测试: 真实 WebSocket 链路。

覆盖:
- 匹配开局、命令往返、隐藏手牌不发给对手
- 重复 op_id 不重复扣费(幂等)
- 响应窗/回合超时
- 掉线宽限与重连恢复
- 服务重启后对局从 SQLite 恢复, 状态哈希一致
- 确定性回放(断线后用 replay 补全, 哈希与 snapshot 一致)
"""

from __future__ import annotations

import asyncio

import pytest

from server.engine.state import state_hash
from server.replay_support import load_match_state
from server import storage

from .conftest import (
    login_pair,
    recv,
    recv_until,
    send,
    ws_connect,
)


def _hand_by_seat(msg: dict):
    return msg["state"]["seats"]


async def _events(ws, timeout=0.6):
    """收集 timeout 内到达的事件消息。"""
    out = []
    try:
        while True:
            out.append(await asyncio.wait_for(recv(ws), timeout=timeout))
    except asyncio.TimeoutError:
        pass
    return out


async def _snapshot(ws):
    msg = await recv_until(ws, "snapshot", timeout=2.0)
    return msg["state"]


# ------------------------------------------------------------------ 基础

async def test_login_matchmake_and_begin(server):
    (w0, m0), (w1, m1) = await login_pair(server)
    assert m0["match_id"] == m1["match_id"]
    assert m0["seat"] == 0 and m1["seat"] == 1
    # 开局事件流里各自至少收到自己的抽牌
    e0 = await _events(w0, 0.4)
    e1 = await _events(w1, 0.4)
    drawn0_self = [m for m in e0 if m["type"] == "event"
                   and m["event"]["type"] == "DRAWN" and m["event"]["seat"] == 0]
    drawn1_other = [m for m in e1 if m["type"] == "event"
                    and m["event"]["type"] == "DRAWN" and m["event"]["seat"] == 0]
    # 自己的抽牌含 cards, 对手视角只有 count
    assert all("cards" in m["event"] for m in drawn0_self)
    assert all(set(m["event"].keys()) == {"type", "seat", "count"}
               for m in drawn1_other)


async def test_opponent_hand_never_leaves_server(server):
    """对手任何时候都只知道手牌数量, 拿不到牌面。"""
    (w0, _), (w1, _) = await login_pair(server)
    await _events(w0, 0.4)
    await _events(w1, 0.4)
    # 主动 sync 触发快照(重连路径外也可校验 snapshot)
    # 通过 replay 视角(非参与者)也拿不到任何手牌
    await send(w0, type="sync", last_seq=0)
    msgs = await _events(w0, 0.5)
    for m in msgs:
        if m.get("type") == "event" and m["event"]["type"] == "MATCH_STARTED":
            # 公开的 MATCH_STARTED 不含 decks
            assert "decks" not in m["event"]
        if m.get("type") == "event" and m["event"]["type"] == "DECK_SHUFFLED":
            pytest.fail("洗牌事件泄露给客户端")


async def test_play_roundtrip_and_idempotent_no_double_charge(server):
    (w0, _), _ = await login_pair(server)
    await _events(w0, 0.4)
    # 需要知道 alice 的手牌 uid: 从她收到的 DRAWN 事件重建
    # 重新连接一次拿 snapshot 更直接
    # (login_pair 已消费掉事件, 改为从 DB 找)
    gs, port = server
    # 从内存对局拿手牌(固定种子下 alice 首回合有民兵)
    live = next(iter(gs.matches.values()))
    uid = next(c["uid"] for c in live.state["seats"][0]["hand"]
               if c["id"] == "militia")
    target = 0

    await send(w0, type="command", op_id="op-1",
               command={"cmd": "PLAY", "uid": uid, "target": target})
    res = await recv_until(w0, "command_result", op_id="op-1")
    assert res["accepted"] is True
    energy_after_play = live.state["seats"][0]["energy"]
    assert energy_after_play == 0

    # 完全相同的 op_id 重发: 必须返回重复标记, 能量不变, 堆叠不增长
    await send(w0, type="command", op_id="op-1",
               command={"cmd": "PLAY", "uid": uid, "target": target})
    res2 = await recv_until(w0, "command_result", op_id="op-1")
    assert res2.get("duplicate") is True
    assert live.state["seats"][0]["energy"] == 0
    assert len(live.state["stack"]) == 1


async def test_invalid_command_charges_nothing(server):
    (w0, _), _ = await login_pair(server)
    await _events(w0, 0.4)
    gs, _ = server
    live = next(iter(gs.matches.values()))
    # 打一张不存在的 uid
    await send(w0, type="command", op_id="bad-1",
               command={"cmd": "PLAY", "uid": "s0-999", "target": 0})
    res = await recv_until(w0, "command_result", op_id="bad-1", timeout=2.0)
    assert res["accepted"] is False and res["code"] == "NO_CARD"
    assert live.state["seats"][0]["energy"] == 1
    assert len(live.state["stack"]) == 0


# ------------------------------------------------------------------ 超时

async def test_response_window_timeout_auto_passes(server):
    """出牌后对手不响应, 响应窗超时应自动通过并最终结算部署。"""
    (w0, _), (w1, _) = await login_pair(server)
    await _events(w0, 0.4)
    await _events(w1, 0.4)
    gs, _ = server
    live = next(iter(gs.matches.values()))
    uid = next(c["uid"] for c in live.state["seats"][0]["hand"]
               if c["id"] == "militia")
    await send(w0, type="command", op_id="p1",
               command={"cmd": "PLAY", "uid": uid, "target": 0})
    await recv_until(w0, "command_result", op_id="p1")
    assert live.state["phase"] == "response"
    # response_seconds=0.3, 两次自动通过各需一次窗口超时(共约 0.6s+)
    await asyncio.sleep(1.2)
    assert live.state["phase"] == "main"
    assert len(live.state["seats"][0]["units"]) == 1
    assert live.state["turn"] == 1  # 还没轮到回合超时


async def test_turn_timeout_advances_turn(server):
    """整个小回合不操作, 超时应自动结束并进入对手回合。"""
    (w0, _), (w1, _) = await login_pair(server)
    await _events(w0, 0.3)
    await _events(w1, 0.3)
    gs, _ = server
    live = next(iter(gs.matches.values()))
    # turn_seconds=0.6, 不做任何操作; 超时自动 END_TURN
    await asyncio.sleep(1.0)
    assert live.state["turn"] == 2 and live.state["active"] == 1


# ------------------------------------------------------------------ 重连

async def test_reconnect_resumes_hidden_state(server):
    """玩家断线后用同一令牌重新登录, 应恢复对局并补全私有事件。"""
    (w0, m0), (w1, _) = await login_pair(server)
    token0 = None
    await _events(w0, 0.3)
    await _events(w1, 0.3)
    gs, port = server
    live = gs.matches[m0["match_id"]]

    # alice 断线
    await w0.close()
    await asyncio.sleep(0.2)
    # 重连(先取她的 token: 直接从 DB 查)
    row = storage.player_by_token  # 名称->token 需要反查
    conn = gs.conn
    token0 = conn.execute("SELECT token FROM players WHERE name='alice'").fetchone()[0]

    w0b = await ws_connect(port)
    await send(w0b, type="login", name="alice", token=token0)
    await recv_until(w0b, "login_ok")
    resume = await recv_until(w0b, "match_resume")
    assert resume["match_id"] == m0["match_id"]
    snap = await recv_until(w0b, "snapshot")
    # 自己手牌必须完整恢复
    hand0 = snap["state"]["seats"][0]["hand"]
    assert isinstance(hand0, list) and len(hand0) >= 4
    # 对手手牌只给数量
    assert snap["state"]["seats"][1]["hand"] == {"count": 4}
    await w0b.close()
    await w1.close()


async def test_disconnect_grace_extends_deadline(server):
    """断线给一次性宽限: 在普通超时长度内不应被超时。"""
    (w0, _), (w1, _) = await login_pair(server)
    await _events(w0, 0.3)
    await _events(w1, 0.3)
    gs, _ = server
    live = next(iter(gs.matches.values()))
    normal_deadline = live.deadline_at
    await w0.close()
    await asyncio.sleep(0.15)
    # 宽限延长 0.8s
    assert live.deadline_at >= normal_deadline + 0.7
    # turn 0.6s 已过但宽限内不切换回合
    await asyncio.sleep(0.65)
    assert live.state["turn"] == 1
    await w1.close()


# ------------------------------------------------------------------ 重启

async def test_server_restart_restores_match_and_hash(server, db_path,
                                                      restart_server):
    """第一服务进程打若干命令 -> 关闭 -> 新进程从 SQLite 恢复, 哈希一致。"""
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0, 0.3)
    await _events(w1, 0.3)
    gs, port = server
    mid = m0["match_id"]
    live = gs.matches[mid]
    uid = next(c["uid"] for c in live.state["seats"][0]["hand"]
               if c["id"] == "militia")
    await send(w0, type="command", op_id="restart-1",
               command={"cmd": "PLAY", "uid": uid, "target": 0})
    await recv_until(w0, "command_result", op_id="restart-1")
    # 让对手通过一次(窗口转到 alice)
    await send(w1, type="command", op_id="restart-2",
               command={"cmd": "PASS"})
    await recv_until(w1, "command_result", op_id="restart-2")
    hash_before = state_hash(live.state)
    deadline_before = live.deadline_at

    # 关闭整个服务端
    await gs.shutdown()
    await w0.close()
    await w1.close()

    # 新进程: 同一 DB 文件
    gs2, ws_server2, port2 = await restart_server()
    try:
        assert mid in gs2.matches
        live2 = gs2.matches[mid]
        assert state_hash(live2.state) == hash_before
        # 截止时间从库里恢复(时钟延续, 不重置全额时间)
        assert abs((live2.deadline_at or 0) - (deadline_before or 0)) < 0.01

        # 用同名重连后继续对局, 引擎状态可继续结算
        conn = gs2.conn
        t0 = conn.execute("SELECT token FROM players WHERE name='alice'").fetchone()[0]
        t1 = conn.execute("SELECT token FROM players WHERE name='bob'").fetchone()[0]
        c0 = await ws_connect(port2)
        c1 = await ws_connect(port2)
        await send(c0, type="login", name="alice", token=t0)
        await send(c1, type="login", name="bob", token=t1)
        await recv_until(c0, "match_resume")
        await recv_until(c1, "match_resume")
        await recv_until(c0, "snapshot")
        await recv_until(c1, "snapshot")

        # alice 也通过 -> 单位部署
        await send(c0, type="command", op_id="restart-3",
                   command={"cmd": "PASS"})
        await recv_until(c0, "command_result", op_id="restart-3")
        assert len(live2.state["seats"][0]["units"]) == 1
    finally:
        ws_server2.close()
        await ws_server2.wait_closed()
        await gs2.shutdown()


# ------------------------------------------------------------------ 回放

async def test_replay_rebuilds_same_hash_and_hides_info(server):
    """replay 消息: 参与者拿到含自己手牌的完整流; 旁观者拿到公开流。"""
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0, 0.3)
    await _events(w1, 0.3)
    gs, port = server
    mid = m0["match_id"]
    live = gs.matches[mid]
    uid = next(c["uid"] for c in live.state["seats"][0]["hand"]
               if c["id"] == "militia")
    await send(w0, type="command", op_id="rep-1",
               command={"cmd": "PLAY", "uid": uid, "target": 0})
    await recv_until(w0, "command_result", op_id="rep-1")

    # 服务端内部: 从 DB 全量重建的权威状态哈希必须等于内存状态
    loaded = load_match_state(gs.conn, mid)
    assert loaded["hash"] == state_hash(live.state)

    # 参与者回放: 能看到自己 DRAWN 的 cards
    await send(w0, type="replay", match_id=mid)
    rep0 = await recv_until(w0, "replay")
    assert rep0["full"] is True
    own_draw = [e for e in rep0["events"]
                if e["event"]["type"] == "DRAWN" and e["event"]["seat"] == 0]
    assert all("cards" in e["event"] for e in own_draw)

    # 第三方登录取旁观回放: 不能含任何牌库/手牌内容
    w3 = await ws_connect(port)
    await send(w3, type="login", name="carol")
    await recv_until(w3, "login_ok")
    await send(w3, type="replay", match_id=mid)
    rep3 = await recv_until(w3, "replay")
    assert rep3["full"] is False
    for e in rep3["events"]:
        assert e["event"]["type"] != "DECK_SHUFFLED"
        if e["event"]["type"] == "DRAWN":
            assert "cards" not in e["event"]
    await w3.close()


async def test_fast_reconnect_old_socket_cannot_evict_new(server):
    """旧连接在新连接登入后才真正关闭, 不得把新连接从座位上踢掉。"""
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0, 0.3)
    await _events(w1, 0.3)
    gs, port = server
    mid = m0["match_id"]
    live = gs.matches[mid]
    token = gs.conn.execute(
        "SELECT token FROM players WHERE name='alice'").fetchone()[0]

    # 新连接先登入并恢复, 然后旧连接才关闭
    c0 = await ws_connect(port)
    await send(c0, type="login", name="alice", token=token)
    await recv_until(c0, "login_ok")
    await recv_until(c0, "match_resume")
    await recv_until(c0, "snapshot")
    await w0.close()
    await asyncio.sleep(0.3)

    # 座位 0 上仍然是新连接对象
    assert live.clients.get(0) is not None
    # 新连接还能继续收到服务端推送(这里直接发命令验证链路活着)
    uid = next(c["uid"] for c in live.state["seats"][0]["hand"]
               if c["id"] == "militia")
    await send(c0, type="command", op_id="race-1",
               command={"cmd": "PLAY", "uid": uid, "target": 0})
    res = await recv_until(c0, "command_result", op_id="race-1")
    assert res["accepted"] is True
    await c0.close()
    await w1.close()


async def test_idempotent_after_reconnect_same_op(server):
    (w0, m0), (w1, _) = await login_pair(server)
    await _events(w0, 0.3)
    await _events(w1, 0.3)
    gs, port = server
    mid = m0["match_id"]
    live = gs.matches[mid]
    uid = next(c["uid"] for c in live.state["seats"][0]["hand"]
               if c["id"] == "militia")
    await send(w0, type="command", op_id="idem-9",
               command={"cmd": "PLAY", "uid": uid, "target": 0})
    await recv_until(w0, "command_result", op_id="idem-9")
    stack_before = len(live.state["stack"])

    await w0.close()
    await asyncio.sleep(0.2)
    token = gs.conn.execute(
        "SELECT token FROM players WHERE name='alice'").fetchone()[0]
    c0 = await ws_connect(port)
    await send(c0, type="login", name="alice", token=token)
    await recv_until(c0, "login_ok")
    await recv_until(c0, "match_resume")
    await recv_until(c0, "snapshot")

    await send(c0, type="command", op_id="idem-9",
               command={"cmd": "PLAY", "uid": uid, "target": 0})
    res = await recv_until(c0, "command_result", op_id="idem-9")
    assert res.get("duplicate") is True
    assert len(live.state["stack"]) == stack_before
    await c0.close()
    await w1.close()
