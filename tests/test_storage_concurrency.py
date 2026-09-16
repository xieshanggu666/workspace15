"""存储层幂等与并发一致性测试。

覆盖:
- 重复 op_id 只首次落库, 命令/事件/检查点均不重复
- 两个连接在"SELECT 预检未命中后同时提交"竞争下, 只有一条胜出,
  败者整事务回滚(事件流不重复)
- apply_command 自身在提交撞键时回滚并返回 duplicate(不向调用方抛错)
- 检查点能抓出被人为重复、会改变状态的事件(模拟重复扣费/重复结算)
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from server import storage
from server.engine.engine import decide, start_match
from server.engine.fold import fold
from server.engine.state import state_hash
from server.replay_support import ReplayMismatch, verify_match

DECK = ["militia"] * 3 + ["infantry"] * 3 + ["vanguard"] * 3 + [
    "guardian", "fireball", "reinforce", "repair", "counter", "banner"]


@pytest.fixture
def seeded(tmp_path):
    path = str(tmp_path / "c.db")
    conn = storage.connect(path)
    storage.init_db(conn)
    storage.create_match(conn, "m", ["a", "b"], [DECK, DECK], 42, 0.0)
    state, events = start_match("m", [DECK, DECK], ["a", "b"], 42)
    storage.save_start_events(conn, "m", events)
    for e in events:
        fold(state, e)
    return path, conn, state


def _apply(conn, state, cmd: dict, op_id: str):
    events = decide(state, cmd)
    for e in events:
        fold(state, e)
    return storage.apply_command(
        conn, "m", op_id, cmd.get("seat"), cmd, events,
        {"accepted": True, "state_hash": state_hash(state)}, None, 0.0)


def _event_count(conn) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS n FROM events WHERE match_id='m'").fetchone()["n"]


def test_duplicate_op_id_no_extra_rows(seeded):
    _, conn, state = seeded
    uid = next(c["uid"] for c in state["seats"][0]["hand"]
               if c["id"] == "militia")
    cmd = {"cmd": "PLAY", "seat": 0, "uid": uid, "target": 0}

    first = _apply(conn, state, cmd, "op-1")
    assert first["duplicate"] is False
    n_after = _event_count(conn)

    # 重复 op_id: 走 cached_result 快路径(空事件列表), 不追加任何行
    dup = storage.apply_command(
        conn, "m", "op-1", 0, cmd, [],
        {"accepted": True}, None, 0.0)
    assert dup["duplicate"] is True
    assert _event_count(conn) == n_after
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM commands WHERE match_id='m' AND op_id='op-1'"
    ).fetchone()["n"] == 1
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM checkpoints WHERE match_id='m'"
    ).fetchone()["n"] == 1
    assert verify_match(conn, "m")["hash"] == state_hash(state)


def test_racing_connections_only_one_commits(seeded):
    path, conn, state = seeded
    uid = next(c["uid"] for c in state["seats"][0]["hand"]
               if c["id"] == "militia")

    # A 正常提交一条 PLAY
    events_a = decide(state, {"cmd": "PLAY", "seat": 0, "uid": uid,
                              "target": 0})
    for e in events_a:
        fold(state, e)
    res_a = storage.apply_command(
        conn, "m", "race", 0, {"cmd": "PLAY"}, events_a,
        {"accepted": True, "state_hash": state_hash(state)}, None, 0.0)
    assert res_a["duplicate"] is False
    n_after = _event_count(conn)
    conn.close()

    # B: 独立连接, 假装预检未命中, 尝试插入同 op_id 的命令+事件。
    # commands 表 (match_id,op_id) 或 idem_results 主键至少有一处撞键,
    # 整个事务必须回滚 —— B 的幽灵事件绝不能进事件流。
    other = sqlite3.connect(path)
    other.execute("PRAGMA journal_mode=WAL")
    other.execute("BEGIN IMMEDIATE")
    with pytest.raises(sqlite3.IntegrityError):
        other.execute(
            "INSERT INTO commands(match_id,op_id,seat,cmd,payload,at)"
            " VALUES('m','race',0,'PLAY','{}',0)")
        other.execute(
            "INSERT INTO events(match_id,after_cmd,evt) VALUES('m',99999,'{}')")
        other.execute(
            "INSERT INTO idem_results(match_id,op_id,result)"
            " VALUES('m','race','{}')")
    other.rollback()
    other.close()

    check = storage.connect(path)
    assert _event_count(check) == n_after
    assert check.execute(
        "SELECT COUNT(*) AS n FROM events WHERE after_cmd=99999"
    ).fetchone()["n"] == 0
    assert verify_match(check, "m")["hash"] == state_hash(state)


def test_apply_command_self_heals_when_commit_races(seeded, monkeypatch):
    """预检被竞争窗口绕过(强行返回未命中)时, apply_command 必须靠数据库
    约束捕获撞键、回滚并返回 duplicate, 而不是抛 IntegrityError。"""
    _, conn, state = seeded
    uid = next(c["uid"] for c in state["seats"][0]["hand"]
               if c["id"] == "militia")
    events = decide(state, {"cmd": "PLAY", "seat": 0, "uid": uid, "target": 0})
    for e in events:
        fold(state, e)
    storage.apply_command(
        conn, "m", "z", 0, {"cmd": "PLAY"}, events,
        {"accepted": True, "state_hash": state_hash(state)}, None, 0.0)

    monkeypatch.setattr(storage, "cached_result", lambda *a, **k: None)
    again = storage.apply_command(
        conn, "m", "z", 0, {"cmd": "PLAY"}, events,
        {"accepted": True, "state_hash": state_hash(state)}, None, 0.0)
    assert again["duplicate"] is True
    assert verify_match(conn, "m")["hash"] == state_hash(state)


def test_checkpoints_catch_duplicated_state_changing_event(seeded):
    """复制一条会改变状态的事件(CARD_PLAYED 重复扣费/重复进堆叠),
    重建哈希必然偏离检查点。"""
    _, conn, state = seeded
    uid = next(c["uid"] for c in state["seats"][0]["hand"]
               if c["id"] == "militia")
    _apply(conn, state, {"cmd": "PLAY", "seat": 0, "uid": uid, "target": 0},
           "x")
    # 找到本命令的 CARD_PLAYED 事件并原样复制一条
    rows = conn.execute(
        "SELECT seq, after_cmd, evt FROM events WHERE match_id='m'"
        " ORDER BY seq").fetchall()
    played = next(r for r in rows
                  if json.loads(r["evt"])["type"] == "CARD_PLAYED")
    conn.execute(
        "INSERT INTO events(match_id,after_cmd,evt) VALUES(?,?,?)",
        ("m", played["after_cmd"], played["evt"]))
    conn.commit()
    with pytest.raises(ReplayMismatch):
        verify_match(conn, "m")
