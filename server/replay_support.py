"""从 SQLite 重建内存对局状态(重启恢复与对外回放共用), 并校验哈希检查点。"""

from __future__ import annotations

import sqlite3

from . import storage
from .engine.fold import fold
from .engine.state import fresh_state, state_hash


class ReplayMismatch(RuntimeError):
    """SQLite 事件流重建出的状态与落库检查点不一致。"""


def load_match_state(conn: sqlite3.Connection, match_id: str,
                     verify: bool = False) -> dict:
    """从事件流重建内存状态。

    verify=True 时, 每条玩家命令对应的事件落库后, 用 checkpoints 表中
    同事务记录的权威哈希逐条比对; 任一不一致即 ReplayMismatch——这只会在
    事件流被重复写入/丢失/乱序时发生, 用来兜底并发缺陷。
    """
    m = storage.get_match(conn, match_id)
    if m is None:
        raise KeyError(match_id)
    state = fresh_state(match_id, m["names"])
    checkpoints = storage.checkpoints(conn, match_id) if verify else {}

    # 一次性取出 (事件 seq, 所属命令 seq), 避免逐事件查询
    event_cmds = storage.event_command_map(conn, match_id)
    cmd_last_event: dict[int, int] = {}
    for event_seq, cmd_seq in event_cmds.items():
        cmd_last_event[cmd_seq] = max(cmd_last_event.get(cmd_seq, 0), event_seq)

    # 单次按序 fold; 在每条命令的最后一条事件处记录重建哈希
    seen_cmd_at: dict[int, str] = {}
    for row in storage.all_events(conn, match_id):
        fold(state, row["event"])
        if verify:
            cmd_seq = event_cmds.get(row["seq"])
            if cmd_seq is not None and cmd_last_event.get(cmd_seq) == row["seq"]:
                seen_cmd_at[cmd_seq] = state_hash(state)

    if verify:
        mismatches = [
            (cmd_seq, expected, seen_cmd_at.get(cmd_seq, "<missing>"))
            for cmd_seq, expected in checkpoints.items()
            if seen_cmd_at.get(cmd_seq) != expected
        ]
        if mismatches:
            raise ReplayMismatch(
                f"对局 {match_id} 事件流与检查点不一致: {mismatches[:5]}")

    return {
        "state": state,
        "names": m["names"],
        "decks": m["decks"],
        "deadline_at": m["deadline_at"],
        "status": m["status"],
        "hash": state_hash(state),
        "checkpoints_verified": bool(verify),
    }


def verify_match(conn: sqlite3.Connection, match_id: str) -> dict:
    """供测试/运维调用: 严格校验一局的事件流与全部检查点。"""
    return load_match_state(conn, match_id, verify=True)
