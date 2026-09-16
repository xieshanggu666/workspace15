"""从 SQLite 重建内存对局状态(重启恢复与对外回放共用)。"""

from __future__ import annotations

import sqlite3

from .engine.fold import fold
from .engine.state import fresh_state, state_hash
from . import storage


def load_match_state(conn: sqlite3.Connection, match_id: str) -> dict:
    m = storage.get_match(conn, match_id)
    if m is None:
        raise KeyError(match_id)
    state = fresh_state(match_id, m["names"])
    for row in storage.all_events(conn, match_id):
        fold(state, row["event"])
    return {
        "state": state,
        "names": m["names"],
        "decks": m["decks"],
        "deadline_at": m["deadline_at"],
        "status": m["status"],
        "hash": state_hash(state),
    }
