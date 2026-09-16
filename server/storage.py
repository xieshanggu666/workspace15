"""SQLite 持久化: 玩家、对局、命令、事件、幂等结果。

一致性要点:
- 一条玩家命令与其产生的所有事件在同一个事务里提交(apply_command),
  因此"出牌成功"与"扣费/落库"原子, 崩溃不会出现扣了费没记录的状态。
- (match_id, op_id) 唯一约束: 重复操作直接返回首次结果, 不重复扣费、
  不重复追加事件。
- events 表保存事件完整 JSON(含隐藏信息), 回放时按座位过滤;
  广播版本不保存隐藏字段到任何对外结构。
"""

from __future__ import annotations

import json
import sqlite3
import secrets
import time
from typing import Any

class MatchNotRunning(Exception):
    """命令事务内发现对局已不是 running(已结束/已隔离), 必须拒绝写入。"""


SCHEMA = """
CREATE TABLE IF NOT EXISTS players (
    name   TEXT PRIMARY KEY,
    token  TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS matches (
    match_id    TEXT PRIMARY KEY,
    created_at  REAL NOT NULL,
    status      TEXT NOT NULL,             -- running | finished | corrupt
    winner      INTEGER,
    end_reason  TEXT,
    seed        INTEGER NOT NULL,
    names       TEXT NOT NULL,             -- JSON [name0, name1]
    decks       TEXT NOT NULL,             -- JSON [[ids], [ids]]
    deadline_at REAL                       -- 当前操作截止时间(unix秒)
);
CREATE TABLE IF NOT EXISTS commands (
    seq       INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id  TEXT NOT NULL,
    op_id     TEXT,                        -- 系统命令为 NULL
    seat      INTEGER,
    cmd       TEXT NOT NULL,
    payload   TEXT NOT NULL,               -- JSON
    at        REAL NOT NULL,
    UNIQUE(match_id, op_id)
);
CREATE TABLE IF NOT EXISTS events (
    seq       INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id  TEXT NOT NULL,
    after_cmd INTEGER,                     -- 该事件所属命令 seq
    evt       TEXT NOT NULL                -- JSON, 含隐藏信息(仅服务端/本人)
);
CREATE INDEX IF NOT EXISTS idx_events_match ON events(match_id, seq);
CREATE TABLE IF NOT EXISTS idem_results (
    match_id TEXT NOT NULL,
    op_id    TEXT NOT NULL,
    result   TEXT NOT NULL,               -- JSON: 首次命令的响应
    PRIMARY KEY (match_id, op_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS checkpoints (
    -- 每条玩家命令落库后的权威状态哈希; 回放/重启时逐条比对,
    -- 证明"SQLite 事件流 fold 出的状态 == 现场内存状态"。
    match_id    TEXT NOT NULL,
    cmd_seq     INTEGER NOT NULL,
    state_hash  TEXT NOT NULL,
    PRIMARY KEY (match_id, cmd_seq)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS corruption (
    -- 校验失败审计: 哪些对局在何时因什么原因被隔离
    match_id  TEXT PRIMARY KEY,
    at        REAL NOT NULL,
    detail    TEXT NOT NULL
);
"""


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


# ------------------------------------------------------------------ 玩家

def login(conn: sqlite3.Connection, name: str) -> str:
    """极简登录: 名字即账号, 自动注册并返回令牌。"""
    row = conn.execute("SELECT token FROM players WHERE name=?", (name,)).fetchone()
    if row:
        return row["token"]
    token = secrets.token_hex(16)
    conn.execute("INSERT INTO players(name, token) VALUES(?,?)", (name, token))
    conn.commit()
    return token


def player_by_token(conn: sqlite3.Connection, token: str) -> str | None:
    row = conn.execute("SELECT name FROM players WHERE token=?", (token,)).fetchone()
    return row["name"] if row else None


# ------------------------------------------------------------------ 对局

def create_match(conn: sqlite3.Connection, match_id: str,
                 names: list[str], decks: list[list[str]], seed: int,
                 deadline_at: float) -> None:
    conn.execute(
        "INSERT INTO matches(match_id, created_at, status, seed, names, decks, deadline_at)"
        " VALUES(?,?,'running',?,?,?,?)",
        (match_id, time.time(), seed, json.dumps(names), json.dumps(decks),
         deadline_at),
    )
    conn.commit()


def get_match(conn: sqlite3.Connection, match_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM matches WHERE match_id=?", (match_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    d["names"] = json.loads(d["names"])
    d["decks"] = json.loads(d["decks"])
    return d


def active_match_for(conn: sqlite3.Connection, name: str) -> str | None:
    """重连用: 找到玩家仍在进行的对局。"""
    row = conn.execute(
        "SELECT match_id FROM matches WHERE status='running' AND "
        "(json_extract(names,'$[0]')=? OR json_extract(names,'$[1]')=?) "
        "ORDER BY created_at DESC LIMIT 1", (name, name),
    ).fetchone()
    return row["match_id"] if row else None


def latest_match_for(conn: sqlite3.Connection, name: str) -> tuple[str, str] | None:
    """玩家最近一局及其状态(含 finished/corrupt), 用于告知重连者不能恢复。"""
    row = conn.execute(
        "SELECT match_id, status FROM matches WHERE "
        "(json_extract(names,'$[0]')=? OR json_extract(names,'$[1]')=?) "
        "ORDER BY created_at DESC LIMIT 1", (name, name),
    ).fetchone()
    return (row["match_id"], row["status"]) if row else None


def set_deadline(conn: sqlite3.Connection, match_id: str,
                 deadline_at: float | None) -> None:
    """只刷新仍在运行的对局截止时间; 已隔离/结束的对局不得被复活。"""
    conn.execute(
        "UPDATE matches SET deadline_at=? WHERE match_id=?"
        " AND status='running'",
        (deadline_at, match_id),
    )
    conn.commit()


def finish_match(conn: sqlite3.Connection, match_id: str,
                 winner: int | None, reason: str) -> None:
    conn.execute(
        "UPDATE matches SET status='finished', winner=?, end_reason=?, deadline_at=NULL "
        "WHERE match_id=?", (winner, reason, match_id),
    )
    conn.commit()


def quarantine_match(conn: sqlite3.Connection, match_id: str,
                     detail: str) -> None:
    """把校验失败的对局标记为 corrupt 并停掉定时器。

    corrupt 与 finished 一样不会被匹配/重连/恢复路径重新装回内存;
    但保留全部事件以便事后用 replay/verify 排查。原因记入 corruption 审计表。
    """
    with conn:
        conn.execute(
            "UPDATE matches SET status='corrupt', winner=NULL, deadline_at=NULL"
            " WHERE match_id=?", (match_id,))
        conn.execute(
            "INSERT OR REPLACE INTO corruption(match_id, at, detail)"
            " VALUES(?,?,?)", (match_id, time.time(), detail))


def match_status(conn: sqlite3.Connection, match_id: str) -> str | None:
    row = conn.execute(
        "SELECT status FROM matches WHERE match_id=?", (match_id,)
    ).fetchone()
    return row["status"] if row else None


# ----------------------------------------------------------- 事件/命令/幂等

def save_start_events(conn: sqlite3.Connection, match_id: str,
                      events: list[dict]) -> list[int]:
    cur = conn.execute("SELECT COALESCE(MAX(seq),0) FROM events WHERE match_id=?",
                       (match_id,))
    seqs = []
    for e in events:
        cur = conn.execute(
            "INSERT INTO events(match_id, after_cmd, evt) VALUES(?,?,?)",
            (match_id, None, json.dumps(e, ensure_ascii=False)),
        )
        seqs.append(cur.lastrowid)
    conn.commit()
    return seqs


def start_events(conn: sqlite3.Connection, match_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT evt FROM events WHERE match_id=? AND after_cmd IS NULL ORDER BY seq",
        (match_id,),
    ).fetchall()
    return [json.loads(r["evt"]) for r in rows]


def all_events(conn: sqlite3.Connection, match_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT seq, evt FROM events WHERE match_id=? ORDER BY seq", (match_id,),
    ).fetchall()
    return [{"seq": r["seq"], "event": json.loads(r["evt"])} for r in rows]


def events_after(conn: sqlite3.Connection, match_id: str,
                 last_seq: int) -> list[dict]:
    rows = conn.execute(
        "SELECT seq, evt FROM events WHERE match_id=? AND seq>? ORDER BY seq",
        (match_id, last_seq),
    ).fetchall()
    return [{"seq": r["seq"], "event": json.loads(r["evt"])} for r in rows]


def last_event_seq(conn: sqlite3.Connection, match_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(seq),0) AS s FROM events WHERE match_id=?",
        (match_id,),
    ).fetchone()
    return row["s"]


def cached_result(conn: sqlite3.Connection, match_id: str,
                  op_id: str) -> dict | None:
    row = conn.execute(
        "SELECT result FROM idem_results WHERE match_id=? AND op_id=?",
        (match_id, op_id),
    ).fetchone()
    if row:
        return {"duplicate": True, **json.loads(row["result"])}
    return None


def apply_command(
    conn: sqlite3.Connection,
    match_id: str,
    op_id: str | None,
    seat: int | None,
    command: dict,
    new_events: list[dict],
    result: dict,
    finished: tuple[int | None, str] | None,
    deadline_at: float | None,
) -> dict[str, Any]:
    """事务化写入一条命令及其事件与状态哈希检查点。

    并发安全: 即便调用方在 SELECT 与本函数之间被另一进程/连接抢先写入同一
    op_id(SELECT 未命中), idem_results 的 PRIMARY KEY 也会让 INSERT 失败,
    整个事务回滚(命令、事件、检查点一个都不会留下), 然后返回首次结果。
    返回 {"duplicate": True, ...} 表示本次未发生任何写入/扣费。
    """
    with conn:  # 自动提交/回滚
        if op_id is not None:
            existing = conn.execute(
                "SELECT result FROM idem_results WHERE match_id=? AND op_id=?",
                (match_id, op_id),
            ).fetchone()
            if existing:
                return {"duplicate": True, **json.loads(existing["result"])}

        # 事务内状态闸门: 排队等锁期间对局可能已被隔离/结束。
        # 任何命令(含系统超时)都只能写 running 的对局; 否则整体回滚,
        # 绝不向 corrupt/finished 对局追加命令与事件。
        row = conn.execute(
            "SELECT status FROM matches WHERE match_id=?", (match_id,)
        ).fetchone()
        if row is None:
            raise MatchNotRunning(match_id)
        if row["status"] != "running":
            raise MatchNotRunning(f"{match_id}: {row['status']}")

        try:
            cur = conn.execute(
                "INSERT INTO commands(match_id, op_id, seat, cmd, payload, at)"
                " VALUES(?,?,?,?,?,?)",
                (match_id, op_id, seat, command.get("cmd"),
                 json.dumps(command, ensure_ascii=False), time.time()),
            )
            cmd_seq = cur.lastrowid
            first = last = None
            for e in new_events:
                cur = conn.execute(
                    "INSERT INTO events(match_id, after_cmd, evt) VALUES(?,?,?)",
                    (match_id, cmd_seq, json.dumps(e, ensure_ascii=False)),
                )
                first = first if first is not None else cur.lastrowid
                last = cur.lastrowid

            # 权威状态哈希与命令同事务落库; 回放时用它证明事件流无重复/无丢失
            conn.execute(
                "INSERT INTO checkpoints(match_id, cmd_seq, state_hash)"
                " VALUES(?,?,?)",
                (match_id, cmd_seq, result["state_hash"]),
            )

            if op_id is not None:
                conn.execute(
                    "INSERT INTO idem_results(match_id, op_id, result)"
                    " VALUES(?,?,?)",
                    (match_id, op_id,
                     json.dumps(dict(result), ensure_ascii=False)),
                )
            if finished is not None:
                winner, reason = finished
                conn.execute(
                    "UPDATE matches SET status='finished', winner=?,"
                    " end_reason=?, deadline_at=NULL WHERE match_id=?",
                    (winner, reason, match_id),
                )
            else:
                # 仅 running 对局刷新截止时间; 条件不满足(已隔离/结束)不动状态
                conn.execute(
                    "UPDATE matches SET deadline_at=? WHERE match_id=?"
                    " AND status='running'",
                    (deadline_at, match_id),
                )
        except MatchNotRunning:
            raise  # 交由 with conn 回滚后向上抛出
        except sqlite3.IntegrityError:
            # 唯一约束(op_id 重复)冲突: with conn 会回滚全部写入
            row = conn.execute(
                "SELECT result FROM idem_results WHERE match_id=? AND op_id=?",
                (match_id, op_id),
            ).fetchone()
            if row is not None:
                return {"duplicate": True, **json.loads(row["result"])}
            raise

    out = dict(result)
    out["duplicate"] = False
    out["cmd_seq"] = cmd_seq
    out["event_from"] = first
    out["event_to"] = last
    return out


# ------------------------------------------------------------- 哈希检查点

def checkpoints(conn: sqlite3.Connection, match_id: str) -> dict[int, str]:
    """{cmd_seq: 落库时的权威状态哈希}。"""
    rows = conn.execute(
        "SELECT cmd_seq, state_hash FROM checkpoints WHERE match_id=?"
        " ORDER BY cmd_seq", (match_id,),
    ).fetchall()
    return {r["cmd_seq"]: r["state_hash"] for r in rows}


def event_command_map(conn: sqlite3.Connection, match_id: str
                      ) -> dict[int, int]:
    """{事件全局 seq: 所属命令 seq}(仅 after_cmd 非空的事件)。"""
    rows = conn.execute(
        "SELECT seq, after_cmd FROM events WHERE match_id=?"
        " AND after_cmd IS NOT NULL ORDER BY seq", (match_id,),
    ).fetchall()
    return {r["seq"]: r["after_cmd"] for r in rows}
