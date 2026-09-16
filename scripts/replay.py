"""命令行确定性回放工具:

    python -m scripts.replay --db arena.db --match <match_id> [--viewer alice]

从 SQLite 读取事件流重建整局, 打印每个关键节点的状态哈希。
--viewer 省略时为旁观视角(隐藏一切手牌/牌库); 指定参与者名时其本人手牌可见。
可用 --verify 校验事件落库后记录的权威哈希(若库中存在)。
"""

from __future__ import annotations

import argparse
import json

from server import storage
from server.engine.fold import fold
from server.engine.state import fresh_state, public_event, state_hash
from server.replay_support import ReplayMismatch, verify_match


def replay(db_path: str, match_id: str, viewer: str | None,
           verify: bool = False):
    conn = storage.connect(db_path)
    m = storage.get_match(conn, match_id)
    if m is None:
        raise SystemExit(f"找不到对局: {match_id}")
    viewer_seat = m["names"].index(viewer) if viewer in m["names"] else None
    state = fresh_state(match_id, m["names"])
    print(f"对局 {match_id}  {m['names'][0]} vs {m['names'][1]}  "
          f"seed={m['seed']} status={m['status']}")
    rows = storage.all_events(conn, match_id)
    for row in rows:
        fold(state, row["event"])
        visible = public_event(row["event"], viewer_seat)
        tag = " " if visible is not None else "H"
        print(f"  [{row['seq']:>3}]{tag} {row['event']['type']:<18} "
              f"hash={state_hash(state)}")
    print("最终状态:", json.dumps({
        "winner": state["winner"],
        "reason": state["end_reason"],
        "turn": state["turn"],
        "sites": state["sites"],
        "hash": state_hash(state),
    }, ensure_ascii=False))
    if verify:
        try:
            result = verify_match(conn, match_id)
        except ReplayMismatch as exc:
            raise SystemExit(f"检查点校验失败: {exc}")
        print(f"检查点校验通过: 全部命令边界哈希一致 (最终 {result['hash']})")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="arena.db")
    p.add_argument("--match", required=True)
    p.add_argument("--viewer", default=None)
    p.add_argument("--verify", action="store_true",
                   help="逐条命令边界比对落库时的权威状态哈希")
    args = p.parse_args()
    replay(args.db, args.match, args.viewer, args.verify)


if __name__ == "__main__":
    main()
