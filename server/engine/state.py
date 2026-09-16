"""对局状态: 构造、查询、规范化哈希、面向座位的视图(隐藏信息)。

状态是一个纯 JSON 兼容的 dict/list 结构, 这样:
1. 事件 fold 出来的状态可直接 json.dumps 做确定性哈希;
2. 快照可直接发给 Godot 客户端;
3. 回放只需要 (初始状态 + 事件流)。
"""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any

from .cards import MAX_SITES
from .errors import RuleError


def fresh_state(match_id: str, names: list[str]) -> dict[str, Any]:
    return {
        "match_id": match_id,
        "turn": 0,
        "round": 0,
        "active": 0,
        "phase": "main",
        "responder": None,
        "stack": [],
        "next_seq": 1,
        "seats": [
            {
                "seat": i,
                "name": names[i] if i < len(names) else f"seat{i}",
                "energy": 0,
                "max_energy": 0,
                "hand": [],
                "deck": [],
                "units": [],
                "graveyard": [],
                "conceded": False,
            }
            for i in range(2)
        ],
        "sites": [{"site": s, "owner": None} for s in range(MAX_SITES)],
        "auras": [],
        "winner": None,
        "end_reason": None,
    }


def find_unit(state: dict, uid: str) -> dict | None:
    for seat in state["seats"]:
        for u in seat["units"]:
            if u["uid"] == uid:
                return u
    return None


def banner_bonus(state: dict, site: int, seat: int) -> int:
    return sum(
        1
        for a in state["auras"]
        if a["site"] == site and a["owner"] == seat and a["id"] == "banner"
    )


def site_power(state: dict, site: int, seat: int) -> int:
    base = sum(
        u["power"] + u.get("buffs", 0)
        for u in state["seats"][seat]["units"]
        if u["site"] == site
    )
    return base + banner_bonus(state, site, seat)


def site_units(state: dict, site: int) -> list[dict]:
    out = []
    for seat in state["seats"]:
        for u in seat["units"]:
            if u["site"] == site:
                out.append(u)
    return out


def get_hand(state: dict, seat: int, uid: str) -> dict | None:
    for c in state["seats"][seat]["hand"]:
        if c["uid"] == uid:
            return c
    return None


def stack_top(state: dict) -> dict | None:
    return state["stack"][-1] if state["stack"] else None


def require_unit(uid: str) -> dict:
    # 便捷包装: 在 decide 内使用
    raise RuleError  # pragma: no cover - 占位, 实际用 find_unit


# ---------------------------------------------------------------- 规范化/哈希

def canonical(state: dict) -> str:
    """确定性 JSON 序列化。只对状态本体调用, 不含时间戳。"""
    return json.dumps(state, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def state_hash(state: dict) -> str:
    return hashlib.sha256(canonical(state).encode("utf-8")).hexdigest()[:16]


def clone(state: dict) -> dict:
    return copy.deepcopy(state)


# --------------------------------------------------------------- 视图/隐藏信息

HIDDEN_EVENT_TYPES = {"DECK_SHUFFLED"}


def public_event(evt: dict, viewer: int | None) -> dict | None:
    """返回某座位(或旁观者)可见的事件副本; None 表示完全不可见。

    对手的抽牌只公布座位与数量, 不公布牌面; 洗牌完全不可见。
    """
    t = evt["type"]
    if t in HIDDEN_EVENT_TYPES:
        return None
    e = dict(evt)
    if t == "DRAWN" and viewer != e["seat"]:
        return {"type": "DRAWN", "seat": e["seat"], "count": len(e["cards"])}
    if t == "MATCH_STARTED":
        # 公开信息不含任何牌库内容
        e.pop("decks", None)
    return e


def public_state(state: dict, viewer: int | None) -> dict:
    """面向某座位的状态快照: 对手手牌只给数量, 双方牌库只给数量。"""
    s = clone(state)
    for i, seat in enumerate(s["seats"]):
        if viewer == i:
            seat["deck"] = {"count": len(seat["deck"])}
        else:
            seat["hand"] = {"count": len(seat["hand"])}
            seat["deck"] = {"count": len(seat["deck"])}
    return s
