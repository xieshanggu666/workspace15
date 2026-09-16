"""事件 fold: 把事件按序应用到状态上。纯函数式更新, 无 I/O、无随机数。

决定阶段(decide)负责"产生哪些事件", fold 负责"事件如何改变状态",
二者分离保证回放与现场结算走同一份代码路径。
"""

from __future__ import annotations

from typing import Any

from .cards import BY_ID
from .errors import RuleError
from .rng import shuffled
from .state import find_unit


def fold(state: dict[str, Any], evt: dict[str, Any]) -> dict[str, Any]:
    t = evt["type"]
    fn = _HANDLERS.get(t)
    if fn is None:
        raise RuleError("BAD_EVENT", f"未知事件: {t}")
    fn(state, evt)
    return state


def _h_match_started(s: dict, e: dict) -> None:
    for seat_data in e["decks"]:
        seat_no = seat_data["seat"]
        s["seats"][seat_no]["deck"] = [dict(c) for c in seat_data["cards"]]


def _h_deck_shuffled(s: dict, e: dict) -> None:
    seat = s["seats"][e["seat"]]
    seat["deck"] = [dict(c) for c in shuffled(seat["deck"], e["seed"])]


def _h_drawn(s: dict, e: dict) -> None:
    seat = s["seats"][e["seat"]]
    cards = {c["uid"]: c for c in seat["deck"]}
    moved = []
    for d in e["cards"]:
        c = cards.pop(d["uid"], None)
        if c is None:
            # 理论不可达: 事件由引擎确定性产生
            raise RuleError("ENGINE_INCONSISTENT", f"抽牌不存在: {d['uid']}")
        moved.append(c)
    seat["deck"] = list(cards.values())
    seat["hand"].extend(moved)


def _h_energy_set(s: dict, e: dict) -> None:
    seat = s["seats"][e["seat"]]
    seat["energy"] = e["energy"]
    seat["max_energy"] = e["max_energy"]


def _h_turn_started(s: dict, e: dict) -> None:
    s["turn"] = e["turn"]
    s["round"] = e["round"]
    s["active"] = e["active"]
    s["phase"] = "main"
    s["responder"] = None


def _h_card_played(s: dict, e: dict) -> None:
    seat = s["seats"][e["seat"]]
    seat["hand"] = [c for c in seat["hand"] if c["uid"] != e["uid"]]
    card = BY_ID[e["id"]]
    entry = {
        "seq": e["stack_seq"],
        "uid": e["uid"],
        "id": e["id"],
        "owner": e["seat"],
        "target": e.get("target"),
        "card": card.to_dict(),
    }
    s["stack"].append(entry)
    # 扣费与出牌事件原子生效: 重复操作在服务端被幂等拦截, 不会二次 fold
    seat["energy"] = max(0, seat["energy"] - card.cost)
    # 事件驱动的序号推进, 保证回放与现场一致
    s["next_seq"] = max(s["next_seq"], e["stack_seq"] + 1)


def _h_response_opened(s: dict, e: dict) -> None:
    s["phase"] = "response"
    s["responder"] = e["responder"]


def _h_window_passed(s: dict, e: dict) -> None:
    if e["responder"] is None:
        # 双方通过, 堆叠清空后回到主阶段
        s["phase"] = "main"
        s["responder"] = None
    else:
        s["responder"] = e["responder"]


def _h_unit_deployed(s: dict, e: dict) -> None:
    s["stack"] = [x for x in s["stack"] if x["seq"] != e["stack_seq"]]
    if e.get("resolves"):
        s["seats"][e["seat"]]["units"].append(
            {
                "uid": e["uid"],
                "id": e["id"],
                "site": e["site"],
                "owner": e["seat"],
                "power": e["power"],
                "hp": e["hp"],
                "max_hp": e["hp"],
                "buffs": 0,
            }
        )


def _h_aura_placed(s: dict, e: dict) -> None:
    s["stack"] = [x for x in s["stack"] if x["seq"] != e["stack_seq"]]
    if e.get("resolves"):
        s["auras"].append(
            {
                "uid": e["uid"],
                "id": e["id"],
                "site": e["site"],
                "owner": e["seat"],
                "remaining": e["duration"],
            }
        )


def _h_spell_effect(s: dict, e: dict) -> None:
    s["stack"] = [x for x in s["stack"] if x["seq"] != e["stack_seq"]]
    if not e.get("resolves"):
        return
    key = e["key"]
    target = find_unit(s, e["target"]) if e.get("target") else None
    if key in ("fireball",) and target is not None:
        target["hp"] -= e["amount"]
    elif key == "reinforce" and target is not None:
        target["buffs"] += e["power"]
        target["power"] += e["power"]
        target["hp"] += e["hp"]
        target["max_hp"] += e["hp"]
    elif key == "repair" and target is not None:
        target["hp"] = min(target["max_hp"], target["hp"] + e["amount"])


def _h_card_countered(s: dict, e: dict) -> None:
    seqs = {e["stack_seq"], e["countered_seq"]}
    removed = [x for x in s["stack"] if x["seq"] in seqs]
    s["stack"] = [x for x in s["stack"] if x["seq"] not in seqs]
    for entry in removed:
        s["seats"][entry["owner"]]["graveyard"].append(
            {"uid": entry["uid"], "id": entry["id"]}
        )


def _h_unit_damaged(s: dict, e: dict) -> None:
    u = find_unit(s, e["uid"])
    if u is not None:
        u["hp"] -= e["amount"]


def _h_units_died(s: dict, e: dict) -> None:
    dead = set(e["uids"])
    for seat in s["seats"]:
        survivors = []
        for u in seat["units"]:
            if u["uid"] in dead:
                seat["graveyard"].append({"uid": u["uid"], "id": u["id"]})
            else:
                survivors.append(u)
        seat["units"] = survivors


def _h_aura_ticked(s: dict, e: dict) -> None:
    remap = {a["uid"]: a["remaining"] for a in e["remaining"]}
    expired = set(e["expired"])
    s["auras"] = [a for a in s["auras"] if a["uid"] not in expired]
    for a in s["auras"]:
        if a["uid"] in remap:
            a["remaining"] = remap[a["uid"]]


def _h_turn_ended(s: dict, e: dict) -> None:
    for ctl in e["control"]:
        s["sites"][ctl["site"]]["owner"] = ctl["owner"]


def _h_round_ended(s: dict, e: dict) -> None:
    # 据点归属已在 TURN_ENDED 中落; 这里仅作为公开标记事件
    pass


def _h_game_ended(s: dict, e: dict) -> None:
    s["winner"] = e["winner"]
    s["end_reason"] = e["reason"]
    s["phase"] = "ended"
    s["responder"] = None
    s["stack"] = []


_HANDLERS = {
    "MATCH_STARTED": _h_match_started,
    "DECK_SHUFFLED": _h_deck_shuffled,
    "DRAWN": _h_drawn,
    "ENERGY_SET": _h_energy_set,
    "TURN_STARTED": _h_turn_started,
    "CARD_PLAYED": _h_card_played,
    "RESPONSE_OPENED": _h_response_opened,
    "WINDOW_PASSED": _h_window_passed,
    "UNIT_DEPLOYED": _h_unit_deployed,
    "AURA_PLACED": _h_aura_placed,
    "SPELL_EFFECT": _h_spell_effect,
    "CARD_COUNTERED": _h_card_countered,
    "UNIT_DAMAGED": _h_unit_damaged,
    "UNITS_DIED": _h_units_died,
    "AURA_TICKED": _h_aura_ticked,
    "TURN_ENDED": _h_turn_ended,
    "ROUND_ENDED": _h_round_ended,
    "GAME_ENDED": _h_game_ended,
}
