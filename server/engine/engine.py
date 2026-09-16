"""权威结算引擎。

设计:
- 所有状态变化都以事件描述。decide(...) 只负责校验并产出事件列表,
  fold(...) 负责把事件应用到状态。回放与线上完全复用这条路径。
- 触发顺序: 出牌 -> 堆叠 -> 响应窗(对手优先) -> 通过/反制 ->
  自顶向下结算 -> (小回合结束) 持续效果 -> 单位死亡 -> 据点归属 -> 胜负。
- 能量在 CARD_PLAYED 同一条决定里先扣, 因此"是否扣费"与"事件落库"
  在同一个事务里, 重复 op_id 由服务端幂等拦截, 不会二次扣费。
"""

from __future__ import annotations

from typing import Any

from .cards import (
    BY_ID,
    ENERGY_CAP,
    HALF_TURNS_CAP,
    HAND_LIMIT,
    INITIAL_HAND,
)
from .deck import validate_deck
from .errors import RuleError
from .fold import fold
from .state import (
    clone,
    find_unit,
    fresh_state,
    site_units,
    stack_top,
)


# ----------------------------------------------------------- 开局/洗牌/抽牌

def make_uids(card_ids: list[str], seat: int) -> list[dict]:
    return [
        {"uid": f"s{seat}-{i}", "id": cid}
        for i, cid in enumerate(card_ids)
    ]


def start_match(match_id: str, decks: list[list[str]], names: list[str],
                shuffle_seed: int) -> tuple[dict, list[dict]]:
    """返回 (初始状态, 事件列表)。事件包含洗好的牌库顺序, 由调用方落库。"""
    if len(decks) != 2:
        raise RuleError("BAD_DECK", "需要两套牌组")
    for d in decks:
        validate_deck(d)

    decks_events = []
    events: list[dict] = [
        {"type": "MATCH_STARTED", "match_id": match_id, "names": list(names),
         "decks": decks_events},
    ]
    for seat, card_ids in enumerate(decks):
        ordered = make_uids(card_ids, seat)
        seed = shuffle_seed + seat * 7919
        # 事件中保存的是洗牌前的有序牌库; DECK_SHUFFLED 负责确定性洗牌
        decks_events.append({"seat": seat, "cards": ordered})
        events.append({"type": "DECK_SHUFFLED", "seat": seat, "seed": seed})

    # 顺序应用开局事件(与回放路径完全一致)到基底状态
    base = fresh_state(match_id, names)
    for e in events:
        fold(base, e)

    # 初始手牌与首回合事件在模拟状态上生成; 调用方会把整段事件再 fold 一次
    sim = clone(base)
    rest: list[dict] = []
    rest.append(_draw_cards(sim, 0, INITIAL_HAND)[1])
    rest.append(_draw_cards(sim, 1, INITIAL_HAND)[1])
    rest.extend(_open_turn(sim, turn=1, active=0))
    return base, events + rest


def _draw_cards(state: dict, seat: int, n: int) -> tuple[dict, dict]:
    s = state["seats"][seat]
    room = max(0, HAND_LIMIT - len(s["hand"]))
    n = min(n, room, len(s["deck"]))
    drawn = [dict(c) for c in s["deck"][:n]]  # deck 顶部
    evt = {"type": "DRAWN", "seat": seat, "cards": drawn}
    fold(state, evt)
    return state, evt


def _open_turn(state: dict, turn: int, active: int) -> list[dict]:
    """进入第 turn 个小回合(1 起)。整轮 = 两个小回合, 能量随整轮增长。"""
    round_no = (turn + 1) // 2
    events = [
        {"type": "TURN_STARTED", "turn": turn, "round": round_no, "active": active},
        {"type": "ENERGY_SET", "seat": active,
         "energy": min(round_no, ENERGY_CAP),
         "max_energy": min(round_no, ENERGY_CAP)},
    ]
    for e in events:
        fold(state, e)
    _, drawn = _draw_cards(state, active, 1)
    events.append(drawn)
    return events


# --------------------------------------------------------------- 主入口

def decide(state: dict, command: dict) -> list[dict]:
    """纯决定函数: 校验命令, 返回应追加的事件。失败抛 RuleError。"""
    if state["winner"] is not None:
        raise RuleError("GAME_OVER", "对局已结束")
    cmd = command.get("cmd")
    seat = command.get("seat")
    if seat not in (0, 1):
        raise RuleError("BAD_SEAT", "座位非法")
    if cmd == "CONCEDE":
        return [{"type": "GAME_ENDED", "winner": 1 - seat, "reason": "concede"}]
    if cmd == "SYSTEM_TIMEOUT":
        return _timeout(state, seat, command.get("reason"))
    if cmd == "PLAY":
        return _play(state, seat, command)
    if cmd == "COUNTER":
        return _counter(state, seat, command)
    if cmd == "PASS":
        return _pass(state, seat)
    if cmd == "END_TURN":
        return _end_turn(state, seat)
    raise RuleError("BAD_CMD", f"未知命令: {cmd}")


# --------------------------------------------------------------- 出牌

def _play(state: dict, seat: int, command: dict) -> list[dict]:
    if state["phase"] != "main" or state["active"] != seat:
        raise RuleError("NOT_YOUR_TURN", "只能在自己的主阶段出牌")
    if state["stack"]:
        raise RuleError("STACK_BUSY", "堆叠结算中, 请先处理响应")
    uid = command.get("uid")
    hand = state["seats"][seat]["hand"]
    card_entry = next((c for c in hand if c["uid"] == uid), None)
    if card_entry is None:
        raise RuleError("NO_CARD", "手牌中没有该牌")
    card = BY_ID[card_entry["id"]]
    if card.kind == "reaction":
        raise RuleError("WRONG_PHASE", "反制牌只能在响应窗打出")
    s = state["seats"][seat]
    if s["energy"] < card.cost:
        raise RuleError("NO_ENERGY", "能量不足")
    target = command.get("target")
    _validate_target(state, card, seat, target)

    seq = state["next_seq"]
    events = [
        {"type": "CARD_PLAYED", "seat": seat, "uid": card_entry["uid"],
         "id": card.id, "target": target, "stack_seq": seq},
        {"type": "RESPONSE_OPENED", "responder": 1 - seat},
    ]
    return events


def _counter(state: dict, seat: int, command: dict) -> list[dict]:
    if state["phase"] != "response" or state["responder"] != seat:
        raise RuleError("NOT_YOUR_WINDOW", "还没轮到你响应")
    uid = command.get("uid")
    target_uid = command.get("target_uid")
    hand = state["seats"][seat]["hand"]
    card_entry = next((c for c in hand if c["uid"] == uid), None)
    if card_entry is None:
        raise RuleError("NO_CARD", "手牌中没有该牌")
    card = BY_ID[card_entry["id"]]
    if card.key != "counter":
        raise RuleError("NOT_COUNTER", "该牌不是反制牌")
    target = next((x for x in state["stack"] if x["uid"] == target_uid), None)
    if target is None:
        raise RuleError("NO_TARGET", "堆叠中没有该目标")
    s = state["seats"][seat]
    if s["energy"] < card.cost:
        raise RuleError("NO_ENERGY", "能量不足")

    seq = state["next_seq"]
    events = [
        {"type": "CARD_PLAYED", "seat": seat, "uid": card_entry["uid"],
         "id": card.id, "target": target_uid, "stack_seq": seq},
        # 下一个响应窗给被反制那张牌的拥有者
        {"type": "WINDOW_PASSED", "responder": target["owner"]},
    ]
    return events


def _pass(state: dict, seat: int) -> list[dict]:
    if state["phase"] != "response":
        raise RuleError("WRONG_PHASE", "当前没有响应窗")
    if state["responder"] != seat:
        raise RuleError("NOT_YOUR_WINDOW", "还没轮到你响应")
    top = stack_top(state)
    other = 1 - seat
    if top is not None and top["owner"] == other:
        # 当前玩家通过, 而堆叠顶属于对手 -> 让对手也过一轮: 窗口交给对手
        return [{"type": "WINDOW_PASSED", "responder": other}]
    # 堆叠顶属于自己(双方均已过) -> 结算
    return _resolve_stack(state)


def _timeout(state: dict, seat: int | None, reason: str | None) -> list[dict]:
    if state["phase"] == "response":
        # 响应窗超时 = 当前响应者强制通过
        r = state["responder"]
        return _pass(state, r)
    if state["phase"] == "main":
        return _end_turn(state, state["active"])
    raise RuleError("WRONG_PHASE", "当前阶段不可超时")


# --------------------------------------------------------------- 堆叠结算

def _resolve_stack(state: dict) -> list[dict]:
    """双方通过后自顶向下结算所有条目。返回事件(含 WINDOW_PASSED None)。"""
    events: list[dict] = [{"type": "WINDOW_PASSED", "responder": None}]
    sim_stack = [dict(x) for x in state["stack"]]
    sim_countered: set[int] = set()
    while sim_stack:
        entry = sim_stack.pop()
        seq = entry["seq"]
        card = BY_ID[entry["id"]]
        if card.key == "counter":
            # 反制目标(其下某条, 通常是顶)
            target = next(
                (x for x in sim_stack if x["uid"] == entry["target"]), None
            )
            if target is not None:
                sim_countered.add(target["seq"])
                sim_stack = [x for x in sim_stack if x["seq"] != target["seq"]]
            events.append({
                "type": "CARD_COUNTERED",
                "seat": entry["owner"],
                "uid": entry["uid"],
                "stack_seq": seq,
                "target_uid": entry["target"],
                "countered_seq": target["seq"] if target else None,
            })
            continue
        if seq in sim_countered:
            # 理论上在上面已移除, 兜底
            continue
        events.extend(_resolve_entry(state, entry))

    # 结算后死亡清理(如火球击杀): 用模拟状态找出 hp<=0 的单位
    sim = clone(state)
    for e in events:
        fold(sim, e)
    dead = sorted(
        u["uid"] for s0 in sim["seats"] for u in s0["units"] if u["hp"] <= 0
    )
    if dead:
        events.append({"type": "UNITS_DIED", "uids": dead})
    return events


def _resolve_entry(state: dict, entry: dict) -> list[dict]:
    card = BY_ID[entry["id"]]
    seat = entry["owner"]
    target_uid = entry.get("target")
    if card.kind == "unit":
        return [{"type": "UNIT_DEPLOYED", "seat": seat, "uid": entry["uid"],
                 "id": card.id, "site": target_uid, "stack_seq": entry["seq"],
                 "power": card.power, "hp": card.hp, "resolves": True}]
    if card.kind == "aura":
        return [{"type": "AURA_PLACED", "seat": seat, "uid": entry["uid"],
                 "id": card.id, "site": target_uid, "stack_seq": entry["seq"],
                 "duration": card.duration, "resolves": True}]
    # spell
    if card.key == "fireball":
        return [{"type": "SPELL_EFFECT", "seat": seat, "key": "fireball",
                 "uid": entry["uid"], "target": target_uid,
                 "stack_seq": entry["seq"], "amount": 4, "resolves": True}]
    if card.key == "reinforce":
        return [{"type": "SPELL_EFFECT", "seat": seat, "key": "reinforce",
                 "uid": entry["uid"], "target": target_uid,
                 "stack_seq": entry["seq"], "power": 1, "hp": 1,
                 "resolves": True}]
    if card.key == "repair":
        return [{"type": "SPELL_EFFECT", "seat": seat, "key": "repair",
                 "uid": entry["uid"], "target": target_uid,
                 "stack_seq": entry["seq"], "amount": 2, "resolves": True}]
    raise RuleError("BAD_CARD", f"无法结算的牌: {card.id}")  # pragma: no cover


def _validate_target(state: dict, card, seat: int, target: Any) -> None:
    if card.target == "none":
        return
    if card.target == "self_site":
        if not isinstance(target, int) or not (0 <= target < 3):
            raise RuleError("BAD_TARGET", "目标据点必须是 0/1/2")
        return
    if card.target == "enemy_unit":
        u = find_unit(state, target) if isinstance(target, str) else None
        if u is None or u["owner"] == seat:
            raise RuleError("BAD_TARGET", "需要一个敌方单位目标")
        return
    if card.target == "friendly_unit":
        u = find_unit(state, target) if isinstance(target, str) else None
        if u is None or u["owner"] != seat:
            raise RuleError("BAD_TARGET", "需要一个友方单位目标")
        return
    raise RuleError("BAD_TARGET", "未知目标类型")  # pragma: no cover


# --------------------------------------------------------------- 回合结束

def _end_turn(state: dict, seat: int) -> list[dict]:
    if state["phase"] != "main" or state["active"] != seat:
        raise RuleError("NOT_YOUR_TURN", "不是你的回合, 不能结束")

    events: list[dict] = []

    # 1) 持续效果 tick(仅当一个完整小回合结束时结算一次)
    remaining = []
    expired = []
    poison_sites: dict[int, int] = {}
    for a in state["auras"]:
        new_rem = a["remaining"] - 1
        if a["id"] == "poison":
            poison_sites[a["site"]] = poison_sites.get(a["site"], 0) + 1
        if new_rem <= 0:
            expired.append(a["uid"])
        else:
            remaining.append({"uid": a["uid"], "remaining": new_rem})
    if state["auras"]:
        events.append({"type": "AURA_TICKED", "remaining": remaining,
                       "expired": expired})

    # 2) 毒雾伤害 -> 死亡
    damage_events = []
    dead_uids = set()
    for site, amount in poison_sites.items():
        for u in site_units(state, site):
            damage_events.append({"type": "UNIT_DAMAGED", "uid": u["uid"],
                                  "amount": amount, "source": "poison",
                                  "site": site})
            if u["hp"] - amount <= 0:
                dead_uids.add(u["uid"])
    events.extend(damage_events)
    if dead_uids:
        events.append({"type": "UNITS_DIED", "uids": sorted(dead_uids)})

    # 3) 据点归属: tick 后仍存活的单位 + 仍生效的战旗
    surviving = {
        u["uid"]: u for s0 in state["seats"] for u in s0["units"]
        if u["uid"] not in dead_uids
    }
    surviving_auras = [
        a for a in state["auras"] if a["uid"] not in set(expired)
    ]

    def power_after(site: int, owner: int) -> int:
        base = sum(
            u["power"] + u.get("buffs", 0)
            for u in surviving.values()
            if u["site"] == site and u["owner"] == owner
        )
        base += sum(
            1 for a in surviving_auras
            if a["site"] == site and a["owner"] == owner and a["id"] == "banner"
        )
        return base

    control = []
    for site in range(3):
        p0 = power_after(site, 0)
        p1 = power_after(site, 1)
        owner = state["sites"][site]["owner"]
        if p0 > p1 and p0 > 0:
            owner = 0
        elif p1 > p0 and p1 > 0:
            owner = 1
        elif p0 == p1:
            owner = None if p0 == 0 else state["sites"][site]["owner"]
        control.append({"site": site, "owner": owner})
    events.append({"type": "TURN_ENDED", "seat": seat, "control": control})

    # 回合结束事件先 fold, 保证下回合判断基于新归属
    sim = clone(state)
    for e in events:
        fold(sim, e)

    counts = [0, 0]
    for site in sim["sites"]:
        if site["owner"] is not None:
            counts[site["owner"]] += 1

    # 一个小回合 = seat0 或 seat1 之一行动; seat1 结束表示一个整轮完成
    round_finished = seat == 1
    next_turn = state["turn"] + 1
    if round_finished:
        events.append({"type": "ROUND_ENDED", "round": state["round"]})
        if counts[0] >= 2 or counts[1] >= 2:
            winner = 0 if counts[0] > counts[1] else 1
            events.append({"type": "GAME_ENDED", "winner": winner,
                           "reason": "strongholds"})
            return events
        if next_turn > HALF_TURNS_CAP:
            if counts[0] == counts[1]:
                events.append({"type": "GAME_ENDED", "winner": None,
                               "reason": "draw"})
            else:
                winner = 0 if counts[0] > counts[1] else 1
                events.append({"type": "GAME_ENDED", "winner": winner,
                               "reason": "cap"})
            return events

    next_active = 1 - seat
    events.extend(_open_turn(sim, next_turn, next_active))
    return events
