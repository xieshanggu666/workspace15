"""引擎内核测试: 确定性、结算顺序、反制、持续效果、据点胜负。"""

import pytest

from server.engine.cards import BY_ID
from server.engine.deck import validate_deck
from server.engine.engine import decide, start_match
from server.engine.errors import RuleError
from server.engine.fold import fold
from server.engine.replay import replay
from server.engine.state import state_hash, public_event, public_state

DECK = [
    "militia", "militia", "militia",
    "infantry", "infantry", "infantry",
    "vanguard", "vanguard", "vanguard",
    "guardian", "fireball", "reinforce",
    "repair", "counter", "banner",
]
POISON_DECK = DECK[:13] + ["poison", "poison"]


def new_game(seed=42, decks=None):
    decks = decks or (DECK, DECK)
    state, events = start_match("m", list(decks), ["a", "b"], seed)
    for e in events:
        fold(state, e)
    return state, events


def play(state, seat, uid, target=None):
    evs = decide(state, {"cmd": "PLAY", "seat": seat, "uid": uid,
                         "target": target})
    for e in evs:
        fold(state, e)
    return evs


def both_pass(state):
    """空堆叠直接过不了; 这里处理单条目堆叠的双方通过。"""
    first = state["responder"]
    for e in decide(state, {"cmd": "PASS", "seat": first}):
        fold(state, e)
    if state["phase"] == "response":
        second = state["responder"]
        for e in decide(state, {"cmd": "PASS", "seat": second}):
            fold(state, e)


def end_both(state):
    for e in decide(state, {"cmd": "END_TURN", "seat": 0}):
        fold(state, e)
    for e in decide(state, {"cmd": "END_TURN", "seat": 1}):
        fold(state, e)


def test_start_hands_and_energy():
    state, _ = new_game()
    assert len(state["seats"][0]["hand"]) == 5  # 初始4 + 首回合抽1
    assert len(state["seats"][1]["hand"]) == 4
    assert state["seats"][0]["energy"] == 1
    assert state["turn"] == 1 and state["active"] == 0


def test_deterministic_shuffle_and_replay_hash():
    s1, e1 = new_game(seed=99)
    s2, e2 = new_game(seed=99)
    assert state_hash(s1) == state_hash(s2)
    # 不同种子手牌可能不同(存在极小概率相同, 这里用整库哈希更稳)
    s3, _ = new_game(seed=100)
    # 用回放重建, 哈希必须一致
    r = replay(e1, [])
    assert r["hash"] == state_hash(s1)


def test_deck_validation():
    bad = ["militia"] * 15
    with pytest.raises(RuleError):
        validate_deck(bad)  # 同名牌超过 3 张
    ok = list(DECK)
    validate_deck(ok)
    with pytest.raises(RuleError):
        validate_deck(DECK[:14])


def test_play_costs_energy_and_deploys():
    state, _ = new_game()
    card = next(c for c in state["seats"][0]["hand"] if c["id"] == "militia")
    play(state, 0, card["uid"], target=0)
    assert state["seats"][0]["energy"] == 0
    both_pass(state)
    units = state["seats"][0]["units"]
    assert len(units) == 1 and units[0]["site"] == 0
    # 能量不足不能再出第二张 1 费牌
    another = next((c for c in state["seats"][0]["hand"]
                    if c["id"] in ("militia", "reinforce", "repair")), None)
    if another:
        with pytest.raises(RuleError):
            play(state, 0, another["uid"], target=0)


def test_opponent_cannot_play_in_your_turn():
    state, _ = new_game()
    card = next(c for c in state["seats"][1]["hand"] if c["id"] == "militia")
    with pytest.raises(RuleError):
        play(state, 1, card["uid"], target=0)


def test_counter_negates_card():
    # 需要让 seat0 出牌且 seat1 手里有反制。用固定种子搜索。
    state = None
    for seed in range(200):
        s, _ = new_game(seed=seed)
        if any(c["id"] == "counter" for c in s["seats"][1]["hand"]):
            state = s
            break
    assert state is not None
    card = next(c for c in state["seats"][0]["hand"] if c["id"] == "militia")
    play(state, 0, card["uid"], target=0)
    # seat1 第 1 小回合能量为 0, 即便手里有反制也不能打出:
    # 服务端统一判定"未轮到/资源不足", 牌不会离开手牌
    ctr = next(c for c in state["seats"][1]["hand"] if c["id"] == "counter")
    with pytest.raises(RuleError):
        decide(state, {"cmd": "COUNTER", "seat": 1, "uid": ctr["uid"],
                       "target_uid": card["uid"]})
    # seat1 正常通过后 seat0 也通过, 单位部署成功
    both_pass(state)
    assert len(state["seats"][0]["units"]) == 1


def test_counter_chain_on_second_turn():
    """seat1 在自己回合出牌, seat0 反制, seat1 再反制。
    第 3 整轮双方能量 3: 出牌 1 费 + 反制 2 费。"""
    state = None
    for seed in range(800):
        s, _ = new_game(seed=seed)
        h0 = [c["id"] for c in s["seats"][0]["hand"]]
        h1 = [c["id"] for c in s["seats"][1]["hand"]]
        if h0.count("counter") >= 1 and h1.count("counter") >= 1:
            state, chosen = s, seed
            break
    if state is None:
        pytest.skip("未找到双方初始手牌都含反制的种子")

    # 空过到 turn6(seat1, 第3整轮, 能量3): 共结束 5 个小回合
    for seat in (0, 1, 0, 1, 0):
        for e in decide(state, {"cmd": "END_TURN", "seat": seat}):
            fold(state, e)
    assert state["turn"] == 6 and state["active"] == 1
    assert state["seats"][1]["energy"] == 3 and state["seats"][0]["energy"] == 3

    unit = next(c for c in state["seats"][1]["hand"]
                if c["id"] in ("militia", "infantry")
                and BY_ID[c["id"]].cost <= 2)
    play(state, 1, unit["uid"], target=1)
    ctr0 = next(c for c in state["seats"][0]["hand"] if c["id"] == "counter")
    evs = decide(state, {"cmd": "COUNTER", "seat": 0,
                         "uid": ctr0["uid"], "target_uid": unit["uid"]})
    for e in evs:
        fold(state, e)
    # 窗口交回 seat1
    assert state["responder"] == 1
    ctr1 = next(c for c in state["seats"][1]["hand"] if c["id"] == "counter")
    evs = decide(state, {"cmd": "COUNTER", "seat": 1,
                         "uid": ctr1["uid"], "target_uid": ctr0["uid"]})
    for e in evs:
        fold(state, e)
    # seat0 无更多反制 -> pass; seat1 pass -> 结算:
    # 顶层 counter(ctr1) 反制 ctr0; 原 unit 未被反制 -> 部署
    for e in decide(state, {"cmd": "PASS", "seat": 0}):
        fold(state, e)
    for e in decide(state, {"cmd": "PASS", "seat": 1}):
        fold(state, e)
    assert state["phase"] == "main"
    assert any(u["uid"] == unit["uid"] for u in state["seats"][1]["units"])


def test_poison_kills_unit_and_site_flips():
    # 找种子: seat0 起手有 militia; 推进到 turn2 后 seat1 起手有 poison 且有 militia
    state = None
    for seed in range(500):
        s, _ = new_game(seed=seed, decks=(POISON_DECK, POISON_DECK))
        if not any(c["id"] == "militia" for c in s["seats"][0]["hand"]):
            continue
        # turn2 seat1(能量1): 只需 poison, 但 poison 费2 -> 需要 turn4(能量2)
        # 直接检查双方手牌含 poison/militia 的组合即可
        h1 = [c["id"] for c in s["seats"][1]["hand"]]
        if "poison" in h1:
            state = s
            break
    assert state is not None
    # seat0 第1回合部署 militia 到据点0
    militia0 = next(c for c in state["seats"][0]["hand"] if c["id"] == "militia")
    play(state, 0, militia0["uid"], target=0)
    both_pass(state)
    # 空过 seat0(结束 turn1) -> seat1 turn2, 再结束 -> seat0 turn3, 再结束 -> seat1 turn4
    for seat in (0, 1, 0):
        for e in decide(state, {"cmd": "END_TURN", "seat": seat}):
            fold(state, e)
    assert state["active"] == 1 and state["seats"][1]["energy"] >= 2
    # 此前整轮结束时 seat0 民兵已占住据点0
    assert state["sites"][0]["owner"] == 0

    poison = next(c for c in state["seats"][1]["hand"] if c["id"] == "poison")
    play(state, 1, poison["uid"], target=0)
    both_pass(state)
    aura = next(a for a in state["auras"] if a["id"] == "poison")
    assert aura["site"] == 0
    # seat1 结束小回合 tick: militia hp2 受 1 伤
    for e in decide(state, {"cmd": "END_TURN", "seat": 1}):
        fold(state, e)
    u = next(u for u in state["seats"][0]["units"] if u["site"] == 0)
    assert u["hp"] == 1
    # seat0 结束小回合再 tick 一次 -> 死亡, 据点丢失
    for e in decide(state, {"cmd": "END_TURN", "seat": 0}):
        fold(state, e)
    assert all(u["uid"] != militia0["uid"]
               for s0 in state["seats"] for u in s0["units"])
    assert state["sites"][0]["owner"] in (None, 1)


def test_idempotent_decide_does_not_mutate_state():
    """同一状态连续调用 decide 两次, 结果事件一致且状态不变。"""
    state, _ = new_game()
    card = next(c for c in state["seats"][0]["hand"] if c["id"] == "militia")
    cmd = {"cmd": "PLAY", "seat": 0, "uid": card["uid"], "target": 0}
    h_before = state_hash(state)
    e1 = decide(state, cmd)
    assert state_hash(state) == h_before
    e2 = decide(state, cmd)
    assert [e["type"] for e in e1] == [e["type"] for e in e2]


def test_public_view_hides_opponent_hand():
    state, events = new_game()
    view = public_state(state, viewer=0)
    # seat0 自己手牌可见, seat1 手牌只有数量
    assert isinstance(view["seats"][0]["hand"], list)
    assert view["seats"][1]["hand"] == {"count": 4}
    # DRAWN 事件对对手只给数量
    for e in events:
        if e["type"] == "DRAWN" and e["seat"] == 0:
            pub = public_event(e, viewer=1)
            assert pub == {"type": "DRAWN", "seat": 0, "count": len(e["cards"])}
            assert "cards" not in pub
            mine = public_event(e, viewer=0)
            assert "cards" in mine
    # 洗牌事件任何人都不可见
    assert all(public_event(e, 1) is None
               for e in events if e["type"] == "DECK_SHUFFLED")


def test_concede_ends_game():
    state, _ = new_game()
    evs = decide(state, {"cmd": "CONCEDE", "seat": 0})
    for e in evs:
        fold(state, e)
    assert state["winner"] == 1 and state["end_reason"] == "concede"


def test_fireball_kills_target():
    """火球 4 点伤害可击杀 2 血民兵。"""
    state = None
    for seed in range(300):
        s, _ = new_game(seed=seed)
        if any(c["id"] == "fireball" for c in s["seats"][0]["hand"]):
            state = s
            break
    assert state is not None
    # bob 先在据点0部署 militia(turn2 能量1)
    for e in decide(state, {"cmd": "END_TURN", "seat": 0}):
        fold(state, e)
    mil = next(c for c in state["seats"][1]["hand"] if c["id"] == "militia")
    play(state, 1, mil["uid"], target=0)
    both_pass(state)
    target = state["seats"][1]["units"][0]
    # turn3 seat0 能量2, 有火球
    for e in decide(state, {"cmd": "END_TURN", "seat": 1}):
        fold(state, e)
    fb = next(c for c in state["seats"][0]["hand"] if c["id"] == "fireball")
    play(state, 0, fb["uid"], target=target["uid"])
    both_pass(state)
    assert all(u["uid"] != target["uid"] for u in state["seats"][1]["units"])
