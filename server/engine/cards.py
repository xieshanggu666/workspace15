"""卡牌目录。

牌张类型:
- unit     单位牌: 部署到据点, 提供战力与生命
- spell    法术牌: 立即结算
- reaction 反制牌: 只能在响应窗打出, 反击堆叠中的牌
- aura     持续牌: 附着于据点, 在每小回合结束时结算持续效果

key:
- deploy   单位部署
- fireball 法术: 对敌方单位造成 4 点伤害
- reinforce 法术: 友方单位 +1/+1
- repair   法术: 友方单位回复 2 点生命
- counter  反制: 反击一张牌
- banner   持续(增益): 该据点友方总战力 +1, 持续 3 小回合
- poison   持续(减益): 每小回合结束对所有单位造成 1 点伤害, 持续 2 小回合
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

KINDS = ("unit", "spell", "reaction", "aura")


@dataclass(frozen=True)
class Card:
    id: str
    name: str
    cost: int
    kind: str
    key: str
    power: int = 0
    hp: int = 0
    duration: int = 0
    target: str = "none"  # none | self_site | enemy_unit | friendly_unit | stack
    desc: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "cost": self.cost,
            "kind": self.kind,
            "key": self.key,
            "power": self.power,
            "hp": self.hp,
            "duration": self.duration,
            "target": self.target,
            "desc": self.desc,
        }


CATALOG: list[Card] = [
    Card("militia", "民兵", 1, "unit", "deploy", power=1, hp=2,
         target="self_site", desc="部署到己方据点, 战力1 生命2"),
    Card("infantry", "步兵", 2, "unit", "deploy", power=2, hp=3,
         target="self_site", desc="部署到己方据点, 战力2 生命3"),
    Card("vanguard", "先锋", 3, "unit", "deploy", power=3, hp=4,
         target="self_site", desc="部署到己方据点, 战力3 生命4"),
    Card("guardian", "守卫", 4, "unit", "deploy", power=3, hp=6,
         target="self_site", desc="部署到己方据点, 战力3 生命6"),
    Card("fireball", "火球", 2, "spell", "fireball",
         target="enemy_unit", desc="对一个敌方单位造成4点伤害"),
    Card("reinforce", "增援", 1, "spell", "reinforce",
         target="friendly_unit", desc="使一个友方单位+1/+1"),
    Card("repair", "修理", 1, "spell", "repair",
         target="friendly_unit", desc="使一个友方单位回复2点生命"),
    Card("counter", "反制", 2, "reaction", "counter",
         target="stack", desc="反击堆叠中的一张牌, 使其无效"),
    Card("banner", "战旗", 2, "aura", "banner", duration=3,
         target="self_site", desc="附着据点: 己方总战力+1, 持续3小回合"),
    Card("poison", "毒雾", 2, "aura", "poison", duration=2,
         target="self_site", desc="附着据点: 每小回合结束所有单位受1点伤害, 持续2小回合"),
]

BY_ID: dict[str, Card] = {c.id: c for c in CATALOG}

# 组牌规则
DECK_SIZE = 15
MAX_COPIES = 3
HAND_LIMIT = 16
INITIAL_HAND = 4
MAX_SITES = 3
ENERGY_CAP = 10
ROUND_CAP = 10  # 满 10 个整轮(共 20 小回合)后按据点判定
HALF_TURNS_CAP = 20
# 每小回合时长(秒), 响应窗时长, 掉线宽限
TURN_SECONDS = 30.0
RESPONSE_SECONDS = 15.0
DISCONNECT_GRACE = 15.0


def get(card_id: str) -> Card:
    return BY_ID[card_id]
