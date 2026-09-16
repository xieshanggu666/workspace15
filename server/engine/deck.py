"""组牌校验。"""

from __future__ import annotations

from collections import Counter

from .cards import BY_ID, DECK_SIZE, MAX_COPIES
from .errors import RuleError


def validate_deck(card_ids: list[str]) -> None:
    if not isinstance(card_ids, list) or len(card_ids) != DECK_SIZE:
        raise RuleError("BAD_DECK", f"牌组必须恰好 {DECK_SIZE} 张")
    for cid in card_ids:
        if cid not in BY_ID:
            raise RuleError("BAD_DECK", f"未知卡牌: {cid}")
    for cid, n in Counter(card_ids).items():
        if n > MAX_COPIES:
            raise RuleError("BAD_DECK", f"{BY_ID[cid].name} 最多 {MAX_COPIES} 张")
