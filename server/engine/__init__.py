"""server.engine: 纯权威结算内核, 不依赖网络与存储。"""
from .cards import BY_ID, CATALOG, Card
from .deck import validate_deck
from .engine import decide, start_match
from .errors import RuleError
from .fold import fold
from .replay import public_event_stream, replay
from .state import public_event, public_state, state_hash

__all__ = [
    "BY_ID", "CATALOG", "Card",
    "validate_deck",
    "decide", "start_match",
    "RuleError",
    "fold",
    "public_event_stream", "replay",
    "public_event", "public_state", "state_hash",
]
