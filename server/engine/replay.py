"""确定性回放: 从落库事件流重建对局并校验哈希。

调用方(服务端/测试)提供:
- start_events: start_match 生成并落库的开局事件
- event_rows : 后续按 seq 排列的对局事件
- expected_hash (可选): 现场在每条命令后记录的权威哈希

回放与线上唯一共享的是 fold() 纯函数与确定性 RNG, 不读时钟、不读网络,
因此只要事件流一致, 重建出的状态哈希必然一致。
"""

from __future__ import annotations

from typing import Any, Iterable

from .fold import fold
from .state import fresh_state, public_event, state_hash


def replay(start_events: list[dict], event_rows: Iterable[dict],
           expected_hashes: dict[int, str] | None = None
           ) -> dict[str, Any]:
    """重建对局。

    start_events: start_match 产出的完整开局事件流(MATCH_STARTED、洗牌、
    初始抽牌、首回合开始), 全部 fold 后即为首个可操作状态。
    event_rows: 后续 [{"seq": int, "event": {...}}, ...] 按 seq 升序。
    expected_hashes: {事件 seq: hash}, 逐条校验。
    返回 {state, checks: [(seq, hash, ok|None)], hash}。
    """
    state = fresh_state(start_events[0].get("match_id", "replay"),
                        start_events[0].get("names", ["seat0", "seat1"]))
    for e in start_events:
        fold(state, e)

    checks: list[tuple[int, str, bool | None]] = []
    expected = expected_hashes or {}
    for row in event_rows:
        fold(state, row["event"])
        seq = row["seq"]
        if seq in expected:
            checks.append((seq, state_hash(state),
                           state_hash(state) == expected[seq]))
        else:
            checks.append((seq, state_hash(state), None))
    return {"state": state, "checks": checks, "hash": state_hash(state)}


def public_event_stream(events: Iterable[dict], viewer: int | None
                        ) -> list[dict]:
    out = []
    for e in events:
        evt = public_event(e, viewer)
        if evt is not None:
            out.append(evt)
    return out
