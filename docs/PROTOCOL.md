# 通信协议（WebSocket / JSON，UTF-8）

所有消息为单个 JSON 对象。客户端先发 `login`，服务端不做任何隐藏信息之外的状态推断。

## 客户端 → 服务端

| type | 字段 | 说明 |
|---|---|---|
| `login` | `name`, `token?` | 名字即账号；带旧 token 用于身份校验与自动恢复对局 |
| `queue` | `deck: string[15]` | 校验 15 张、同名 ≤3；两人排队即开局 |
| `command` | `op_id`, `command` | `op_id` 客户端生成且在对局内唯一；**同 op_id 重发不重复扣费/落库** |
| `sync` | `last_seq` | 拉取该序号之后的事件并回一张 snapshot（重连补发） |
| `replay` | `match_id` | 请求整局事件流（按视角过滤） |

`command` 对象：

```json
{"cmd": "PLAY",     "uid": "s0-3", "target": 0}
{"cmd": "PLAY",     "uid": "s0-7", "target": "s1-2"}   // 法术指定单位 uid
{"cmd": "COUNTER",  "uid": "s0-12", "target_uid": "s1-5"}
{"cmd": "PASS"}
{"cmd": "END_TURN"}
{"cmd": "CONCEDE"}
```

系统超时不来自客户端：服务端定时任务以 `SYSTEM_TIMEOUT` 自行落库，
防止恶意客户端靠不响应卡住对局。

## 服务端 → 客户端

| type | 说明 |
|---|---|
| `login_ok` / `login_fail` | 返回持久 token |
| `match_begin` / `match_resume` | `{match_id, seat}` |
| `event` | `{seq, event}`，**按座位过滤**：对手抽牌只有 `count`；洗牌事件不下发 |
| `snapshot` | `{state, deadline_at}` 完整权威状态（对手手牌/牌库只有 count） |
| `command_result` | `{op_id, accepted, duplicate?, code?}` |
| `match_end` | `{winner: 0|1|null, reason, state_hash}` |
| `replay` | `{match_id, events, full}` |

## 事件类型（落库 + 回放共用，按触发顺序）

```
MATCH_STARTED / DECK_SHUFFLED(仅服务端可见) / DRAWN / ENERGY_SET /
TURN_STARTED
  CARD_PLAYED → RESPONSE_OPENED
    COUNTER: CARD_PLAYED(counter) → WINDOW_PASSED(交给被反制者)
    PASS:    WINDOW_PASSED(交给对方) 或 WINDOW_PASSED(null)=双方通过
  WINDOW_PASSED(null) 后自顶向下结算:
    UNIT_DEPLOYED / AURA_PLACED / SPELL_EFFECT / CARD_COUNTERED / UNITS_DIED
  END_TURN:
    AURA_TICKED(持续效果: 战旗/毒雾) → UNIT_DAMAGED → UNITS_DIED →
    TURN_ENDED(据点归属结算) → [ROUND_ENDED] →
    TURN_STARTED(下一行动者) / GAME_ENDED
```

胜负：任一整轮结束时占据 ≥2 据点立即获胜；20 个小回合后按据点数判定
（相等为平局 `winner=null`）；投降立即判负。
