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

### 并发与幂等

所有命令（玩家命令与系统超时）走同一把对局锁，串行经过
「校验 → 在克隆状态上结算 → SQLite 事务提交（命令+事件+该命令后的权威
状态哈希检查点）→ 替换内存状态 → 重排定时器（epoch 自增）→ 锁外投递广播」。
重复 `op_id` 直接返回首次结果；即使两个连接/进程绕过预检同时提交，
`(match_id, op_id)` 唯一约束也会让败者的整个事务回滚，事件流不重复。
用 `python -m scripts.replay --verify` 可逐条检查点核对事件流。

## 服务端 → 客户端

| type | 说明 |
|---|---|
| `login_ok` / `login_fail` | 返回持久 token |
| `match_begin` / `match_resume` | `{match_id, seat}` |
| `match_corrupt` | `{match_id, message}` 事件流校验失败, 对局已隔离, 不会也不能恢复 |
| `event` | `{seq, event}`，**按座位过滤**：对手抽牌只有 `count`；洗牌事件不下发 |
| `snapshot` | `{state, deadline_at}` 完整权威状态（对手手牌/牌库只有 count） |
| `command_result` | `{op_id, accepted, duplicate?, code?}` |
| `match_end` | `{winner: 0|1|null, reason, state_hash}` |
| `replay` | `{match_id, events, full, status, verify_error}`；只读审计，已隔离对局仍可查看，`status="corrupt"` 且 `verify_error` 非空 |

## 损坏事件流隔离

重启恢复、断线重连（即使内存已有 LiveMatch，也会在持锁时重新校验磁盘事件流
并比对内存/磁盘哈希）、运行期幂等兜底重建共用唯一入口 `_load_verified`，
用 checkpoints 逐条正向校验 + 反向确认每条玩家命令都有锚点。任何不一致
（事件重复/丢失/乱序、哈希分叉、检查点缺失）都会：把 `matches.status` 置为
`corrupt`、原因写入 `corruption` 审计表、摘除内存对象并取消定时器，向在场
连接和重连者发 `match_corrupt`。`corrupt` 对局不会被 `active_match_for`
找回、不能借重连复活、定时器不再触发；其事件保留仅供 `replay` / `--verify`
排查（回放响应带 `status` 与 `verify_error`）。

## 排队操作与隔离的并发安全

命令与超时任务在取锁**之前**就持有 `live` 对象引用。为防止“排队等锁期间
对局被隔离/结束，取锁后仍继续结算并覆盖隔离状态”，有三层防护：

1. **锁内闸门** `_check_live_gate`：取锁后首先验证 `live` 仍是注册表中的
   对象、DB `status='running'`、内存无赢家；不满足直接返回 `corrupt`/`ended`，
   不 decide、不 fold、不写库（发现 DB 已 corrupt 时顺带摘除内存对象）。
2. **DB 事务守卫**：`apply_command` 在同一事务内、写任何行之前先查
   `status`，非 running 即抛 `MatchNotRunning` 并整体回滚——即使绕过内存闸门
   （跨连接/进程竞争），命令、事件、检查点也一个写不进去。
3. **截止时间条件更新**：`set_deadline` 与普通命令的截止刷新都带
   `AND status='running'`，不会把 corrupt/finished 对局复活。

结算核心拆为 `_apply_under_lock`（玩家命令，自管锁）与 `_apply_locked`
（超时任务已持锁时调用，避免重入死锁）；广播邮件在锁内构造、锁外投递。

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
