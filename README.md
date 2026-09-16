# 据点卡牌竞技场（Stronghold Card Arena）

Godot 4 客户端 + **无界面权威服务端**（Python / asyncio / WebSocket / SQLite）的多人回合制卡牌竞技场。
玩家自行组牌、匹配后争夺 3 个据点；卡牌覆盖**消耗（能量）**、**反制（堆叠响应）**和
**持续效果（光环，每小回合 tick）**。服务端统一决定触发顺序、持有全部隐藏信息；
客户端只渲染服务端过滤后的状态。

## 为什么可以信任这个服务端

- **权威结算，零客户端逻辑**：规则只存在于 `server/engine`。客户端发的是意图
  （`PLAY/COUNTER/PASS/END_TURN`），服务端校验后产出事件。
- **事件溯源 + 确定性回放**：每条状态变化都是 JSON 事件，写入 SQLite。
  回放 = 空状态按序 `fold(events)`，与线上走同一份代码；洗牌使用显式种子的
  确定性 RNG（SplitMix64 + Fisher-Yates），同种子同事件必然得到同哈希状态。
- **隐藏信息不出进程**：`public_event/public_state` 按座位过滤——对手抽牌事件
  只有 `{seat,count}`，洗牌事件完全不下发，对手手牌/牌库只有数量。
- **重复操作不重复扣费**：命令携带客户端生成的 `op_id`，`(match_id, op_id)`
  唯一约束。出牌事件与扣费在同一个 SQLite 事务；重复 op_id 直接回首次结果，
  不二次结算。
- **掉线可恢复**：令牌持久化；重连登录后自动 `match_resume` → 补发缺失事件 →
  下发权威 snapshot。断线在每个回合/响应窗给予一次宽限，宽限落库，重启后延续。
- **超时由服务端驱动**：客户端无法靠不响应卡住对局。响应窗超时=强制通过，
  回合超时=强制结束；系统超时同样以事件落库，可回放。
- **服务重启恢复**：启动时从 SQLite 重建所有 `running` 对局的内存状态，
  校验状态哈希一致，按落库的截止时刻重新调度定时器。

## 目录结构

```
server/
  engine/            纯结算内核(无 I/O, 可独立单测)
    cards.py         卡牌目录/费用/组牌与时长常量
    deck.py          组牌校验(15 张, 同名 ≤3)
    rng.py           确定性洗牌
    engine.py        decide(): 校验命令 -> 事件(触发顺序)
    fold.py          fold(): 事件 -> 状态(扣费/部署/伤害/死亡/光环)
    state.py         状态结构/规范化哈希/按座位视图(隐藏信息)
    replay.py        事件流重建 + 哈希校验
  storage.py         SQLite: matches/commands/events/idem_results(事务+幂等)
  replay_support.py  从库重建内存对局
  server.py          WebSocket 连接/匹配/广播/超时/重连/重启恢复
  __main__.py        python -m server
client/godot/        Godot 4 工程(主场景 Main.tscn, 纯渲染+转发)
tests/               12 项引擎单测 + 11 项真实 WebSocket 集成测试
scripts/             启动/测试/回放 CLI
docs/PROTOCOL.md     消息与事件协议
```

## 规则速览

- 牌组 15 张、同名最多 3 张；初始手牌 4 张，每小回合开始抽 1 张。
- 能量在每个**整轮**（双方各一个小回合）增长 1 点（上限 10）。
- 单位/光环打出后进入堆叠，对手优先获得响应窗：可打出「反制」使牌无效；
  双方通过后自顶向下结算。反制也可被反制（结算时按堆叠顺序递归处理）。
- 每个小回合结束：光环 tick（战旗加攻、毒雾造成伤害）→ 死亡清理 →
  按该据点总战力决定归属。一轮结束时占 ≥2 据点即胜；满 20 小回合按据点数判平/胜。

## 运行

```bash
pip install -r requirements.txt
python -m server --port 8765 --db arena.db          # 启动服务端
bash scripts/run_tests.sh                            # 全量测试

# 确定性回放某局(旁观视角; --viewer alice 给本人视角)
python -m scripts.replay --db arena.db --match <match_id> [--viewer alice]
```

Godot 客户端：用 Godot 4.2+ 打开 `client/godot/project.godot` 运行；
大厅填名字（token 会存在 `user://tokens.cfg`）、组满 15 张牌后点“登录并匹配”。

## 协议

见 `docs/PROTOCOL.md`。关键点：所有命令带 `op_id`；服务端推送
`event`（按座位过滤）与 `snapshot`（权威渲染状态）；`command_result.duplicate=true`
表示重复操作被幂等拦截。

## 测试覆盖

`tests/test_engine.py`（确定性/洗牌回放哈希、费用、反制链、火球击杀、
毒雾持续效果与据点易主、隐藏视图、投降）；
`tests/test_server.py` 跑**真实 WebSocket**：匹配与私有事件、手牌绝不泄露、
同 op_id 不二次扣费、非法命令零扣费、响应窗超时、回合超时、断线宽限、
令牌重连恢复、**杀进程后用同一 DB 文件重启并校验哈希一致后继续对局**、
参与者/旁观者两种回放视角。

## 安全边界（已知简化）

登录是“名字 + 自保存 token”的极简方案（无密码），适合作本地/局域网演示；
生产化应换成带签名的会话凭证。匹配为先来先配的双人队列。
