"""无界面权威服务端: WebSocket(JSON) + 内存会话 + SQLite 持久化。

职责边界:
- engine 包只做纯结算; 本模块负责时钟、连接、匹配、广播、重连、重启恢复。
- 每条命令: 取对局锁 -> 幂等检查 -> engine.decide -> 事务落库(命令+事件)
  -> fold 内存状态 -> 按座位过滤后广播。重复 op_id 直接回首次结果。
- 超时由每对局单个 asyncio 定时任务驱动; 断线给予一次性宽限并延长截止时间,
  截止时间落库, 重启后按剩余时间重新调度。
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from . import storage
from .engine.cards import DISCONNECT_GRACE, RESPONSE_SECONDS, TURN_SECONDS
from .engine.deck import validate_deck
from .engine.engine import decide, start_match
from .engine.errors import RuleError
from .engine.fold import fold
from .engine.state import clone, public_event, public_state, state_hash
from .replay_support import load_match_state

log = logging.getLogger("cardarena")


@dataclass(eq=False)
class Client:
    name: str
    ws: Any
    seq: int = 0  # 已发送到客户端的最后一条事件 seq
    alive: bool = True


@dataclass
class LiveMatch:
    match_id: str
    seats: list[str]
    state: dict
    clients: dict[int, Client] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    deadline_at: float | None = None
    grace_used_window: int = -1  # 已用过断线宽限的响应窗口/回合序号
    timer_task: asyncio.Task | None = None
    timer_epoch: int = 0  # 每次重新 arm 自增, 旧定时任务据此自我作废

    def client_for(self, name: str) -> Client | None:
        for seat, n in enumerate(self.seats):
            if n == name:
                return self.clients.get(seat)
        return None


class GameServer:
    def __init__(self, db_path: str, turn_seconds: float = TURN_SECONDS,
                 response_seconds: float = RESPONSE_SECONDS,
                 grace: float = DISCONNECT_GRACE):
        self.db_path = db_path
        self.conn = storage.connect(db_path)
        storage.init_db(self.conn)
        self.turn_seconds = turn_seconds
        self.response_seconds = response_seconds
        self.grace = grace
        self.matches: dict[str, LiveMatch] = {}
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self._by_name: dict[str, Client] = {}
        self._pending_decks: dict[str, list[str]] = {}
        self._queued_names: set[str] = set()
        # 测试钩子: 置位后新开对局使用该种子而非随机种子
        self.forced_match_seed: int | None = None
        self.matchmaking: asyncio.Task | None = None
        self._restore_running()

    # ------------------------------------------------------------- 启动/恢复

    async def start(self) -> None:
        self.matchmaking = asyncio.create_task(self._matchmaking_loop())
        for mid in list(self.matches):
            self._arm_timer(mid)

    async def shutdown(self) -> None:
        if self.matchmaking:
            self.matchmaking.cancel()
        for m in self.matches.values():
            if m.timer_task:
                m.timer_task.cancel()
        self.conn.close()

    def _restore_running(self) -> None:
        """重启恢复: 从 SQLite 重建所有 running 对局的内存状态, 不重新匹配。"""
        rows = self.conn.execute(
            "SELECT match_id FROM matches WHERE status='running'"
        ).fetchall()
        for row in rows:
            mid = row["match_id"]
            try:
                loaded = load_match_state(self.conn, mid, verify=True)
            except Exception as exc:  # 事件流损坏必须显式暴露, 不得静默续局
                log.error("对局 %s 恢复校验失败, 跳过: %s", mid[:8], exc)
                continue
            m = LiveMatch(
                match_id=mid,
                seats=loaded["names"],
                state=loaded["state"],
                deadline_at=loaded["deadline_at"],
            )
            self.matches[mid] = m
        if rows:
            log.info("恢复 %d 个进行中的对局", len(rows))

    # ------------------------------------------------------------- 匹配

    async def _matchmaking_loop(self) -> None:
        waiting: str | None = None
        while True:
            try:
                name = await self.queue.get()
            except asyncio.CancelledError:
                return
            if waiting is None:
                waiting = name
                continue
            if waiting == name:
                # 同名重复排队(理论上令牌唯一), 丢弃后到的
                waiting = None
                continue
            await self._begin_match(waiting, name)
            waiting = None

    async def _begin_match(self, name0: str, name1: str) -> None:
        # 牌组在排队消息中提供; 这里从连接注册表取
        c0 = self._by_name.get(name0)
        c1 = self._by_name.get(name1)
        deck0 = self._pending_decks.pop(name0, None)
        deck1 = self._pending_decks.pop(name1, None)
        if not deck0 or not deck1:
            # 缺牌组(例如断线残留), 退回大厅
            for c, name in ((c0, name0), (c1, name1)):
                if c:
                    await self._send(c.ws, {"type": "error",
                                            "code": "NO_DECK"})
            return
        mid = uuid.uuid4().hex
        seed = (self.forced_match_seed
                if self.forced_match_seed is not None
                else uuid.uuid4().int & ((1 << 63) - 1))
        deadline = time.time() + self.turn_seconds
        storage.create_match(self.conn, mid, [name0, name1],
                             [deck0, deck1], seed=seed,
                             deadline_at=deadline)
        m = storage.get_match(self.conn, mid)
        state, events = start_match(mid, m["decks"], m["names"], m["seed"])
        # start_match 返回基底状态; 完整内存状态需把整段事件(含抽牌/首回合) fold 完
        for e in events:
            fold(state, e)
        storage.save_start_events(self.conn, mid, events)

        live = LiveMatch(match_id=mid, seats=[name0, name1], state=state,
                         deadline_at=deadline)
        for seat, (name, c) in enumerate(((name0, c0), (name1, c1))):
            if c is not None:
                self._attach(live, seat, c)
        async with live.lock:
            self.matches[mid] = live
            self._arm_timer_locked(live)
            mail = await self._build_broadcast_mail_locked(live)
            for seat, c in live.clients.items():
                mail[c].insert(0, {"type": "match_begin",
                                   "match_id": mid, "seat": seat})
        await self._deliver_mail(mail)
        log.info("对局开始 %s: %s vs %s", mid[:8], name0, name1)

    # ------------------------------------------------------------- 连接

    async def handle(self, ws) -> None:
        client: Client | None = None
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                try:
                    client = await self._dispatch(ws, msg, client)
                except Exception as exc:  # 单条消息失败不杀连接
                    log.exception("处理消息失败: %s", exc)
                    await self._send(ws, {"type": "error",
                                          "code": "INTERNAL"})
        except ConnectionClosed:
            pass
        finally:
            if client:
                await self._on_disconnect(client, ws)

    async def _dispatch(self, ws, msg: dict, client: Client | None) -> Client:
        t = msg.get("type")
        if t == "login":
            name = str(msg.get("name", ""))[:32]
            token = str(msg.get("token", ""))
            if not name:
                await self._send(ws, {"type": "login_fail",
                                      "code": "BAD_NAME"})
                return client
            if token:
                who = storage.player_by_token(self.conn, token)
                if who != name:
                    await self._send(ws, {"type": "login_fail",
                                          "code": "BAD_TOKEN"})
                    return client
            new_token = storage.login(self.conn, name)
            c = Client(name=name, ws=ws)
            self._by_name[name] = c
            await self._send(ws, {"type": "login_ok", "token": new_token})
            # 自动恢复进行中的对局
            await self._try_resume(c)
            return c

        if client is None:
            await self._send(ws, {"type": "error", "code": "NOT_LOGGED_IN"})
            return client

        if t == "queue":
            deck = msg.get("deck")
            try:
                validate_deck(deck)
            except RuleError as e:
                await self._send(ws, {"type": "error", "code": e.code,
                                      "message": str(e)})
                return client
            self._pending_decks[client.name] = deck
            # 防止重复排队
            if client.name not in self._queued_names:
                self._queued_names.add(client.name)
                await self.queue.put(client.name)
            return client

        if t == "command":
            await self._handle_command(client, msg)
            return client

        if t == "sync":
            live = self._live_for(client.name)
            if live:
                seat = live.seats.index(client.name)
                async with live.lock:
                    rows = storage.events_after(
                        self.conn, live.match_id,
                        int(msg.get("last_seq", client.seq)))
                    messages = []
                    last = client.seq
                    for row in rows:
                        last = row["seq"]
                        visible = public_event(row["event"], seat)
                        if visible is not None:
                            messages.append({"type": "event",
                                             "seq": row["seq"],
                                             "event": visible})
                    snap = {
                        "type": "snapshot",
                        "state": public_state(live.state, seat),
                        "deadline_at": live.deadline_at,
                    }
                    client.seq = last
                for m in messages:
                    await self._send(client.ws, m)
                await self._send(client.ws, snap)
            return client

        if t == "replay":
            await self._handle_replay(client, str(msg.get("match_id", "")))
            return client

        await self._send(ws, {"type": "error", "code": "BAD_TYPE"})
        return client

    # ------------------------------------------------------------- 命令处理

    async def _handle_command(self, client: Client, msg: dict) -> None:
        live = self._live_for(client.name)
        if live is None:
            await self._send(client.ws, {"type": "error",
                                         "code": "NOT_IN_MATCH"})
            return
        seat = live.seats.index(client.name)
        command = msg.get("command") or {}
        op_id = msg.get("op_id")
        if not isinstance(command, dict) or not op_id:
            await self._send(client.ws, {"type": "error",
                                         "code": "BAD_COMMAND"})
            return
        command = dict(command)
        command["seat"] = seat

        outcome = await self._apply_under_lock(live, command, op_id)

        if outcome["status"] == "duplicate":
            # 重复 op_id: 回首次结果; 不结算、不扣费、不追加事件
            await self._send(client.ws, {
                "type": "command_result", "op_id": op_id,
                "duplicate": True, "accepted": True,
                "state_hash": outcome.get("state_hash"),
            })
            return
        if outcome["status"] == "rejected":
            await self._send(client.ws, {
                "type": "command_result", "op_id": op_id,
                "accepted": False, "code": outcome["code"],
                "message": outcome["message"],
            })
            return

        await self._send(client.ws, {
            "type": "command_result", "op_id": op_id,
            "accepted": True, "state_hash": outcome["state_hash"],
        })

    async def _apply_under_lock(self, live: LiveMatch, command: dict,
                                op_id: str | None) -> dict:
        """玩家命令与系统超时唯一的结算入口。

        关键不变量:
        1. 全程持有 live.lock(仅在网络发送时不持有, 见下), 并发命令(含同
           op_id 重发、玩家命令与超时竞争)被严格串行化, 不会重复结算。
        2. 先在状态克隆上 fold 并提交 SQLite, 提交成功后才替换 live.state;
           任何中途异常都不会污染权威内存状态。
        3. 广播载荷在锁内基于一致状态构造并推进 c.seq, 网络发送移到锁外,
           避免发送期间让出锁而与后续命令交错。
        """
        async with live.lock:
            if op_id is not None:
                cached = storage.cached_result(self.conn, live.match_id, op_id)
                if cached is not None:
                    return {"status": "duplicate",
                            "state_hash": cached.get("state_hash")}

            try:
                new_events = decide(live.state, command)
            except RuleError as e:
                return {"status": "rejected", "code": e.code,
                        "message": str(e)}

            # 在克隆上结算; 此刻 live.state 仍未改变
            staged = clone(live.state)
            finished = None
            for e in new_events:
                fold(staged, e)
                if e["type"] == "GAME_ENDED":
                    finished = (e["winner"], e["reason"])

            deadline = None if finished else self._compute_deadline_locked(
                staged)
            state_h = state_hash(staged)
            stored = storage.apply_command(
                self.conn, live.match_id, op_id, command.get("seat"),
                command, new_events, {"accepted": True, "state_hash": state_h},
                finished, deadline,
            )
            if stored.get("duplicate"):
                # 存储层兜底命中(跨进程/竞争窗口): 本次 staged 状态作废,
                # 用 SQLite 事件流权威重建内存, 保证二者一致。
                self._rebuild_live_state(live)
                return {"status": "duplicate",
                        "state_hash": stored.get("state_hash")}

            # 事务提交成功后才发布到权威内存状态
            live.state = staged
            live.deadline_at = deadline
            self._arm_timer_locked(live)

            # 在锁内基于一致状态构造全部待发载荷(并推进 c.seq)
            mail = await self._build_broadcast_mail_locked(live)
            if finished is not None:
                h = state_hash(live.state)
                for c in list(live.clients.values()):
                    mail[c].append({"type": "match_end",
                                    "match_id": live.match_id,
                                    "winner": finished[0],
                                    "reason": finished[1], "state_hash": h})

        # 锁外发送不可变载荷
        for c, messages in mail.items():
            for m in messages:
                await self._send(c.ws, m)
        return {"status": "applied", "state_hash": state_h}

    def _compute_deadline_locked(self, state: dict) -> float:
        now = time.time()
        if state["phase"] == "response":
            return now + self.response_seconds
        return now + self.turn_seconds

    def _rebuild_live_state(self, live: LiveMatch) -> None:
        """用 SQLite 事件流重建内存状态(幂等竞争兜底/重启恢复共用)。"""
        loaded = load_match_state(self.conn, live.match_id)
        live.state = loaded["state"]
        live.deadline_at = loaded["deadline_at"]
        self._arm_timer_locked(live)

    # ------------------------------------------------------------- 广播/补发

    async def _build_broadcast_mail_locked(self, live: LiveMatch) -> dict:
        """在持有 live.lock 时调用。基于当前一致状态为每条连接构造待发载荷
        (缺失事件 + 权威快照)并推进 c.seq; 不做任何网络 I/O。
        返回 {client: [message, ...]}, 调用方在锁外投递。"""
        mail: dict[Client, list[dict]] = {}
        deadline = live.deadline_at
        for seat, c in list(live.clients.items()):
            messages: list[dict] = []
            rows = storage.events_after(self.conn, live.match_id, c.seq)
            last = c.seq
            for row in rows:
                last = row["seq"]
                visible = public_event(row["event"], seat)
                if visible is not None:
                    messages.append({"type": "event", "seq": row["seq"],
                                     "event": visible})
            messages.append({
                "type": "snapshot",
                "state": public_state(live.state, seat),
                "deadline_at": deadline,
            })
            c.seq = last
            mail[c] = messages
        return mail

    async def _deliver_mail(self, mail: dict) -> None:
        for c, messages in mail.items():
            for m in messages:
                await self._send(c.ws, m)

    # ------------------------------------------------------------- 重连

    async def _try_resume(self, c: Client) -> None:
        mid = storage.active_match_for(self.conn, c.name)
        if not mid:
            return
        live = self.matches.get(mid)
        if live is None:  # 极端情况: 内存缺失则重新加载
            loaded = load_match_state(self.conn, mid)
            live = LiveMatch(mid, loaded["names"], loaded["state"],
                             deadline_at=loaded["deadline_at"])
            self.matches[mid] = live
            async with live.lock:
                self._arm_timer_locked(live)
        seat = live.seats.index(c.name) if c.name in live.seats else None
        if seat is None:
            return
        # 座位占用与事件补发必须在对局锁内完成: 不能让一条并发命令在
        # "已 attach、未补发"之间推进 c.seq 造成事件乱序。
        async with live.lock:
            self._attach(live, seat, c)
            messages = [{"type": "match_resume", "match_id": mid,
                         "seat": seat}]
            rows = storage.events_after(self.conn, mid, 0)
            last = 0
            for row in rows:
                last = row["seq"]
                visible = public_event(row["event"], seat)
                if visible is not None:
                    messages.append({"type": "event", "seq": row["seq"],
                                     "event": visible})
            messages.append({
                "type": "snapshot",
                "state": public_state(live.state, seat),
                "deadline_at": live.deadline_at,
            })
            c.seq = last
        for m in messages:
            await self._send(c.ws, m)

    def _attach(self, live: LiveMatch, seat: int, c: Client) -> None:
        live.clients[seat] = c
        c.alive = True

    async def _on_disconnect(self, c: Client, ws) -> None:
        c.alive = False
        # 只有当前连接才从注册表移除(同名快速重连可能已替换)
        if self._by_name.get(c.name) is c:
            self._by_name.pop(c.name, None)
        self._queued_names.discard(c.name)
        live = self._live_for(c.name)
        if live:
            for seat, name in enumerate(live.seats):
                if name == c.name and live.clients.get(seat) is c:
                    # 快速重连时该座位可能已被新连接占据, 绝不能误弹
                    live.clients.pop(seat, None)
        else:
            # 对局已结束也要判定是否存在已结束的 LiveMatch
            for m in self.matches.values():
                if c.name in m.seats:
                    seat = m.seats.index(c.name)
                    if m.clients.get(seat) is c:
                        m.clients.pop(seat, None)
                    live = m
                    break
        if not live or live.state["winner"] is not None:
            return
        # 一次性断线宽限: 宽限判定与定时器重排必须在对局锁内, 否则可能与
        # 正在排队的超时任务交错, 导致 deadline 被旧值覆盖。
        async with live.lock:
            window = (live.state["turn"], live.state["phase"],
                      live.state["responder"])
            window_key = hash(window)
            if live.grace_used_window != window_key and live.deadline_at:
                live.grace_used_window = window_key
                live.deadline_at += self.grace
                storage.set_deadline(self.conn, live.match_id,
                                     live.deadline_at)
                self._arm_timer_locked(live)
                log.info("玩家 %s 断线, 对局 %s 宽限 %.0fs",
                         c.name, live.match_id[:8], self.grace)

    # ------------------------------------------------------------- 超时

    def _arm_timer(self, match_id: str) -> None:
        """仅用于启动恢复(尚无连接/命令竞争); 运行期一律用 _arm_timer_locked。"""
        live = self.matches.get(match_id)
        if live is None:
            return
        live.timer_epoch += 1
        self._schedule(live)

    def _arm_timer_locked(self, live: LiveMatch) -> None:
        """必须在持有 live.lock 时调用: 自增代数让任何旧超时任务作废。"""
        live.timer_epoch += 1
        self._schedule(live)

    def _schedule(self, live: LiveMatch) -> None:
        if live.timer_task:
            live.timer_task.cancel()
            live.timer_task = None
        if live.deadline_at is None:
            return
        epoch = live.timer_epoch
        delay = max(0.0, live.deadline_at - time.time())
        live.timer_task = asyncio.create_task(
            self._fire_timer(live.match_id, epoch, delay))

    async def _fire_timer(self, match_id: str, epoch: int,
                          delay: float) -> None:
        await asyncio.sleep(delay)
        live = self.matches.get(match_id)
        if not live or live.state["winner"] is not None:
            return
        async with live.lock:
            # 三重作废检查: 已被更新的定时器取代 / 截止时间被宽限或命令延后 /
            # 对局已结束。任何一条成立都不得结算, 杜绝陈旧超时重复落事件。
            if (live.timer_epoch != epoch
                    or live.timer_task is not asyncio.current_task()
                    or live.state["winner"] is not None):
                return
            if live.deadline_at and time.time() < live.deadline_at - 0.01:
                return
            seat = (live.state["responder"]
                    if live.state["phase"] == "response"
                    else live.state["active"])
            command = {"cmd": "SYSTEM_TIMEOUT", "seat": seat,
                       "reason": "deadline"}
            try:
                new_events = decide(live.state, command)
            except RuleError:
                return
            staged = clone(live.state)
            finished = None
            for e in new_events:
                fold(staged, e)
                if e["type"] == "GAME_ENDED":
                    finished = (e["winner"], e["reason"])
            deadline = None if finished else self._compute_deadline_locked(
                staged)
            h = state_hash(staged)
            storage.apply_command(
                self.conn, match_id, None, seat, command,
                new_events, {"accepted": True, "state_hash": h},
                finished, deadline,
            )
            live.state = staged
            live.deadline_at = deadline
            self._arm_timer_locked(live)
            mail = await self._build_broadcast_mail_locked(live)
            if finished is not None:
                for c in list(live.clients.values()):
                    mail.setdefault(c, []).append({
                        "type": "match_end", "match_id": match_id,
                        "winner": finished[0], "reason": finished[1],
                        "state_hash": h})
        await self._deliver_mail(mail)

    # ------------------------------------------------------------- 回放

    async def _handle_replay(self, c: Client, match_id: str) -> None:
        m = storage.get_match(self.conn, match_id)
        if not m:
            await self._send(c.ws, {"type": "error", "code": "NO_MATCH"})
            return
        is_player = c.name in m["names"]
        events = storage.all_events(self.conn, match_id)
        out = []
        for row in events:
            viewer = m["names"].index(c.name) if is_player else None
            visible = public_event(row["event"], viewer)
            if visible is not None:
                out.append({"seq": row["seq"], "event": visible})
        await self._send(c.ws, {"type": "replay", "match_id": match_id,
                                "events": out,
                                "full": is_player})

    # ------------------------------------------------------------- 辅助

    async def _send(self, ws, payload: dict) -> None:
        try:
            await ws.send(json.dumps(payload, ensure_ascii=False))
        except ConnectionClosed:
            pass

    def _live_for(self, name: str) -> LiveMatch | None:
        for m in self.matches.values():
            if name in m.seats and m.state["winner"] is None:
                return m
        return None


async def serve(db_path: str = "arena.db", host: str = "0.0.0.0",
                port: int = 8765, **timing) -> tuple:
    server = GameServer(db_path, **timing)
    await server.start()
    ws_server = await websockets.serve(server.handle, host, port)
    actual_port = ws_server.sockets[0].getsockname()[1]
    return server, ws_server, actual_port
