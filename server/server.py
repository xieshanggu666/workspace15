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
from .engine.state import public_event, public_state, state_hash
from .replay_support import load_match_state

log = logging.getLogger("cardarena")


@dataclass
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
            loaded = load_match_state(self.conn, mid)
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
        self.matches[mid] = live
        for seat, (name, c) in enumerate(((name0, c0), (name1, c1))):
            if c is not None:
                self._attach(live, seat, c)
        # 先通知座位, 再补发过滤后的事件, 最后给权威快照(客户端据此渲染)
        for seat, c in live.clients.items():
            await self._send(c.ws, {"type": "match_begin", "match_id": mid,
                                    "seat": seat})
        for seat, c in live.clients.items():
            await self._catch_up(live, seat, c, since=0)
            await self._send_snapshot(live, seat, c)
        self._arm_timer(mid)
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
                await self._catch_up(live, seat, client,
                                     since=int(msg.get("last_seq", 0)))
                await self._send_snapshot(live, seat, client)
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

        async with live.lock:
            cached = storage.cached_result(self.conn, live.match_id, op_id)
            if cached is not None:
                # 重复操作: 回首次结果, 不再结算、不扣费
                await self._send(client.ws, {
                    "type": "command_result", "op_id": op_id,
                    "duplicate": True, "accepted": True,
                    "state_hash": cached.get("state_hash"),
                })
                return

            try:
                new_events = decide(live.state, command)
            except RuleError as e:
                # 拒绝: 不写命令、不扣资源; 记一条幂等失败也没必要
                await self._send(client.ws, {
                    "type": "command_result", "op_id": op_id,
                    "accepted": False, "code": e.code, "message": str(e),
                })
                return

            finished = None
            for e in new_events:
                fold(live.state, e)
                if e["type"] == "GAME_ENDED":
                    finished = (e["winner"], e["reason"])

            deadline = None if finished else self._compute_deadline(live)
            result = {"accepted": True,
                      "state_hash": state_hash(live.state)}
            stored = storage.apply_command(
                self.conn, live.match_id, op_id, seat, command,
                new_events, result, finished, deadline,
            )
            live.deadline_at = deadline
            self._arm_timer(live.match_id)

        # 广播在锁外即可; 每条事件按座位过滤
        await self._broadcast_events(live)
        if finished is not None:
            await self._announce_end(live, finished)

        await self._send(client.ws, {
            "type": "command_result", "op_id": op_id,
            "accepted": True, "state_hash": result["state_hash"],
        })

    def _compute_deadline(self, live: LiveMatch) -> float:
        now = time.time()
        if live.state["phase"] == "response":
            return now + self.response_seconds
        return now + self.turn_seconds

    # ------------------------------------------------------------- 广播/补发

    async def _broadcast_events(self, live: LiveMatch) -> None:
        for seat, c in list(live.clients.items()):
            await self._catch_up(live, seat, c, since=c.seq)
            await self._send_snapshot(live, seat, c)

    async def _send_snapshot(self, live: LiveMatch, seat: int,
                             c: Client) -> None:
        await self._send(c.ws, {
            "type": "snapshot",
            "state": public_state(live.state, seat),
            "deadline_at": live.deadline_at,
        })

    async def _catch_up(self, live: LiveMatch, seat: int, c: Client,
                        since: int) -> None:
        rows = storage.events_after(self.conn, live.match_id, since)
        for row in rows:
            visible = public_event(row["event"], seat)
            if visible is not None:
                await self._send(c.ws, {"type": "event",
                                        "seq": row["seq"], "event": visible})
            c.seq = row["seq"]

    async def _announce_end(self, live: LiveMatch,
                            finished: tuple[int | None, str]) -> None:
        winner, reason = finished
        for seat, c in live.clients.items():
            await self._send(c.ws, {"type": "match_end",
                                    "match_id": live.match_id,
                                    "winner": winner, "reason": reason,
                                    "state_hash": state_hash(live.state)})

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
            self._arm_timer(mid)
        seat = live.seats.index(c.name) if c.name in live.seats else None
        if seat is None:
            return
        self._attach(live, seat, c)
        await self._send(c.ws, {"type": "match_resume", "match_id": mid,
                                "seat": seat})
        await self._catch_up(live, seat, c, since=0)
        await self._send(c.ws, {"type": "snapshot",
                                "state": public_state(live.state, seat),
                                "deadline_at": live.deadline_at})

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
        # 一次性断线宽限: 每个小回合/响应窗只给一次, 延长截止时间并落库
        window = (live.state["turn"], live.state["phase"],
                  live.state["responder"])
        window_key = hash(window)
        if live.grace_used_window != window_key and live.deadline_at:
            live.grace_used_window = window_key
            live.deadline_at += self.grace
            storage.set_deadline(self.conn, live.match_id, live.deadline_at)
            self._arm_timer(live.match_id)
            log.info("玩家 %s 断线, 对局 %s 宽限 %.0fs",
                     c.name, live.match_id[:8], self.grace)

    # ------------------------------------------------------------- 超时

    def _arm_timer(self, match_id: str) -> None:
        live = self.matches.get(match_id)
        if live is None or live.deadline_at is None:
            return
        if live.timer_task:
            live.timer_task.cancel()
        delay = max(0.0, live.deadline_at - time.time())
        live.timer_task = asyncio.create_task(self._fire_timer(match_id, delay))

    async def _fire_timer(self, match_id: str, delay: float) -> None:
        await asyncio.sleep(delay)
        live = self.matches.get(match_id)
        if not live or live.state["winner"] is not None:
            return
        # 可能因重连重新 arm 过; 二次确认
        if live.deadline_at and time.time() < live.deadline_at - 0.05:
            return
        async with live.lock:
            if live.state["winner"] is not None:
                return
            active = live.state["active"]
            seat = (live.state["responder"]
                    if live.state["phase"] == "response" else active)
            command = {"cmd": "SYSTEM_TIMEOUT", "seat": seat,
                       "reason": "deadline"}
            try:
                new_events = decide(live.state, command)
            except RuleError:
                return
            finished = None
            for e in new_events:
                fold(live.state, e)
                if e["type"] == "GAME_ENDED":
                    finished = (e["winner"], e["reason"])
            deadline = None if finished else self._compute_deadline(live)
            storage.apply_command(
                self.conn, match_id, None, seat, command,
                new_events, {"accepted": True,
                             "state_hash": state_hash(live.state)},
                finished, deadline,
            )
            live.deadline_at = deadline
        await self._broadcast_events(live)
        if finished is not None:
            await self._announce_end(live, finished)
        else:
            self._arm_timer(match_id)

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
