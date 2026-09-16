"""测试夹具: 真实 WebSocket 服务端 + JSON 消息辅助。"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile

import pytest
import websockets

from server.server import GameServer


VALID_DECK = [
    "militia", "militia", "militia",
    "infantry", "infantry", "infantry",
    "vanguard", "vanguard", "vanguard",
    "guardian", "fireball", "reinforce",
    "repair", "counter", "banner",
]

# 含两张毒雾, 便于持续效果测试
POISON_DECK = [
    "militia", "militia", "militia",
    "infantry", "infantry",
    "vanguard", "guardian",
    "fireball", "reinforce", "repair",
    "counter", "counter", "banner",
    "poison", "poison",
]


@pytest.fixture
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    # mkstemp 会创建空文件, 删除以保证数据库全新初始化
    os.unlink(path)
    yield path
    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(path + suffix)
        except OSError:
            pass


@pytest.fixture
def fast_timing():
    return {"turn_seconds": 1.5, "response_seconds": 0.5, "grace": 1.0}


@pytest.fixture
async def server(db_path, fast_timing):
    gs = GameServer(db_path, **fast_timing)
    gs.forced_match_seed = 12345
    await gs.start()
    async with websockets.serve(gs.handle, "127.0.0.1", 0) as ws:
        port = ws.sockets[0].getsockname()[1]
        yield gs, port
    await gs.shutdown()


@pytest.fixture
def restart_server(db_path, fast_timing):
    async def _start():
        gs = GameServer(db_path, **fast_timing)
        await gs.start()
        ws = await websockets.serve(gs.handle, "127.0.0.1", 0)
        port = ws.sockets[0].getsockname()[1]
        return gs, ws, port

    return _start


async def ws_connect(port):
    return await websockets.connect(f"ws://127.0.0.1:{port}")


async def send(ws, **kwargs):
    await ws.send(json.dumps(kwargs))


async def recv(ws, timeout: float = 3.0):
    async def _get():
        raw = await ws.recv()
        return json.loads(raw)
    return await asyncio.wait_for(_get(), timeout=timeout)


async def recv_until(ws, mtype: str, timeout: float = 3.0, **fields):
    """读取消息直到出现指定类型; 中途消息忽略, 超时失败。"""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        msg = await recv(ws, timeout=max(0.05, remaining))
        if msg.get("type") == mtype:
            assert all(msg.get(k) == v for k, v in fields.items()), msg
            return msg
        if remaining <= 0:
            raise AssertionError(f"等待 {mtype} 超时, 最后消息: {msg}")


async def login_pair(server_fixture, name0="alice", name1="bob",
                     deck0=None, deck1=None):
    _, port = server_fixture
    deck0 = deck0 or VALID_DECK
    deck1 = deck1 or VALID_DECK
    w0 = await ws_connect(port)
    w1 = await ws_connect(port)
    await send(w0, type="login", name=name0)
    await send(w1, type="login", name=name1)
    await recv_until(w0, "login_ok")
    await recv_until(w1, "login_ok")
    await send(w0, type="queue", deck=deck0)
    await send(w1, type="queue", deck=deck1)
    m0 = await recv_until(w0, "match_begin")
    m1 = await recv_until(w1, "match_begin")
    return (w0, m0), (w1, m1)
