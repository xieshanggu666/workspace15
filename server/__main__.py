"""服务端入口: python -m server --host 0.0.0.0 --port 8765 --db arena.db"""

from __future__ import annotations

import argparse
import asyncio
import logging

from .server import serve


async def amain() -> None:
    p = argparse.ArgumentParser(description="据点卡牌竞技场权威服务端")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--db", default="arena.db")
    p.add_argument("--turn-seconds", type=float, default=30.0)
    p.add_argument("--response-seconds", type=float, default=15.0)
    p.add_argument("--grace", type=float, default=15.0)
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    server, ws_server, port = await serve(
        args.db, args.host, args.port,
        turn_seconds=args.turn_seconds,
        response_seconds=args.response_seconds,
        grace=args.grace,
    )
    logging.info("卡牌竞技场服务端监听 ws://%s:%s (db=%s)",
                 args.host, port, args.db)
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        ws_server.close()
        await ws_server.wait_closed()
        await server.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass
