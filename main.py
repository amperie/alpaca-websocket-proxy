"""
Alpaca WebSocket Proxy — Entry Point
"""

import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

from proxy import ProxyServer


def setup_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def main() -> None:
    load_dotenv()

    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_API_KEY")
    feed = os.getenv("ALPACA_FEED", "iex")
    port = int(os.getenv("PROXY_PORT", "8765"))
    log_level = os.getenv("LOG_LEVEL", "INFO")

    setup_logging(log_level)
    log = logging.getLogger(__name__)

    if not api_key or not secret_key:
        log.error("ALPACA_API_KEY and ALPACA_SECRET_API_KEY must be set in .env")
        sys.exit(1)

    server = ProxyServer(api_key, secret_key, feed, port)
    try:
        await server.start()
    except RuntimeError as exc:
        log.error("Fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
