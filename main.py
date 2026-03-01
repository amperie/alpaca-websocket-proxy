"""
Alpaca WebSocket Proxy — Entry Point
=====================================
Loads configuration from a .env file and/or command-line arguments, then starts
the proxy server.  Command-line arguments take precedence over environment variables.

Usage
-----
    python main.py [options]

    Options are documented in parse_args() below and in the project README.
"""

import argparse
import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

from proxy import ProxyServer


def parse_args() -> argparse.Namespace:
    """
    Define and parse command-line arguments.

    All arguments are optional; omitting one falls back to the corresponding
    environment variable (or its default).  Providing one overrides the env var.
    """
    parser = argparse.ArgumentParser(
        description="Alpaca WebSocket proxy — aggregates subscriptions across multiple clients.",
    )
    parser.add_argument("--alpaca-api-key", metavar="KEY",
                        help="Alpaca API key (overrides ALPACA_API_KEY env var)")
    parser.add_argument("--alpaca-secret-api-key", metavar="SECRET",
                        help="Alpaca secret key (overrides ALPACA_SECRET_API_KEY env var)")
    parser.add_argument("--alpaca-feed", metavar="FEED",
                        help="Data feed: iex (free) or sip (paid) (overrides ALPACA_FEED env var)")
    parser.add_argument("--alpaca-endpoint-url", metavar="URL",
                        help="Override Alpaca WebSocket endpoint URL (overrides ALPACA_ENDPOINT_URL env var)")
    parser.add_argument("--proxy-port", metavar="PORT", type=int,
                        help="Local proxy port (overrides PROXY_PORT env var)")
    parser.add_argument("--log-level", metavar="LEVEL",
                        help="Logging level: DEBUG, INFO, WARNING, ERROR (overrides LOG_LEVEL env var)")
    return parser.parse_args()


def setup_logging(level_name: str) -> None:
    """Configure root logger with a timestamped format at the requested level."""
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def main() -> None:
    """
    Entry point coroutine.

    Priority order for each setting (highest → lowest):
        1. Command-line argument
        2. Environment variable (loaded from .env if present)
        3. Hard-coded default
    """
    load_dotenv()
    args = parse_args()

    # CLI args take precedence over env vars; env vars fall back to defaults.
    api_key      = args.alpaca_api_key          or os.getenv("ALPACA_API_KEY")
    secret_key   = args.alpaca_secret_api_key   or os.getenv("ALPACA_SECRET_API_KEY")
    feed         = args.alpaca_feed             or os.getenv("ALPACA_FEED", "sip")
    endpoint_url = args.alpaca_endpoint_url     or os.getenv("ALPACA_ENDPOINT_URL")
    port         = args.proxy_port              or int(os.getenv("PROXY_PORT", "8765"))
    log_level    = args.log_level               or os.getenv("LOG_LEVEL", "INFO")

    setup_logging(log_level)
    log = logging.getLogger(__name__)

    if not api_key or not secret_key:
        log.error("Alpaca API key and secret are required. Set ALPACA_API_KEY / "
                  "ALPACA_SECRET_API_KEY in .env or pass --alpaca-api-key / "
                  "--alpaca-secret-api-key on the command line.")
        sys.exit(1)

    server = ProxyServer(api_key, secret_key, feed, port, endpoint_url=endpoint_url)
    try:
        await server.start()
    except RuntimeError as exc:
        log.error("Fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
