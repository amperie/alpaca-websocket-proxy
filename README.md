# Alpaca WebSocket Proxy

A lightweight Python async proxy that sits between multiple trading algorithm clients and Alpaca's v2 market data WebSocket API.

Instead of every algorithm opening its own connection to Alpaca, they all connect to this local proxy. The proxy aggregates their subscriptions into a single upstream Alpaca connection and routes incoming bar data back to the right clients.

## Why use this?

- **One Alpaca connection, many clients** — Alpaca enforces connection limits; the proxy lets multiple local processes share a single connection.
- **Drop-in compatible** — the proxy speaks the same MessagePack wire protocol as Alpaca's own endpoint, so existing code using the Alpaca Python SDK needs no changes beyond pointing at `ws://localhost:8765`.
- **Automatic reconnect** — if the upstream Alpaca connection drops, the proxy reconnects within 30 seconds without any action from clients.

## Architecture

```
[Client 1] ──┐
[Client 2] ──┤  ws://0.0.0.0:8765     ┌───────────────┐   wss://stream.data.alpaca.markets
[Client 3] ──┘ ─────────────────────> │  ProxyServer  │ ──────────────────────────────────>
                                      │ AlpacaClient (single conn)
                                      │ ClientManager │ <──────────────────────────────────
                                      └───────────────┘        bar data routed to clients
```

- Subscriptions are **additive** — each client sends its own symbols and the proxy subscribes Alpaca to the union.
- When a client disconnects, the proxy reconnects Alpaca without that client's symbols.
- A watchdog task polls every 30 seconds and reconnects Alpaca if the upstream connection has silently died.

## Requirements

- Python 3.11+
- An [Alpaca](https://alpaca.markets) account with API credentials

## Installation

```bash
# Clone the repo
git clone https://github.com/amperie/alpaca-websocket-proxy.git
cd alpaca-websocket-proxy

# Create a virtual environment and install dependencies
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv sync
```

## Configuration

Configuration can be provided via a `.env` file, environment variables, or command-line arguments. **Command-line arguments override environment variables.**

### `.env` file

Copy the example and fill in your credentials:

```bash
cp .env.example .env
```

| Variable | Required | Default | Description |
|---|---|---|---|
| `ALPACA_API_KEY` | Yes | — | Your Alpaca API key |
| `ALPACA_SECRET_API_KEY` | Yes | — | Your Alpaca secret key |
| `ALPACA_FEED` | No | `sip` | `iex` (free tier) or `sip` (paid tier) |
| `ALPACA_ENDPOINT_URL` | No | — | Override the Alpaca WebSocket URL |
| `PROXY_PORT` | No | `8765` | Port the local proxy listens on |
| `LOG_LEVEL` | No | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |

### Command-line arguments

All settings can also be passed on the command line:

```
usage: main.py [-h] [--alpaca-api-key KEY] [--alpaca-secret-api-key SECRET]
               [--alpaca-feed FEED] [--alpaca-endpoint-url URL]
               [--proxy-port PORT] [--log-level LEVEL]
```

## Running

```bash
# Using .env for credentials
python main.py

# Passing credentials on the command line
python main.py --alpaca-api-key YOUR_KEY --alpaca-secret-api-key YOUR_SECRET

# Full example with all options
python main.py \
  --alpaca-api-key YOUR_KEY \
  --alpaca-secret-api-key YOUR_SECRET \
  --alpaca-feed sip \
  --proxy-port 8765 \
  --log-level DEBUG
```

The proxy is ready when you see:
```
2026-01-01 12:00:00 [INFO] proxy: Proxy server listening on ws://0.0.0.0:8765
```

## Connecting a client

Point your Alpaca SDK at the local proxy instead of Alpaca's servers:

```python
from alpaca.data.live import StockDataStream

stream = StockDataStream(api_key, secret_key, websocket_params={"url": "ws://localhost:8765"})

async def on_bar(bar):
    print(bar)

stream.subscribe_bars(on_bar, "AAPL", "MSFT")
stream.run()
```

The proxy accepts any credentials on the auth handshake — your real Alpaca credentials only need to be configured in the proxy itself.

## Logging

| Level | Events |
|---|---|
| `INFO` | Client connect/disconnect, subscription changes, Alpaca reconnects |
| `DEBUG` | Each bar received, each bar routed to each client, auth messages |
| `WARNING` | Malformed client messages, unknown actions, watchdog reconnects |
| `ERROR` | Alpaca auth failures, unexpected connection errors |

## Project structure

```
proxy.py          # Core: ClientManager, AlpacaClient, ProxyServer
main.py           # Entry point: argument parsing, logging setup, asyncio.run
requirements.txt  # Dependencies
.env.example      # Environment variable reference
```
