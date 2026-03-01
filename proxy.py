"""
Alpaca WebSocket Proxy — Core
==============================
Exposes a local WebSocket server that mirrors Alpaca's v2 market data protocol,
aggregates bar subscriptions across multiple local clients, and maintains a single
shared upstream connection to Alpaca.

Architecture
------------
    [Client 1] ──┐
    [Client 2] ──┤  ws://localhost:8765   ┌───────────────┐   wss://stream.data.alpaca.markets
    [Client 3] ──┘ ─────────────────────> │  ProxyServer  │ ──────────────────────────────────>
                                           │               │     AlpacaClient (single conn)
                                           │ ClientManager │ <──────────────────────────────────
                                           └───────────────┘        bar data routed to clients

Wire protocol (client ↔ proxy)
-------------------------------
All frames are binary MessagePack (mirrors what the official Alpaca Python SDK expects).

    Connect     →  server sends: [{"T":"success","msg":"connected"}]
    Auth        ←  client sends: {"action":"auth","key":"...","secret":"..."}
                →  server sends: [{"T":"success","msg":"authenticated"}]
    Subscribe   ←  client sends: {"action":"subscribe","bars":["AAPL","MSFT"]}
                →  server sends: [{"T":"subscription","trades":[],"quotes":[],"bars":["AAPL","MSFT"]}]
    Bar data    →  server sends: [{"T":"b","S":"AAPL","o":...,"h":...,"l":...,"c":...,"v":...}]
    Disconnect  →  client closes socket; proxy removes its subscriptions and reconnects
                   Alpaca with the remaining symbol set.

Upstream protocol (proxy ↔ Alpaca)
------------------------------------
Plain JSON text frames as documented at https://docs.alpaca.markets/reference/market-data-wss
"""

import asyncio
import json
import logging
import uuid
from typing import Callable

import msgpack

import websockets
from websockets.exceptions import ConnectionClosed

log = logging.getLogger(__name__)

# Default upstream endpoint; can be overridden via --alpaca-endpoint-url / ALPACA_ENDPOINT_URL.
ALPACA_WS_URL_TEMPLATE = "wss://stream.data.alpaca.markets/v2/{feed}"


# ---------------------------------------------------------------------------
# ClientManager
# ---------------------------------------------------------------------------

class ClientManager:
    """
    Pure synchronous data structure that tracks connected clients and their subscriptions.

    Each client is identified by a UUID string and stored alongside its WebSocket
    connection and the set of bar symbols it has subscribed to.  No I/O is performed
    here; all async work happens in ProxyServer.
    """

    def __init__(self):
        self._clients: dict[str, tuple] = {}  # client_id → (ws, set[symbol])

    def add_client(self, client_id: str, ws) -> None:
        """Register a newly connected client with an empty subscription set."""
        self._clients[client_id] = (ws, set())

    def remove_client(self, client_id: str) -> None:
        """Remove a client and all its subscriptions (called on disconnect)."""
        self._clients.pop(client_id, None)

    def update_subscriptions(self, client_id: str, symbols: set) -> None:
        """Replace a client's subscription set with the given symbol set."""
        if client_id in self._clients:
            ws, _ = self._clients[client_id]
            self._clients[client_id] = (ws, set(symbols))

    def get_all_symbols(self) -> set:
        """Return the union of all symbols subscribed to across every client."""
        result = set()
        for _, (_, symbols) in self._clients.items():
            result |= symbols
        return result

    def get_clients_for_symbol(self, symbol: str) -> list:
        """Return the WebSocket objects for every client subscribed to *symbol*."""
        return [ws for _, (ws, symbols) in self._clients.items() if symbol in symbols]

    def get_subscriptions(self, client_id: str) -> set:
        """Return the current subscription set for a specific client."""
        if client_id in self._clients:
            return set(self._clients[client_id][1])
        return set()


# ---------------------------------------------------------------------------
# AlpacaClient
# ---------------------------------------------------------------------------

class AlpacaClient:
    """
    Manages the single upstream WebSocket connection to Alpaca's market data stream.

    Lifecycle
    ---------
    connect_and_subscribe(symbols)  — open connection, authenticate, subscribe, start receive loop
    reconnect(symbols)              — tear down existing connection then call connect_and_subscribe
    disconnect()                    — cancel receive loop task and close the WebSocket

    The receive loop runs as a background asyncio Task and calls the *on_bar* callback for
    every bar message received from Alpaca.  If the connection drops unexpectedly the task
    ends silently; the watchdog in ProxyServer will detect this and trigger a reconnect.

    Upstream protocol
    -----------------
    Alpaca speaks plain JSON text frames.  Auth and subscribe handshakes are synchronous
    (send → wait for confirmation message) before the receive loop starts.
    """

    def __init__(self, api_key: str, secret_key: str, feed: str, on_bar: Callable,
                 endpoint_url: str | None = None):
        """
        Parameters
        ----------
        api_key:      Alpaca API key.
        secret_key:   Alpaca secret key.
        feed:         Data feed identifier, e.g. "iex" or "sip".
        on_bar:       Async callback invoked with (symbol: str, bar: dict) for each bar.
        endpoint_url: Optional full WebSocket URL; overrides the default feed-based URL.
        """
        self._api_key = api_key
        self._secret_key = secret_key
        self._feed = feed
        self._on_bar = on_bar
        self._endpoint_url = endpoint_url
        self._ws = None
        self._receive_task: asyncio.Task | None = None
        self._symbols: set = set()

    @property
    def symbols(self) -> set:
        """The set of symbols currently subscribed to on the upstream connection."""
        return set(self._symbols)

    async def connect_and_subscribe(self, symbols: set) -> None:
        """
        Open a new upstream connection, authenticate, and subscribe to *symbols*.
        Starts the background receive loop task on success.
        Raises RuntimeError if authentication or subscription is rejected by Alpaca.
        """
        url = self._endpoint_url or ALPACA_WS_URL_TEMPLATE.format(feed=self._feed)
        log.info("Connecting to Alpaca: %s", url)
        self._ws = await websockets.connect(url)
        # Alpaca sends an initial [{"T":"success","msg":"connected"}] on connect.
        await self._ws.recv()
        await self._auth(self._ws)
        await self._subscribe(self._ws, symbols)
        self._symbols = set(symbols)
        self._receive_task = asyncio.create_task(self._receive_loop(self._ws))
        log.info("Alpaca connection established; subscribed to %s", symbols)

    async def reconnect(self, symbols: set) -> None:
        """Disconnect from Alpaca and immediately reconnect with a new symbol set."""
        log.info("Reconnecting Alpaca with symbols: %s", symbols)
        await self.disconnect()
        await self.connect_and_subscribe(symbols)

    async def disconnect(self) -> None:
        """
        Cancel the receive loop task and close the upstream WebSocket.
        Safe to call when already disconnected.
        """
        if self._receive_task and not self._receive_task.done():
            self._receive_task.cancel()
            try:
                await self._receive_task
            except (asyncio.CancelledError, Exception):
                pass
        self._receive_task = None
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._ws = None
        self._symbols = set()
        log.info("Alpaca connection closed")

    async def _auth(self, ws) -> None:
        """Send auth credentials and wait for Alpaca's confirmation."""
        payload = json.dumps({"action": "auth", "key": self._api_key, "secret": self._secret_key})
        log.debug("Sending auth to Alpaca")
        await ws.send(payload)
        while True:
            raw = await ws.recv()
            msgs = json.loads(raw)
            for msg in msgs:
                if msg.get("T") == "success" and msg.get("msg") == "authenticated":
                    log.debug("Alpaca auth successful")
                    return
                if msg.get("T") == "error":
                    raise RuntimeError(f"Alpaca auth failed: {msg}")

    async def _subscribe(self, ws, symbols: set) -> None:
        """Send a bar subscription request and wait for Alpaca's confirmation."""
        payload = json.dumps({"action": "subscribe", "bars": sorted(symbols)})
        log.debug("Sending subscribe to Alpaca: %s", symbols)
        await ws.send(payload)
        while True:
            raw = await ws.recv()
            msgs = json.loads(raw)
            for msg in msgs:
                if msg.get("T") == "subscription":
                    log.debug("Alpaca subscription confirmed: %s", msg)
                    return
                if msg.get("T") == "error":
                    raise RuntimeError(f"Alpaca subscribe failed: {msg}")

    async def _receive_loop(self, ws) -> None:
        """
        Continuously read messages from Alpaca and invoke the on_bar callback for bars.
        Runs until the connection is closed or the task is cancelled.
        """
        log.debug("Alpaca receive loop started")
        try:
            async for raw in ws:
                try:
                    msgs = json.loads(raw)
                except json.JSONDecodeError:
                    log.warning("Malformed JSON from Alpaca: %r", raw)
                    continue
                for item in msgs:
                    if item.get("T") == "b":
                        symbol = item.get("S", "")
                        log.debug("Bar received from Alpaca: %s", symbol)
                        await self._on_bar(symbol, item)
        except asyncio.CancelledError:
            log.debug("Alpaca receive loop cancelled")
        except ConnectionClosed as exc:
            log.error("Alpaca connection closed unexpectedly: %s", exc)
        except Exception as exc:
            log.error("Alpaca receive loop error: %s", exc)


# ---------------------------------------------------------------------------
# ProxyServer
# ---------------------------------------------------------------------------

class ProxyServer:
    """
    Orchestrates the local WebSocket server, ClientManager, and AlpacaClient.

    Responsibilities
    ----------------
    - Accept local client connections and speak the Alpaca wire protocol (MessagePack).
    - Track each client's bar subscriptions via ClientManager.
    - Maintain one shared AlpacaClient upstream connection.
    - Recompute the aggregate symbol set and reconnect Alpaca whenever subscriptions change
      (new client subscribes, or a client disconnects).
    - Run a watchdog task that detects a dead upstream connection and reconnects.

    Reconnect safety
    ----------------
    _maybe_reconnect acquires a lock before computing the new symbol set, so concurrent
    subscribe/disconnect events collapse into a single reconnect rather than racing.
    """

    def __init__(self, api_key: str, secret_key: str, feed: str, port: int,
                 endpoint_url: str | None = None):
        """
        Parameters
        ----------
        api_key:      Alpaca API key.
        secret_key:   Alpaca secret key.
        feed:         Data feed identifier ("iex" or "sip").
        port:         Local port to listen on.
        endpoint_url: Optional override for the Alpaca WebSocket URL.
        """
        self._api_key = api_key
        self._secret_key = secret_key
        self._feed = feed
        self._port = port
        self._endpoint_url = endpoint_url
        self._manager = ClientManager()
        self._alpaca: AlpacaClient | None = None
        self._reconnect_lock = asyncio.Lock()

    async def start(self) -> None:
        """Create the AlpacaClient, start the watchdog, and begin accepting local connections."""
        self._alpaca = AlpacaClient(
            self._api_key, self._secret_key, self._feed, self._on_bar,
            endpoint_url=self._endpoint_url,
        )
        watchdog_task = asyncio.create_task(self._alpaca_watchdog())
        log.info("Proxy server listening on ws://0.0.0.0:%d", self._port)
        async with websockets.serve(self._client_handler, "0.0.0.0", self._port):
            await watchdog_task  # runs forever

    async def _client_handler(self, ws) -> None:
        """
        Per-client coroutine.  Handles the full lifetime of one local WebSocket connection:
        register → send connected message → dispatch incoming messages → cleanup on exit.
        """
        client_id = str(uuid.uuid4())
        remote = ws.remote_address
        log.info("Client connected: %s from %s", client_id, remote)
        self._manager.add_client(client_id, ws)
        try:
            await ws.send(msgpack.packb([{"T": "success", "msg": "connected"}], use_bin_type=True))
            async for raw in ws:
                try:
                    msg = msgpack.unpackb(raw, raw=False)
                except Exception:
                    log.warning("Malformed msgpack from client %s: %r", client_id, raw)
                    await self._send_error(ws, 400, "malformed message")
                    continue

                action = msg.get("action")
                if action == "auth":
                    await self._handle_auth(ws, client_id, msg)
                elif action == "subscribe":
                    await self._handle_subscribe(ws, client_id, msg)
                else:
                    log.warning("Unknown action from client %s: %r", client_id, action)
                    await self._send_error(ws, 400, "invalid action")
        except ConnectionClosed:
            log.info("Client disconnected: %s", client_id)
        except Exception as exc:
            log.error("Unexpected error in client handler %s: %s", client_id, exc)
        finally:
            # Always remove the client and recalculate the upstream subscription.
            self._manager.remove_client(client_id)
            log.info("Client removed: %s", client_id)
            asyncio.create_task(self._maybe_reconnect())

    async def _handle_auth(self, ws, client_id: str, msg: dict) -> None:
        """
        Accept any credentials — authentication is handled by Alpaca upstream.
        The proxy only validates credentials with Alpaca, not with local clients.
        """
        log.debug("Auth from client %s (accepting any credentials)", client_id)
        await ws.send(msgpack.packb([{"T": "success", "msg": "authenticated"}], use_bin_type=True))

    async def _handle_subscribe(self, ws, client_id: str, msg: dict) -> None:
        """
        Merge newly requested symbols into the client's existing subscription set (additive),
        send a subscription confirmation, then trigger an upstream reconnect if the aggregate
        symbol set has changed.
        """
        new_symbols = set(msg.get("bars", []))
        existing = self._manager.get_subscriptions(client_id)
        merged = existing | new_symbols
        self._manager.update_subscriptions(client_id, merged)
        log.info("Client %s subscribed to bars: %s (total: %s)", client_id, new_symbols, merged)
        await ws.send(msgpack.packb([{
            "T": "subscription",
            "trades": [],
            "quotes": [],
            "bars": sorted(merged),
        }], use_bin_type=True))
        asyncio.create_task(self._maybe_reconnect())

    async def _on_bar(self, symbol: str, bar: dict) -> None:
        """Callback invoked by AlpacaClient for each bar received from Alpaca."""
        log.debug("Routing bar for symbol: %s", symbol)
        await self._route_bar(symbol, bar)

    async def _route_bar(self, symbol: str, bar: dict) -> None:
        """
        Forward a bar to every local client subscribed to *symbol*.
        Sends are issued in parallel via asyncio.gather; a client that has already
        disconnected is silently skipped.
        """
        clients = self._manager.get_clients_for_symbol(symbol)
        if not clients:
            return
        payload = msgpack.packb([bar], use_bin_type=True)

        async def send_to(ws):
            try:
                await ws.send(payload)
                log.debug("Bar routed to client for symbol %s", symbol)
            except ConnectionClosed:
                log.debug("Client gone while routing bar for %s", symbol)

        await asyncio.gather(*(send_to(ws) for ws in clients))

    async def _maybe_reconnect(self) -> None:
        """
        Recalculate the aggregate symbol set and reconnect Alpaca only if it has changed.

        The asyncio.Lock ensures that concurrent calls (e.g. two clients subscribing at
        the same time, or a subscribe and a disconnect racing) collapse into a single
        reconnect using the latest symbol set.
        """
        async with self._reconnect_lock:
            new_symbols = self._manager.get_all_symbols()
            if new_symbols == self._alpaca.symbols:
                return  # Nothing changed; skip reconnect.
            if not new_symbols:
                log.info("No active subscriptions; disconnecting Alpaca")
                await self._alpaca.disconnect()
            else:
                await self._alpaca.reconnect(new_symbols)

    async def _alpaca_watchdog(self) -> None:
        """
        Background task that polls every 30 seconds.
        If there are active subscriptions but the Alpaca receive task is no longer
        running (e.g. the connection was dropped mid-stream), a reconnect is triggered.
        """
        log.debug("Watchdog started (interval=30s)")
        while True:
            await asyncio.sleep(30)
            symbols = self._manager.get_all_symbols()
            task_alive = (
                self._alpaca._receive_task is not None
                and not self._alpaca._receive_task.done()
            )
            if symbols and not task_alive:
                log.warning("Watchdog: Alpaca task dead with active subscriptions; reconnecting")
                asyncio.create_task(self._maybe_reconnect())

    @staticmethod
    async def _send_error(ws, code: int, msg: str) -> None:
        """Send a MessagePack-encoded error frame to a local client."""
        try:
            await ws.send(msgpack.packb([{"T": "error", "code": code, "msg": msg}], use_bin_type=True))
        except ConnectionClosed:
            pass
