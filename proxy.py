"""
Alpaca WebSocket Proxy — Core
Exposes a local WebSocket server that mirrors Alpaca's v2 market data protocol,
aggregates subscriptions, and maintains a single upstream Alpaca connection.
"""

import asyncio
import json
import logging
import uuid
from typing import Callable

import websockets
from websockets.exceptions import ConnectionClosed

log = logging.getLogger(__name__)

ALPACA_WS_URL_TEMPLATE = "wss://stream.data.alpaca.markets/v2/{feed}"


# ---------------------------------------------------------------------------
# ClientManager
# ---------------------------------------------------------------------------

class ClientManager:
    """Pure sync data structure. Tracks client_id → (ws, set[symbol])."""

    def __init__(self):
        self._clients: dict[str, tuple] = {}  # id → (ws, set[str])

    def add_client(self, client_id: str, ws) -> None:
        self._clients[client_id] = (ws, set())

    def remove_client(self, client_id: str) -> None:
        self._clients.pop(client_id, None)

    def update_subscriptions(self, client_id: str, symbols: set) -> None:
        if client_id in self._clients:
            ws, _ = self._clients[client_id]
            self._clients[client_id] = (ws, set(symbols))

    def get_all_symbols(self) -> set:
        result = set()
        for _, (_, symbols) in self._clients.items():
            result |= symbols
        return result

    def get_clients_for_symbol(self, symbol: str) -> list:
        result = []
        for _, (ws, symbols) in self._clients.items():
            if symbol in symbols:
                result.append(ws)
        return result

    def get_subscriptions(self, client_id: str) -> set:
        if client_id in self._clients:
            return set(self._clients[client_id][1])
        return set()


# ---------------------------------------------------------------------------
# AlpacaClient
# ---------------------------------------------------------------------------

class AlpacaClient:
    """Manages the single upstream Alpaca WebSocket connection."""

    def __init__(self, api_key: str, secret_key: str, feed: str, on_bar: Callable):
        self._api_key = api_key
        self._secret_key = secret_key
        self._feed = feed
        self._on_bar = on_bar
        self._ws = None
        self._receive_task: asyncio.Task | None = None
        self._symbols: set = set()

    @property
    def symbols(self) -> set:
        return set(self._symbols)

    async def connect_and_subscribe(self, symbols: set) -> None:
        url = ALPACA_WS_URL_TEMPLATE.format(feed=self._feed)
        log.info("Connecting to Alpaca: %s", url)
        self._ws = await websockets.connect(url)
        # Read the initial connected message
        await self._ws.recv()
        await self._auth(self._ws)
        await self._subscribe(self._ws, symbols)
        self._symbols = set(symbols)
        self._receive_task = asyncio.create_task(self._receive_loop(self._ws))
        log.info("Alpaca connection established; subscribed to %s", symbols)

    async def reconnect(self, symbols: set) -> None:
        log.info("Reconnecting Alpaca with symbols: %s", symbols)
        await self.disconnect()
        await self.connect_and_subscribe(symbols)

    async def disconnect(self) -> None:
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
                        await self._on_bar(symbol, json.dumps(item))
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
    """Orchestrates ClientManager, AlpacaClient, and the local WebSocket server."""

    def __init__(self, api_key: str, secret_key: str, feed: str, port: int):
        self._api_key = api_key
        self._secret_key = secret_key
        self._feed = feed
        self._port = port
        self._manager = ClientManager()
        self._alpaca: AlpacaClient | None = None
        self._reconnect_lock = asyncio.Lock()

    async def start(self) -> None:
        self._alpaca = AlpacaClient(
            self._api_key, self._secret_key, self._feed, self._on_bar
        )
        watchdog_task = asyncio.create_task(self._alpaca_watchdog())
        log.info("Proxy server listening on ws://localhost:%d", self._port)
        async with websockets.serve(self._client_handler, "localhost", self._port):
            await watchdog_task  # runs forever

    async def _client_handler(self, ws) -> None:
        client_id = str(uuid.uuid4())
        remote = ws.remote_address
        log.info("Client connected: %s from %s", client_id, remote)
        self._manager.add_client(client_id, ws)
        try:
            await ws.send(json.dumps([{"T": "success", "msg": "connected"}]))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    log.warning("Malformed JSON from client %s: %r", client_id, raw)
                    await self._send_error(ws, 400, "malformed JSON")
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
            self._manager.remove_client(client_id)
            log.info("Client removed: %s", client_id)
            asyncio.create_task(self._maybe_reconnect())

    async def _handle_auth(self, ws, client_id: str, msg: dict) -> None:
        log.debug("Auth from client %s (accepting any credentials)", client_id)
        await ws.send(json.dumps([{"T": "success", "msg": "authenticated"}]))

    async def _handle_subscribe(self, ws, client_id: str, msg: dict) -> None:
        new_symbols = set(msg.get("bars", []))
        existing = self._manager.get_subscriptions(client_id)
        merged = existing | new_symbols
        self._manager.update_subscriptions(client_id, merged)
        log.info("Client %s subscribed to bars: %s (total: %s)", client_id, new_symbols, merged)
        await ws.send(json.dumps([{
            "T": "subscription",
            "trades": [],
            "quotes": [],
            "bars": sorted(merged),
        }]))
        asyncio.create_task(self._maybe_reconnect())

    async def _on_bar(self, symbol: str, raw_json: str) -> None:
        log.debug("Routing bar for symbol: %s", symbol)
        await self._route_bar(symbol, raw_json)

    async def _route_bar(self, symbol: str, raw_json: str) -> None:
        clients = self._manager.get_clients_for_symbol(symbol)
        if not clients:
            return
        payload = f"[{raw_json}]"

        async def send_to(ws):
            try:
                await ws.send(payload)
                log.debug("Bar routed to client for symbol %s", symbol)
            except ConnectionClosed:
                log.debug("Client gone while routing bar for %s", symbol)

        await asyncio.gather(*(send_to(ws) for ws in clients))

    async def _maybe_reconnect(self) -> None:
        async with self._reconnect_lock:
            new_symbols = self._manager.get_all_symbols()
            if new_symbols == self._alpaca.symbols:
                return
            if not new_symbols:
                log.info("No active subscriptions; disconnecting Alpaca")
                await self._alpaca.disconnect()
            else:
                await self._alpaca.reconnect(new_symbols)

    async def _alpaca_watchdog(self) -> None:
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
        try:
            await ws.send(json.dumps([{"T": "error", "code": code, "msg": msg}]))
        except ConnectionClosed:
            pass
