from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
import uuid
from typing import Any, Awaitable, Callable, Optional

import websockets
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    ProtocolError,
)
from websockets.server import WebSocketServerProtocol

logger = logging.getLogger(__name__)


def _ptype(p: dict | None) -> str:
    try:
        return (p or {}).get("type") or "(none)"
    except Exception:
        return "(?)"


def _json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"))
    except Exception as e:
        return f'{{"ok":false,"error":"json-dumps-failed:{e!r}"}}'


def _bytes_len(s: str | bytes) -> int:
    if isinstance(s, bytes):
        return len(s)
    try:
        return len(s.encode("utf-8"))
    except Exception:
        return len(s)


class WebsocketManager:
    """Outbound `send`/`request`, inbound `start_server`. Adapted from Copycord."""

    def __init__(
        self,
        send_url: Optional[str] = None,
        listen_host: Optional[str] = None,
        listen_port: Optional[int] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.send_url = send_url
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.logger = logger or logging.getLogger("WebsocketManager")
        self._shutting_down = False

    def begin_shutdown(self) -> None:
        self._shutting_down = True

    async def stop(self) -> None:
        self.begin_shutdown()

    async def start_server(
        self,
        handler: Callable[[dict], Awaitable[dict | None]],
    ) -> None:
        server = await websockets.serve(
            lambda ws, path: self._serve_loop(ws, path, handler),
            self.listen_host,
            self.listen_port,
            max_size=None,
        )
        self.logger.info(
            "WS server listening on %s:%s", self.listen_host, self.listen_port
        )
        try:
            await asyncio.Future()
        finally:
            server.close()
            await server.wait_closed()

    async def _serve_loop(
        self,
        ws: WebSocketServerProtocol,
        path: str,
        handler: Callable[[dict], Awaitable[dict | None]],
    ):
        peer = getattr(ws, "remote_address", None)
        self.logger.debug("[ws≺] open path=%s peer=%s", path, peer)
        try:
            while True:
                try:
                    raw = await ws.recv()
                except (ConnectionClosedOK, ConnectionClosedError):
                    break

                try:
                    req = json.loads(raw)
                except Exception:
                    if not await self._safe_send(ws, _json({"ok": False, "error": "bad-json"})):
                        break
                    continue

                rid = req.get("rid") or str(uuid.uuid4())
                req["rid"] = rid

                try:
                    response = await handler(req)
                    if response is None:
                        response = {"ok": True}
                    if isinstance(response, dict):
                        response.setdefault("rid", rid)
                except Exception:
                    self.logger.exception("WS handler failed type=%s", _ptype(req))
                    response = {"ok": False, "error": "handler-failed", "rid": rid}

                if not await self._safe_send(ws, _json(response)):
                    break
        finally:
            await self._close_quietly(ws)

    async def _safe_send(self, ws, payload: str) -> bool:
        if ws.closed:
            return False
        try:
            await ws.send(payload)
            return True
        except (ConnectionClosedOK, ConnectionClosedError):
            return False
        except Exception:
            self.logger.debug("send failed", exc_info=True)
            return False

    async def _close_quietly(self, ws) -> None:
        with contextlib.suppress(Exception):
            await ws.close()

    async def _sleep_backoff(self, attempt: int, base: float, cap: float, jitter: float) -> None:
        delay = min(cap, base * (2 ** (attempt - 1)))
        delay += random.random() * (jitter * delay)
        await asyncio.sleep(delay)

    async def send(
        self,
        payload: dict | str,
        *,
        max_attempts: int = 5,
        base_backoff: float = 0.5,
        backoff_cap: float = 8.0,
        jitter: float = 0.2,
        connect_timeout: float | None = 5.0,
        send_timeout: float | None = 5.0,
    ) -> None:
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {"type": "(none)", "data": payload}

        rid = payload.get("rid") or str(uuid.uuid4())
        payload = dict(payload)
        payload["rid"] = rid

        if self._shutting_down:
            max_attempts = 1
            connect_timeout = min(connect_timeout or 0.25, 0.25)
            send_timeout = min(send_timeout or 0.25, 0.25)

        for attempt in range(1, max_attempts + 1):
            try:
                if connect_timeout is not None:
                    ws = await asyncio.wait_for(
                        websockets.connect(self.send_url, max_size=None, ping_interval=None),
                        connect_timeout,
                    )
                else:
                    ws = await websockets.connect(self.send_url, max_size=None, ping_interval=None)
                try:
                    raw = _json(payload)
                    if send_timeout is not None:
                        await asyncio.wait_for(ws.send(raw), send_timeout)
                    else:
                        await ws.send(raw)
                finally:
                    await self._close_quietly(ws)
                return
            except (asyncio.TimeoutError, OSError) as e:
                self.logger.warning(
                    "[WS] send error attempt %d/%d: %s", attempt, max_attempts, e
                )
                if self._shutting_down or attempt >= max_attempts:
                    break
                await self._sleep_backoff(attempt, base_backoff, backoff_cap, jitter)
            except Exception as e:
                self.logger.error("[WS] send unexpected: %s", e)
                break

    async def request(
        self,
        payload: dict,
        *,
        timeout: float | None = 30.0,
        max_attempts: int = 5,
        base_backoff: float = 0.5,
        backoff_cap: float = 8.0,
        jitter: float = 0.2,
        connect_timeout: float | None = 5.0,
    ) -> dict | None:
        rid = payload.get("rid") or str(uuid.uuid4())
        payload = dict(payload)
        payload["rid"] = rid

        if self._shutting_down:
            max_attempts = 1
            connect_timeout = min(connect_timeout or 0.25, 0.25)
            timeout = min(timeout or 0.25, 0.25)

        for attempt in range(1, max_attempts + 1):
            try:
                if connect_timeout is not None:
                    ws = await asyncio.wait_for(
                        websockets.connect(self.send_url, max_size=None, ping_interval=None),
                        connect_timeout,
                    )
                else:
                    ws = await websockets.connect(self.send_url, max_size=None, ping_interval=None)
                try:
                    await ws.send(_json(payload))
                    if timeout is not None:
                        raw_in = await asyncio.wait_for(ws.recv(), timeout)
                    else:
                        raw_in = await ws.recv()
                    return json.loads(raw_in)
                finally:
                    await self._close_quietly(ws)
            except asyncio.CancelledError:
                return None
            except (asyncio.TimeoutError, OSError, ConnectionClosedError, ProtocolError) as e:
                self.logger.warning(
                    "[WS] request attempt %d/%d: %s", attempt, max_attempts, e
                )
                if self._shutting_down or attempt >= max_attempts:
                    return None
                await self._sleep_backoff(attempt, base_backoff, backoff_cap, jitter)
            except Exception as e:
                self.logger.error("[WS] request unexpected: %s", e)
                break
        return None
