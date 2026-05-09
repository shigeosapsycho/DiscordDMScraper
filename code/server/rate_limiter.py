from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import Dict, Optional, Tuple


class ActionType(Enum):
    WEBHOOK_MESSAGE = "webhook_message"
    WEBHOOK_CREATE = "webhook_create"
    WEBHOOK_DELETE = "webhook_delete"
    CREATE_CHANNEL = "create_channel"
    EDIT_CHANNEL = "edit_channel"
    DELETE_CHANNEL = "delete_channel"
    CREATE_CATEGORY = "create_category"


class RateLimiter:
    def __init__(self, max_rate: int, time_window: float):
        self._max_rate = max_rate
        self._time_window = time_window
        self._allowance = float(max_rate)
        self._last_check = time.monotonic()
        self._lock = asyncio.Lock()
        self._cooldown_until = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._cooldown_until:
                await asyncio.sleep(self._cooldown_until - now)
                now = time.monotonic()

            elapsed = now - self._last_check
            self._last_check = now
            self._allowance = min(
                self._max_rate,
                self._allowance + elapsed * (self._max_rate / self._time_window),
            )
            if self._allowance < 1.0:
                wait = (1.0 - self._allowance) * (self._time_window / self._max_rate)
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last_check = time.monotonic()
                self._allowance = 0.0
            else:
                self._allowance -= 1.0

    def backoff(self, seconds: float) -> None:
        candidate = time.monotonic() + max(0.0, seconds)
        if candidate > self._cooldown_until:
            self._cooldown_until = candidate


class RateLimitManager:
    def __init__(self, config: Optional[Dict[ActionType, Tuple[int, float]]] = None):
        self._cfg = config or {
            ActionType.WEBHOOK_MESSAGE: (5, 2.5),
            ActionType.WEBHOOK_CREATE: (1, 30.0),
            ActionType.WEBHOOK_DELETE: (1, 10.0),
            ActionType.CREATE_CHANNEL: (2, 15.0),
            ActionType.CREATE_CATEGORY: (1, 15.0),
            ActionType.EDIT_CHANNEL: (3, 15.0),
            ActionType.DELETE_CHANNEL: (3, 15.0),
        }
        self._buckets: Dict[ActionType, Dict[str, RateLimiter]] = {a: {} for a in self._cfg}

    def _get(self, action: ActionType, key: Optional[str]) -> RateLimiter:
        scope = str(key) if key is not None else "GLOBAL"
        bucket = self._buckets[action]
        lim = bucket.get(scope)
        if lim is None:
            rate, window = self._cfg[action]
            lim = RateLimiter(rate, window)
            bucket[scope] = lim
        return lim

    async def acquire(self, action: ActionType, key: Optional[str] = None) -> None:
        await self._get(action, key).acquire()

    def penalize(self, action: ActionType, seconds: float, key: Optional[str] = None) -> None:
        self._get(action, key).backoff(seconds)
