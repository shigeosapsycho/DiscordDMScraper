from __future__ import annotations

import logging
import os
from typing import Optional

from common.db import DBManager


class Config:
    """Loads runtime config from env vars with optional override from `app_config` table.

    Pattern lifted from Copycord's config.py — DB takes precedence over env so the dashboard
    can adjust settings without restarting.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = (logger or logging.getLogger(__name__)).getChild("Config")

        self.DB_PATH = os.getenv("DB_PATH", "/data/data.db")
        self.db = DBManager(self.DB_PATH)

        self.CLIENT_TOKEN = self._str("CLIENT_TOKEN")
        self.SERVER_TOKEN = self._str("SERVER_TOKEN")

        try:
            self.DEST_GUILD_ID = int(self._str("DEST_GUILD_ID", "0") or "0")
        except ValueError:
            self.DEST_GUILD_ID = 0

        self.DMS_CATEGORY_NAME = self._str("DMS_CATEGORY_NAME", "DMs") or "DMs"

        self.SERVER_WS_HOST = self._str("SERVER_WS_HOST", "server") or "server"
        self.SERVER_WS_PORT = self._int("SERVER_WS_PORT", "8765")
        self.SERVER_WS_URL = (
            self._str("WS_SERVER_URL")
            or f"ws://{self.SERVER_WS_HOST}:{self.SERVER_WS_PORT}"
        )

        self.BACKFILL_CONCURRENCY = self._int("BACKFILL_CONCURRENCY", "3")
        self.ATTACHMENT_MAX_BYTES = self._int("ATTACHMENT_MAX_BYTES", "25000000")
        self.INCLUDE_GROUP_DMS = self._bool("INCLUDE_GROUP_DMS", True)

        self.ADMIN_PORT = self._int("ADMIN_PORT", "6767")

    def _db_get(self, key: str) -> Optional[str]:
        try:
            return self.db.get_config(key) or None
        except Exception:
            return None

    def _str(self, key: str, env_default: Optional[str] = None) -> Optional[str]:
        v = self._db_get(key)
        if v is None or (isinstance(v, str) and v.strip() == ""):
            v = os.getenv(key, env_default)
        return v

    def _int(self, key: str, env_default: str = "0") -> int:
        raw = self._str(key, env_default)
        try:
            return int(str(raw).strip())
        except Exception:
            try:
                return int(env_default)
            except Exception:
                return 0

    def _bool(self, key: str, env_default: bool = False) -> bool:
        raw = self._str(key, "true" if env_default else "false")
        return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")
