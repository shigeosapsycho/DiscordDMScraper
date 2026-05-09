from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from typing import Optional


class DBManager:
    """SQLite manager for DMScraper.

    Schema:
      - app_config(key, value)               key/value config (overrides env)
      - dm_mappings(...)                     one row per source DM channel ↔ destination text channel
      - messages(...)                        one row per forwarded message (dedupe + edit/delete tracking)
      - backfill_runs(...)                   resume state per channel backfill
    """

    def __init__(self, db_path: str, init_schema: bool = True):
        self.path = db_path
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode = WAL;")
        self.conn.execute("PRAGMA synchronous = NORMAL;")
        self.conn.execute("PRAGMA busy_timeout = 5000;")
        self.lock = threading.RLock()
        if init_schema:
            self._init_schema()

    def _init_schema(self) -> None:
        with self.lock, self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS app_config (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS dm_mappings (
                    original_channel_id  INTEGER PRIMARY KEY,
                    partner_user_id      INTEGER,
                    partner_label        TEXT,
                    is_group             INTEGER NOT NULL DEFAULT 0,
                    cloned_channel_id    INTEGER UNIQUE,
                    cloned_category_id   INTEGER,
                    channel_webhook_url  TEXT,
                    created_at           INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))
                );
                CREATE INDEX IF NOT EXISTS ix_dm_partner ON dm_mappings(partner_user_id);

                CREATE TABLE IF NOT EXISTS messages (
                    original_message_id  INTEGER PRIMARY KEY,
                    original_channel_id  INTEGER NOT NULL,
                    cloned_message_id    INTEGER,
                    cloned_channel_id    INTEGER,
                    webhook_url          TEXT,
                    created_at           INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))
                );
                CREATE INDEX IF NOT EXISTS ix_msg_orig_channel  ON messages(original_channel_id);
                CREATE INDEX IF NOT EXISTS ix_msg_clone_channel ON messages(cloned_channel_id);

                CREATE TABLE IF NOT EXISTS backfill_runs (
                    run_id                  TEXT PRIMARY KEY,
                    original_channel_id     INTEGER NOT NULL,
                    status                  TEXT NOT NULL,
                    delivered               INTEGER NOT NULL DEFAULT 0,
                    expected_total          INTEGER,
                    last_orig_message_id    INTEGER,
                    last_orig_timestamp     INTEGER,
                    started_at              INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
                    finished_at             INTEGER
                );
                CREATE INDEX IF NOT EXISTS ix_bf_channel_status
                    ON backfill_runs(original_channel_id, status);
                """
            )

    # ---------- app_config ----------

    def set_config(self, key: str, value: str) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO app_config(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def get_config(self, key: str, default: str = "") -> str:
        row = self.conn.execute(
            "SELECT value FROM app_config WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else default

    def get_all_config(self) -> dict[str, str]:
        return {
            r["key"]: r["value"]
            for r in self.conn.execute("SELECT key, value FROM app_config")
        }

    # ---------- dm_mappings ----------

    def get_dm_mapping(self, original_channel_id: int) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM dm_mappings WHERE original_channel_id=?",
            (int(original_channel_id),),
        ).fetchone()

    def all_dm_mappings(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM dm_mappings ORDER BY created_at ASC"
        ).fetchall()

    def upsert_dm_mapping(
        self,
        *,
        original_channel_id: int,
        partner_user_id: Optional[int],
        partner_label: str,
        is_group: bool,
        cloned_channel_id: Optional[int] = None,
        cloned_category_id: Optional[int] = None,
        channel_webhook_url: Optional[str] = None,
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO dm_mappings(
                    original_channel_id, partner_user_id, partner_label, is_group,
                    cloned_channel_id, cloned_category_id, channel_webhook_url
                ) VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(original_channel_id) DO UPDATE SET
                    partner_user_id      = COALESCE(excluded.partner_user_id, partner_user_id),
                    partner_label        = COALESCE(excluded.partner_label, partner_label),
                    is_group             = excluded.is_group,
                    cloned_channel_id    = COALESCE(excluded.cloned_channel_id, cloned_channel_id),
                    cloned_category_id   = COALESCE(excluded.cloned_category_id, cloned_category_id),
                    channel_webhook_url  = COALESCE(excluded.channel_webhook_url, channel_webhook_url)
                """,
                (
                    int(original_channel_id),
                    int(partner_user_id) if partner_user_id is not None else None,
                    partner_label,
                    1 if is_group else 0,
                    int(cloned_channel_id) if cloned_channel_id is not None else None,
                    int(cloned_category_id) if cloned_category_id is not None else None,
                    channel_webhook_url,
                ),
            )

    def clear_dm_clone(self, original_channel_id: int) -> None:
        """Forget that a destination channel ever existed for this DM (so it gets recreated)."""
        with self.lock, self.conn:
            self.conn.execute(
                "UPDATE dm_mappings SET cloned_channel_id=NULL, channel_webhook_url=NULL "
                "WHERE original_channel_id=?",
                (int(original_channel_id),),
            )

    # ---------- messages ----------

    def remember_message(
        self,
        *,
        original_message_id: int,
        original_channel_id: int,
        cloned_message_id: Optional[int],
        cloned_channel_id: Optional[int],
        webhook_url: Optional[str],
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO messages(
                    original_message_id, original_channel_id,
                    cloned_message_id, cloned_channel_id, webhook_url, created_at
                ) VALUES (?,?,?,?,?, CAST(strftime('%s','now') AS INTEGER))
                """,
                (
                    int(original_message_id),
                    int(original_channel_id),
                    int(cloned_message_id) if cloned_message_id is not None else None,
                    int(cloned_channel_id) if cloned_channel_id is not None else None,
                    webhook_url,
                ),
            )

    def lookup_message(self, original_message_id: int) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM messages WHERE original_message_id=?",
            (int(original_message_id),),
        ).fetchone()

    def message_exists(self, original_message_id: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM messages WHERE original_message_id=?",
            (int(original_message_id),),
        ).fetchone()
        return row is not None

    def forget_message(self, original_message_id: int) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "DELETE FROM messages WHERE original_message_id=?",
                (int(original_message_id),),
            )

    # ---------- backfill_runs ----------

    def backfill_get_incomplete(self, original_channel_id: int) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM backfill_runs WHERE original_channel_id=? AND status='running' "
            "ORDER BY started_at DESC LIMIT 1",
            (int(original_channel_id),),
        ).fetchone()

    def backfill_start(
        self, original_channel_id: int, expected_total: Optional[int] = None
    ) -> str:
        run_id = uuid.uuid4().hex
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO backfill_runs(
                    run_id, original_channel_id, status, delivered, expected_total
                ) VALUES (?,?, 'running', 0, ?)
                """,
                (run_id, int(original_channel_id), expected_total),
            )
        return run_id

    def backfill_checkpoint(
        self,
        run_id: str,
        *,
        delivered: int,
        last_orig_message_id: Optional[int],
        last_orig_timestamp: Optional[int],
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                UPDATE backfill_runs SET
                    delivered = ?,
                    last_orig_message_id = COALESCE(?, last_orig_message_id),
                    last_orig_timestamp  = COALESCE(?, last_orig_timestamp)
                WHERE run_id = ?
                """,
                (int(delivered), last_orig_message_id, last_orig_timestamp, run_id),
            )

    def backfill_finish(self, run_id: str, status: str = "completed") -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "UPDATE backfill_runs SET status=?, finished_at=? WHERE run_id=?",
                (status, int(time.time()), run_id),
            )

    def backfill_progress_summary(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT b.*, m.partner_label, m.cloned_channel_id
            FROM backfill_runs b
            LEFT JOIN dm_mappings m ON m.original_channel_id = b.original_channel_id
            ORDER BY b.started_at DESC
            """
        ).fetchall()
