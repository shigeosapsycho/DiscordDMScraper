from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import sys
from typing import Optional

import aiohttp
import discord
from dotenv import load_dotenv

# allow `python -m server.server` from /app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import Config  # noqa: E402
from common.websockets import WebsocketManager  # noqa: E402
from server.attachments import reupload_all  # noqa: E402
from server.rate_limiter import ActionType, RateLimitManager  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
)
logger = logging.getLogger("server")

CHECKPOINT_EVERY = 25  # backfill messages between DB checkpoints


def _slug(name: str, max_len: int = 90) -> str:
    name = (name or "").strip().lower()
    name = re.sub(r"[^a-z0-9]+", "-", name).strip("-")
    return (name or "dm")[:max_len] or "dm"


class DMServer:
    def __init__(self, config: Config):
        self.config = config
        self.db = config.db
        self.ratelimit = RateLimitManager()

        intents = discord.Intents.default()
        intents.guilds = True
        intents.message_content = True
        self.bot = discord.Bot(intents=intents)

        self.ws = WebsocketManager(
            listen_host="0.0.0.0",
            listen_port=config.SERVER_WS_PORT,
            logger=logger.getChild("ws"),
        )

        self.session: Optional[aiohttp.ClientSession] = None
        self._channel_locks: dict[int, asyncio.Lock] = {}
        self._active_runs: dict[int, dict] = {}
        self._ready_event = asyncio.Event()

        self.bot.event(self.on_ready)

    # ---------- discord lifecycle ----------

    async def on_ready(self) -> None:
        guild = self.bot.get_guild(self.config.DEST_GUILD_ID)
        if guild is None:
            logger.error(
                "Bot is not in DEST_GUILD_ID=%s — invite it and restart",
                self.config.DEST_GUILD_ID,
            )
            return
        logger.info(
            "Logged in as %s; destination guild = %s (%s)",
            self.bot.user, guild.name, guild.id,
        )
        self._ready_event.set()

    # ---------- helpers ----------

    def _channel_lock(self, original_channel_id: int) -> asyncio.Lock:
        lock = self._channel_locks.get(original_channel_id)
        if lock is None:
            lock = asyncio.Lock()
            self._channel_locks[original_channel_id] = lock
        return lock

    async def _get_dest_guild(self) -> discord.Guild:
        await self._ready_event.wait()
        guild = self.bot.get_guild(self.config.DEST_GUILD_ID)
        if guild is None:
            raise RuntimeError(
                f"Destination guild {self.config.DEST_GUILD_ID} not found — "
                "is the bot still in the server?"
            )
        return guild

    async def _ensure_category(self, guild: discord.Guild) -> discord.CategoryChannel:
        name = self.config.DMS_CATEGORY_NAME
        for c in guild.categories:
            if c.name == name:
                return c
        await self.ratelimit.acquire(ActionType.CREATE_CATEGORY, key=str(guild.id))
        return await guild.create_category(name=name)

    async def _ensure_dm_channel(
        self,
        *,
        original_channel_id: int,
        partner_user_id: Optional[int],
        partner_label: str,
        is_group: bool,
    ) -> dict:
        """Create (or return cached) destination text channel + webhook for a DM."""
        guild = await self._get_dest_guild()
        row = self.db.get_dm_mapping(original_channel_id)

        # validate cached channel still exists in the guild
        if row and row["cloned_channel_id"]:
            existing = guild.get_channel(int(row["cloned_channel_id"]))
            if existing and row["channel_webhook_url"]:
                return {
                    "ok": True,
                    "cloned_channel_id": int(row["cloned_channel_id"]),
                    "webhook_url": row["channel_webhook_url"],
                }
            # channel was deleted upstream — wipe the stale rows and recreate
            self.db.clear_dm_clone(original_channel_id)

        category = await self._ensure_category(guild)
        prefix = "group" if is_group else "dm"
        channel_name = f"{prefix}-{_slug(partner_label)}"

        await self.ratelimit.acquire(ActionType.CREATE_CHANNEL, key=str(guild.id))
        channel = await guild.create_text_channel(
            name=channel_name,
            category=category,
            topic=(
                f"Mirror of group DM ({partner_label})"
                if is_group
                else f"Mirror of DM with {partner_label} (user id {partner_user_id})"
            ),
        )

        await self.ratelimit.acquire(ActionType.WEBHOOK_CREATE, key=str(channel.id))
        webhook = await channel.create_webhook(name="DM Mirror")

        self.db.upsert_dm_mapping(
            original_channel_id=original_channel_id,
            partner_user_id=partner_user_id,
            partner_label=partner_label,
            is_group=is_group,
            cloned_channel_id=channel.id,
            cloned_category_id=category.id,
            channel_webhook_url=webhook.url,
        )

        # pin a header so the channel is identifiable even after rename
        try:
            header_lines = [
                f"**Mirror of {'group DM' if is_group else 'DM'}: {partner_label}**",
                f"Original channel id: `{original_channel_id}`",
            ]
            if partner_user_id and not is_group:
                header_lines.append(f"Partner user id: `{partner_user_id}`")
            header = await channel.send("\n".join(header_lines))
            await header.pin(reason="DM mirror header")
        except Exception as e:
            logger.warning("Failed to pin header in channel %s: %s", channel.id, e)

        logger.info(
            "Created DM channel #%s for original=%s (group=%s)",
            channel.name, original_channel_id, is_group,
        )
        return {
            "ok": True,
            "cloned_channel_id": channel.id,
            "webhook_url": webhook.url,
        }

    def _build_content(self, payload: dict, attachment_placeholders: list[str]) -> str:
        content = payload.get("content") or ""
        # group DM: prefix sender (1:1 webhook spoofs author so prefix not needed)
        if payload.get("is_group"):
            author = (payload.get("author") or {}).get("name") or "?"
            if content:
                content = f"**{author}**: {content}"
            else:
                content = f"**{author}**:"
        if attachment_placeholders:
            tail = "\n".join(attachment_placeholders)
            content = f"{content}\n{tail}" if content else tail

        # discord webhook content limit
        if len(content) > 1900:
            content = content[:1900] + "…"
        return content

    async def _post_dm_message(self, payload: dict) -> dict:
        """Forward one serialized DM message into its mirrored channel via webhook."""
        original_channel_id = int(payload["channel_id"])
        original_message_id = int(payload["id"])

        # dedupe: if we've already posted this message, skip
        if self.db.message_exists(original_message_id):
            return {"ok": True, "skipped": "duplicate"}

        # look up mapping (must already exist — client sends dm_register first)
        row = self.db.get_dm_mapping(original_channel_id)
        if not row or not row["channel_webhook_url"]:
            ensured = await self._ensure_dm_channel(
                original_channel_id=original_channel_id,
                partner_user_id=payload.get("partner_user_id"),
                partner_label=payload.get("partner_label") or "unknown",
                is_group=bool(payload.get("is_group")),
            )
            webhook_url = ensured["webhook_url"]
            cloned_channel_id = ensured["cloned_channel_id"]
        else:
            webhook_url = row["channel_webhook_url"]
            cloned_channel_id = int(row["cloned_channel_id"])

        author = payload.get("author") or {}
        username = (author.get("name") or "Unknown")[:80]
        avatar_url = author.get("avatar_url")

        # serialize per-channel so backfill order is preserved
        async with self._channel_lock(original_channel_id):
            await self.ratelimit.acquire(
                ActionType.WEBHOOK_MESSAGE, key=str(cloned_channel_id)
            )

            results = await reupload_all(
                self.session,
                payload.get("attachments") or [],
                max_bytes=self.config.ATTACHMENT_MAX_BYTES,
            )
            files = [r.file for r in results if r.succeeded]
            placeholders = [r.placeholder for r in results if not r.succeeded and r.placeholder]
            content = self._build_content(payload, placeholders)

            try:
                webhook = discord.Webhook.from_url(webhook_url, session=self.session)
                sent = await webhook.send(
                    content=content or "​",  # zero-width space if empty
                    username=username,
                    avatar_url=avatar_url,
                    files=files,
                    wait=True,
                )
            except discord.HTTPException as e:
                if getattr(e, "status", None) == 429:
                    retry = float(getattr(e, "retry_after", 5.0) or 5.0)
                    self.ratelimit.penalize(
                        ActionType.WEBHOOK_MESSAGE, retry, key=str(cloned_channel_id)
                    )
                logger.warning("Webhook send failed for %s: %s", original_message_id, e)
                return {"ok": False, "error": f"http {getattr(e,'status','?')}"}
            except Exception as e:
                logger.exception("Webhook send unexpected: %s", e)
                return {"ok": False, "error": "send-failed"}

            self.db.remember_message(
                original_message_id=original_message_id,
                original_channel_id=original_channel_id,
                cloned_message_id=int(sent.id) if sent else None,
                cloned_channel_id=cloned_channel_id,
                webhook_url=webhook_url,
            )

            # checkpoint backfill if this message arrived during a run
            run = self._active_runs.get(original_channel_id)
            if run is not None:
                run["delivered"] += 1
                run["last_orig_message_id"] = original_message_id
                ts = payload.get("timestamp_unix")
                if ts is not None:
                    run["last_orig_timestamp"] = int(ts)
                if run["delivered"] % CHECKPOINT_EVERY == 0:
                    self.db.backfill_checkpoint(
                        run["run_id"],
                        delivered=run["delivered"],
                        last_orig_message_id=run["last_orig_message_id"],
                        last_orig_timestamp=run.get("last_orig_timestamp"),
                    )

        return {"ok": True, "cloned_message_id": int(sent.id) if sent else None}

    async def _edit_dm_message(self, payload: dict) -> dict:
        original_message_id = int(payload["id"])
        row = self.db.lookup_message(original_message_id)
        if not row or not row["webhook_url"] or not row["cloned_message_id"]:
            return {"ok": False, "error": "unknown-message"}

        new_content = payload.get("content") or "​"
        try:
            webhook = discord.Webhook.from_url(row["webhook_url"], session=self.session)
            await webhook.edit_message(int(row["cloned_message_id"]), content=new_content)
        except Exception as e:
            logger.warning("Edit failed for %s: %s", original_message_id, e)
            return {"ok": False, "error": str(e)}
        return {"ok": True}

    async def _delete_dm_message(self, payload: dict) -> dict:
        original_message_id = int(payload["id"])
        row = self.db.lookup_message(original_message_id)
        if not row or not row["webhook_url"] or not row["cloned_message_id"]:
            return {"ok": False, "error": "unknown-message"}
        try:
            webhook = discord.Webhook.from_url(row["webhook_url"], session=self.session)
            await webhook.delete_message(int(row["cloned_message_id"]))
        except Exception as e:
            logger.warning("Delete failed for %s: %s", original_message_id, e)
            return {"ok": False, "error": str(e)}
        self.db.forget_message(original_message_id)
        return {"ok": True}

    async def _backfill_start(self, payload: dict) -> dict:
        original_channel_id = int(payload["channel_id"])
        existing = self.db.backfill_get_incomplete(original_channel_id)
        if existing:
            run_id = existing["run_id"]
            self._active_runs[original_channel_id] = {
                "run_id": run_id,
                "delivered": int(existing["delivered"] or 0),
                "last_orig_message_id": existing["last_orig_message_id"],
                "last_orig_timestamp": existing["last_orig_timestamp"],
            }
            logger.info(
                "Resuming backfill for channel=%s run=%s delivered=%s after=%s",
                original_channel_id, run_id, existing["delivered"],
                existing["last_orig_message_id"],
            )
            return {
                "ok": True,
                "run_id": run_id,
                "resume_after_message_id": existing["last_orig_message_id"],
                "delivered": int(existing["delivered"] or 0),
            }

        run_id = self.db.backfill_start(
            original_channel_id, expected_total=payload.get("expected_total")
        )
        self._active_runs[original_channel_id] = {
            "run_id": run_id,
            "delivered": 0,
            "last_orig_message_id": None,
            "last_orig_timestamp": None,
        }
        logger.info("Started backfill for channel=%s run=%s", original_channel_id, run_id)
        return {"ok": True, "run_id": run_id, "resume_after_message_id": None, "delivered": 0}

    async def _backfill_finish(self, payload: dict) -> dict:
        original_channel_id = int(payload["channel_id"])
        run = self._active_runs.pop(original_channel_id, None)
        if run:
            self.db.backfill_checkpoint(
                run["run_id"],
                delivered=run["delivered"],
                last_orig_message_id=run.get("last_orig_message_id"),
                last_orig_timestamp=run.get("last_orig_timestamp"),
            )
            self.db.backfill_finish(run["run_id"], status=payload.get("status", "completed"))
            logger.info(
                "Finished backfill for channel=%s delivered=%s",
                original_channel_id, run["delivered"],
            )
            return {"ok": True, "delivered": run["delivered"]}
        return {"ok": True, "delivered": 0}

    # ---------- WS dispatch ----------

    async def _ws_handler(self, msg: dict) -> dict:
        t = msg.get("type")
        try:
            if t == "ping":
                return {"ok": True, "pong": True}
            if t == "dm_register":
                return await self._ensure_dm_channel(
                    original_channel_id=int(msg["channel_id"]),
                    partner_user_id=msg.get("partner_user_id"),
                    partner_label=msg.get("partner_label") or "unknown",
                    is_group=bool(msg.get("is_group")),
                )
            if t == "dm_message":
                return await self._post_dm_message(msg)
            if t == "dm_message_edit":
                return await self._edit_dm_message(msg)
            if t == "dm_message_delete":
                return await self._delete_dm_message(msg)
            if t == "backfill_start":
                return await self._backfill_start(msg)
            if t == "backfill_finish":
                return await self._backfill_finish(msg)
            return {"ok": False, "error": f"unknown-type:{t}"}
        except Exception as e:
            logger.exception("WS handler crash type=%s", t)
            return {"ok": False, "error": f"server-exception:{e!r}"}

    # ---------- entrypoint ----------

    async def run(self) -> None:
        if not self.config.SERVER_TOKEN:
            raise SystemExit("SERVER_TOKEN env var is required")
        if not self.config.DEST_GUILD_ID:
            raise SystemExit("DEST_GUILD_ID env var is required")

        self.session = aiohttp.ClientSession()
        ws_task = asyncio.create_task(self.ws.start_server(self._ws_handler))
        try:
            await self.bot.start(self.config.SERVER_TOKEN)
        finally:
            ws_task.cancel()
            with contextlib.suppress(Exception):
                await ws_task
            if self.session:
                await self.session.close()


def main() -> None:
    load_dotenv()
    config = Config(logger=logger)
    server = DMServer(config)
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        logger.info("Interrupted; shutting down")


if __name__ == "__main__":
    main()
