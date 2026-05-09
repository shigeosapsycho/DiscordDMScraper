from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import Optional, Tuple

import discord
from dotenv import load_dotenv

# allow `python -m client.client` from /app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client.message_utils import serialize_dm_message  # noqa: E402
from common.config import Config  # noqa: E402
from common.websockets import WebsocketManager  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
)
logger = logging.getLogger("client")


def _channel_descriptor(ch: discord.abc.Messageable) -> Tuple[Optional[int], str, bool]:
    """Return (partner_user_id, partner_label, is_group) for a DM/group channel."""
    if isinstance(ch, discord.DMChannel):
        recipient = ch.recipient
        return (
            recipient.id if recipient else None,
            (getattr(recipient, "global_name", None) or recipient.name) if recipient else "unknown",
            False,
        )
    if isinstance(ch, discord.GroupChannel):
        members = list(getattr(ch, "recipients", []) or [])
        if ch.name:
            label = ch.name
        elif members:
            names = [getattr(m, "global_name", None) or m.name for m in members[:3]]
            label = ", ".join(names)
            if len(members) > 3:
                label += f" +{len(members) - 3}"
        else:
            label = f"group-{ch.id}"
        return (None, label, True)
    return (None, "unknown", False)


class DMClient:
    def __init__(self, config: Config):
        self.config = config
        self.ws = WebsocketManager(
            send_url=config.SERVER_WS_URL,
            logger=logger.getChild("ws"),
        )

        # discord.py-self exposes a regular Client; self_bot=True is implicit.
        self.bot = discord.Client(chunk_guilds_at_startup=False)

        self._backfill_sem = asyncio.Semaphore(max(1, config.BACKFILL_CONCURRENCY))
        self._backfilled: set[int] = set()
        # channels currently being backfilled buffer live messages here so they
        # aren't dropped during the history() pagination window
        self._live_buffer: dict[int, list[discord.Message]] = {}
        self._ready_once = asyncio.Event()

        self.bot.event(self.on_ready)
        self.bot.event(self.on_message)
        self.bot.event(self.on_message_edit)
        self.bot.event(self.on_message_delete)

    # ---------- lifecycle ----------

    async def on_ready(self) -> None:
        if self._ready_once.is_set():
            logger.info("Reconnected as %s", self.bot.user)
            return
        self._ready_once.set()
        logger.info("Logged in as %s", self.bot.user)

        # wait briefly for server to be reachable (best-effort ping)
        await self._wait_for_server()

        # enumerate DM channels and start backfill in parallel
        dm_channels = list(self.bot.private_channels or [])
        logger.info("Found %d open DM channels", len(dm_channels))

        tasks = []
        for ch in dm_channels:
            if isinstance(ch, discord.GroupChannel) and not self.config.INCLUDE_GROUP_DMS:
                continue
            if not isinstance(ch, (discord.DMChannel, discord.GroupChannel)):
                continue
            tasks.append(asyncio.create_task(self._backfill_channel(ch)))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Initial backfill complete; entering live mirror mode")

    async def _wait_for_server(self, attempts: int = 30, delay: float = 1.0) -> None:
        for _ in range(attempts):
            resp = await self.ws.request({"type": "ping"}, timeout=2.0, max_attempts=1)
            if resp and resp.get("ok"):
                return
            await asyncio.sleep(delay)
        logger.warning("Server WS not reachable after %ds; continuing anyway", attempts)

    # ---------- backfill ----------

    async def _backfill_channel(self, channel: discord.abc.Messageable) -> None:
        async with self._backfill_sem:
            partner_user_id, partner_label, is_group = _channel_descriptor(channel)
            channel_id = int(channel.id)
            self._live_buffer.setdefault(channel_id, [])

            # 1. register destination channel + webhook
            register_resp = await self.ws.request(
                {
                    "type": "dm_register",
                    "channel_id": str(channel_id),
                    "partner_user_id": str(partner_user_id) if partner_user_id else None,
                    "partner_label": partner_label,
                    "is_group": is_group,
                },
                timeout=60.0,
            )
            if not register_resp or not register_resp.get("ok"):
                logger.error(
                    "dm_register failed for channel=%s: %s", channel_id, register_resp
                )
                return

            # 2. open / resume backfill run
            start_resp = await self.ws.request(
                {"type": "backfill_start", "channel_id": str(channel_id)},
                timeout=30.0,
            )
            if not start_resp or not start_resp.get("ok"):
                logger.error("backfill_start failed for channel=%s", channel_id)
                return

            resume_after = start_resp.get("resume_after_message_id")
            after_obj: Optional[discord.Object] = (
                discord.Object(id=int(resume_after)) if resume_after else None
            )

            logger.info(
                "Backfilling channel=%s (%s) resume_after=%s",
                channel_id, partner_label, resume_after,
            )

            count = 0
            try:
                # oldest_first=True so messages arrive in chronological order
                async for msg in channel.history(
                    limit=None, oldest_first=True, after=after_obj
                ):
                    payload = serialize_dm_message(
                        msg,
                        partner_user_id=partner_user_id,
                        partner_label=partner_label,
                        is_group=is_group,
                    )
                    payload["type"] = "dm_message"
                    # request rather than fire-and-forget so server can push back on rate limits
                    resp = await self.ws.request(payload, timeout=120.0)
                    if not resp or not resp.get("ok"):
                        logger.warning(
                            "Failed to mirror message %s: %s", msg.id, resp
                        )
                    count += 1
                    if count % 100 == 0:
                        logger.info(
                            "channel=%s backfilled %d messages", channel_id, count
                        )
            except discord.Forbidden:
                logger.warning("Forbidden reading history for channel=%s", channel_id)
            except Exception:
                logger.exception("Backfill error for channel=%s", channel_id)
                await self.ws.send(
                    {
                        "type": "backfill_finish",
                        "channel_id": str(channel_id),
                        "status": "failed",
                    }
                )
                return

            # drain any live messages that arrived during backfill (server dedupes)
            pending = self._live_buffer.pop(channel_id, [])
            for msg in pending:
                live_payload = serialize_dm_message(
                    msg,
                    partner_user_id=partner_user_id,
                    partner_label=partner_label,
                    is_group=is_group,
                )
                live_payload["type"] = "dm_message"
                await self.ws.request(live_payload, timeout=60.0)

            await self.ws.send(
                {
                    "type": "backfill_finish",
                    "channel_id": str(channel_id),
                    "status": "completed",
                }
            )
            self._backfilled.add(channel_id)
            logger.info(
                "Done channel=%s (%s) — %d messages (+%d live during backfill)",
                channel_id, partner_label, count, len(pending),
            )

    # ---------- live events ----------

    def _is_dm(self, channel) -> bool:
        if isinstance(channel, discord.DMChannel):
            return True
        if isinstance(channel, discord.GroupChannel):
            return self.config.INCLUDE_GROUP_DMS
        return False

    async def on_message(self, message: discord.Message) -> None:
        if not self._is_dm(message.channel):
            return
        cid = int(message.channel.id)

        # backfill in progress: buffer for drain at end (history may miss tail)
        if cid in self._live_buffer:
            self._live_buffer[cid].append(message)
            return

        # never seen this channel: spawn a backfill (someone just opened a new DM with us)
        if cid not in self._backfilled:
            asyncio.create_task(self._backfill_channel(message.channel))
            self._live_buffer.setdefault(cid, []).append(message)
            return

        partner_user_id, partner_label, is_group = _channel_descriptor(message.channel)
        payload = serialize_dm_message(
            message,
            partner_user_id=partner_user_id,
            partner_label=partner_label,
            is_group=is_group,
        )
        payload["type"] = "dm_message"
        await self.ws.send(payload)

    async def on_message_edit(
        self, before: discord.Message, after: discord.Message
    ) -> None:
        if not self._is_dm(after.channel):
            return
        if int(after.channel.id) not in self._backfilled:
            return
        await self.ws.send(
            {
                "type": "dm_message_edit",
                "id": str(after.id),
                "channel_id": str(after.channel.id),
                "content": after.content or "",
            }
        )

    async def on_message_delete(self, message: discord.Message) -> None:
        if not self._is_dm(message.channel):
            return
        if int(message.channel.id) not in self._backfilled:
            return
        await self.ws.send(
            {
                "type": "dm_message_delete",
                "id": str(message.id),
                "channel_id": str(message.channel.id),
            }
        )

    # ---------- entrypoint ----------

    async def run(self) -> None:
        if not self.config.CLIENT_TOKEN:
            raise SystemExit("CLIENT_TOKEN env var is required")
        await self.bot.start(self.config.CLIENT_TOKEN)


def main() -> None:
    load_dotenv()
    config = Config(logger=logger)
    client = DMClient(config)
    try:
        asyncio.run(client.run())
    except KeyboardInterrupt:
        logger.info("Interrupted; shutting down")


if __name__ == "__main__":
    main()
