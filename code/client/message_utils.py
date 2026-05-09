from __future__ import annotations

import logging
import re
from typing import Optional

import discord

logger = logging.getLogger(__name__)

_MENTION_RE = re.compile(r"<@!?(\d+)>")


def humanize_mentions(content: str, message: discord.Message) -> str:
    """Replace `<@123>` with `@DisplayName` using the channel's recipient list."""
    if not content:
        return content

    id_to_name: dict[str, str] = {}
    channel = message.channel

    # in DM/Group DM, recipients are channel.recipients
    recipients = getattr(channel, "recipients", None) or []
    me = getattr(channel, "me", None)
    pool = list(recipients)
    if me:
        pool.append(me)
    pool.append(message.author)

    for u in pool:
        try:
            id_to_name[str(u.id)] = f"@{getattr(u, 'display_name', None) or u.name}"
        except Exception:
            continue

    def repl(m: re.Match) -> str:
        return id_to_name.get(m.group(1), m.group(0))

    return _MENTION_RE.sub(repl, content)


def serialize_dm_message(
    message: discord.Message,
    *,
    partner_user_id: Optional[int],
    partner_label: str,
    is_group: bool,
) -> dict:
    """Convert a discord.py-self DM message into a wire-safe dict for the server."""
    author = message.author
    avatar_url = None
    try:
        if getattr(author, "avatar", None):
            avatar_url = str(author.avatar.url)
        elif getattr(author, "display_avatar", None):
            avatar_url = str(author.display_avatar.url)
    except Exception:
        avatar_url = None

    data: dict = {
        "id": str(message.id),
        "channel_id": str(message.channel.id),
        "is_group": is_group,
        "partner_user_id": str(partner_user_id) if partner_user_id else None,
        "partner_label": partner_label,
        "timestamp": message.created_at.isoformat() if message.created_at else None,
        "timestamp_unix": int(message.created_at.timestamp()) if message.created_at else None,
        "edited_timestamp": (
            message.edited_at.isoformat() if message.edited_at else None
        ),
        "author": {
            "id": str(author.id),
            "name": str(author.name),
            "display_name": str(getattr(author, "display_name", author.name)),
            "bot": bool(getattr(author, "bot", False)),
            "avatar_url": avatar_url,
        },
        "content": humanize_mentions(message.content or "", message),
    }

    if message.attachments:
        data["attachments"] = [
            {
                "id": str(a.id),
                "filename": a.filename,
                "url": a.url,
                "size": a.size,
                "content_type": getattr(a, "content_type", None),
            }
            for a in message.attachments
        ]

    if message.stickers:
        data["stickers"] = [
            {"id": str(s.id), "name": s.name, "url": str(getattr(s, "url", "") or "")}
            for s in message.stickers
        ]

    if message.embeds:
        try:
            data["embeds"] = [e.to_dict() for e in message.embeds]
        except Exception:
            data["embeds"] = []

    return data
