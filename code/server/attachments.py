from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import List, Optional

import aiohttp
import discord

logger = logging.getLogger(__name__)


@dataclass
class AttachmentResult:
    """One attachment after a download attempt."""
    file: Optional[discord.File]
    placeholder: Optional[str]
    filename: str
    url: str

    @property
    def succeeded(self) -> bool:
        return self.file is not None


async def reupload_attachment(
    session: aiohttp.ClientSession,
    *,
    url: str,
    filename: str,
    max_bytes: int,
) -> AttachmentResult:
    """Stream a Discord CDN attachment into memory and wrap it as a discord.File.

    On any failure (HTTP error, expired CDN URL, file too large), returns an
    AttachmentResult with `placeholder` set to a markdown line that can be
    appended to the message body in lieu of the real file.
    """
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status != 200:
                return AttachmentResult(
                    file=None,
                    placeholder=f"`[attachment unavailable: {filename} (HTTP {resp.status})]`",
                    filename=filename,
                    url=url,
                )

            buf = io.BytesIO()
            total = 0
            async for chunk in resp.content.iter_chunked(64 * 1024):
                total += len(chunk)
                if total > max_bytes:
                    return AttachmentResult(
                        file=None,
                        placeholder=f"`[attachment too large to mirror: {filename}]` <{url}>",
                        filename=filename,
                        url=url,
                    )
                buf.write(chunk)
            buf.seek(0)
            return AttachmentResult(
                file=discord.File(buf, filename=filename),
                placeholder=None,
                filename=filename,
                url=url,
            )
    except Exception as e:
        logger.warning("Attachment re-upload failed for %s: %s", filename, e)
        return AttachmentResult(
            file=None,
            placeholder=f"`[attachment expired: {filename}]`",
            filename=filename,
            url=url,
        )


async def reupload_all(
    session: aiohttp.ClientSession,
    attachments: List[dict],
    *,
    max_bytes: int,
) -> List[AttachmentResult]:
    """Download every attachment in `attachments` (the serialized dict form)."""
    import asyncio

    if not attachments:
        return []

    tasks = [
        reupload_attachment(
            session,
            url=a.get("url") or "",
            filename=a.get("filename") or "file",
            max_bytes=max_bytes,
        )
        for a in attachments
    ]
    return await asyncio.gather(*tasks)
