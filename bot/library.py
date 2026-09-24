"""Main Channel Library System — one album message per series in the Telegram channel."""

from __future__ import annotations
import asyncio
import logging
import os
import re
import tempfile
from datetime import datetime, timezone

import aiohttp
from pyrogram import Client, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from bot.database import Database

log = logging.getLogger(__name__)


async def _download_poster(url: str) -> str | None:
    """Download poster image to a temp file, return path or None."""
    if not url:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    log.warning("Poster download failed: HTTP %d for %s", resp.status, url[:80])
                    return None
                ct = resp.content_type or ""
                ext = ".jpg"
                if "png" in ct:
                    ext = ".png"
                elif "webp" in ct:
                    ext = ".webp"
                data = await resp.read()
                if len(data) < 1000:
                    log.warning("Poster too small (%d bytes), skipping", len(data))
                    return None
                tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False, dir=tempfile.gettempdir())
                tmp.write(data)
                tmp.close()
                return tmp.name
    except Exception as e:
        log.warning("Poster download error: %s", e)
        return None

CAPTION_LIMIT = 1024

# Per-series lock to prevent race conditions
_locks: dict[str, asyncio.Lock] = {}


def _get_lock(key: str) -> asyncio.Lock:
    if key not in _locks:
        _locks[key] = asyncio.Lock()
    if len(_locks) > 500:
        oldest = list(_locks.keys())[:200]
        for k in oldest:
            if not _locks[k].locked():
                _locks.pop(k, None)
    return _locks[key]


class LibraryManager:
    def __init__(self, client: Client, db: Database, main_channel: int, bot_username: str):
        self.client = client
        self.db = db
        self.channel = main_channel
        self.bot_username = bot_username

    async def get_target_channel(self, series_slug: str, genres: list[str] | None = None) -> int:
        """Resolve the target channel ID using database mapping or default channel."""
        if self.db:
            target = await self.db.get_mapped_channel_for_series(series_slug, genres)
            if target:
                return target
        return self.channel

    async def save_to_library(
        self,
        series_slug: str,
        series_title: str,
        quality: str,
        episode_key: str,
        file_id: str,
        file_unique_id: str,
        poster_url: str | None = None,
        is_movie: bool = False,
        genres: list[str] | None = None,
        target_channel: int | None = None,
    ):
        """
        Save a downloaded file and update/create the single series album
        message in the mapped Telegram channel.

        One message per series — always updated, never duplicated.
        """
        channel_id = target_channel or await self.get_target_channel(series_slug, genres)
        if not channel_id:
            return

        async with _get_lock(series_slug):
            await self._save_locked(
                series_slug, series_title, quality, episode_key,
                file_id, file_unique_id, poster_url, is_movie, channel_id,
            )

    async def _save_locked(
        self,
        series_slug: str,
        series_title: str,
        quality: str,
        episode_key: str,
        file_id: str,
        file_unique_id: str,
        poster_url: str | None,
        is_movie: bool,
        channel_id: int,
    ):
        now = datetime.now(timezone.utc).isoformat()

        # Save file mapping
        await self.db.files.update_one(
            {
                "series_slug": series_slug,
                "quality": quality,
                "episode_key": episode_key,
            },
            {"$set": {
                "file_id": file_id,
                "file_unique_id": file_unique_id,
                "series_slug": series_slug,
                "series_title": series_title,
                "episode_key": episode_key,
                "quality": quality,
                "updated_at": now,
            }},
            upsert=True,
        )

        # Get ALL files for this series (all episodes, all qualities)
        cursor = self.db.files.find({"series_slug": series_slug})
        all_files = await cursor.to_list(length=None)

        # Build episode/quality map
        episodes: dict[str, set[str]] = {}  # ep_key -> set of qualities
        all_qualities: set[str] = set()
        for f in all_files:
            ep = f["episode_key"]
            q = f["quality"]
            episodes.setdefault(ep, set()).add(q)
            all_qualities.add(q)

        # Sort episodes
        sorted_eps = sorted(episodes.keys(), key=_ep_sort_key)
        sorted_qualities = _sort_qualities(all_qualities)

        # Build caption
        caption = self._format_album_caption(
            series_title, sorted_eps, sorted_qualities, is_movie, poster_url,
        )

        # Build buttons
        markup = self._build_album_buttons(series_slug, sorted_eps, sorted_qualities, is_movie)

        # Check if album message already exists for this series
        entry = await self.db.library.find_one({"series_slug": series_slug, "type": "album"})

        if entry and entry.get("message_id"):
            msg_id = entry["message_id"]
            old_channel = entry.get("channel_id", channel_id)
            try:
                if old_channel == channel_id:
                    if entry.get("has_poster"):
                        await self.client.edit_message_caption(
                            chat_id=channel_id,
                            message_id=msg_id,
                            caption=caption[:CAPTION_LIMIT],
                            parse_mode=enums.ParseMode.HTML,
                            reply_markup=markup,
                        )
                    else:
                        await self.client.edit_message_text(
                            chat_id=channel_id,
                            message_id=msg_id,
                            text=caption[:CAPTION_LIMIT],
                            parse_mode=enums.ParseMode.HTML,
                            disable_web_page_preview=True,
                            reply_markup=markup,
                        )
                    # Update DB entry
                    await self.db.library.update_one(
                        {"_id": entry["_id"]},
                        {"$set": {
                            "series_title": series_title,
                            "episode_count": len(sorted_eps),
                            "qualities": sorted_qualities,
                            "updated_at": now,
                            "channel_id": channel_id,
                            "poster_url": poster_url or entry.get("poster_url"),
                        }},
                    )
                    log.info("Updated album for %s: %d episodes in channel %d",
                             series_slug, len(sorted_eps), channel_id)
                    return
                else:
                    # Target channel changed — try deleting old message
                    try:
                        await self.client.delete_messages(old_channel, msg_id)
                    except Exception:
                        pass
            except Exception as e:
                log.warning("Failed to update album message %d in channel %d, recreating: %s", msg_id, old_channel, e)
                # Delete old message if possible
                try:
                    await self.client.delete_messages(old_channel, msg_id)
                except Exception:
                    pass

        # Create new album message
        try:
            has_poster = False
            poster_path = None
            if poster_url:
                poster_path = await _download_poster(poster_url)

            if poster_path:
                try:
                    msg = await self.client.send_photo(
                        chat_id=channel_id,
                        photo=poster_path,
                        caption=caption[:CAPTION_LIMIT],
                        parse_mode=enums.ParseMode.HTML,
                        reply_markup=markup,
                    )
                    has_poster = True
                except Exception as e:
                    log.warning("Failed to send poster photo to channel %d, sending text: %s", channel_id, e)
                    msg = await self.client.send_message(
                        chat_id=channel_id,
                        text=caption[:CAPTION_LIMIT],
                        parse_mode=enums.ParseMode.HTML,
                        disable_web_page_preview=True,
                        reply_markup=markup,
                    )
                finally:
                    try:
                        os.remove(poster_path)
                    except Exception:
                        pass
            else:
                if poster_url:
                    log.warning("Poster URL exists but download failed: %s", poster_url[:100])
                msg = await self.client.send_message(
                    chat_id=channel_id,
                    text=caption[:CAPTION_LIMIT],
                    parse_mode=enums.ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=markup,
                )

            # Upsert album entry (one per series)
            await self.db.library.update_one(
                {"series_slug": series_slug, "type": "album"},
                {"$set": {
                    "series_slug": series_slug,
                    "series_title": series_title,
                    "type": "album",
                    "channel_id": channel_id,
                    "message_id": msg.id,
                    "has_poster": has_poster,
                    "poster_url": poster_url,
                    "episode_count": len(sorted_eps),
                    "qualities": sorted_qualities,
                    "updated_at": now,
                }},
                upsert=True,
            )
            log.info("Created album for %s: %d episodes in channel %d", series_slug, len(sorted_eps), channel_id)
        except Exception as e:
            log.error("Failed to create album message in channel %d: %s", channel_id, e)

    def _format_album_caption(
        self,
        title: str,
        episodes: list[str],
        qualities: list[str],
        is_movie: bool,
        poster_url: str | None,
    ) -> str:
        import html as htmlmod
        title_esc = htmlmod.escape(title)
        audio = "Multi Audio (Japanese, English & Hindi)"
        quality_str = " | ".join(qualities)

        if is_movie:
            ep_info = "🎬 Movie"
        else:
            # Group episodes by season
            seasons: dict[int, list[int]] = {}
            for ep in episodes:
                m = re.match(r"S(\d+)E(\d+)", ep, re.IGNORECASE)
                if m:
                    s, e = int(m.group(1)), int(m.group(2))
                    seasons.setdefault(s, []).append(e)

            ep_lines = []
            for s_num in sorted(seasons.keys()):
                eps = sorted(seasons[s_num])
                if len(eps) <= 3:
                    ep_str = ", ".join(str(e) for e in eps)
                else:
                    ep_str = f"{eps[0]}-{eps[-1]}"
                ep_lines.append(f"Season {s_num}: Episode {ep_str}")

            ep_info = "\n".join(f"➥ {line}" for line in ep_lines) if ep_lines else f"➥ {len(episodes)} episode(s)"

        caption = (
            f"◆ {title_esc} ◆ ❞\n"
            f"⟐━━━━━━━━━━━━━━━━━⟐\n"
            f"{ep_info}\n"
            f"➥ Qᴜᴀʟɪᴛʏ:- {quality_str}\n"
            f"➥ Aᴜᴅɪᴏ:- {audio}\n"
            f"➥ Tᴏᴛᴀʟ:- {len(episodes)} {'file' if len(episodes) == 1 else 'files'}\n"
            f"⟐━━━━━━━━━━━━━━━━━⟐\n"
            f"⟲ Pᴏᴡᴇʀᴇᴅ ʙʏ:- @{self.bot_username}"
        )
        return caption

    def _build_album_buttons(
        self,
        series_slug: str,
        episodes: list[str],
        qualities: list[str],
        is_movie: bool,
    ) -> InlineKeyboardMarkup:
        buttons = []

        if is_movie:
            # One row per quality for movies
            row = []
            for q in qualities:
                deep = f"https://t.me/{self.bot_username}?start=get_{series_slug}_{q}_movie"
                row.append(InlineKeyboardButton(f"📥 {q}", url=deep))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
        else:
            # "Get All" button per quality
            for q in qualities:
                deep = f"https://t.me/{self.bot_username}?start=get_{series_slug}_{q}_all"
                buttons.append([InlineKeyboardButton(f"📥 Get All Episodes [{q}]", url=deep)])

        return InlineKeyboardMarkup(buttons)

    async def get_file(self, series_slug: str, quality: str, episode_key: str) -> str | None:
        """Get file_id for a specific episode."""
        entry = await self.db.files.find_one({
            "series_slug": series_slug,
            "quality": quality,
            "episode_key": episode_key,
        })
        return entry.get("file_id") if entry else None

    async def delete_album(self, series_slug: str):
        """Delete the album message for a series from the channel."""
        entry = await self.db.library.find_one({"series_slug": series_slug, "type": "album"})
        if entry and entry.get("message_id"):
            ch_id = entry.get("channel_id", self.channel)
            try:
                await self.client.delete_messages(ch_id, entry["message_id"])
            except Exception as e:
                log.warning("Could not delete album message: %s", e)
        await self.db.library.delete_many({"series_slug": series_slug})


def _ep_sort_key(key: str):
    m = re.match(r"S(\d+)E(\d+)", key, re.IGNORECASE)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    if key.lower() == "movie":
        return (0, 0)
    return (999, 0)


def _sort_qualities(qualities: set[str]) -> list[str]:
    order = {"360p": 1, "480p": 2, "720p": 3, "1080p": 4, "auto": 5}
    return sorted(qualities, key=lambda q: order.get(q, 99))


# Singleton
library_manager: LibraryManager | None = None
