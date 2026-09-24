"""Auto Monitoring Website Service — periodically monitors website for new anime releases and auto downloads/uploads them."""

from __future__ import annotations
import asyncio
import logging
import os
import re
import tempfile
from pathlib import Path

from pyrogram import Client

from api.client import api
from api.models import SearchResult, Series, Movie, Episode, Quality, VideoServer
from bot.database import db
from bot.downloader import download_media, make_episode_filename, make_movie_filename
from bot.library import library_manager
from bot.logger import bot_logger
from config.settings import settings

log = logging.getLogger(__name__)

_TEMP_BASE = Path(tempfile.gettempdir()) / "animedekho_dl"
_TEMP_BASE.mkdir(parents=True, exist_ok=True)


class AutoMonitor:
    def __init__(self, client: Client, interval_seconds: int = 600):
        self.client = client
        self.interval = interval_seconds
        self._task: asyncio.Task | None = None
        self._running = False

    def start(self):
        """Start the auto monitoring background task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        log.info("Auto monitor service started (interval: %ds)", self.interval)

    async def stop(self):
        """Stop the auto monitoring service."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        log.info("Auto monitor service stopped")

    async def _monitor_loop(self):
        # Initial delay before first check
        await asyncio.sleep(10)

        while self._running:
            try:
                enabled = await db.get_monitor_status() if db else True
                if enabled:
                    log.info("Running auto monitoring check cycle...")
                    await self.check_new_content()
                else:
                    log.debug("Auto monitoring is disabled in config")
            except Exception as e:
                log.exception("Error in auto monitor loop: %s", e)
                if bot_logger:
                    await bot_logger.log_error("AutoMonitor._monitor_loop", str(e))

            await asyncio.sleep(self.interval)

    async def check_new_content(self) -> dict:
        """
        Check website for recent series and movies, download any newly released episode/movie.
        Returns summary dict of processed count.
        """
        summary = {"series_checked": 0, "movies_checked": 0, "downloaded": 0, "errors": 0}

        if not db:
            log.warning("Database not initialized, skipping auto monitor check")
            return summary

        # 1. Check Recent Series
        try:
            recent_series_res = await api.get_recent_series(page=1)
            series_items = recent_series_res.items[:10] if recent_series_res and recent_series_res.items else []
            summary["series_checked"] = len(series_items)

            for item in series_items:
                try:
                    downloaded_count = await self._process_series(item.slug)
                    summary["downloaded"] += downloaded_count
                except Exception as e:
                    summary["errors"] += 1
                    log.error("Failed to process series %s in auto monitor: %s", item.slug, e)
        except Exception as e:
            log.error("Failed to fetch recent series in auto monitor: %s", e)

        # 2. Check Recent Movies
        try:
            recent_movies_res = await api.get_recent_movies(page=1)
            movie_items = recent_movies_res.items[:10] if recent_movies_res and recent_movies_res.items else []
            summary["movies_checked"] = len(movie_items)

            for item in movie_items:
                try:
                    downloaded_count = await self._process_movie(item.slug)
                    summary["downloaded"] += downloaded_count
                except Exception as e:
                    summary["errors"] += 1
                    log.error("Failed to process movie %s in auto monitor: %s", item.slug, e)
        except Exception as e:
            log.error("Failed to fetch recent movies in auto monitor: %s", e)

        log.info("Auto monitor check complete: %d series, %d movies checked, %d downloaded/uploaded",
                 summary["series_checked"], summary["movies_checked"], summary["downloaded"])
        return summary

    async def _process_series(self, slug: str) -> int:
        """Process a series, checking each episode for new releases."""
        downloaded = 0
        try:
            series = await api.get_series(slug)
            if not series or not series.seasons:
                return 0

            for season_num, season_obj in series.seasons.items():
                for ep in season_obj.episodes:
                    item_id = f"ep:{slug}:{season_num}:{ep.number}"
                    if await db.is_monitored_item_processed(item_id):
                        continue

                    ep_key = f"S{season_num}E{ep.number:02d}"
                    # Check if already in files collection
                    cached = await db.get_cached_file(slug, "720p", ep_key)
                    if not cached:
                        cached = await db.get_cached_file(slug, "auto", ep_key)

                    if cached:
                        await db.mark_monitored_item_processed(item_id, {"status": "already_cached", "file_id": cached})
                        continue

                    # Process download/upload
                    log.info("Auto-monitor detected new episode: %s S%dE%d", series.title, season_num, ep.number)
                    success = await self._auto_download_episode(series, season_num, ep)
                    if success:
                        downloaded += 1
                        await db.mark_monitored_item_processed(item_id, {"status": "downloaded", "ep_key": ep_key})
                        # Pause between downloads
                        await asyncio.sleep(5)
                    else:
                        log.warning("Auto download failed for %s S%dE%d", series.title, season_num, ep.number)

        except Exception as e:
            log.error("Error processing series %s: %s", slug, e)

        return downloaded

    async def _auto_download_episode(self, series: Series, season_num: int, ep: Episode) -> bool:
        """Download a newly released episode and upload to mapped Telegram channel."""
        try:
            full_ep = await api.get_episode(ep.slug)
            if not full_ep or not full_ep.servers:
                log.warning("No servers for auto ep %s", ep.slug)
                return False

            quality_pref = "720p"
            resolved_servers = await api.resolve_all_servers(full_ep.servers)
            if not resolved_servers:
                resolved_servers = full_ep.servers

            from bot.handlers.callbacks import _lazy_resolve_servers, _find_quality_candidates
            resolved = await _lazy_resolve_servers(resolved_servers, quality_pref)
            candidates = _find_quality_candidates(resolved or resolved_servers, quality_pref)

            if not candidates:
                candidates = _find_quality_candidates(resolved or resolved_servers, "480p")
            if not candidates:
                candidates = _find_quality_candidates(resolved or resolved_servers, "auto")

            target_channel = await db.get_mapped_channel_for_series(series.slug, series.genres)
            if not target_channel:
                log.warning("No channel configured for auto upload of %s — use /addchannel default <id> or set MAIN_CHANNEL env var", series.slug)
                return False

            title = f"{series.title} S{season_num}E{ep.number:02d}"
            ep_key = f"S{season_num}E{ep.number:02d}"

            # Create candidate list including fallbacks (AnimeDrive -> ToonFlix)
            success = False
            file_id = None
            file_unique_id = None
            chosen_quality_str = quality_pref

            for srv, q in candidates:
                chosen_quality_str = q.resolution
                filename = make_episode_filename(series.title, season_num, ep.number, chosen_quality_str)
                output_path = str(_TEMP_BASE / filename)

                log.info("Auto-monitoring downloading %s via %s [%s]", title, srv.name, chosen_quality_str)
                dl_ok = await download_media(
                    q.master_url or q.url, chosen_quality_str, output_path,
                    title=title, variant_url=q.url,
                )

                if dl_ok and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                    # Upload to channel
                    try:
                        sent_msg = await self.client.send_document(
                            chat_id=target_channel,
                            document=output_path,
                            file_name=filename,
                            caption=f"📺 <b>{title}</b> [{chosen_quality_str}]\n🤖 <i>Auto Uploaded</i>",
                        )
                        if sent_msg.video:
                            file_id = sent_msg.video.file_id
                            file_unique_id = sent_msg.video.file_unique_id
                        elif sent_msg.document:
                            file_id = sent_msg.document.file_id
                            file_unique_id = sent_msg.document.file_unique_id

                        success = True
                    except Exception as upload_err:
                        log.error("Failed to upload auto-monitored file to channel %d: %s", target_channel, upload_err)
                    finally:
                        if os.path.exists(output_path):
                            try: os.remove(output_path)
                            except Exception: pass

                if success:
                    break

            if success and file_id and file_unique_id:
                # Save file cache
                await db.save_file(
                    series_slug=series.slug,
                    series_title=series.title,
                    quality=chosen_quality_str,
                    episode_key=ep_key,
                    file_id=file_id,
                    file_unique_id=file_unique_id,
                )

                # Save to library
                if library_manager:
                    await library_manager.save_to_library(
                        series_slug=series.slug,
                        series_title=series.title,
                        quality=chosen_quality_str,
                        episode_key=ep_key,
                        file_id=file_id,
                        file_unique_id=file_unique_id,
                        poster_url=series.poster,
                        genres=series.genres,
                        target_channel=target_channel,
                    )

                if bot_logger:
                    await bot_logger.log_download_complete(title, chosen_quality_str, 0)

                return True

        except Exception as e:
            log.exception("Error in _auto_download_episode for %s: %s", ep.slug, e)

        return False

    async def _process_movie(self, slug: str) -> int:
        """Process a movie, checking if it was already processed."""
        item_id = f"movie:{slug}"
        if await db.is_monitored_item_processed(item_id):
            return 0

        cached = await db.get_cached_file(slug, "720p", "movie")
        if not cached:
            cached = await db.get_cached_file(slug, "auto", "movie")

        if cached:
            await db.mark_monitored_item_processed(item_id, {"status": "already_cached", "file_id": cached})
            return 0

        try:
            movie = await api.get_movie(slug)
            if not movie or not movie.servers:
                return 0

            log.info("Auto-monitor detected new movie: %s", movie.title)
            success = await self._auto_download_movie(movie)
            if success:
                await db.mark_monitored_item_processed(item_id, {"status": "downloaded"})
                return 1
        except Exception as e:
            log.error("Error processing movie %s: %s", slug, e)

        return 0

    async def _auto_download_movie(self, movie: Movie) -> bool:
        """Download a newly released movie and upload to mapped Telegram channel."""
        try:
            quality_pref = "720p"
            resolved_servers = await api.resolve_all_servers(movie.servers)
            if not resolved_servers:
                resolved_servers = movie.servers

            from bot.handlers.callbacks import _lazy_resolve_servers, _find_quality_candidates
            resolved = await _lazy_resolve_servers(resolved_servers, quality_pref)
            candidates = _find_quality_candidates(resolved or resolved_servers, quality_pref)

            if not candidates:
                candidates = _find_quality_candidates(resolved or resolved_servers, "480p")
            if not candidates:
                candidates = _find_quality_candidates(resolved or resolved_servers, "auto")

            target_channel = await db.get_mapped_channel_for_series(movie.slug, movie.genres)
            if not target_channel:
                log.warning("No channel configured for auto upload of movie %s — use /addchannel default <id> or set MAIN_CHANNEL env var", movie.slug)
                return False

            title = movie.title
            success = False
            file_id = None
            file_unique_id = None
            chosen_quality_str = quality_pref

            for srv, q in candidates:
                chosen_quality_str = q.resolution
                filename = make_movie_filename(movie.title, chosen_quality_str)
                output_path = str(_TEMP_BASE / filename)

                log.info("Auto-monitoring downloading movie %s via %s [%s]", title, srv.name, chosen_quality_str)
                dl_ok = await download_media(
                    q.master_url or q.url, chosen_quality_str, output_path,
                    title=title, variant_url=q.url,
                )

                if dl_ok and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                    try:
                        sent_msg = await self.client.send_document(
                            chat_id=target_channel,
                            document=output_path,
                            file_name=filename,
                            caption=f"🎬 <b>{title}</b> [{chosen_quality_str}]\n🤖 <i>Auto Uploaded</i>",
                        )
                        if sent_msg.video:
                            file_id = sent_msg.video.file_id
                            file_unique_id = sent_msg.video.file_unique_id
                        elif sent_msg.document:
                            file_id = sent_msg.document.file_id
                            file_unique_id = sent_msg.document.file_unique_id

                        success = True
                    except Exception as upload_err:
                        log.error("Failed to upload auto-monitored movie to channel %d: %s", target_channel, upload_err)
                    finally:
                        if os.path.exists(output_path):
                            try: os.remove(output_path)
                            except Exception: pass

                if success:
                    break

            if success and file_id and file_unique_id:
                await db.save_file(
                    series_slug=movie.slug,
                    series_title=movie.title,
                    quality=chosen_quality_str,
                    episode_key="movie",
                    file_id=file_id,
                    file_unique_id=file_unique_id,
                )

                if library_manager:
                    await library_manager.save_to_library(
                        series_slug=movie.slug,
                        series_title=movie.title,
                        quality=chosen_quality_str,
                        episode_key="movie",
                        file_id=file_id,
                        file_unique_id=file_unique_id,
                        poster_url=movie.poster,
                        is_movie=True,
                        genres=movie.genres,
                        target_channel=target_channel,
                    )

                if bot_logger:
                    await bot_logger.log_download_complete(title, chosen_quality_str, 0)

                return True

        except Exception as e:
            log.exception("Error in _auto_download_movie for %s: %s", movie.slug, e)

        return False


# Singleton
auto_monitor: AutoMonitor | None = None
