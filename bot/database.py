"""MongoDB integration using motor (async driver)."""

from __future__ import annotations
import logging
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

log = logging.getLogger(__name__)


import json
import re
from pathlib import Path

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_LOCAL_DB_FILE = _DATA_DIR / "local_db.json"


class LocalCursor:
    def __init__(self, docs: list[dict]):
        self._docs = docs
        self._sort_key = None
        self._sort_dir = 1
        self._limit = None

    def sort(self, key_or_list, direction=1):
        if isinstance(key_or_list, list):
            self._sort_key = key_or_list[0][0]
            self._sort_dir = key_or_list[0][1]
        else:
            self._sort_key = key_or_list
            self._sort_dir = direction
        return self

    def limit(self, l: int):
        self._limit = l
        return self

    async def to_list(self, length=None):
        res = list(self._docs)
        if self._sort_key:
            res.sort(key=lambda d: d.get(self._sort_key, ""), reverse=(self._sort_dir == -1))
        if self._limit is not None:
            res = res[:self._limit]
        elif length is not None:
            res = res[:length]
        return res

    def __aiter__(self):
        res = list(self._docs)
        if self._sort_key:
            res.sort(key=lambda d: d.get(self._sort_key, ""), reverse=(self._sort_dir == -1))
        if self._limit is not None:
            res = res[:self._limit]
        self._iter = iter(res)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


def _match_doc(doc: dict, query: dict) -> bool:
    if not query:
        return True

    for k, v in query.items():
        if k == "$or":
            if not any(_match_doc(doc, clause) for clause in v):
                return False
            continue

        doc_val = doc.get(k)
        if isinstance(v, dict):
            if "$in" in v:
                if doc_val not in v["$in"]:
                    return False
            elif "$regex" in v:
                pattern = v["$regex"]
                flags = re.IGNORECASE if "i" in v.get("$options", "") else 0
                if not doc_val or not re.search(pattern, str(doc_val), flags):
                    return False
        else:
            if doc_val != v:
                return False
    return True


class LocalCollection:
    def __init__(self, name: str, parent_db):
        self.name = name
        self.parent_db = parent_db

    def _get_docs(self) -> list[dict]:
        return self.parent_db._data.setdefault(self.name, [])

    async def create_index(self, keys, **kwargs):
        pass

    async def insert_one(self, doc: dict):
        docs = self._get_docs()
        new_doc = dict(doc)
        if "_id" not in new_doc:
            new_doc["_id"] = str(len(docs) + 1)
        docs.append(new_doc)
        self.parent_db._save()
        return type("Result", (), {"inserted_id": new_doc["_id"]})()

    async def find_one(self, query: dict = None, projection: dict = None):
        docs = self._get_docs()
        for d in docs:
            if _match_doc(d, query or {}):
                res = dict(d)
                return res
        return None

    def find(self, query: dict = None, projection: dict = None):
        docs = self._get_docs()
        matched = [dict(d) for d in docs if _match_doc(d, query or {})]
        return LocalCursor(matched)

    async def update_one(self, filter: dict, update: dict, upsert: bool = False):
        docs = self._get_docs()
        for d in docs:
            if _match_doc(d, filter):
                if "$set" in update:
                    d.update(update["$set"])
                self.parent_db._save()
                return type("Result", (), {"matched_count": 1, "modified_count": 1})()

        if upsert:
            new_doc = dict(filter)
            if "$set" in update:
                new_doc.update(update["$set"])
            if "_id" not in new_doc:
                new_doc["_id"] = str(len(docs) + 1)
            docs.append(new_doc)
            self.parent_db._save()
            return type("Result", (), {"matched_count": 0, "modified_count": 1})()

        return type("Result", (), {"matched_count": 0, "modified_count": 0})()

    async def delete_one(self, filter: dict):
        docs = self._get_docs()
        for i, d in enumerate(docs):
            if _match_doc(d, filter):
                docs.pop(i)
                self.parent_db._save()
                return type("Result", (), {"deleted_count": 1})()
        return type("Result", (), {"deleted_count": 0})()

    async def delete_many(self, filter: dict):
        docs = self._get_docs()
        initial = len(docs)
        new_docs = [d for d in docs if not _match_doc(d, filter)]
        self.parent_db._data[self.name] = new_docs
        self.parent_db._save()
        return type("Result", (), {"deleted_count": initial - len(new_docs)})()

    async def count_documents(self, filter: dict) -> int:
        docs = self._get_docs()
        return sum(1 for d in docs if _match_doc(d, filter))

    def aggregate(self, pipeline: list):
        docs = self._get_docs()
        groups = {}
        for d in docs:
            group_key = d.get("series_slug")
            if group_key not in groups:
                groups[group_key] = {
                    "_id": group_key,
                    "title": d.get("series_title"),
                    "count": 0,
                }
            groups[group_key]["count"] += 1
        res = list(groups.values())
        res.sort(key=lambda x: str(x.get("title") or ""))
        return LocalCursor(res)


class Database:
    def __init__(self, mongo_uri: str, db_name: str = "animedekho"):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = AsyncIOMotorClient(mongo_uri, serverSelectionTimeoutMS=3000)
        self.db = self.client[db_name]

        self.users = self.db["users"]
        self.library = self.db["library"]
        self.files = self.db["files"]
        self.downloads = self.db["downloads"]
        self.config = self.db["config"]
        self.ai_history = self.db["ai_history"]
        self.ai_facts = self.db["ai_facts"]
        self.channel_mappings = self.db["channel_mappings"]
        self.monitored_items = self.db["monitored_items"]

        self._fallback_mode = False
        self._data: dict[str, list[dict]] = {}
        self._load_local_db()

    def _load_local_db(self):
        try:
            _DATA_DIR.mkdir(parents=True, exist_ok=True)
            if _LOCAL_DB_FILE.exists():
                self._data = json.loads(_LOCAL_DB_FILE.read_text())
        except Exception as e:
            log.warning("Failed to load local fallback DB: %s", e)

    def _save(self):
        if not self._fallback_mode:
            return
        try:
            _DATA_DIR.mkdir(parents=True, exist_ok=True)
            _LOCAL_DB_FILE.write_text(json.dumps(self._data, indent=2))
        except Exception as e:
            log.warning("Failed to save local fallback DB: %s", e)

    def _enable_fallback(self):
        log.warning("⚠️ MongoDB connection unavailable. Switching to Local JSON File Persistence!")
        self._fallback_mode = True
        self.users = LocalCollection("users", self)
        self.library = LocalCollection("library", self)
        self.files = LocalCollection("files", self)
        self.downloads = LocalCollection("downloads", self)
        self.config = LocalCollection("config", self)
        self.ai_history = LocalCollection("ai_history", self)
        self.ai_facts = LocalCollection("ai_facts", self)
        self.channel_mappings = LocalCollection("channel_mappings", self)
        self.monitored_items = LocalCollection("monitored_items", self)

    async def init_indexes(self):
        """Create necessary indexes or switch to local fallback on connection error."""
        try:
            # Ping database to test connection with 3s timeout
            await self.client.admin.command('ping')
            await self.users.create_index("user_id", unique=True)
            await self.channel_mappings.create_index("key", unique=True)
            await self.monitored_items.create_index("item_id", unique=True)
            await self.library.create_index(
                [("series_slug", 1), ("quality", 1), ("part", 1)],
                unique=True,
            )
            await self.files.create_index("file_unique_id", unique=True)
            await self.files.create_index(
                [("series_slug", 1), ("quality", 1), ("episode_key", 1)],
                unique=True,
            )
            await self.downloads.create_index("user_id")
            await self.downloads.create_index("timestamp")
            await self.ai_history.create_index([("chat_id", 1), ("timestamp", 1)])
            await self.ai_facts.create_index([("chat_id", 1), ("key", 1)], unique=True)
            log.info("MongoDB connected and indexes created")
        except Exception as e:
            log.warning("MongoDB ping/index init failed: %s", e)
            self._enable_fallback()

    # ── User management ───────────────────────────────────────────

    async def add_user(self, user_id: int, username: str = "", added_by: int = 0) -> bool:
        """Add approved user. Returns True if newly added."""
        try:
            await self.users.insert_one({
                "user_id": user_id,
                "username": username,
                "added_at": datetime.now(timezone.utc).isoformat(),
                "added_by": added_by,
            })
            return True
        except Exception:
            # Duplicate key = already exists
            return False

    async def remove_user(self, user_id: int) -> bool:
        """Remove user. Returns True if removed."""
        result = await self.users.delete_one({"user_id": user_id})
        return result.deleted_count > 0

    async def is_approved(self, user_id: int) -> bool:
        """Check if user is approved."""
        doc = await self.users.find_one({"user_id": user_id})
        return doc is not None

    async def get_users(self) -> list[int]:
        """Get all approved user IDs."""
        cursor = self.users.find({}, {"user_id": 1})
        return sorted([doc["user_id"] async for doc in cursor])

    # ── Config (channel invite link, etc.) ────────────────────────

    async def get_config(self, key: str, default=None):
        """Get a config value."""
        doc = await self.config.find_one({"_id": key})
        return doc["value"] if doc else default

    async def set_config(self, key: str, value):
        """Set a config value."""
        await self.config.update_one(
            {"_id": key},
            {"$set": {"value": value}},
            upsert=True,
        )

    # ── File cache (duplicate prevention) ────────────────────────

    async def find_cached_file(
        self,
        series_identifier: str,
        episode_key: str = "",
        quality: str = "",
    ) -> dict | None:
        """
        Flexible lookup for a cached anime file in DB.
        Matches series by slug, clean slug, or title (case-insensitive regex),
        normalizes episode keys (e.g. S1E1 == S01E01, movie),
        and matches quality (or any quality if quality='auto' or empty).
        Returns dict with file_id, quality, episode_key, series_title, series_slug or None.
        """
        import re

        clean_id = (series_identifier or "").strip()
        if not clean_id:
            return None

        # Build episode query condition if specified
        ep_condition = None
        if episode_key:
            ep_clean = episode_key.strip()
            if ep_clean.lower() in ("movie", "film"):
                ep_condition = {"$in": ["movie", "Movie", "MOVIE"]}
            else:
                m = re.search(r"S(\d+)E(\d+)", ep_clean, re.I)
                if m:
                    s_num, ep_num = int(m.group(1)), int(m.group(2))
                    variants = {
                        f"S{s_num:02d}E{ep_num:02d}",
                        f"S{s_num}E{ep_num}",
                        f"S{s_num:02d}E{ep_num}",
                        f"S{s_num}E{ep_num:02d}",
                        f"s{s_num:02d}e{ep_num:02d}",
                    }
                    ep_condition = {"$in": list(variants)}
                else:
                    ep_condition = {"$regex": f"^{re.escape(ep_clean)}$", "$options": "i"}

        # Build quality condition
        q_condition = None
        if quality and quality.lower() not in ("auto", "any", ""):
            q_clean = quality.strip()
            if q_clean.lower() in ("4k", "2160p", "2160"):
                q_condition = {"$in": ["4K", "4k", "2160p", "2160P", "2160"]}
            else:
                q_condition = {"$regex": f"^{re.escape(q_clean)}$", "$options": "i"}

        # Build series matching criteria
        slug_candidate = re.sub(r'[^a-zA-Z0-9]+', '-', clean_id).strip('-').lower()
        core_title = re.sub(r'(?i)\s*(season\s*\d+|s\d+|hindi|dubbed|multi-audio|tamil|telugu).*$', '', clean_id).strip()
        core_slug = re.sub(r'[^a-zA-Z0-9]+', '-', core_title).strip('-').lower()

        series_clauses = [
            {"series_slug": clean_id},
            {"series_slug": slug_candidate},
            {"series_slug": {"$regex": f"^{re.escape(slug_candidate)}", "$options": "i"}},
            {"series_title": {"$regex": f"^{re.escape(clean_id)}$", "$options": "i"}},
        ]
        if core_title and core_title != clean_id:
            series_clauses.append({"series_title": {"$regex": f"^{re.escape(core_title)}", "$options": "i"}})
        if core_slug and core_slug != slug_candidate:
            series_clauses.append({"series_slug": {"$regex": f"^{re.escape(core_slug)}", "$options": "i"}})

        query: dict = {"$or": series_clauses}
        if ep_condition:
            query["episode_key"] = ep_condition
        if q_condition:
            query["quality"] = q_condition

        # 1. Try matching with exact criteria
        doc = await self.files.find_one(query)
        if doc:
            return doc

        # 2. Try direct series_slug match if not matched above
        direct_query = {"series_slug": clean_id}
        if ep_condition:
            direct_query["episode_key"] = ep_condition
        if q_condition:
            direct_query["quality"] = q_condition
        doc = await self.files.find_one(direct_query)
        if doc:
            return doc

        return None

    async def get_cached_file(
        self, series_slug: str, quality: str, episode_key: str
    ) -> str | None:
        """
        Check if this series+quality+episode was already downloaded.
        Returns file_id if cached, None otherwise.
        """
        cached = await self.find_cached_file(series_slug, episode_key, quality)
        return cached["file_id"] if cached else None

    async def save_file(
        self,
        series_slug: str,
        series_title: str,
        quality: str,
        episode_key: str,
        file_id: str,
        file_unique_id: str,
    ):
        """Save a downloaded file reference for future cache lookups."""
        try:
            await self.files.update_one(
                {
                    "series_slug": series_slug,
                    "quality": quality,
                    "episode_key": episode_key,
                },
                {"$set": {
                    "series_title": series_title,
                    "file_id": file_id,
                    "file_unique_id": file_unique_id,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }},
                upsert=True,
            )
        except Exception as e:
            log.warning("Failed to save file cache: %s", e)

    # ── Download logging ──────────────────────────────────────────

    async def log_download(
        self,
        user_id: int,
        series_slug: str,
        episode: str,
        quality: str,
        file_id: str,
    ):
        """Log a download to history."""
        await self.downloads.insert_one({
            "user_id": user_id,
            "series_slug": series_slug,
            "episode": episode,
            "quality": quality,
            "file_id": file_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    # ── AI Persistent Memory & Conversation History ───────────────

    async def save_ai_message(self, chat_id: int, role: str, content: str):
        """Save a message to persistent AI conversation history for a chat."""
        if not content:
            return
        now = datetime.now(timezone.utc).isoformat()
        try:
            await self.ai_history.insert_one({
                "chat_id": chat_id,
                "role": role,
                "content": content,
                "timestamp": now,
            })
            # Keep latest 50 messages per chat to keep database bounded
            count = await self.ai_history.count_documents({"chat_id": chat_id})
            if count > 50:
                oldest = await self.ai_history.find({"chat_id": chat_id}).sort("timestamp", 1).limit(count - 40).to_list(length=None)
                if oldest:
                    ids = [doc["_id"] for doc in oldest]
                    await self.ai_history.delete_many({"_id": {"$in": ids}})
        except Exception as e:
            log.warning("Failed to save AI message: %s", e)

    async def get_ai_history(self, chat_id: int, limit: int = 20) -> list[dict[str, str]]:
        """Retrieve recent conversation history in chronological order (oldest to newest)."""
        try:
            cursor = self.ai_history.find(
                {"chat_id": chat_id},
                {"_id": 0, "role": 1, "content": 1},
            ).sort("timestamp", -1).limit(limit)
            docs = await cursor.to_list(length=limit)
            docs.reverse()  # Oldest to newest
            return docs
        except Exception as e:
            log.warning("Failed to retrieve AI history: %s", e)
            return []

    async def clear_ai_history(self, chat_id: int | None = None):
        """Clear conversation history for a specific chat or all chats."""
        try:
            query = {"chat_id": chat_id} if chat_id is not None else {}
            await self.ai_history.delete_many(query)
        except Exception as e:
            log.warning("Failed to clear AI history: %s", e)

    async def save_ai_fact(self, chat_id: int, key: str, value: str):
        """Save or update a persistent long-term memory fact / preference."""
        now = datetime.now(timezone.utc).isoformat()
        try:
            await self.ai_facts.update_one(
                {"chat_id": chat_id, "key": key.strip().lower()},
                {"$set": {
                    "chat_id": chat_id,
                    "key": key.strip().lower(),
                    "value": value.strip(),
                    "updated_at": now,
                }},
                upsert=True,
            )
        except Exception as e:
            log.warning("Failed to save AI memory fact: %s", e)

    async def get_ai_facts(self, chat_id: int) -> list[dict]:
        """Retrieve all stored permanent memory facts for a chat."""
        try:
            cursor = self.ai_facts.find({"chat_id": chat_id}, {"_id": 0, "key": 1, "value": 1, "updated_at": 1})
            return await cursor.to_list(length=100)
        except Exception as e:
            log.warning("Failed to retrieve AI memory facts: %s", e)
            return []

    async def delete_ai_fact(self, chat_id: int, key: str) -> bool:
        """Delete a specific permanent memory fact."""
        try:
            res = await self.ai_facts.delete_one({"chat_id": chat_id, "key": key.strip().lower()})
            return res.deleted_count > 0
        except Exception as e:
            log.warning("Failed to delete AI memory fact: %s", e)
            return False

    # ── Channel Mappings ──────────────────────────────────────────

    async def set_channel_mapping(self, key: str, channel_id: int):
        """Set a channel mapping for a key (slug, genre, or 'default')."""
        clean_key = key.strip().lower()
        now = datetime.now(timezone.utc).isoformat()
        await self.channel_mappings.update_one(
            {"key": clean_key},
            {"$set": {
                "key": clean_key,
                "channel_id": channel_id,
                "updated_at": now,
            }},
            upsert=True,
        )

    async def get_channel_mapping(self, key: str) -> int | None:
        """Get mapped channel ID for a key."""
        clean_key = key.strip().lower()
        doc = await self.channel_mappings.find_one({"key": clean_key})
        return doc["channel_id"] if doc else None

    async def delete_channel_mapping(self, key: str) -> bool:
        """Delete a channel mapping."""
        clean_key = key.strip().lower()
        res = await self.channel_mappings.delete_one({"key": clean_key})
        return res.deleted_count > 0

    async def get_all_channel_mappings(self) -> list[dict]:
        """Get all channel mappings."""
        cursor = self.channel_mappings.find({}, {"_id": 0})
        return await cursor.to_list(length=1000)

    async def get_mapped_channel_for_series(
        self, series_slug: str, genres: list[str] | None = None
    ) -> int:
        """
        Resolve target channel ID for a series.
        Order of precedence:
          1. Direct series_slug mapping
          2. Genre/category mapping (first matching genre)
          3. 'default' mapping key
          4. Fallback to settings.bot.main_channel
        """
        from config.settings import settings

        if series_slug:
            mapped = await self.get_channel_mapping(series_slug)
            if mapped:
                return mapped

        if genres:
            for g in genres:
                g_clean = re.sub(r'[^a-zA-Z0-9]+', '-', g).strip('-').lower()
                mapped = await self.get_channel_mapping(g_clean)
                if mapped:
                    return mapped
                mapped_raw = await self.get_channel_mapping(g.strip().lower())
                if mapped_raw:
                    return mapped_raw

        default_mapped = await self.get_channel_mapping("default")
        if default_mapped:
            return default_mapped

        return settings.bot.main_channel

    # ── Auto Monitoring Tracking ─────────────────────────────────

    async def set_monitor_status(self, enabled: bool):
        """Enable or disable auto monitoring."""
        await self.set_config("auto_monitor_enabled", enabled)

    async def get_monitor_status(self) -> bool:
        """Check if auto monitoring is enabled (default: True)."""
        val = await self.get_config("auto_monitor_enabled", True)
        return bool(val)

    async def is_monitored_item_processed(self, item_id: str) -> bool:
        """Check if an anime episode/movie item has already been auto-processed."""
        doc = await self.monitored_items.find_one({"item_id": item_id})
        return doc is not None

    async def mark_monitored_item_processed(self, item_id: str, item_data: dict | None = None):
        """Mark an item as auto-processed."""
        now = datetime.now(timezone.utc).isoformat()
        try:
            await self.monitored_items.update_one(
                {"item_id": item_id},
                {"$set": {
                    "item_id": item_id,
                    "processed_at": now,
                    "data": item_data or {},
                }},
                upsert=True,
            )
        except Exception as e:
            log.warning("Failed to mark monitored item processed: %s", e)

    def close(self):
        """Close the MongoDB connection."""
        self.client.close()


# Singleton — set during post_init
db: Database | None = None
