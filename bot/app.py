"""Application factory — builds and configures the Pyrogram bot."""

import os
import logging
from aiohttp import web

from pyrogram import Client

from config.settings import settings

log = logging.getLogger(__name__)

active_bot_client: Client | None = None
_health_runner: web.AppRunner | None = None


async def _start_health_server():
    """Start lightweight HTTP server for deployment health checks (Koyeb/Railway/Render)."""
    global _health_runner
    port_str = os.environ.get("PORT", "8000")
    try:
        port = int(port_str)
    except ValueError:
        port = 8000

    app = web.Application()

    async def _health_handler(request):
        return web.Response(text="OK", status=200)

    app.router.add_get("/", _health_handler)
    app.router.add_get("/health", _health_handler)

    _health_runner = web.AppRunner(app)
    await _health_runner.setup()
    site = web.TCPSite(_health_runner, "0.0.0.0", port)
    await site.start()
    log.info("HTTP health check server started on port %d", port)


async def _stop_health_server():
    global _health_runner
    if _health_runner:
        await _health_runner.cleanup()
        _health_runner = None
        log.info("HTTP health check server stopped")


async def _on_start(client: Client):
    """Called after client starts — init HTTP client, DB & logger."""
    global active_bot_client
    active_bot_client = client

    from utils.http import http_client
    await http_client.start()
    log.info("HTTP client started")

    # Init MongoDB
    from bot.database import Database
    import bot.database as db_mod
    db = Database(settings.bot.mongo_uri)
    await db.init_indexes()
    db_mod.db = db
    log.info("MongoDB connected")

    # Init AI config
    from bot.ai import ai_config
    await ai_config.load()
    log.info("AI Agent config loaded (model: %s)", ai_config.model)

    # Init bot logger
    from bot.logger import BotLogger
    import bot.logger as logger_mod
    logger_mod.bot_logger = BotLogger(client)
    log.info("Bot logger initialized")

    # Init Library Manager
    from bot.library import LibraryManager
    import bot.library as lib_mod
    me = await client.get_me()
    bot_username = me.username or ""
    lib_mod.library_manager = LibraryManager(
        client=client,
        db=db,
        main_channel=settings.bot.main_channel,
        bot_username=bot_username,
    )
    log.info("Library manager initialized (bot: @%s)", bot_username)

    # Start HTTP Health Check Server for platform deployment health checks
    await _start_health_server()

    # Init Auto Monitor Service
    from bot.monitor import AutoMonitor
    import bot.monitor as monitor_mod
    monitor_mod.auto_monitor = AutoMonitor(client=client, interval_seconds=600)
    monitor_mod.auto_monitor.start()
    log.info("Auto monitor initialized and started")

    # Resolve channel peers so Pyrogram can send to them
    # Try get_chat first, fall back to raw API (needed on fresh sessions)
    for name, cid in [("main", settings.bot.main_channel), ("log", settings.bot.log_channel)]:
        if cid:
            try:
                chat = await client.get_chat(cid)
                log.info("Resolved %s channel: %s (id: %d)", name, chat.title, chat.id)
            except Exception:
                # Raw API fallback for fresh sessions without cached peers
                try:
                    from pyrogram.raw.functions.channels import GetChannels
                    from pyrogram.raw.types import InputChannel
                    raw_id = abs(cid) % (10 ** 10)  # Strip -100 prefix
                    peer = InputChannel(channel_id=raw_id, access_hash=0)
                    result = await client.invoke(GetChannels(id=[peer]))
                    if result.chats:
                        log.info("Resolved %s channel via raw API: %s", name, result.chats[0].title)
                    else:
                        log.warning("Could not resolve %s channel %d via raw API", name, cid)
                except Exception as e2:
                    log.warning("Could not resolve %s channel %d: %s", name, cid, e2)
    # Auto-generate channel invite link if not set
    if settings.bot.main_channel:
        try:
            from bot.database import db as app_db
            existing_link = await app_db.get_config("channel_invite_link") if app_db else None
            if not existing_link:
                chat = await client.get_chat(settings.bot.main_channel)
                if chat.invite_link:
                    invite_link = chat.invite_link
                else:
                    invite_link = (await client.create_chat_invite_link(settings.bot.main_channel)).invite_link
                if invite_link and app_db:
                    await app_db.set_config("channel_invite_link", invite_link)
                    log.info("Auto-set channel invite link: %s", invite_link)
        except Exception as e:
            log.warning("Could not auto-generate invite link: %s", e)

    # Set bot commands menu
    from pyrogram.types import BotCommand, BotCommandScopeChat, BotCommandScopeDefault
    try:
        # Default commands for everyone
        await client.set_bot_commands([
            BotCommand("start", "Main menu"),
            BotCommand("search", "Search anime or movies"),
            BotCommand("help", "Show help message"),
        ], scope=BotCommandScopeDefault())

        # Owner commands (shown to the owner)
        if settings.bot.owner_id:
            await client.set_bot_commands([
                BotCommand("start", "Main menu"),
                BotCommand("search", "Search anime or movies"),
                BotCommand("ai", "Autonomous AI Agent"),
                BotCommand("setai", "Configure AI model, key & persona"),
                BotCommand("help", "Show help message"),
                BotCommand("adduser", "Approve a user"),
                BotCommand("removeuser", "Remove a user"),
                BotCommand("users", "List approved users"),
                BotCommand("addchannel", "Map a channel for slug/genre/default"),
                BotCommand("removechannel", "Remove a channel mapping"),
                BotCommand("channels", "List channel mappings"),
                BotCommand("monitor", "Auto monitoring website control"),
                BotCommand("setchannellink", "Set channel invite link"),
                BotCommand("delete", "Delete a series or file"),
            ], scope=BotCommandScopeChat(settings.bot.owner_id))
        log.info("Bot commands menu set successfully")
    except Exception as e:
        log.warning("Failed to set bot commands: %s", e)


async def _on_stop(client: Client):
    """Called on shutdown — cleanup."""
    await _stop_health_server()

    from bot.monitor import auto_monitor
    if auto_monitor:
        await auto_monitor.stop()

    from utils.http import http_client
    await http_client.close()
    from bot.database import db
    if db:
        db.close()
    log.info("Auto monitor stopped, HTTP client & MongoDB closed")


def create_app() -> Client:
    """Build the Pyrogram Client with all handlers registered."""
    if not settings.bot.token:
        raise RuntimeError("BOT_TOKEN environment variable is required")
    if not settings.bot.owner_id:
        raise RuntimeError("OWNER_ID environment variable is required")
    if not settings.bot.api_id:
        raise RuntimeError("API_ID environment variable is required")
    if not settings.bot.api_hash:
        raise RuntimeError("API_HASH environment variable is required")

    app = Client(
        "animedekho_bot",
        api_id=settings.bot.api_id,
        api_hash=settings.bot.api_hash,
        bot_token=settings.bot.token,
        workers=20,
    )

    # Register startup/shutdown hooks
    app.on_start = _on_start
    app.on_stop = _on_stop

    # Register all handlers
    from bot.handlers import register_handlers
    register_handlers(app)

    return app
