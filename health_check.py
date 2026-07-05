"""
health_check.py - keeps the bot alive on platforms like Koyeb's free tier.

- Runs a tiny aiohttp web server with a /health endpoint so Koyeb's
  health checker gets a 200 OK.
- Optionally self-pings its own public URL (PING_URL) every
  PING_INTERVAL_SECONDS so the service is never considered idle.
"""

import os
import asyncio
import logging
from aiohttp import web, ClientSession, ClientTimeout

logger = logging.getLogger("health_check")

PORT = int(os.getenv("PORT", "8000"))
PING_URL = os.getenv("PING_URL")  # e.g. https://your-app.koyeb.app/health
PING_INTERVAL = int(os.getenv("PING_INTERVAL_SECONDS", "600"))  # 10 min default


async def _health(request):
    return web.Response(text="OK")


async def start_health_server():
    """Starts the aiohttp server in the current event loop (non-blocking)."""
    app = web.Application()
    app.router.add_get("/", _health)
    app.router.add_get("/health", _health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Health check server started on port %s", PORT)


async def self_ping_loop():
    """Periodically pings PING_URL so the platform doesn't sleep the app."""
    if not PING_URL:
        logger.warning("PING_URL not set - skipping self-ping loop.")
        return

    timeout = ClientTimeout(total=15)
    async with ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with session.get(PING_URL) as resp:
                    logger.info("Self-ping -> %s (status %s)", PING_URL, resp.status)
            except Exception as e:
                logger.warning("Self-ping failed: %s", e)
            await asyncio.sleep(PING_INTERVAL)
