#!/usr/bin/env python3
"""Run with Python 3.11+, no third-party packages required."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from gift_offer.config import Config, load_dotenv
from gift_offer.service import Service
from gift_offer.store import Store
from gift_offer.telegram import Telegram, TelegramError, TransportError


log = logging.getLogger("gift_offer")
ALLOWED_UPDATES = ["message", "business_connection", "business_message", "callback_query", "deleted_business_messages"]


def health_server(port: int, health: dict):
    if port == 0:
        return None

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in {"/", "/healthz"}:
                self.send_response(404)
                self.end_headers()
                return
            ok = bool(health["ready"] and time.time() - health["last_poll"] < 90)
            payload = json.dumps({"status": "ok" if ok else "starting_or_disconnected"}).encode()
            self.send_response(200 if ok else 503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


async def wait_or_stop(stop: asyncio.Event, seconds: float):
    try:
        await asyncio.wait_for(stop.wait(), seconds)
    except asyncio.TimeoutError:
        pass


async def poll(service: Service, stop: asyncio.Event, health: dict):
    while not stop.is_set():
        offset = int(service.store.get_setting("offset"))
        try:
            updates = await service.api.call("getUpdates", offset=offset, timeout=25,
                                             allowed_updates=ALLOWED_UPDATES, limit=50)
            health["last_poll"] = time.time()
        except TelegramError as exc:
            if exc.code in {401, 409}:
                log.error("Polling stopped: %s. Check the token, webhook and number of running instances.", exc)
                stop.set()
                raise RuntimeError("Telegram rejected polling") from exc
            log.warning("Polling error: %s", exc)
            await wait_or_stop(stop, 5)
            continue
        except TransportError as exc:
            log.warning("Polling error: %s", exc)
            await wait_or_stop(stop, 5)
            continue
        for update in updates:
            if stop.is_set():
                break
            try:
                await service.handle(update)
            except Exception:
                # Preserve the offset on an unexpected processing failure.
                # Offer source uniqueness prevents duplicate creation on redelivery.
                log.exception("Update %s failed; it will be retried", update.get("update_id"))
                await wait_or_stop(stop, 5)
                break
            service.store.set_setting("offset", str(update["update_id"] + 1))


async def maintenance(service: Service, stop: asyncio.Event):
    while not stop.is_set():
        try:
            await service.tick()
        except Exception:
            log.exception("Offer maintenance failed")
        await wait_or_stop(stop, 5)


async def run(config: Config):
    api = Telegram(config.token)
    me = await api.call("getMe")
    webhook = await api.call("getWebhookInfo")
    if webhook.get("url"):
        raise RuntimeError("У бота уже настроен webhook. Отключите его для перехода на polling; бот сам его не удаляет.")
    store = Store(config.db_path)
    service = Service(config, store, api, me["id"])
    store.recover_interrupted_sends(int(time.time()))
    health = {"ready": False, "last_poll": time.time()}
    server = None
    tasks: list[asyncio.Task] = []
    stop = asyncio.Event()
    try:
        server = health_server(config.port, health)
        await api.call("setMyCommands", commands=[
            {"command": "start", "description": "Как создать оффер / Create an offer"},
            {"command": "admin", "description": "Админ-панель"},
            {"command": "terms", "description": "Как работает бот / How the bot works"},
        ])
        if not me.get("can_connect_to_business"):
            log.warning("Enable Secretary Mode (Business Mode) in @BotFather to use .buy in business chats.")
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop.set)
            except (NotImplementedError, RuntimeError):
                pass
        health["ready"] = True
        log.info("@%s started. Business offers expire after %s seconds.", me.get("username", "bot"), config.offer_ttl)
        tasks = [asyncio.create_task(poll(service, stop, health)),
                 asyncio.create_task(maintenance(service, stop))]
        await asyncio.gather(*tasks)
    finally:
        health["ready"] = False
        stop.set()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if server:
            server.shutdown()
            server.server_close()
        store.close()


def main():
    parser = argparse.ArgumentParser(description="Gift Offer Telegram Business bot")
    parser.add_argument("--check", action="store_true", help="Validate configuration without Telegram requests")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        root = Path(__file__).resolve().parent
        os.chdir(root)
        load_dotenv(root / ".env")
        config = Config.from_env()
        if args.check:
            print(f"Конфигурация корректна. Владельцы определяются автоматически. Язык: {config.default_language}.")
            return 0
        asyncio.run(run(config))
    except KeyboardInterrupt:
        return 0
    except (ValueError, RuntimeError, OSError) as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
