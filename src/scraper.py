import os
import sys
import json
import time
import asyncio
import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from loguru import logger
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import MessageMediaPhoto

from datalake import write_channel_messages_json, write_manifest


def setup_logging(date_str: str) -> None:
    os.makedirs("logs", exist_ok=True)
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add(
        os.path.join("logs", f"scrape_{date_str}.log"),
        level="INFO",
        encoding="utf-8"
    )


def normalize_channel(channel: str) -> str:
    # Accept "https://t.me/xyz", "@xyz", or "xyz"
    channel = channel.strip()
    if "t.me/" in channel:
        channel = channel.split("t.me/")[-1]
    if channel.startswith("@"):
        channel = channel[1:]
    return channel


async def scrape_channel(
    client: TelegramClient,
    channel_username: str,
    base_path: str,
    date_str: str,
    limit: int,
    message_delay: float,
) -> int:
    """
    Scrape messages for one channel and store:
      - JSON: data/raw/telegram_messages/YYYY-MM-DD/<channel>.json
      - Images: data/raw/images/<channel>/<message_id>.jpg
    """
    channel_name = normalize_channel(channel_username)
    logger.info(f"Scraping channel={channel_name} limit={limit}")

    entity = await client.get_entity(channel_username if channel_username.startswith("@") else f"@{channel_name}")

    channel_image_dir = os.path.join(base_path, "raw", "images", channel_name)
    os.makedirs(channel_image_dir, exist_ok=True)

    messages: List[Dict[str, Any]] = []
    count = 0

    async for msg in client.iter_messages(entity, limit=limit):
        try:
            has_media = msg.media is not None
            image_path: Optional[str] = None

            # download only photos
            if has_media and isinstance(msg.media, MessageMediaPhoto):
                filename = f"{msg.id}.jpg"
                image_path = os.path.join(channel_image_dir, filename)
                try:
                    await client.download_media(msg.media, image_path)
                except Exception as e:
                    logger.warning(f"Image download failed message_id={msg.id}: {e}")
                    image_path = None

            row = {
                "message_id": msg.id,
                "channel_name": channel_name,
                "message_date": msg.date.isoformat() if msg.date else None,
                "message_text": msg.message or "",
                "has_media": has_media,
                "image_path": image_path,
                "views": msg.views or 0,
                "forwards": msg.forwards or 0,
            }
            messages.append(row)
            count += 1

            if message_delay > 0:
                await asyncio.sleep(message_delay)

        except Exception as e:
            logger.warning(f"Failed to parse message in {channel_name}: {e}")

    out_json = write_channel_messages_json(
        base_path=base_path,
        date_str=date_str,
        channel_name=channel_name,
        messages=messages,
    )
    logger.info(f"Saved channel JSON: {out_json} messages={count}")
    return count


async def run(
    base_path: str,
    channels: List[str],
    limit: int,
    date_str: str,
    message_delay: float,
) -> None:
    from pathlib import Path
    ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=ENV_PATH)


    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    session_name = os.getenv("TELEGRAM_SESSION", "telegram_session")

    if not api_id or not api_hash:
        raise RuntimeError("Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in .env")

    setup_logging(date_str)

    client = TelegramClient(session_name, int(api_id), api_hash)

    channel_counts: Dict[str, int] = {}

    async with client:
        for ch in channels:
            ch_norm = normalize_channel(ch)
            try:
                n = await scrape_channel(
                    client=client,
                    channel_username=ch,
                    base_path=base_path,
                    date_str=date_str,
                    limit=limit,
                    message_delay=message_delay,
                )
                channel_counts[ch_norm] = n
            except FloodWaitError as e:
                wait_seconds = max(int(getattr(e, "seconds", 1) or 1), 1)
                logger.warning(f"FloodWaitError channel={ch_norm} sleep={wait_seconds}s")
                await asyncio.sleep(wait_seconds)
                # retry once after sleeping
                n = await scrape_channel(
                    client=client,
                    channel_username=ch,
                    base_path=base_path,
                    date_str=date_str,
                    limit=limit,
                    message_delay=message_delay,
                )
                channel_counts[ch_norm] = n
            except Exception as e:
                logger.error(f"Channel failed channel={ch_norm}: {e}")
                channel_counts[ch_norm] = 0

    write_manifest(
        base_path=base_path,
        date_str=date_str,
        channel_message_counts=channel_counts,
        extra={"channels_input": channels, "limit": limit},
    )
    logger.info(f"Done. Total messages={sum(channel_counts.values())}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Telegram scraper (Task 1)")
    parser.add_argument(
        "--path",
        type=str,
        default="data",
        help="Base data directory (default: data)",
    )
    parser.add_argument(
        "--channels",
        nargs="+",
        required=True,
        help="Channel usernames or links (e.g., @tikvahpharma https://t.me/lobelia4cosmetics)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Max messages per channel (default: 200)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Partition date for output (YYYY-MM-DD). Default=today",
    )
    parser.add_argument(
        "--message-delay",
        type=float,
        default=0.5,
        help="Delay between messages to reduce rate limits (seconds). Default=0.5",
    )

    args = parser.parse_args()

    asyncio.run(
        run(
            base_path=args.path,
            channels=args.channels,
            limit=args.limit,
            date_str=args.date,
            message_delay=args.message_delay,
        )
    )
