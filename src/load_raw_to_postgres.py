import json
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values


def parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def main() -> None:
    # Always load .env from repo root
    env_path = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=env_path, override=True)

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_name = os.getenv("DB_NAME", "med_warehouse")
    db_user = os.getenv("DB_USER", "med_user")
    db_password = os.getenv("DB_PASSWORD", "med_password")

    print("Using DB creds:", db_host, db_port, db_name, db_user, db_password)

    base_dir = Path("data/raw/telegram_messages")
    json_files = [p for p in base_dir.rglob("*.json") if p.name != "_manifest.json"]
    if not json_files:
        raise RuntimeError("No JSON files found under data/raw/telegram_messages")

    rows: List[Tuple] = []
    for fp in json_files:
        with open(fp, "r", encoding="utf-8") as f:
            messages = json.load(f)

        if not isinstance(messages, list):
            continue

        for m in messages:
            rows.append((
                int(m.get("message_id")),
                str(m.get("channel_name")),
                parse_ts(m.get("message_date")),
                m.get("message_text") or "",
                bool(m.get("has_media", False)),
                m.get("image_path"),
                int(m.get("views") or 0),
                int(m.get("forwards") or 0),
            ))

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )

    sql = """
        INSERT INTO raw.telegram_messages
        (message_id, channel_name, message_date, message_text, has_media, image_path, views, forwards)
        VALUES %s
        ON CONFLICT (channel_name, message_id) DO UPDATE SET
            message_date = EXCLUDED.message_date,
            message_text = EXCLUDED.message_text,
            has_media = EXCLUDED.has_media,
            image_path = EXCLUDED.image_path,
            views = EXCLUDED.views,
            forwards = EXCLUDED.forwards;
    """

    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)

    conn.close()
    print(f"Loaded {len(rows)} rows from {len(json_files)} JSON files into raw.telegram_messages")


if __name__ == "__main__":
    main()
