import os
import csv
from pathlib import Path
from typing import List, Tuple

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values


def main() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=env_path, override=True)

    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_port = int(os.getenv("DB_PORT", "5433"))
    db_name = os.getenv("DB_NAME", "med_warehouse")
    db_user = os.getenv("DB_USER", "med_user")
    db_password = os.getenv("DB_PASSWORD", "med_password")

    csv_path = Path("data/yolo_detections.csv")
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing {csv_path}. Run python src/yolo_detect.py first.")

    rows: List[Tuple] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                int(r["message_id"]),
                str(r["channel_name"]).lower().strip(),
                r.get("detected_objects") or "",
                float(r["confidence_score"]) if r.get("confidence_score") else 0.0,
                r.get("image_category") or "other",
                r.get("image_path"),
            ))

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )

    sql = """
        INSERT INTO raw.yolo_detections
        (message_id, channel_name, detected_objects, confidence_score, image_category, image_path)
        VALUES %s
        ON CONFLICT (channel_name, message_id) DO UPDATE SET
            detected_objects = EXCLUDED.detected_objects,
            confidence_score = EXCLUDED.confidence_score,
            image_category = EXCLUDED.image_category,
            image_path = EXCLUDED.image_path;
    """

    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)

    conn.close()
    print(f"Loaded {len(rows)} rows into raw.yolo_detections")


if __name__ == "__main__":
    main()
