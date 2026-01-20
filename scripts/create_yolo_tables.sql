CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.yolo_detections (
  message_id         BIGINT NOT NULL,
  channel_name       TEXT NOT NULL,
  detected_objects   TEXT,
  confidence_score   DOUBLE PRECISION,
  image_category     TEXT,
  image_path         TEXT,
  ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (channel_name, message_id)
);
