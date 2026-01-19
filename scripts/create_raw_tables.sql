CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    message_id      BIGINT NOT NULL,
    channel_name    TEXT NOT NULL,
    message_date    TIMESTAMP NULL,
    message_text    TEXT NULL,
    has_media       BOOLEAN NOT NULL DEFAULT FALSE,
    image_path      TEXT NULL,
    views           BIGINT NOT NULL DEFAULT 0,
    forwards        BIGINT NOT NULL DEFAULT 0,
    ingested_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_name, message_id)
);
