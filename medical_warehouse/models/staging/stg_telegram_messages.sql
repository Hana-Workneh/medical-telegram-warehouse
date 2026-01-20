with source as (
    select
        message_id,
        channel_name,
        message_date,
        message_text,
        has_media,
        image_path,
        views,
        forwards,
        ingested_at
    from raw.telegram_messages
),

clean as (
    select
        cast(message_id as bigint) as message_id,
        lower(trim(channel_name)) as channel_name,
        cast(message_date as timestamp) as message_timestamp,
        nullif(trim(message_text), '') as message_text,
        coalesce(cast(has_media as boolean), false) as has_media,
        image_path,
        greatest(coalesce(cast(views as bigint), 0), 0) as view_count,
        greatest(coalesce(cast(forwards as bigint), 0), 0) as forward_count,
        length(coalesce(message_text, '')) as message_length,
        case
            when image_path is not null and trim(image_path) <> '' then true
            else false
        end as has_image,
        ingested_at
    from source
)

select * from clean
