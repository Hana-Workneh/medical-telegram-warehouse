with det as (
    select
        cast(message_id as bigint) as message_id,
        lower(trim(channel_name)) as channel_name,
        detected_objects,
        cast(confidence_score as double precision) as confidence_score,
        lower(trim(image_category)) as image_category,
        image_path
    from raw.yolo_detections
),

msg as (
    select
        message_id,
        channel_key,
        date_key,
        view_count
    from {{ ref('fct_messages') }}
),

ch as (
    select channel_key, channel_name
    from {{ ref('dim_channels') }}
)

select
    d.message_id,
    c.channel_key,
    m.date_key,
    d.detected_objects,
    d.confidence_score,
    d.image_category,
    d.image_path,
    m.view_count
from det d
join ch c
  on d.channel_name = c.channel_name
join msg m
  on m.message_id = d.message_id
 and m.channel_key = c.channel_key
