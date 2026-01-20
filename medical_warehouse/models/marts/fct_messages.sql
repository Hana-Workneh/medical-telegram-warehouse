with msgs as (
    select
        message_id,
        channel_name,
        date(message_timestamp) as message_date,
        message_timestamp,
        message_text,
        message_length,
        view_count,
        forward_count,
        has_image
    from {{ ref('stg_telegram_messages') }}
),

joined as (
    select
        m.message_id,
        c.channel_key,
        to_char(m.message_date, 'YYYYMMDD')::int as date_key,
        m.message_timestamp,
        m.message_text,
        m.message_length,
        m.view_count,
        m.forward_count,
        m.has_image
    from msgs m
    join {{ ref('dim_channels') }} c
      on m.channel_name = c.channel_name
)

select * from joined
