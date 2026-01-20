from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from api.database import get_db
from api.schemas import TopProduct, ChannelActivityPoint, MessageResult, VisualContentStat

app = FastAPI(
    title="Medical Telegram Analytical API",
    version="1.0.0",
    description="Analytical API on top of dbt marts (analytics schema)."
)

ANALYTICS_SCHEMA = "analytics"

@app.get("/api/reports/top-products", response_model=list[TopProduct])
def top_products(limit: int = Query(10, ge=1, le=100), db: Session = Depends(get_db)):
    # Simple tokenization: split on whitespace and count (demo-friendly).
    # For production you’d use better NLP/tokenization.
    q = text(f"""
        with tokens as (
            select unnest(regexp_split_to_array(lower(message_text), '\\s+')) as term
            from {ANALYTICS_SCHEMA}.fct_messages
            where message_text is not null and message_text <> ''
        )
        select term, count(*)::int as count
        from tokens
        where length(term) >= 3
        group by term
        order by count desc
        limit :limit
    """)
    rows = db.execute(q, {"limit": limit}).fetchall()
    return [{"term": r[0], "count": r[1]} for r in rows]


@app.get("/api/channels/{channel_name}/activity", response_model=list[ChannelActivityPoint])
def channel_activity(channel_name: str, db: Session = Depends(get_db)):
    q = text(f"""
        select d.full_date::text as date, count(*)::int as posts
        from {ANALYTICS_SCHEMA}.fct_messages m
        join {ANALYTICS_SCHEMA}.dim_channels c on m.channel_key = c.channel_key
        join {ANALYTICS_SCHEMA}.dim_dates d on m.date_key = d.date_key
        where c.channel_name = :channel_name
        group by d.full_date
        order by d.full_date
    """)
    rows = db.execute(q, {"channel_name": channel_name.lower().strip()}).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="Channel not found or no activity.")
    return [{"date": r[0], "posts": r[1]} for r in rows]


@app.get("/api/search/messages", response_model=list[MessageResult])
def search_messages(
    query: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db)
):
    q = text(f"""
        select
            m.message_id,
            c.channel_name,
            m.message_timestamp::text as message_timestamp,
            m.message_text,
            m.view_count,
            m.forward_count,
            m.has_image
        from {ANALYTICS_SCHEMA}.fct_messages m
        join {ANALYTICS_SCHEMA}.dim_channels c on m.channel_key = c.channel_key
        where lower(m.message_text) like lower(:pattern)
        order by m.message_timestamp desc
        limit :limit
    """)
    rows = db.execute(q, {"pattern": f"%{query}%", "limit": limit}).fetchall()
    return [
        {
            "message_id": r[0],
            "channel_name": r[1],
            "message_timestamp": r[2],
            "message_text": r[3],
            "view_count": r[4],
            "forward_count": r[5],
            "has_image": r[6],
        }
        for r in rows
    ]


@app.get("/api/reports/visual-content", response_model=list[VisualContentStat])
def visual_content(db: Session = Depends(get_db)):
    q = text(f"""
        select
            c.channel_name,
            count(d.message_id)::int as image_posts,
            count(m.message_id)::int as total_posts,
            round(100.0*count(d.message_id)/nullif(count(m.message_id),0),2)::float as pct_with_images
        from {ANALYTICS_SCHEMA}.fct_messages m
        join {ANALYTICS_SCHEMA}.dim_channels c on m.channel_key=c.channel_key
        left join {ANALYTICS_SCHEMA}.fct_image_detections d
          on d.message_id=m.message_id and d.channel_key=m.channel_key
        group by c.channel_name
        order by pct_with_images desc
    """)
    rows = db.execute(q).fetchall()
    return [
        {
            "channel_name": r[0],
            "image_posts": r[1],
            "total_posts": r[2],
            "pct_with_images": r[3],
        }
        for r in rows
    ]
