from pydantic import BaseModel
from typing import List, Optional

class TopProduct(BaseModel):
    term: str
    count: int

class ChannelActivityPoint(BaseModel):
    date: str
    posts: int

class MessageResult(BaseModel):
    message_id: int
    channel_name: str
    message_timestamp: str
    message_text: str
    view_count: int
    forward_count: int
    has_image: bool

class VisualContentStat(BaseModel):
    channel_name: str
    image_posts: int
    total_posts: int
    pct_with_images: float
