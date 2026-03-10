from beanie import Document
from pydantic import BaseModel, Field
from pymongo import IndexModel, ASCENDING
from datetime import datetime, timezone
from typing import Optional


class GeoLocation(BaseModel):
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    isp: Optional[str] = None
    timezone: Optional[str] = None


class UserTraffic(Document):
    user_id: str
    ip_address: str
    geo_location: GeoLocation
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Settings:
        name = "user_traffic"
        indexes = [
            IndexModel(
                [("user_id", ASCENDING), ("ip_address", ASCENDING)],
                unique=True  # DB level guarantee — no duplicate user+ip ever
            )
        ]