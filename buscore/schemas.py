from datetime import datetime
from pydantic import BaseModel

class Signal(BaseModel):
    bus_code: str
    ts: datetime
    lat: float
    lon: float
    speed_kmh: float
    doors_open: bool = False

class EventCandidate(BaseModel):
    type: str
    ts_start: datetime
    ts_end: datetime | None = None
    lat: float
    lon: float
    confidence: float = 1.0
    details: dict = {}
