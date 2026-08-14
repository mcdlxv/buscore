from datetime import datetime, date
from sqlalchemy import (String, Integer, Float, Boolean, DateTime, Date,
                        ForeignKey, JSON, Text, UniqueConstraint, Index)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from buscore.db import Base

class Bus(Base):
    __tablename__ = "buses"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(16), unique=True)       # борт «117»
    plate: Mapped[str] = mapped_column(String(16), default="")
    link_id: Mapped[str] = mapped_column(String(64), default="")     # id в Link+ (План 3)
    route_id: Mapped[int | None] = mapped_column(ForeignKey("routes.id"))

class Driver(Base):
    __tablename__ = "drivers"
    id: Mapped[int] = mapped_column(primary_key=True)
    full_name: Mapped[str] = mapped_column(String(128))
    personnel_no: Mapped[str] = mapped_column(String(32), default="")

class Shift(Base):
    __tablename__ = "shifts"
    id: Mapped[int] = mapped_column(primary_key=True)
    bus_id: Mapped[int] = mapped_column(ForeignKey("buses.id"))
    driver_id: Mapped[int] = mapped_column(ForeignKey("drivers.id"))
    day: Mapped[date] = mapped_column(Date)
    __table_args__ = (UniqueConstraint("bus_id", "day"),)

class Route(Base):
    __tablename__ = "routes"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(32))                    # «45»
    speed_limit_kmh: Mapped[float] = mapped_column(Float, default=60)
    polyline: Mapped[list] = mapped_column(JSON)                     # [[lat, lon], ...]

class Stop(Base):
    __tablename__ = "stops"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    radius_m: Mapped[float] = mapped_column(Float, default=60)
    is_depot: Mapped[bool] = mapped_column(Boolean, default=False)

class RouteStop(Base):
    __tablename__ = "route_stops"
    id: Mapped[int] = mapped_column(primary_key=True)
    route_id: Mapped[int] = mapped_column(ForeignKey("routes.id"))
    stop_id: Mapped[int] = mapped_column(ForeignKey("stops.id"))
    seq: Mapped[int] = mapped_column(Integer)
    mandatory: Mapped[bool] = mapped_column(Boolean, default=True)

class SignalRow(Base):
    __tablename__ = "signals"
    id: Mapped[int] = mapped_column(primary_key=True)
    bus_id: Mapped[int] = mapped_column(ForeignKey("buses.id"))
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    speed_kmh: Mapped[float] = mapped_column(Float)
    doors_open: Mapped[bool] = mapped_column(Boolean, default=False)
    __table_args__ = (Index("ix_signals_bus_ts", "bus_id", "ts"),)

class Event(Base):
    __tablename__ = "events"
    id: Mapped[int] = mapped_column(primary_key=True)
    type: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(16), default="new")   # new|confirmed|rejected
    bus_id: Mapped[int] = mapped_column(ForeignKey("buses.id"))
    driver_id: Mapped[int | None] = mapped_column(ForeignKey("drivers.id"))
    route_id: Mapped[int | None] = mapped_column(ForeignKey("routes.id"))
    ts_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ts_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    details: Mapped[dict] = mapped_column(JSON, default=dict)
    clip_id: Mapped[int | None] = mapped_column(ForeignKey("clips.id", ondelete="SET NULL"))
    verdict_by: Mapped[int | None] = mapped_column(ForeignKey("users.id"))
    verdict_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reject_reason: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (Index("ix_events_status_created", "status", "created_at"),
                      Index("ix_events_bus_created", "bus_id", "created_at"),
                      Index("ix_events_driver_created", "driver_id", "created_at"),
                      Index("ix_events_route_created", "route_id", "created_at"),
                      Index("ix_events_clip", "clip_id"))

class Clip(Base):
    __tablename__ = "clips"
    id: Mapped[int] = mapped_column(primary_key=True)
    path: Mapped[str] = mapped_column(Text)
    duration_s: Mapped[float] = mapped_column(Float, default=0)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

class RuleConfig(Base):
    __tablename__ = "rule_configs"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[float] = mapped_column(Float)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_by: Mapped[str | None] = mapped_column(String(64))

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    login: Mapped[str] = mapped_column(String(64), unique=True)
    password_hash: Mapped[str] = mapped_column(Text)
    role: Mapped[str] = mapped_column(String(16))                    # admin|security|manager
    tg_chat_id: Mapped[str] = mapped_column(String(32), default="")
    notify_types: Mapped[list] = mapped_column(JSON, default=list)   # [] = все типы
