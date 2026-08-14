from datetime import datetime, timedelta, timezone
from sqlalchemy import delete
from buscore import models as m

SIGNAL_RETENTION_DAYS = 90

async def purge_old_signals(session, now: datetime | None = None) -> int:
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=SIGNAL_RETENTION_DAYS)
    result = await session.execute(
        delete(m.SignalRow).where(m.SignalRow.ts < cutoff))
    await session.commit()
    return result.rowcount
