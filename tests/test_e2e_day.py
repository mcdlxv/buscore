"""Сквозной тест: сгенерированный день → replay → правильные события в БД."""
import json
from sqlalchemy import func, select
from scripts.seed_demo import seed
from scripts.gen_demo_day import EXPECTED, generate
from buscore import models as m
from buscore.ingest.replay import ReplayPlayer
from buscore.runtime import Runtime
from buscore.events.store import EventStore

async def test_demo_day_produces_expected_events(session, tmp_path):
    await seed(session)
    path = tmp_path / "day.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in generate()),
                    encoding="utf-8")

    runtime = Runtime(EventStore())
    await runtime.start()                      # без REPLAY_FILE replay-задачи нет
    try:
        player = ReplayPlayer(str(path), speed=100000.0, rebase=False,
                              sink=runtime.sink)
        await player.run()
    finally:
        await runtime.stop()

    q = select(m.Event.type, func.count()).group_by(m.Event.type)
    counts = dict((await session.execute(q)).all())
    # спека §10: «найди ровно эти, не найди лишних»
    assert counts == EXPECTED, f"расхождение с золотым днём: {counts}"
    # атрибуция: каждое событие должно найти водителя по смене
    unattributed = await session.scalar(
        select(func.count(m.Event.id)).where(m.Event.driver_id.is_(None)))
    assert unattributed == 0, "события без водителя — сломана привязка смены"
