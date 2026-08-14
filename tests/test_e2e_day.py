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
    for type_, minimum in EXPECTED.items():
        assert counts.get(type_, 0) >= minimum, f"{type_}: {counts}"
    total = sum(counts.values())
    assert total <= 30, f"дневной бюджет нарушен: {counts}"
