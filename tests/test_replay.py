import json
from datetime import datetime, timezone, timedelta
from buscore.ingest.replay import ReplayPlayer

def _jsonl(tmp_path, rows):
    p = tmp_path / "track.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    return str(p)

BASE = datetime(2026, 8, 13, 9, 0, 0, tzinfo=timezone.utc)

def _row(sec, speed=40.0, doors=False):
    return {"bus_code": "117", "ts": (BASE + timedelta(seconds=sec)).isoformat(),
            "lat": 43.24, "lon": 76.945, "speed_kmh": speed, "doors_open": doors}

async def test_replay_emits_all_signals_in_order(tmp_path):
    path = _jsonl(tmp_path, [_row(0), _row(5), _row(10)])
    got = []
    async def sink(sig):
        got.append(sig)
    player = ReplayPlayer(path, speed=1000.0, rebase=False, sink=sink)
    await player.run()
    assert [s.ts.second for s in got] == [0, 5, 10]
    assert got[0].bus_code == "117" and got[0].speed_kmh == 40.0

async def test_replay_rebase_moves_track_to_now(tmp_path):
    path = _jsonl(tmp_path, [_row(0), _row(5)])
    got = []
    async def sink(sig):
        got.append(sig)
    await ReplayPlayer(path, speed=1000.0, rebase=True, sink=sink).run()
    age = abs((datetime.now(timezone.utc) - got[0].ts).total_seconds())
    assert age < 60  # первая точка «сейчас»
