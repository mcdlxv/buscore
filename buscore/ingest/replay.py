import asyncio
import json
from datetime import datetime, timezone
from typing import Awaitable, Callable
from buscore.schemas import Signal

class ReplayPlayer:
    """Проигрывает JSONL-трек как живой поток сигналов.

    speed=60 → час записи за минуту. rebase=True сдвигает метки времени так,
    чтобы первая точка оказалась «сейчас» (лента выглядит живой).
    """

    def __init__(self, path: str, speed: float, rebase: bool,
                 sink: Callable[[Signal], Awaitable[None]]):
        self.path, self.speed, self.rebase, self.sink = path, speed, rebase, sink

    async def run(self) -> None:
        signals = self._load()
        if not signals:
            return
        offset = (datetime.now(timezone.utc) - signals[0].ts) if self.rebase else None
        prev_ts = signals[0].ts
        for sig in signals:
            delay = (sig.ts - prev_ts).total_seconds() / self.speed
            if delay > 0:
                await asyncio.sleep(delay)
            prev_ts = sig.ts
            if offset is not None:
                sig = sig.model_copy(update={"ts": sig.ts + offset})
            await self.sink(sig)

    def _load(self) -> list[Signal]:
        with open(self.path, encoding="utf-8") as f:
            rows = [Signal(**json.loads(line)) for line in f if line.strip()]
        return sorted(rows, key=lambda s: s.ts)
