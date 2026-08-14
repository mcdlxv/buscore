from datetime import datetime
from typing import Awaitable, Callable
from zoneinfo import ZoneInfo
import httpx
from buscore.config import get_settings

TYPE_LABELS_SHORT = {
    "speeding": "Превышение скорости", "harsh_brake": "Резкое торможение",
    "harsh_accel": "Резкое ускорение", "route_deviation": "Отход от маршрута",
    "stop_skip": "Пропуск остановки", "long_idle": "Долгая стоянка",
}

def in_quiet_hours(now_utc: datetime, start_h: int, end_h: int, tz: str) -> bool:
    h = now_utc.astimezone(ZoneInfo(tz)).hour
    return h >= start_h or h < end_h    # интервал через полночь, напр. 22–5

async def tg_send(chat_id: str, text: str) -> None:
    token = get_settings().tg_bot_token
    if not token:
        return                            # без токена — тихий no-op (dev)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(url, json={"chat_id": chat_id, "text": text})

class Notifier:
    def __init__(self, send: Callable[[str, str], Awaitable[None]] = tg_send,
                 quiet: tuple[int, int] = (22, 5), tz: str = "Asia/Almaty"):
        self.send, self.quiet, self.tz = send, quiet, tz
        self._summary_sent_day = ""

    async def notify(self, users: list[dict], payload: dict) -> None:
        now = datetime.fromisoformat(payload["ts"])
        if in_quiet_hours(now, self.quiet[0], self.quiet[1], self.tz):
            return
        label = TYPE_LABELS_SHORT.get(payload["type"], payload["type"])
        text = (f"⚠ {label}\nборт {payload['bus_code']}\n"
                f"{now.astimezone(ZoneInfo(self.tz)).strftime('%H:%M')} · "
                f"подробности в ленте")
        for u in users:
            if u["types"] and payload["type"] not in u["types"]:
                continue
            await self.send(u["chat_id"], text)

    async def budget_summary(self, admin_chat: str, day: str, hidden: int) -> None:
        if self._summary_sent_day == day or not admin_chat:
            return
        self._summary_sent_day = day
        await self.send(admin_chat,
                        f"Дневной бюджет тревог исчерпан, ещё {hidden}+ событий "
                        f"смотрите в ленте")

class Watchdog:
    """Возвращает текст тревоги, если в рабочие часы нет сигналов дольше порога."""

    def __init__(self, threshold_s: int, work_hours: tuple[int, int], tz: str):
        self.threshold_s, self.work_hours, self.tz = threshold_s, work_hours, tz
        self._alerted = False

    def check(self, last_signal_at: datetime | None, now: datetime) -> str | None:
        h = now.astimezone(ZoneInfo(self.tz)).hour
        if not (self.work_hours[0] <= h < self.work_hours[1]):
            self._alerted = False
            return None
        if last_signal_at is None:
            return None
        gap = int((now - last_signal_at).total_seconds())
        if gap >= self.threshold_s and not self._alerted:
            self._alerted = True
            return f"⛔ Нет данных от бортов {gap} секунд — проверьте источник"
        if gap < self.threshold_s:
            self._alerted = False
        return None
