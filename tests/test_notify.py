from datetime import datetime, timezone, timedelta
from buscore.notify.telegram import Notifier, in_quiet_hours, Watchdog

def test_quiet_hours_wrap_midnight():
    tz = "Asia/Almaty"
    # 23:00 Алматы — тихо; 12:00 — нет. UTC+5.
    assert in_quiet_hours(datetime(2026, 8, 13, 18, 0, tzinfo=timezone.utc),
                          22, 5, tz) is True    # 23:00 местного
    assert in_quiet_hours(datetime(2026, 8, 13, 7, 0, tzinfo=timezone.utc),
                          22, 5, tz) is False   # 12:00 местного

async def test_notifier_sends_to_subscribed_users():
    sent = []
    async def fake_send(chat_id, text):
        sent.append((chat_id, text))
    n = Notifier(send=fake_send, quiet=(22, 5), tz="Asia/Almaty")
    users = [{"chat_id": "111", "types": []},               # все типы
             {"chat_id": "222", "types": ["phone_use"]}]    # только телефон
    await n.notify(users, {"type": "speeding", "bus_code": "117",
                           "ts": "2026-08-13T07:00:00+00:00"})
    assert [c for c, _ in sent] == ["111"]
    assert "117" in sent[0][1]

def test_watchdog_alerts_once_per_gap():
    wd = Watchdog(threshold_s=600, work_hours=(5, 22), tz="Asia/Almaty")
    t0 = datetime(2026, 8, 13, 7, 0, tzinfo=timezone.utc)   # 12:00 местного
    assert wd.check(last_signal_at=t0, now=t0 + timedelta(seconds=300)) is None
    alert = wd.check(last_signal_at=t0, now=t0 + timedelta(seconds=700))
    assert alert is not None and "700" in alert
    assert wd.check(last_signal_at=t0, now=t0 + timedelta(seconds=800)) is None
