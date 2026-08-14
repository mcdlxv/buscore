from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://bus:bus@localhost:5432/buscore"
    session_secret: str = "dev-secret-change-me"
    tz: str = "Asia/Almaty"
    tg_bot_token: str = ""
    tg_admin_chat_id: str = ""
    quiet_start_hour: int = 22   # тихие часы 22:00–05:00
    quiet_end_hour: int = 5
    work_start_hour: int = 5     # рабочие часы парка 05:00–22:00
    work_end_hour: int = 22
    replay_file: str | None = None
    replay_speed: float = 60.0
    replay_rebase: bool = True

    model_config = {"env_file": ".env", "extra": "ignore"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
