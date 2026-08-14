# buscore — runbook оператора

## Первый запуск (VPS или локально)

1. Установить Docker + Docker Compose (docs.docker.com/engine/install).
2. `git clone <repo> && cd <repo>`
3. `cp .env.example .env` → вписать `SESSION_SECRET` (длинная случайная строка),
   `TG_BOT_TOKEN` (от @BotFather), `TG_ADMIN_CHAT_ID` (ваш chat id).
4. `make up` — поднимает БД и приложение, накатывает миграции.
5. `make seed` — демо-данные. СРАЗУ смените пароли admin/sb/boss в БД
   (страница управления пользователями появится в Плане 2; пока — через psql).
6. `make demo` + `REPLAY_FILE=/app/data/demo_day.jsonl` в `.env` + `make up` —
   живая лента на синтетическом дне: http://localhost:8000/feed (sb/sb123).
   Файл лежит в `./data` на хосте и переживает пересборку контейнера.

## Подключить Telegram-уведомления

1. Создать бота у @BotFather → токен в `TG_BOT_TOKEN` (`.env`).
2. Узнать свой chat id у @userinfobot → в `TG_ADMIN_CHAT_ID` (`.env`) —
   на него пойдут тревоги watchdog и сводки бюджета; при `make seed`
   он же подставится админу.
3. Включить личные уведомления сотруднику (пока без UI, одной командой):
   `docker compose exec db psql -U bus -d buscore -c "UPDATE users SET tg_chat_id='<CHAT_ID>' WHERE login='sb'"`
   Пустой `notify_types` = все типы событий; список типов — выборочно.

## Разработка без Docker (Windows, локальный PostgreSQL)

1. PostgreSQL 17 запущен на localhost:5432, роль `bus`/`bus`, база `buscore`
   (создание: `psql -U postgres -c "CREATE ROLE bus LOGIN PASSWORD 'bus' CREATEDB"`,
   затем `psql -U postgres -c "CREATE DATABASE buscore OWNER bus"`).
2. `python -m pip install -r requirements-dev.txt`
3. `python -m alembic upgrade head`
4. `python -m pytest -q` — вся проверка проекта.
5. Живая лента локально: `python -m scripts.seed_demo`,
   `python -m scripts.gen_demo_day demo_day.jsonl`, затем
   `set REPLAY_FILE=demo_day.jsonl && python -m uvicorn buscore.main:app` →
   http://localhost:8000/feed (sb/sb123).

## Ежедневно

- Лента: `/feed` (роль security). Отчёт: `/report` (роль manager). Пороги
  правил: `/admin/rules` (роль admin, применяются после перезапуска конвейера).
- Бэкап: `make backup` (складывает в `backups/`). Настройте cron: `0 2 * * *`.

## Если лента молчит

1. `docker compose ps` — все контейнеры Up?
2. `docker compose logs app --tail 100` — ошибки?
3. Не пришла ли тревога watchdog в Telegram («нет данных от бортов»)?
4. `curl localhost:8000/health` — `{"status":"ok"}`?
5. Перезапуск: `docker compose restart app`.

## Обновление версии

`make deploy` (git pull + пересборка + миграции). Откат: `git checkout <тег> && make deploy`.

## Восстановление из бэкапа

`make restore FILE=backups/buscore-<дата>.sql.gz`
