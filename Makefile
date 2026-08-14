.PHONY: up seed demo test deploy backup restore
up:
	docker compose up -d --build
	docker compose exec app alembic upgrade head
seed:
	docker compose exec app python -m scripts.seed_demo
demo:
	docker compose exec app python -m scripts.gen_demo_day /app/data/demo_day.jsonl
	@echo "REPLAY_FILE=/app/data/demo_day.jsonl в .env и 'make up' — лента оживёт"
test:
	python -m pytest -q
deploy:
	git pull
	docker compose up -d --build
	docker compose exec app alembic upgrade head
backup:
	bash scripts/backup.sh
restore:
	bash scripts/restore.sh $(FILE)
