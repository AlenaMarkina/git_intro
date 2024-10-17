up:
	docker compose up --build

down:
	docker compose down -v

lint:
	ruff format .
	ruff check . --fix --show-fixes

.DEFAULT_GOAL := up
