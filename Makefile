.PHONY: up down test test-race

up:
	docker compose up -d --wait

down:
	docker compose down -v

test:
	go test ./...

test-race:
	go test -race ./...
