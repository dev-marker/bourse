.PHONY: up down test test-race bench fuzz flow profile proto build run-engine

up:
	docker compose up -d --wait

down:
	docker compose down -v

test:
	go test ./...

test-race:
	go test -race ./...

bench:
	go test -run '^$$' -bench . -benchmem ./...

fuzz:
	go test -fuzz=FuzzBook -fuzztime=20s ./internal/book/
	go test -fuzz=FuzzEntry -fuzztime=20s ./internal/journal/

flow:
	@mkdir -p tmp
	go test -run '^$$' -bench Flow -benchmem -count 6 ./internal/book/ \
		| tee tmp/flow.txt | tr '\t' ' '

profile:
	@mkdir -p tmp
	go test -run '^$$' -bench Flow -o tmp/book.test \
		-cpuprofile tmp/cpu.prof -memprofile tmp/mem.prof ./internal/book/

proto:
	buf lint
	buf generate

build:
	go build -o bin/ ./cmd/...

run-engine:
	go build -o bin/ ./cmd/engine && bin/engine
