.PHONY: up down test test-race bench fuzz flow profile proto build run-engine run-gateway run-publisher

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

run-gateway:
	go build -o bin/ ./cmd/gateway && bin/gateway

run-publisher:
	go build -o bin/ ./cmd/publisher && bin/publisher

.PHONY: run-ledger
run-ledger:
	go build -o bin/ ./cmd/ledger && bin/ledger

.PHONY: run-marketdata
run-marketdata:
	go build -o bin/ ./cmd/marketdata && bin/marketdata

.PHONY: bot-accounts run-bots
# An account for every bot, a maker and a taker on each symbol: an API key in tmp/, and money to trade.
bot-accounts:
	@mkdir -p tmp && go build -o bin/ ./cmd/gateway ./cmd/ledger
	@for s in ACME GLBX NOVA; do for a in maker taker; do \
		bin/gateway key $$a-$$s > tmp/$$a-$$s.env && \
		bin/ledger deposit $$a-$$s USD 1000000.00 $$s 100000 || exit 1; \
	done; done

run-bots:
	go build -o bin/ ./cmd/bots && bin/bots
