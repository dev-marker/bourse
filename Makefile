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

.PHONY: run load-accounts
# The whole exchange in one terminal, each line starting with its service's name. Ctrl-C stops it all.
run: build
	@trap 'wait; exit 0' INT; for s in engine publisher ledger gateway marketdata; do \
		bin/$$s 2>&1 | awk -v s=$$s '{ sub(/^time=[^ ]+ /, ""); printf "%-11s%s\n", s, $$0; fflush() }' & \
		sleep 0.5; \
	done; wait

# Twenty accounts for the load generator: an API key in tmp/ for each, and money to trade.
load-accounts:
	@mkdir -p tmp && go build -o bin/ ./cmd/gateway ./cmd/ledger
	@for i in $$(seq -w 1 20); do \
		bin/gateway key load-$$i > tmp/load-$$i.env && \
		bin/ledger deposit load-$$i USD 10000000.00 ACME 1000000 GLBX 1000000 NOVA 1000000 \
			>/dev/null || exit 1; \
	done
	@echo "20 accounts, keys in tmp/load-*.env"

.PHONY: cpu
# Ten seconds of a running service's CPU, our own functions only, the costliest first: make cpu PORT=8084
cpu:
	@go tool pprof -top -cum -show bourse/internal -nodecount 12 \
		"http://localhost:$(PORT)/debug/pprof/profile?seconds=10" 2>/dev/null \
		| sed -n '/flat%/,$$p' | sed 's|github.com/[^/]*/bourse/internal/||' | cut -c 31-
