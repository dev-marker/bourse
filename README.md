# Build a Stock Exchange Backend in Go

The code from the YouTube course **Build a Stock Exchange Backend in Go**. Every line was typed on camera and every decision is explained in
the videos.

- ▶ **The course on YouTube**: _link coming soon_

## Follow along: one branch per episode

Each episode ends with a commit, and each episode has its own branch holding the code **as it is at the end of that
episode**. To start an episode where the video starts, check out the previous episode's branch:

```bash
git clone https://github.com/dev-marker/bourse.git
cd bourse
git checkout ep01   # the code at the end of episode 1, where episode 2 begins
```

`main` holds the latest episode plus this README.

| Branch | Episode | What we build |
|---|---|---|
| [`ep01`](https://github.com/dev-marker/bourse/tree/ep01) | 1. How an Exchange Works | Project skeleton, local stack and the market package |
| [`ep02`](https://github.com/dev-marker/bourse/tree/ep02) | 2. The Order Book | Price levels, FIFO queues and an order index |
| [`ep03`](https://github.com/dev-marker/bourse/tree/ep03) | 3. Matching | Matching, every order type, and a fuzz test |
| [`ep04`](https://github.com/dev-marker/bourse/tree/ep04) | 4. A Million Orders a Second | Reuse nodes, levels and trades, and give the index room |
| [`ep05`](https://github.com/dev-marker/bourse/tree/ep05) | 5. One Writer | One goroutine per symbol, sequence numbers and a gRPC API |
| [`ep06`](https://github.com/dev-marker/bourse/tree/ep06) | 6. The Journal | A write-ahead log with checksums, fsync and group commit |
| [`ep07`](https://github.com/dev-marker/bourse/tree/ep07) | 7. Crash and Replay | Crash recovery by replay, snapshots and journal rotation |
| [`ep08`](https://github.com/dev-marker/bourse/tree/ep08) | 8. The Front Door | REST API with signed requests, idempotent orders and rate limits |
| [`ep09`](https://github.com/dev-marker/bourse/tree/ep09) | 9. Money Up Front | Balances and holds on every order, and only the owner may cancel |
| [`ep10`](https://github.com/dev-marker/bourse/tree/ep10) | 10. Events Out | Executions to Kafka from the journal, exactly once |
| [`ep11`](https://github.com/dev-marker/bourse/tree/ep11) | 11. Settlement | Double-entry settlement from the stream, exactly once |
| [`ep12`](https://github.com/dev-marker/bourse/tree/ep12) | 12. Market Data | Depth, trades and candles over WebSocket, slow clients dropped |
| [`ep13`](https://github.com/dev-marker/bourse/tree/ep13) | 13. Watching the Market | A market maker and takers through the gateway, and a live page |
| [`ep14`](https://github.com/dev-marker/bourse/tree/ep14) | 14. Under Load | A load generator, pprof, Prometheus metrics, Grafana and tracing |
| [`ep15`](https://github.com/dev-marker/bourse/tree/ep15) | 15. Ready for Production | Settings, graceful stops, backups, images and a deploy behind Caddy |

## Run it

Requirements: Go 1.27+, Docker with Compose, and `make`.

The whole exchange, the way episode 15 deploys it (behind Caddy on http://localhost):

```bash
cp .env.example .env
make images                        # one small image with every service
make deploy                        # engine, gateway, ledger, market data and Caddy, all healthy in about 10 s
make bot-accounts && make run-bots # traders, so the live page at http://localhost/ moves
```

While you develop:

```bash
make up          # Postgres, Kafka, Prometheus, Grafana and Jaeger in Docker Compose
make run         # every service on this machine
make test        # unit tests; integration tests run when `make up` is running
make test-race   # the same, with the race detector
make bench       # the order book and matching benchmarks
```

The Go module path is `github.com/fiston-user/bourse`, exactly as it's typed in the videos. You don't need to change
it: cloning and building work as they are.

## License

MIT, see [LICENSE](LICENSE).
