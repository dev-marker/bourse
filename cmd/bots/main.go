// Command bots trades on the exchange through the gateway, like its customers do: on every symbol, a
// market maker that quotes both sides, and a taker that crosses the book. Each one is an account of its
// own, with an API key in tmp/<account>.env (make bot-accounts makes them, and funds them).
//
//	bots                         a maker and a taker on every symbol, until Ctrl-C
//	bots -symbols GLBX -rate 20  only GLBX, with its taker sending 20 orders a second
//	bots take GLBX buy 2000      one big IOC buy through GLBX's book, from its taker
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fiston-user/bourse/internal/auth"
	"github.com/fiston-user/bourse/internal/bots"
	"github.com/fiston-user/bourse/internal/market"
)

const (
	gateway    = "http://localhost:8080"
	marketdata = "http://localhost:8081"
)

// opens is where a symbol's fair value starts, when it has never traded.
var opens = map[string]market.Price{"ACME": 42_00, "GLBX": 101_00, "NOVA": 18_50}

func main() {
	// No time on each line: the bots' terminal is for watching, not for keeping.
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))
	symbols := flag.String("symbols", "ACME,GLBX,NOVA", "the symbols to trade")
	rate := flag.Float64("rate", 8, "each taker's orders a second, on average")
	seed := flag.Uint64("seed", 1, "the random seed: the same seed makes the same choices")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	if flag.Arg(0) == "take" {
		err = take(ctx, flag.Args()[1:])
	} else {
		err = run(ctx, log, strings.Split(*symbols, ","), *rate, *seed)
	}
	if err != nil {
		log.Error("bots failed", "err", err)
		os.Exit(1)
	}
}

// run starts a maker and a taker on every symbol, and waits for them.
func run(ctx context.Context, log *slog.Logger, symbols []string, rate float64, seed uint64) error {
	var (
		wg      sync.WaitGroup
		errs    = make([]error, 2*len(symbols))
		clients []*bots.Client
	)
	// Every bot gets a random stream of its own: the same seed, and its place in the list.
	rnd := func(n int) *rand.Rand { return rand.New(rand.NewPCG(seed, uint64(n))) }
	for i, sym := range symbols {
		in, err := instrument(sym)
		if err != nil {
			return err
		}
		maker, err := client("maker-" + sym)
		if err != nil {
			return err
		}
		taker, err := client("taker-" + sym)
		if err != nil {
			return err
		}
		clients = append(clients, maker, taker)
		fair, err := open(ctx, in)
		if err != nil {
			return err
		}
		m := &bots.Maker{
			Quoter: bots.Quoter{In: in, Levels: 5, Spread: 1, Size: 50 * in.LotSize,
				Limit: 1000 * in.LotSize, Skew: 20},
			Client: maker,
			Fair:   &bots.Walk{Price: fair, Tick: in.TickSize, Rand: rnd(2 * i)},
			Every:  500 * time.Millisecond,
			Log:    log,
		}
		t := &bots.Taker{In: in, Client: taker, MarketData: marketdata, Rate: rate, Lots: 10, Through: 2,
			Rand: rnd(2*i + 1), Log: log}
		wg.Go(func() { errs[2*i] = m.Run(ctx) })
		wg.Go(func() { errs[2*i+1] = t.Run(ctx) })
		log.Info("trading", "symbol", sym, "fair", in.FormatPrice(fair))
	}
	go report(ctx, log, clients)
	wg.Wait()
	return errors.Join(errs...)
}

// report says how many requests the bots sent, every five seconds.
func report(ctx context.Context, log *slog.Logger, clients []*bots.Client) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		var sent, trades, limited int64
		for _, c := range clients {
			sent += c.Sent.Swap(0)
			trades += c.Trades.Swap(0)
			limited += c.Limited.Swap(0)
		}
		log.Info("last 5s", "requests_per_second", sent/5, "trades_per_second", trades/5,
			"rate_limited", limited)
	}
}

// open is where a symbol's fair value starts: at its last trade, or where it opens.
func open(ctx context.Context, in market.Instrument) (market.Price, error) {
	last, err := bots.Last(ctx, marketdata, in.Symbol)
	if last == 0 && err == nil {
		last = opens[in.Symbol]
	}
	return last, err
}

// take sends one IOC order through a symbol's book: take GLBX buy 2000.
func take(ctx context.Context, args []string) error {
	if len(args) != 3 || args[1] != "buy" && args[1] != "sell" {
		return errors.New("usage: bots take SYMBOL buy|sell QTY")
	}
	in, err := instrument(args[0])
	if err != nil {
		return err
	}
	qty, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return err
	}
	c, err := client("taker-" + in.Symbol)
	if err != nil {
		return err
	}
	side := market.Buy
	if args[1] == "sell" {
		side = market.Sell
	}
	t := &bots.Taker{In: in, Client: c, MarketData: marketdata, Through: 20}
	p, err := t.Take(ctx, side, market.Qty(qty))
	if err != nil {
		return err
	}
	var trades, filled int64
	var prices []string
	for _, e := range p.Events {
		if e.Kind == "traded" {
			trades, filled = trades+1, filled+e.Qty
			if !slices.Contains(prices, e.Price) {
				prices = append(prices, e.Price)
			}
		}
	}
	fmt.Printf("%s %d %s: %d filled in %d trades, at %s\n", args[1], qty, in.Symbol, filled, trades,
		strings.Join(prices, " "))
	return nil
}

func instrument(sym string) (market.Instrument, error) {
	i := slices.IndexFunc(market.Listed, func(in market.Instrument) bool { return in.Symbol == sym })
	if i < 0 {
		return market.Instrument{}, fmt.Errorf("we don't trade %q", sym)
	}
	return market.Listed[i], nil
}

// client is a gateway client for an account, with its key from tmp/<account>.env.
func client(account string) (*bots.Client, error) {
	f, err := os.Open("tmp/" + account + ".env")
	if err != nil {
		return nil, fmt.Errorf("no API key for %s: %w", account, err)
	}
	defer f.Close()
	key := auth.Key{Account: account}
	for s := bufio.NewScanner(f); s.Scan(); {
		name, value, _ := strings.Cut(s.Text(), "=")
		switch name {
		case "BOURSE_KEY":
			key.ID = value
		case "BOURSE_SECRET":
			key.Secret = value
		}
	}
	return bots.NewClient(gateway, key), nil
}
