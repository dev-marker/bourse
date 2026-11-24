// Command bourse is a trader's toolbox: orders straight to the engine over gRPC, and signed requests
// through the gateway.
//
//	bourse buy ACME 300 101.27     a limit order, good till cancelled
//	bourse sell ACME 500 market    a market order
//	bourse cancel ACME 3 buy       cancel buy order 3 (the CLI's buys and sells are two accounts)
//	bourse flood 8 5000            8 clients at once, 5,000 orders each
//	bourse proxy                   sign every request on :8079, and pass it on to the gateway
//	bourse proxy :8078             the same, on another port, for a second key
//	bourse burst 20                20 orders through the gateway at once, and what's left after them
//	bourse events                  every execution, as it's published to Kafka
//	bourse events stats            how many executions arrive each second, and how late
//	bourse events check            every symbol's seqs on the whole topic: no gaps, no duplicates
package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "bourse:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) > 0 && args[0] == "proxy" {
		addr := ":8079"
		if len(args) == 2 {
			addr = args[1] // a second proxy, for a second account
		}
		return proxy(addr)
	}
	if len(args) > 0 && args[0] == "events" {
		return events(strings.Join(args[1:], " "))
	}
	if len(args) == 2 && args[0] == "burst" {
		n, _ := strconv.Atoi(args[1])
		return burst(n)
	}
	if len(args) < 3 {
		return errors.New("usage: bourse buy|sell SYMBOL QTY PRICE|market, cancel SYMBOL ID, flood CLIENTS ORDERS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if args[0] == "flood" {
		clients, _ := strconv.Atoi(args[1])
		orders, _ := strconv.Atoi(args[2])
		return flood(ctx, clients, orders)
	}

	conn, err := dial()
	if err != nil {
		return err
	}
	defer conn.Close()
	c := boursev1.NewEngineServiceClient(conn)
	var evs []*boursev1.Event
	switch args[0] {
	case "buy", "sell":
		if len(args) < 4 {
			return errors.New("usage: bourse buy|sell SYMBOL QTY PRICE|market")
		}
		res, err := c.SubmitOrder(ctx, request(args[0], args[1], args[2], args[3]))
		if err != nil {
			return err
		}
		evs = res.GetEvents()
	case "cancel":
		if len(args) < 4 {
			return errors.New("usage: bourse cancel SYMBOL ID buy|sell")
		}
		id, _ := strconv.ParseUint(args[2], 10, 64)
		res, err := c.CancelOrder(ctx, &boursev1.CancelOrderRequest{Symbol: args[1], OrderId: id,
			Account: "cli-" + args[3]})
		if err != nil {
			return err
		}
		evs = res.GetEvents()
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	for _, ev := range evs {
		fmt.Println(line(ev))
	}
	return nil
}

func dial() (*grpc.ClientConn, error) {
	return grpc.NewClient("localhost:9083", grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// request builds an order. Buys and sells come from two accounts, so they can trade with each other.
func request(side, sym, qty, price string) *boursev1.SubmitOrderRequest {
	req := &boursev1.SubmitOrderRequest{Symbol: sym, Account: "cli-" + side, Side: boursev1.Side_SIDE_BUY,
		Type: boursev1.OrderType_ORDER_TYPE_LIMIT, TimeInForce: boursev1.TimeInForce_TIME_IN_FORCE_GTC, Price: price}
	if side == "sell" {
		req.Side = boursev1.Side_SIDE_SELL
	}
	if price == "market" {
		req.Type, req.TimeInForce, req.Price = boursev1.OrderType_ORDER_TYPE_MARKET, boursev1.TimeInForce_TIME_IN_FORCE_IOC, ""
	}
	req.Qty, _ = strconv.ParseInt(qty, 10, 64)
	return req
}

// line shows one event on one line.
func line(ev *boursev1.Event) string {
	kind := strings.ToLower(strings.TrimPrefix(ev.GetKind().String(), "EVENT_KIND_"))
	s := fmt.Sprintf("seq %-4d %-9s order %d", ev.GetSeq(), kind, ev.GetOrderId())
	if ev.GetQty() != 0 {
		s += fmt.Sprintf("  %d", ev.GetQty())
	}
	if ev.GetPrice() != "" {
		s += " @ " + ev.GetPrice()
	}
	if ev.GetMakerId() != 0 {
		s += fmt.Sprintf("  with order %d", ev.GetMakerId())
	}
	if ev.GetReason() != "" {
		s += "  " + ev.GetReason()
	}
	return s
}

var symbols = []string{"ACME", "GLBX", "NOVA"}

// flood sends random orders from many clients at once, each on a connection of its own. Then it checks that
// every symbol's sequence numbers came back as one unbroken run, with no gaps and no duplicates.
func flood(ctx context.Context, clients, orders int) error {
	seqs := make([]map[string][]uint64, clients)
	errs := make([]error, clients)
	var wg sync.WaitGroup
	start := time.Now()
	for i := range clients {
		seqs[i] = map[string][]uint64{}
		wg.Go(func() { errs[i] = client(ctx, i, orders, seqs[i]) })
	}
	wg.Wait()
	took := time.Since(start)
	if err := errors.Join(errs...); err != nil {
		// The engine went away in the middle. Say how far each symbol got, in the answers we did get.
		for _, sym := range symbols {
			var last uint64
			for i := range clients {
				if s := seqs[i][sym]; len(s) > 0 {
					last = max(last, s[len(s)-1])
				}
			}
			fmt.Printf("%s  the last answer was seq %d\n", sym, last)
		}
		return fmt.Errorf("the engine went away: %v", status.Code(err))
	}
	n := clients * orders
	fmt.Printf("%d orders, %d at a time, in %v: %.0f orders a second\n", n, clients,
		took.Round(time.Millisecond), float64(n)/took.Seconds())
	for _, sym := range symbols {
		var all []uint64
		for i := range clients {
			all = append(all, seqs[i][sym]...)
		}
		slices.Sort(all)
		for j := 1; j < len(all); j++ {
			if all[j] != all[j-1]+1 {
				return fmt.Errorf("%s: seq %d comes after %d", sym, all[j], all[j-1])
			}
		}
		fmt.Printf("%s  %d events, seq %d to %d, no gaps, no duplicates\n", sym, len(all), all[0], all[len(all)-1])
	}
	return nil
}

// client is one trader in the flood. It keeps the sequence number of every event it gets back.
func client(ctx context.Context, id, orders int, seqs map[string][]uint64) error {
	conn, err := dial()
	if err != nil {
		return err
	}
	defer conn.Close()
	c := boursev1.NewEngineServiceClient(conn)
	r := rand.New(rand.NewPCG(uint64(id), 1))
	for range orders {
		sym := symbols[r.IntN(len(symbols))]
		res, err := c.SubmitOrder(ctx, &boursev1.SubmitOrderRequest{
			Symbol: sym, Account: fmt.Sprint("client", id), Side: boursev1.Side(1 + r.IntN(2)),
			Type: boursev1.OrderType_ORDER_TYPE_LIMIT, TimeInForce: boursev1.TimeInForce_TIME_IN_FORCE_GTC,
			Price: fmt.Sprintf("%d.%02d", 100+r.IntN(2), 5*r.IntN(20)), Qty: int64(10 * (1 + r.IntN(10))),
		})
		if err != nil {
			return err
		}
		for _, ev := range res.GetEvents() {
			seqs[sym] = append(seqs[sym], ev.GetSeq())
		}
	}
	return nil
}
