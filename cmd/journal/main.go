// Command journal prints the entries of a journal, and checks every record on the way.
//
//	journal var/engine.journal         the first ten entries, and how the journal ends
//	journal -n 3 var/engine.journal    only the first three
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/fiston-user/bourse/internal/journal"
	"github.com/fiston-user/bourse/internal/market"
)

func main() {
	n := flag.Int("n", 10, "how many entries to print")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: journal [-n N] FILE")
		os.Exit(2)
	}
	if err := run(flag.Arg(0), *n); err != nil {
		fmt.Fprintln(os.Stderr, "journal:", err)
		os.Exit(1)
	}
}

func run(path string, n int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := journal.NewReader(f)
	for count := 0; ; count++ {
		e, err := r.Next()
		if errors.Is(err, io.EOF) {
			fmt.Printf("%d entries in %d bytes, and the journal ends cleanly\n", count, r.Offset())
			return nil
		}
		if err != nil {
			return fmt.Errorf("%d good entries, then %w", count, err)
		}
		if count < n {
			fmt.Println(line(e))
		}
	}
}

var (
	sides = map[market.Side]string{market.Buy: "buy", market.Sell: "sell"}
	tifs  = map[market.TimeInForce]string{market.GTC: "GTC", market.IOC: "IOC", market.FOK: "FOK"}
)

// line shows one entry on one line. Prices are in the instrument's smallest unit, as the book sees them.
func line(e journal.Entry) string {
	at := time.Unix(0, e.Time).UTC().Format("15:04:05.000000")
	s := fmt.Sprintf("%6d  %s  %-4s  ", e.Seq, at, e.Symbol)
	if e.Cancel != 0 {
		return s + fmt.Sprintf("cancel order %d", e.Cancel)
	}
	o := e.Order
	price := "market"
	if o.Type == market.Limit {
		price = fmt.Sprint(o.Price)
	}
	return s + fmt.Sprintf("%-4s %5d @ %-6s %s  %s", sides[o.Side], o.Qty, price, tifs[o.TIF], o.Account)
}
