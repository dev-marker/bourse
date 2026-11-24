package engine_test

import (
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/fiston-user/bourse/internal/engine"
	"github.com/fiston-user/bourse/internal/journal"
	"github.com/fiston-user/bourse/internal/market"
)

// TestJournal checks that the journal holds every input that reached a book, in order, and nothing else.
func TestJournal(t *testing.T) {
	dir := t.TempDir()
	e, err := engine.New(dir, acme)
	if err != nil {
		t.Fatal(err)
	}
	go e.Run(t.Context())
	buy, bad := order(0, 0), order(1, 1)
	bad.Qty = -5 // turned away at the door, so it never reaches the journal
	r, err := e.Submit(t.Context(), buy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(t.Context(), bad); err == nil {
		t.Fatal("an order for -5 shares was accepted")
	}
	if _, err := e.Cancel(t.Context(), "ACME", r.OrderID, buy.Account); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(filepath.Join(dir, "engine.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	jr := journal.NewReader(f)
	var got []journal.Entry
	for {
		entry, err := jr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		entry.Time = 0 // the one field that's different on every run
		got = append(got, entry)
	}
	want := []journal.Entry{
		{Seq: 1, Symbol: "ACME", Order: buy},
		{Seq: 2, Symbol: "ACME", Cancel: r.OrderID, Order: market.Order{Account: buy.Account}},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("the journal holds\n%+v\nwant\n%+v", got, want)
	}
}
