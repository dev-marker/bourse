package book_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/fiston-user/bourse/internal/book"
	"github.com/fiston-user/bourse/internal/market"
)

var acme = market.Instrument{Symbol: "ACME", Decimals: 2, TickSize: 1, LotSize: 1}

func limit(id uint64, side market.Side, price market.Price, qty market.Qty) market.Order {
	return market.Order{ID: id, Side: side, Type: market.Limit, TIF: market.GTC, Price: price, Qty: qty}
}

func TestBestPrices(t *testing.T) {
	b := book.New(acme)
	b.Add(limit(1, market.Buy, 10124, 2000))
	b.Add(limit(2, market.Buy, 10125, 400))
	b.Add(limit(3, market.Sell, 10128, 1200))
	b.Add(limit(4, market.Sell, 10127, 300))

	if p, q, _ := b.Best(market.Buy); p != 10125 || q != 400 {
		t.Errorf("best bid: got %d x %d, want 10125 x 400", p, q)
	}
	if p, q, _ := b.Best(market.Sell); p != 10127 || q != 300 {
		t.Errorf("best ask: got %d x %d, want 10127 x 300", p, q)
	}
}

func TestCancel(t *testing.T) {
	b := book.New(acme)
	b.Add(limit(1, market.Sell, 10127, 300))
	b.Add(limit(2, market.Sell, 10127, 200))
	b.Add(limit(3, market.Sell, 10128, 500))

	if !b.Cancel(1) {
		t.Fatal("cancel 1: order not found")
	}
	if p, q, _ := b.Best(market.Sell); p != 10127 || q != 200 {
		t.Errorf("after cancelling 1: got %d x %d, want 10127 x 200", p, q)
	}
	b.Cancel(2)
	if p, q, _ := b.Best(market.Sell); p != 10128 || q != 500 {
		t.Errorf("after cancelling 2: got %d x %d, want 10128 x 500", p, q)
	}
	if b.Cancel(2) {
		t.Error("cancelling 2 twice: want false")
	}
	if b.Len() != 1 {
		t.Errorf("len: got %d, want 1", b.Len())
	}
}

func TestDuplicateID(t *testing.T) {
	b := book.New(acme)
	b.Add(limit(1, market.Buy, 10125, 100))
	if err := b.Add(limit(1, market.Buy, 10120, 100)); err != book.ErrDuplicateID {
		t.Errorf("got %v, want ErrDuplicateID", err)
	}
}

func ExampleBook_Depth() {
	b := book.New(acme)
	b.Add(limit(1, market.Sell, 10130, 500))
	b.Add(limit(2, market.Sell, 10127, 300))
	b.Add(limit(3, market.Sell, 10127, 100))
	b.Add(limit(4, market.Buy, 10125, 400))
	b.Add(limit(5, market.Buy, 10120, 700))

	for _, l := range b.Depth(market.Sell, 5) {
		fmt.Printf("ask %s  %4d  (%d orders)\n", acme.FormatPrice(l.Price), l.Qty, l.Orders)
	}
	for _, l := range b.Depth(market.Buy, 5) {
		fmt.Printf("bid %s  %4d  (%d orders)\n", acme.FormatPrice(l.Price), l.Qty, l.Orders)
	}
	// Output:
	// ask 101.27   400  (2 orders)
	// ask 101.30   500  (1 orders)
	// bid 101.25   400  (1 orders)
	// bid 101.20   700  (1 orders)
}

// a book with depth: 10,000 orders spread over 200 price levels on each side of 100.00
func deepBook() *book.Book {
	b := book.New(acme)
	r := rand.New(rand.NewPCG(1, 2))
	for id := uint64(1); id <= 10_000; id++ {
		side, price := market.Buy, market.Price(9999-r.IntN(200))
		if id%2 == 0 {
			side, price = market.Sell, market.Price(10001+r.IntN(200))
		}
		b.Add(limit(id, side, price, market.Qty(1+r.IntN(500))))
	}
	return b
}

func BenchmarkAdd(b *testing.B) {
	bk := deepBook()
	id := uint64(1_000_000)
	for b.Loop() {
		id++
		bk.Add(limit(id, market.Buy, market.Price(9999-id%200), 100))
	}
}

func BenchmarkAddCancel(b *testing.B) {
	bk := deepBook()
	id := uint64(1_000_000)
	for b.Loop() {
		id++
		bk.Add(limit(id, market.Sell, market.Price(10001+id%200), 100))
		bk.Cancel(id)
	}
}

func BenchmarkBest(b *testing.B) {
	bk := deepBook()
	for b.Loop() {
		bk.Best(market.Buy)
	}
}
