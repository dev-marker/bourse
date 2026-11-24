package book_test

import (
	"reflect"
	"testing"

	"github.com/fiston-user/bourse/internal/book"
	"github.com/fiston-user/bourse/internal/market"
)

// standard is the ACME book from the slides, every order from one market maker.
func standard() *book.Book {
	b := book.New(acme)
	for _, o := range []market.Order{
		limit(1, market.Sell, 10127, 300),
		limit(2, market.Sell, 10127, 200),
		limit(3, market.Sell, 10128, 1200),
		limit(4, market.Buy, 10125, 400),
		limit(5, market.Buy, 10124, 2000),
	} {
		o.Account = "maker"
		b.Add(o)
	}
	return b
}

// buy and sell are incoming limit orders from account "you", always ID 10.
func buy(price market.Price, qty market.Qty, tif market.TimeInForce) market.Order {
	return market.Order{ID: 10, Account: "you", Side: market.Buy, Type: market.Limit,
		TIF: tif, Price: price, Qty: qty}
}

func sell(price market.Price, qty market.Qty, tif market.TimeInForce) market.Order {
	o := buy(price, qty, tif)
	o.Side = market.Sell
	return o
}

// tr is a trade against our incoming order 10.
func tr(maker uint64, price market.Price, qty market.Qty) book.Trade {
	return book.Trade{Maker: maker, Taker: 10, Price: price, Qty: qty, MakerAccount: "maker"}
}

func TestSubmit(t *testing.T) {
	gtc, ioc, fok := market.GTC, market.IOC, market.FOK
	tests := []struct {
		name              string
		in                market.Order
		trades            []book.Trade
		rested, cancelled market.Qty
		err               error
	}{
		{name: "rests when it can't trade", in: buy(10126, 100, gtc), rested: 100},
		{name: "trades at the maker's price", in: buy(10130, 100, gtc),
			trades: []book.Trade{tr(1, 10127, 100)}},
		{name: "time priority, then a partial fill", in: buy(10127, 400, gtc),
			trades: []book.Trade{tr(1, 10127, 300), tr(2, 10127, 100)}},
		{name: "walks two levels, rests the rest", in: buy(10128, 2000, gtc),
			trades: []book.Trade{tr(1, 10127, 300), tr(2, 10127, 200), tr(3, 10128, 1200)}, rested: 300},
		{name: "IOC cancels what's left", in: buy(10127, 700, ioc),
			trades: []book.Trade{tr(1, 10127, 300), tr(2, 10127, 200)}, cancelled: 200},
		{name: "FOK that can't fill in full", in: buy(10127, 700, fok), err: book.ErrCannotFill},
		{name: "post-only that would trade", in: postOnly(sell(10125, 100, gtc)), err: book.ErrWouldTake},
	}
	for _, tt := range tests {
		got, err := standard().Submit(tt.in)
		want := book.Result{Trades: tt.trades, Rested: tt.rested, Cancelled: tt.cancelled}
		if err != tt.err || !reflect.DeepEqual(got, want) {
			t.Errorf("%s:\n got %+v, %v\nwant %+v, %v", tt.name, got, err, want, tt.err)
		}
	}
}

func postOnly(o market.Order) market.Order {
	o.PostOnly = true
	return o
}

func TestSelfTrade(t *testing.T) {
	o := buy(10127, 100, market.GTC)
	o.Account = "maker"
	b := standard()
	one, _ := b.Order(1)
	two, _ := b.Order(2)
	got, _ := b.Submit(o)
	want := book.Result{Rested: 100, Removed: []market.Order{one, two}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
