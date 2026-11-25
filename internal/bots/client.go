package bots

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/fiston-user/bourse/internal/auth"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/marketdata"
)

// Client is one account at the gateway. It signs every request with the account's API key.
type Client struct {
	base    string
	http    *http.Client
	Sent    atomic.Int64 // requests the gateway took
	Limited atomic.Int64 // requests it turned away with a 429
	Trades  atomic.Int64 // trades its orders made
}

// NewClient returns a client of the gateway at base, signing with key.
func NewClient(base string, key auth.Key) *Client {
	t := &auth.Transport{Key: key.ID, Secret: key.Secret}
	return &Client{base: base, http: &http.Client{Transport: t, Timeout: 10 * time.Second}}
}

// Order is a new order, the way the gateway takes it.
type Order struct {
	ClientOrderID string `json:"client_order_id"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	TimeInForce   string `json:"time_in_force"`
	Price         string `json:"price"`
	Qty           int64  `json:"qty"`
	PostOnly      bool   `json:"post_only,omitempty"`
}

// Placed is what happened to an order, event by event.
type Placed struct {
	OrderID uint64  `json:"order_id"`
	Events  []Event `json:"events"`
}

// Event is one thing that happened to an order.
type Event struct {
	Kind  string `json:"kind"`
	Price string `json:"price"`
	Qty   int64  `json:"qty"`
}

// Rests reports whether some of the order is waiting in the book.
func (p Placed) Rests() bool {
	return slices.ContainsFunc(p.Events, func(e Event) bool { return e.Kind == "rested" })
}

// ErrGone means the order isn't in the book any more: it traded, or it was cancelled already.
var ErrGone = errors.New("the order isn't in the book")

// StatusError is an answer from the gateway that isn't a success.
type StatusError struct {
	Status int
	Msg    string
}

func (e *StatusError) Error() string { return fmt.Sprintf("%d %s", e.Status, e.Msg) }

// Place sends an order. A retry can't place it twice: the gateway knows its client order ID.
func (c *Client) Place(ctx context.Context, o Order) (Placed, error) {
	var p Placed
	err := c.do(ctx, http.MethodPost, "/v1/orders", o, &p)
	for _, e := range p.Events {
		if e.Kind == "traded" {
			c.Trades.Add(1)
		}
	}
	return p, err
}

// Cancel takes a resting order out of the book.
func (c *Client) Cancel(ctx context.Context, symbol string, id uint64) error {
	err := c.do(ctx, http.MethodDelete, fmt.Sprintf("/v1/orders/%s/%d", symbol, id), nil, nil)
	if se := (*StatusError)(nil); errors.As(err, &se) && se.Status == http.StatusNotFound {
		return ErrGone
	}
	return err
}

// Holding is how much of an asset the account owns, held for orders or not.
func (c *Client) Holding(ctx context.Context, asset string) (int64, error) {
	var balances []struct{ Asset, Available, Held string }
	if err := c.do(ctx, http.MethodGet, "/v1/balances", nil, &balances); err != nil {
		return 0, err
	}
	for _, b := range balances {
		if b.Asset == asset {
			available, err := strconv.ParseInt(b.Available, 10, 64)
			held, err2 := strconv.ParseInt(b.Held, 10, 64)
			return available + held, errors.Join(err, err2)
		}
	}
	return 0, nil
}

// Best is the best price on one side of a symbol's book, from marketdata at base: the highest bid or
// the lowest ask. Zero means nobody is there.
func Best(ctx context.Context, base, symbol string, side market.Side) (market.Price, error) {
	var book marketdata.Message
	if err := get(ctx, base+"/v1/book/"+symbol+"?depth=1", &book); err != nil {
		return 0, err
	}
	levels := book.Bids
	if side == market.Sell {
		levels = book.Asks
	}
	if len(levels) == 0 {
		return 0, nil
	}
	return levels[0].Price, nil
}

// Last is the price of a symbol's last trade, from marketdata at base. Zero means it never traded.
func Last(ctx context.Context, base, symbol string) (market.Price, error) {
	var candles []marketdata.Candle
	if err := get(ctx, base+"/v1/candles/"+symbol, &candles); err != nil {
		return 0, err
	}
	if len(candles) == 0 {
		return 0, nil
	}
	return candles[len(candles)-1].Close, nil
}

// get reads one of marketdata's public answers. Nothing to sign: market data is for everyone.
func get(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("marketdata: %w", errors.Unwrap(err))
	}
	return decode(res, out)
}

// do sends one request, and decodes the answer into out. A 429 means the account is over its limit:
// we wait as long as Retry-After says, and try again.
func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body []byte
	if in != nil {
		var err error
		if body, err = json.Marshal(in); err != nil {
			return err
		}
	}
	for {
		req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		res, err := c.http.Do(req)
		if err != nil {
			return err
		}
		if res.StatusCode != http.StatusTooManyRequests {
			c.Sent.Add(1)
			return decode(res, out)
		}
		res.Body.Close()
		c.Limited.Add(1)
		wait, _ := strconv.Atoi(res.Header.Get("Retry-After"))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(max(wait, 1)) * time.Second):
		}
	}
}

// decode reads an answer: out on a success, and a StatusError otherwise.
func decode(res *http.Response, out any) error {
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		var p struct{ Error string }
		json.NewDecoder(res.Body).Decode(&p)
		return &StatusError{res.StatusCode, p.Error}
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(res.Body).Decode(out)
}
