package market

import "errors"

// Side says whether an order buys or sells.
type Side uint8

const (
	Buy Side = iota + 1
	Sell
)

// Type says how an order is priced.
type Type uint8

const (
	Limit  Type = iota + 1 // at this price or better
	Market                 // at whatever price the book offers
)

// TimeInForce says how long an order may wait in the book.
type TimeInForce uint8

const (
	GTC TimeInForce = iota + 1 // good till cancelled: waits in the book
	IOC                        // immediate or cancel: fill what you can now, cancel the rest
	FOK                        // fill or kill: all of it now, or nothing
)

// Order is a request to buy or sell an instrument.
type Order struct {
	ID       uint64 // set by the engine, in sequence
	ClientID string // set by the client, so a retry can't place the order twice
	Account  string
	Symbol   string
	Side     Side
	Type     Type
	TIF      TimeInForce
	Price    Price // the limit, unused for market orders
	Qty      Qty
	PostOnly bool // only add to the book, never take from it
}

var (
	ErrBadSide     = errors.New("side must be buy or sell")
	ErrBadType     = errors.New("type must be limit or market")
	ErrMarketRests = errors.New("a market order can't wait in the book")
)

// Validate checks an order against its instrument before it gets anywhere near a book.
func (o Order) Validate(in Instrument) error {
	if o.Side != Buy && o.Side != Sell {
		return ErrBadSide
	}
	if err := in.CheckQty(o.Qty); err != nil {
		return err
	}
	switch o.Type {
	case Limit:
		if o.Price <= 0 || o.Price%in.TickSize != 0 {
			return ErrOffTick
		}
	case Market:
		if o.TIF == GTC || o.PostOnly {
			return ErrMarketRests
		}
	default:
		return ErrBadType
	}
	return nil
}
