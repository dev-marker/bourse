// Package market has the words every part of the exchange shares: instruments, prices, quantities and orders.
package market

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Price is a price in the instrument's smallest unit, cents for most stocks.
type Price int64

// Qty is a number of shares.
type Qty int64

// Instrument is something that can be traded, like the ACME stock.
type Instrument struct {
	Symbol   string
	Decimals int   // digits after the point in a price: 2 means cents
	TickSize Price // every price is a multiple of this
	LotSize  Qty   // every quantity is a multiple of this
}

var (
	ErrBadPrice = errors.New("not a valid price")
	ErrOffTick  = errors.New("price is not a multiple of the tick size")
	ErrBadQty   = errors.New("quantity is not a positive multiple of the lot size")
)

// ParsePrice turns a decimal string like "101.25" into a Price, without ever touching a float.
func (in Instrument) ParsePrice(s string) (Price, error) {
	whole, frac, _ := strings.Cut(s, ".")
	if len(frac) > in.Decimals {
		return 0, fmt.Errorf("%w: %q has more than %d decimals", ErrBadPrice, s, in.Decimals)
	}
	frac += strings.Repeat("0", in.Decimals-len(frac))
	n, err := strconv.ParseInt(whole+frac, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%w: %q", ErrBadPrice, s)
	}
	p := Price(n)
	if p%in.TickSize != 0 {
		return 0, fmt.Errorf("%w: %q", ErrOffTick, s)
	}
	return p, nil
}

// FormatPrice turns a Price back into the decimal string a person reads.
func (in Instrument) FormatPrice(p Price) string {
	if in.Decimals == 0 {
		return fmt.Sprint(p)
	}
	s := fmt.Sprintf("%0*d", in.Decimals+1, p)
	cut := len(s) - in.Decimals
	return s[:cut] + "." + s[cut:]
}

// CheckQty reports whether q is a quantity this instrument can trade.
func (in Instrument) CheckQty(q Qty) error {
	if q <= 0 || q%in.LotSize != 0 {
		return ErrBadQty
	}
	return nil
}
