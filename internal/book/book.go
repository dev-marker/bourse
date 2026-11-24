// Package book keeps the resting orders of one instrument, sorted by price and then by time.
package book

import (
	"cmp"
	"errors"
	"slices"

	"github.com/fiston-user/bourse/internal/market"
)

// ErrDuplicateID is returned when an order with the same ID is already resting.
var ErrDuplicateID = errors.New("an order with this ID is already in the book")

// side is one side of the book.
type side struct {
	levels  []*level // sorted from the worst price to the best, so the best is last
	byPrice map[market.Price]*level
	dir     int // +1 for bids, where higher is better; -1 for asks, where lower is better
}

// find returns where the level for p is, or where it would go.
func (s *side) find(p market.Price) int {
	i, _ := slices.BinarySearchFunc(s.levels, p, func(l *level, p market.Price) int {
		return s.dir * cmp.Compare(l.price, p)
	})
	return i
}

// levelAt returns the level for p, making it if it doesn't exist yet.
func (s *side) levelAt(p market.Price) *level {
	if l, ok := s.byPrice[p]; ok {
		return l
	}
	l := &level{price: p}
	s.levels = slices.Insert(s.levels, s.find(p), l)
	s.byPrice[p] = l
	return l
}

// drop removes an empty level.
func (s *side) drop(l *level) {
	i := s.find(l.price)
	s.levels = slices.Delete(s.levels, i, i+1)
	delete(s.byPrice, l.price)
}

// best is the level with the best price, or nil.
func (s *side) best() *level {
	if len(s.levels) == 0 {
		return nil
	}
	return s.levels[len(s.levels)-1]
}

// Book is the order book of one instrument. It isn't safe for concurrent use: one goroutine owns it.
type Book struct {
	Instrument market.Instrument
	bids, asks side
	orders     map[uint64]*node
}

// New returns an empty book for in.
func New(in market.Instrument) *Book {
	return &Book{
		Instrument: in,
		bids:       side{byPrice: map[market.Price]*level{}, dir: 1},
		asks:       side{byPrice: map[market.Price]*level{}, dir: -1},
		orders:     map[uint64]*node{},
	}
}

func (b *Book) sideOf(s market.Side) *side {
	if s == market.Buy {
		return &b.bids
	}
	return &b.asks
}

// Add puts a limit order at the back of the queue at its price.
func (b *Book) Add(o market.Order) error {
	if _, ok := b.orders[o.ID]; ok {
		return ErrDuplicateID
	}
	n := &node{order: o}
	b.sideOf(o.Side).levelAt(o.Price).push(n)
	b.orders[o.ID] = n
	return nil
}

// Cancel takes an order out of the book, and reports whether it was there.
func (b *Book) Cancel(id uint64) bool {
	n, ok := b.orders[id]
	if !ok {
		return false
	}
	l := n.level
	l.remove(n)
	if l.count == 0 {
		b.sideOf(n.order.Side).drop(l)
	}
	delete(b.orders, id)
	return true
}

// Best returns the best price on a side and the quantity there, or false if that side is empty.
func (b *Book) Best(s market.Side) (market.Price, market.Qty, bool) {
	l := b.sideOf(s).best()
	if l == nil {
		return 0, 0, false
	}
	return l.price, l.qty, true
}

// Level is one line of the book, as the outside world sees it.
type Level struct {
	Price  market.Price
	Qty    market.Qty
	Orders int
}

// Depth returns up to n levels of a side, best first.
func (b *Book) Depth(s market.Side, n int) []Level {
	levels := b.sideOf(s).levels
	out := make([]Level, 0, min(n, len(levels)))
	for i := len(levels) - 1; i >= 0 && len(out) < n; i-- {
		l := levels[i]
		out = append(out, Level{Price: l.price, Qty: l.qty, Orders: l.count})
	}
	return out
}

// Len is the number of resting orders.
func (b *Book) Len() int {
	return len(b.orders)
}
