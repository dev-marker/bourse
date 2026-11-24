package book

import "fmt"

// Check verifies everything the book promises about itself. It only exists in tests.
func (b *Book) Check() error {
	seen := 0
	for _, s := range []*side{&b.bids, &b.asks} {
		for i, l := range s.levels {
			if i > 0 && s.dir*int(l.price-s.levels[i-1].price) <= 0 {
				return fmt.Errorf("levels out of order at %d", l.price)
			}
			qty, count := 0, 0
			for n := l.head; n != nil; n = n.next {
				if n.order.Qty <= 0 || n.level != l || b.orders[n.order.ID] != n {
					return fmt.Errorf("bad order %d at %d", n.order.ID, l.price)
				}
				qty += int(n.order.Qty)
				count++
			}
			if count == 0 || count != l.count || qty != int(l.qty) {
				return fmt.Errorf("level %d: %d x %d, queue holds %d x %d", l.price, l.qty, l.count, qty, count)
			}
			seen += count
		}
	}
	if seen != len(b.orders) {
		return fmt.Errorf("index has %d orders, levels hold %d", len(b.orders), seen)
	}
	if bid, ask := b.bids.best(), b.asks.best(); bid != nil && ask != nil && bid.price >= ask.price {
		return fmt.Errorf("crossed: bid %d, ask %d", bid.price, ask.price)
	}
	return nil
}
