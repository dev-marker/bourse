package book

import "github.com/fiston-user/bourse/internal/market"

// node is one resting order in a level's queue.
type node struct {
	order      market.Order
	level      *level
	prev, next *node
}

// level is every resting order at one price, oldest first.
type level struct {
	price      market.Price
	qty        market.Qty
	count      int
	head, tail *node
}

// push puts n at the back of the queue.
func (l *level) push(n *node) {
	n.level = l
	n.prev = l.tail
	if l.tail != nil {
		l.tail.next = n
	} else {
		l.head = n
	}
	l.tail = n
	l.qty += n.order.Qty
	l.count++
}

// remove takes n out of the queue, wherever it is.
func (l *level) remove(n *node) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		l.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		l.tail = n.prev
	}
	n.prev, n.next = nil, nil
	l.qty -= n.order.Qty
	l.count--
}
