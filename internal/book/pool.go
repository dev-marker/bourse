package book

import "github.com/fiston-user/bourse/internal/market"

// alloc returns a node for o, reusing one that has left the book if there is one.
func (b *Book) alloc(o market.Order) *node {
	n := b.free
	if n == nil {
		return &node{order: o}
	}
	b.free = n.next
	n.next = nil
	n.order = o
	return n
}

// release keeps a node that has left the book, ready to be used again.
func (b *Book) release(n *node) {
	*n = node{next: b.free}
	b.free = n
}
