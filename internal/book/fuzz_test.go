package book_test

import (
	"testing"

	"github.com/fiston-user/bourse/internal/book"
	"github.com/fiston-user/bourse/internal/market"
)

// FuzzBook plays random orders and cancels against one book, and checks every promise after each one.
func FuzzBook(f *testing.F) {
	f.Add([]byte{0, 3, 40, 0, 5, 41, 4, 0, 90})
	f.Fuzz(func(t *testing.T, data []byte) {
		b := book.New(acme)
		var id uint64
		for i := 0; i+3 <= len(data); i += 3 {
			kind, p, q := data[i], data[i+1], data[i+2]
			id++
			if kind%6 == 5 {
				b.Cancel(uint64(p)%id + 1)
			} else {
				o := market.Order{ID: id, Account: string(rune('a' + kind%3))}
				o.Side, o.Type, o.TIF = market.Side(q%2+1), market.Limit, market.GTC
				o.Price, o.Qty = market.Price(10_000+int(p%20)), market.Qty(q/2%50+1)
				switch kind % 6 {
				case 1:
					o.TIF = market.IOC
				case 2:
					o.TIF = market.FOK
				case 3:
					o.PostOnly = true
				case 4:
					o.Type, o.TIF = market.Market, market.IOC
				}
				res, err := b.Submit(o)
				var traded market.Qty
				for _, tr := range res.Trades {
					traded += tr.Qty
				}
				if err == nil && traded+res.Rested+res.Cancelled != o.Qty {
					t.Fatalf("order %d: %d traded + %d rested + %d cancelled, want %d",
						id, traded, res.Rested, res.Cancelled, o.Qty)
				}
			}
			if err := b.Check(); err != nil {
				t.Fatalf("after step %d: %v", id, err)
			}
		}
	})
}
