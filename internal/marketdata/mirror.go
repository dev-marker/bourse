package marketdata

import (
	"errors"
	"fmt"

	"github.com/fiston-user/bourse/internal/market"
)

// ErrGap means an update never arrived. A mirror that missed one is wrong from then on, so it needs a
// new snapshot.
var ErrGap = errors.New("an update is missing")

// Mirror is a client's copy of one symbol's market data: a snapshot, and then every update, in order.
type Mirror struct {
	Seq        uint64
	Bids, Asks map[market.Price]market.Qty
	Last       Trade // the latest trade
}

// Apply applies the next message from the feed.
func (m *Mirror) Apply(msg Message) error {
	if msg.Type == "snapshot" {
		*m = Mirror{Seq: msg.Seq, Bids: map[market.Price]market.Qty{}, Asks: map[market.Price]market.Qty{}}
		if n := len(msg.Trades); n > 0 {
			m.Last = msg.Trades[n-1]
		}
	} else if m.Bids == nil {
		return errors.New("an update came before the snapshot")
	} else if msg.Seq != m.Seq+1 {
		return fmt.Errorf("%w: seq %d came after %d", ErrGap, msg.Seq, m.Seq)
	}
	m.Seq = msg.Seq
	set(m.Bids, msg.Bids)
	set(m.Asks, msg.Asks)
	if msg.Trade != nil {
		m.Last = *msg.Trade
	}
	return nil
}

// set gives each level its new quantity. Zero means nothing rests there any more.
func set(side map[market.Price]market.Qty, levels []Level) {
	for _, l := range levels {
		if l.Qty == 0 {
			delete(side, l.Price)
		} else {
			side[l.Price] = l.Qty
		}
	}
}

// Best returns the best level on a side: the highest bid or the lowest ask.
func (m *Mirror) Best(s market.Side) (Level, bool) {
	var best Level
	for p, q := range m.side(s) {
		if best.Qty == 0 || s == market.Buy && p > best.Price || s == market.Sell && p < best.Price {
			best = Level{p, q}
		}
	}
	return best, best.Qty > 0
}

func (m *Mirror) side(s market.Side) map[market.Price]market.Qty {
	if s == market.Buy {
		return m.Bids
	}
	return m.Asks
}
