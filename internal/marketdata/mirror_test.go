package marketdata_test

import (
	"errors"
	"testing"

	"github.com/fiston-user/bourse/internal/marketdata"
)

// TestGap feeds a mirror a snapshot and the updates after it, with one missing. The mirror must notice
// the gap, and a new snapshot must make it whole again.
func TestGap(t *testing.T) {
	type level = marketdata.Level
	var m marketdata.Mirror
	steps := []struct {
		msg  marketdata.Message
		want error
	}{
		{marketdata.Message{Type: "snapshot", Seq: 41, Bids: []level{{Price: 100_95, Qty: 300}}}, nil},
		{marketdata.Message{Type: "book", Seq: 42, Bids: []level{{Price: 100_95, Qty: 100}}}, nil},
		{marketdata.Message{Type: "book", Seq: 44, Bids: []level{{Price: 100_95}}}, marketdata.ErrGap},
		{marketdata.Message{Type: "snapshot", Seq: 44}, nil},
		{marketdata.Message{Type: "book", Seq: 45, Asks: []level{{Price: 101_00, Qty: 50}}}, nil},
	}
	for _, s := range steps {
		if err := m.Apply(s.msg); !errors.Is(err, s.want) {
			t.Fatalf("seq %d: got %v, want %v", s.msg.Seq, err, s.want)
		}
	}
	if len(m.Bids) != 0 || m.Asks[101_00] != 50 || m.Seq != 45 {
		t.Fatalf("the mirror has bids %v, asks %v at seq %d", m.Bids, m.Asks, m.Seq)
	}
}
