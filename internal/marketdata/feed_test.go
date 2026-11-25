package marketdata_test

import (
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/marketdata"
)

// TestSlowClient follows GLBX with two real WebSocket clients: one reads everything, the other never
// reads at all. Updates keep coming until the feed drops the slow one, and the fast one must have
// every update, in order, all along.
func TestSlowClient(t *testing.T) {
	feed := marketdata.New(slog.New(slog.DiscardHandler), market.Listed...)
	srv := httptest.NewServer(feed.Handler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/stream?symbol=GLBX"
	dial := func() *websocket.Conn {
		conn, _, err := websocket.Dial(t.Context(), url, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.CloseNow() })
		return conn
	}
	_, fast := dial(), dial() // the first one never reads

	seqs := make(chan uint64)
	go func() {
		for {
			_, b, err := fast.Read(t.Context())
			if err != nil {
				return
			}
			var msgs []marketdata.Message
			if err := json.Unmarshal(b, &msgs); err != nil {
				t.Error(err)
				return
			}
			for _, msg := range msgs {
				seqs <- msg.Seq
			}
		}
	}()
	<-seqs // the snapshot
	var seq uint64
	for feed.Clients("GLBX") == 2 {
		if seq++; seq > 1_000_000 {
			t.Fatal("the slow client is still there after a million updates")
		}
		// Every order that rests changes a level of the book, and that's one update.
		x := &boursev1.Execution{Symbol: "GLBX", Seq: seq, Kind: boursev1.EventKind_EVENT_KIND_RESTED,
			Side: boursev1.Side_SIDE_BUY, Price: int64(100_00 - 5*(seq%100)), Qty: 10}
		if err := feed.Apply([]*boursev1.Execution{x}); err != nil {
			t.Fatal(err)
		}
		if got := <-seqs; got != seq {
			t.Fatalf("the fast client got seq %d, want %d", got, seq)
		}
	}
	t.Logf("the slow client was dropped after %d updates", seq)
}
