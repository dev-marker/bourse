package gateway

import (
	"net/http"
	"testing"
	"time"

	"github.com/fiston-user/bourse/internal/market"
)

// TestRepliesBounded: with room for two answers, a third one pushes out the oldest, long before its
// ten minutes are up, and the newest ones are still there.
func TestRepliesBounded(t *testing.T) {
	rs := newReplies(10*time.Minute, 2)
	for _, key := range []string{"a", "b", "c"} {
		p, _ := rs.claim(key, market.Order{})
		rs.finish(p, http.StatusCreated, nil, true)
	}
	if len(rs.byKey) != 2 {
		t.Fatalf("%d answers kept, want 2", len(rs.byKey))
	}
	if _, first := rs.claim("c", market.Order{}); first {
		t.Error("the newest answer was forgotten")
	}
	if _, first := rs.claim("a", market.Order{}); !first {
		t.Error("the oldest answer is still kept")
	}
}
