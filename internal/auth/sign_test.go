package auth_test

import (
	"testing"

	"github.com/fiston-user/bourse/internal/auth"
)

const (
	secret = "DEMO-SECRET-FOR-TESTS-ONLY"
	body   = `{"client_order_id":"a-1","symbol":"GLBX","side":"buy","price":"101.25","qty":300}`
)

// TestVerify signs one request, and then changes one thing at a time. Only the request exactly as it
// was signed may pass.
func TestVerify(t *testing.T) {
	sig := auth.Sign(secret, "POST", "/v1/orders", 1_764_000_000, []byte(body))
	tests := []struct {
		name, secret, sig, method, path string
		ts                              int64
		body                            string
		want                            bool
	}{
		{"as signed", secret, sig, "POST", "/v1/orders", 1_764_000_000, body, true},
		{"another secret", "GUESSED", sig, "POST", "/v1/orders", 1_764_000_000, body, false},
		{"another method", secret, sig, "DELETE", "/v1/orders", 1_764_000_000, body, false},
		{"another path", secret, sig, "POST", "/v1/orders/GLBX/1", 1_764_000_000, body, false},
		{"another time", secret, sig, "POST", "/v1/orders", 1_764_000_001, body, false},
		{"another body", secret, sig, "POST", "/v1/orders", 1_764_000_000, body + " ", false},
		{"no signature", secret, "", "POST", "/v1/orders", 1_764_000_000, body, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := auth.Verify(tt.secret, tt.sig, tt.method, tt.path, tt.ts, []byte(tt.body))
			if got != tt.want {
				t.Fatalf("Verify = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkVerify(b *testing.B) {
	sig := auth.Sign(secret, "POST", "/v1/orders", 1_764_000_000, []byte(body))
	for b.Loop() {
		auth.Verify(secret, sig, "POST", "/v1/orders", 1_764_000_000, []byte(body))
	}
}
