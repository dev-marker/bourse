// Package auth signs API requests and checks their signatures, with HMAC-SHA256.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// The headers every signed request carries.
const (
	KeyHeader       = "X-Bourse-Key"
	TimestampHeader = "X-Bourse-Timestamp"
	SignatureHeader = "X-Bourse-Signature"
)

// Sign returns a request's signature: an HMAC-SHA256 of its timestamp, method, path and body hash.
func Sign(secret, method, path string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d\n%s\n%s\n%x", ts, method, path, sha256.Sum256(body))
	return hex.EncodeToString(mac.Sum(nil))
}

// Verify reports whether sig is the request's signature. It takes as long whatever sig is.
func Verify(secret, sig, method, path string, ts int64, body []byte) bool {
	want := Sign(secret, method, path, ts, body)
	return hmac.Equal([]byte(sig), []byte(want))
}
