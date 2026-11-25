package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/fiston-user/bourse/internal/auth"
)

const (
	window  = 5 * time.Second // how far a request's timestamp may be from our clock
	maxBody = 64 << 10        // the biggest body we read
)

// accountKey is where authenticate puts the account, in a request's context.
type accountKey struct{}

// account is who signed the request.
func account(r *http.Request) string {
	return r.Context().Value(accountKey{}).(string)
}

// authenticate lets a request through only if it's signed with a valid key, and recently.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sig := r.Header.Get(auth.SignatureHeader)
		ts, err := strconv.ParseInt(r.Header.Get(auth.TimestampHeader), 10, 64)
		if sig == "" || err != nil {
			fail(w, http.StatusUnauthorized, "sign the request with your API key")
			return
		}
		if s.now().Sub(time.Unix(ts, 0)).Abs() > window {
			fail(w, http.StatusUnauthorized, "the timestamp is too far from our clock")
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			fail(w, http.StatusRequestEntityTooLarge, "the body is too big")
			return
		}
		ctx, span := tracer.Start(r.Context(), "look up the key")
		key, err := s.keys.Lookup(ctx, r.Header.Get(auth.KeyHeader))
		span.End()
		if err != nil && !errors.Is(err, auth.ErrUnknownKey) {
			s.log.Error("looking up a key", "err", err)
			fail(w, http.StatusInternalServerError, "something went wrong on our side")
			return
		}
		if err != nil || !auth.Verify(key.Secret, sig, r.Method, r.URL.RequestURI(), ts, body) {
			fail(w, http.StatusUnauthorized, "the key or the signature is wrong")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), accountKey{}, key.Account)))
	})
}

// limit turns an account away with a 429 once it's out of tokens, and says when to come back.
func (s *Server) limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok, wait := s.limits.Allow(account(r))
		if !ok {
			limited.Inc()
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
			fail(w, http.StatusTooManyRequests, "too many requests")
			return
		}
		next.ServeHTTP(w, r)
	})
}
