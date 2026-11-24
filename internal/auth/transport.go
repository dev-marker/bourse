package auth

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Transport signs every request it sends with an API key, the way a client library does.
type Transport struct {
	Key, Secret string
	Base        http.RoundTripper // nil means http.DefaultTransport
}

func (t *Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	var body []byte
	if r.Body != nil {
		b, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			return nil, err
		}
		body = b
	}
	r = r.Clone(r.Context())
	r.Body = io.NopCloser(bytes.NewReader(body))
	ts := time.Now().Unix()
	if v := r.Header.Get(TimestampHeader); v != "" {
		ts, _ = strconv.ParseInt(v, 10, 64) // a timestamp that's already there is kept, to try an old one
	}
	r.Header.Set(KeyHeader, t.Key)
	r.Header.Set(TimestampHeader, strconv.FormatInt(ts, 10))
	r.Header.Set(SignatureHeader, Sign(t.Secret, r.Method, r.URL.RequestURI(), ts, body))
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(r)
}
