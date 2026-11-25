package web

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHandler asks for every file of the page. Each one comes out of the binary with its content type,
// and no-cache, and a file that isn't there is a 404.
func TestHandler(t *testing.T) {
	srv := httptest.NewServer(Handler())
	defer srv.Close()
	for _, tc := range []struct {
		path, ctype string
		status      int
	}{
		{"/", "text/html; charset=utf-8", http.StatusOK},
		{"/app.js", "text/javascript; charset=utf-8", http.StatusOK},
		{"/style.css", "text/css; charset=utf-8", http.StatusOK},
		{"/nope.js", "text/plain; charset=utf-8", http.StatusNotFound},
	} {
		res, err := http.Get(srv.URL + tc.path)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if got := res.Header.Get("Content-Type"); res.StatusCode != tc.status || got != tc.ctype {
			t.Errorf("GET %s: %d %q, want %d %q", tc.path, res.StatusCode, got, tc.status, tc.ctype)
		}
		if res.StatusCode == http.StatusOK && res.Header.Get("Cache-Control") != "no-cache" {
			t.Errorf("GET %s: Cache-Control %q", tc.path, res.Header.Get("Cache-Control"))
		}
	}
}
