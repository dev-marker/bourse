package main

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/fiston-user/bourse/internal/auth"
)

const gatewayURL = "http://localhost:8080"

// signer signs requests with the API key in BOURSE_KEY and BOURSE_SECRET.
func signer() (*auth.Transport, error) {
	t := &auth.Transport{Key: os.Getenv("BOURSE_KEY"), Secret: os.Getenv("BOURSE_SECRET")}
	if t.Key == "" || t.Secret == "" {
		return nil, fmt.Errorf("set BOURSE_KEY and BOURSE_SECRET first")
	}
	return t, nil
}

// proxy passes every request on :8079 to the gateway, signed. It's for trying the API by hand, with
// tools that can't sign a request themselves.
func proxy() error {
	t, err := signer()
	if err != nil {
		return err
	}
	target, _ := url.Parse(gatewayURL)
	p := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) { r.SetURL(target) }, Transport: t}
	fmt.Println("signing requests on :8079 with", t.Key)
	s := &http.Server{Addr: ":8079", Handler: p, ReadHeaderTimeout: 5 * time.Second}
	return s.ListenAndServe()
}

// burst sends n orders through the gateway as fast as it can, and counts the answers.
func burst(n int) error {
	t, err := signer()
	if err != nil {
		return err
	}
	c := &http.Client{Transport: t, Timeout: 10 * time.Second}
	var codes []int
	count := map[int]int{}
	var retry string
	run := time.Now().UnixNano()
	for i := range n {
		body := fmt.Sprintf(`{"client_order_id":"burst-%d-%d","symbol":"NOVA","side":"buy",`+
			`"price":"90.00","qty":1}`, run, i)
		res, err := c.Post(gatewayURL+"/v1/orders", "application/json", strings.NewReader(body))
		if err != nil {
			return err
		}
		res.Body.Close()
		if count[res.StatusCode] == 0 {
			codes = append(codes, res.StatusCode)
		}
		count[res.StatusCode]++
		if v := res.Header.Get("Retry-After"); v != "" {
			retry = v
		}
	}
	for _, code := range codes {
		fmt.Printf("%3d  %s\n", count[code], http.StatusText(code))
	}
	if retry != "" {
		fmt.Printf("retry after %s s\n", retry)
	}
	return nil
}
