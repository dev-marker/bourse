package main

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
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

// proxy passes every request on addr to the gateway, signed. It's for trying the API by hand, with
// tools that can't sign a request themselves.
func proxy(addr string) error {
	t, err := signer()
	if err != nil {
		return err
	}
	target, _ := url.Parse(gatewayURL)
	p := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) { r.SetURL(target) }, Transport: t}
	fmt.Println("signing requests on", addr, "with", t.Key)
	s := &http.Server{Addr: addr, Handler: p, ReadHeaderTimeout: 5 * time.Second}
	return s.ListenAndServe()
}

// burst sends n orders through the gateway, all at once, and counts the answers. Then it shows what
// the account has left.
func burst(n int) error {
	t, err := signer()
	if err != nil {
		return err
	}
	c := &http.Client{Transport: t, Timeout: 10 * time.Second}
	var (
		mu    sync.Mutex
		count = map[int]int{}
		retry string
		errs  []error
		wg    sync.WaitGroup
	)
	run := time.Now().UnixNano()
	for i := range n {
		wg.Go(func() {
			body := fmt.Sprintf(`{"client_order_id":"burst-%d-%d","symbol":"NOVA","side":"buy",`+
				`"price":"90.00","qty":1}`, run, i)
			res, err := c.Post(gatewayURL+"/v1/orders", "application/json", strings.NewReader(body))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			res.Body.Close()
			count[res.StatusCode]++
			if v := res.Header.Get("Retry-After"); v != "" {
				retry = v
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	for _, code := range slices.Sorted(maps.Keys(count)) {
		fmt.Printf("%3d  %s\n", count[code], http.StatusText(code))
	}
	if retry != "" {
		fmt.Printf("retry after %s s\n", retry)
	}
	res, err := c.Get(gatewayURL + "/v1/balances")
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, err = io.Copy(os.Stdout, res.Body)
	fmt.Println()
	return err
}
