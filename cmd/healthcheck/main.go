// Command healthcheck asks a service's admin port whether it's healthy, for a container's health
// check: our images have no shell and no curl. It exits with 0 for a 200, and 1 for anything else.
//
//	healthcheck http://localhost:8083/readyz
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: healthcheck URL")
		os.Exit(2)
	}
	client := http.Client{Timeout: 2 * time.Second}
	res, err := client.Get(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "unhealthy:", err)
		os.Exit(1)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "unhealthy:", res.Status)
		os.Exit(1)
	}
}
