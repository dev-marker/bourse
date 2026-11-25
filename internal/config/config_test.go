package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/fiston-user/bourse/internal/config"
)

// env is a made-up environment, for one test.
func env(vars map[string]string) *config.Env {
	return config.New(func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	})
}

// TestDefaults: with nothing set, every setting has its default, except a secret, which is an error.
func TestDefaults(t *testing.T) {
	e := env(nil)
	if got := e.String("ADDR", ":8080", "the public port"); got != ":8080" {
		t.Errorf("ADDR is %q, want :8080", got)
	}
	if got := e.Duration("DRAIN", 10*time.Second, "how long to drain"); got != 10*time.Second {
		t.Errorf("DRAIN is %v, want 10s", got)
	}
	e.Secret("ACCOUNTS_URL", "the accounts database")
	err := e.Err()
	if err == nil || !strings.Contains(err.Error(), "ACCOUNTS_URL isn't set") {
		t.Errorf("a missing secret gave %v", err)
	}
}

// TestWrong: every value that's wrong is reported at once, each with its variable's name.
func TestWrong(t *testing.T) {
	e := env(map[string]string{"RATE_LIMIT": "fast", "SNAPSHOT_EVERY": "-5", "KAFKA": "kafka:19092"})
	e.Float("RATE_LIMIT", 10, "requests a second")
	e.Int("SNAPSHOT_EVERY", 100_000, "inputs between snapshots")
	if got := e.String("KAFKA", "localhost:9092", "the brokers"); got != "kafka:19092" {
		t.Errorf("KAFKA is %q, want kafka:19092", got)
	}
	err := e.Err()
	for _, want := range []string{`RATE_LIMIT is "fast"`, `SNAPSHOT_EVERY is "-5"`} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("the error %v doesn't say %s", err, want)
		}
	}
}
