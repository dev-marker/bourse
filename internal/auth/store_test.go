package auth_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fiston-user/bourse/internal/auth"
	"github.com/fiston-user/bourse/internal/migrate"
)

// TestStore needs Postgres from the Compose stack. Without it, the test is skipped, and says why.
func TestStore(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://bourse:bourse@localhost:5432/accounts"
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, url)
	if err == nil {
		err = db.Ping(ctx)
	}
	if err != nil {
		t.Skipf("Postgres isn't running (make up starts it): %v", err)
	}
	defer db.Close()
	if err := migrate.Up(ctx, db, auth.Migrations); err != nil {
		t.Fatal(err)
	}

	store := auth.NewStore(db)
	k, err := store.Create(ctx, "test-account")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := store.Lookup(ctx, k.ID); err != nil || got != k {
		t.Fatalf("Lookup = %+v, %v, want %+v", got, err, k)
	}
	if _, err := store.Lookup(ctx, "ak_NOT_A_KEY"); !errors.Is(err, auth.ErrUnknownKey) {
		t.Fatalf("Lookup of a key nobody made = %v, want ErrUnknownKey", err)
	}
}
