// Package pgtest gives a test a Postgres database of its own: a fresh schema in one of the Compose
// stack's databases, migrated, and dropped when the test ends.
package pgtest

import (
	"cmp"
	"context"
	"fmt"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fiston-user/bourse/internal/migrate"
)

// Open connects to database in the Compose stack, in a schema nobody else uses, and runs these
// migrations there. Without Postgres, the test is skipped, and says why.
func Open(t testing.TB, database string, migrations ...fs.FS) *pgxpool.Pool {
	t.Helper()
	url := cmp.Or(os.Getenv("POSTGRES_URL"), "postgres://bourse:bourse@localhost:5432/") + database
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("test_%d", time.Now().UnixNano())
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err == nil {
		_, err = db.Exec(ctx, "CREATE SCHEMA "+schema)
	}
	if err != nil {
		t.Skipf("Postgres isn't running (make up starts it): %v", err)
	}
	t.Cleanup(func() {
		db.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		db.Close()
	})
	for _, m := range migrations {
		if err := migrate.Up(ctx, db, m); err != nil {
			t.Fatal(err)
		}
	}
	return db
}
