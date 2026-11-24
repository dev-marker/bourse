// Package migrate brings a database's schema up to date, from SQL files built into the program.
package migrate

import (
	"context"
	"fmt"
	"io/fs"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Up runs every .sql file in the migrations folder of fsys that hasn't run yet, in the order of their
// names, each in a transaction of its own. A lock keeps two programs that start together from running
// one twice.
func Up(ctx context.Context, db *pgxpool.Pool, fsys fs.FS) error {
	names, err := fs.Glob(fsys, "migrations/*.sql")
	if err != nil {
		return err
	}
	slices.Sort(names)
	conn, err := db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(7271)`); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(7271)`)
	_, err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name   text PRIMARY KEY,
		ran_at timestamptz NOT NULL DEFAULT now())`)
	if err != nil {
		return err
	}
	for _, name := range names {
		sql, err := fs.ReadFile(fsys, name)
		if err != nil {
			return err
		}
		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			tag, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (name) VALUES ($1) ON CONFLICT DO NOTHING`, name)
			if err != nil || tag.RowsAffected() == 0 {
				return err // it ran before
			}
			_, err = tx.Exec(ctx, string(sql))
			return err
		})
		if err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}
