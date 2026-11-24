package auth

import (
	"context"
	"crypto/rand"
	"embed"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Migrations create the tables the store needs.
//
//go:embed migrations/*.sql
var Migrations embed.FS

// ErrUnknownKey means there's no such key, or it was revoked.
var ErrUnknownKey = errors.New("unknown API key")

// Key is an API key: who it belongs to, and the secret its requests are signed with.
type Key struct {
	ID, Account, Secret string
}

// Store keeps API keys in Postgres.
type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// Lookup returns the key with this ID, unless it was revoked.
func (s *Store) Lookup(ctx context.Context, id string) (Key, error) {
	k := Key{ID: id}
	err := s.db.QueryRow(ctx,
		`SELECT account, secret FROM api_keys WHERE id = $1 AND revoked_at IS NULL`, id,
	).Scan(&k.Account, &k.Secret)
	if errors.Is(err, pgx.ErrNoRows) {
		return Key{}, ErrUnknownKey
	}
	return k, err
}

// Create makes a new key for an account, with a random ID and a random secret.
func (s *Store) Create(ctx context.Context, account string) (Key, error) {
	k := Key{ID: "ak_" + rand.Text()[:16], Account: account, Secret: rand.Text()}
	_, err := s.db.Exec(ctx, `INSERT INTO api_keys (id, account, secret) VALUES ($1, $2, $3)`,
		k.ID, k.Account, k.Secret)
	return k, err
}
