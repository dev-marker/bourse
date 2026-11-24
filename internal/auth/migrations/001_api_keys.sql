-- An API key: its ID goes with every request, its secret never does.
CREATE TABLE api_keys (
    id         text PRIMARY KEY,
    account    text NOT NULL,
    secret     text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);
