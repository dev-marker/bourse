-- One transaction for every execution that moves something, and for every deposit.
CREATE TABLE transactions (
    id     bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream text        NOT NULL, -- the symbol that traded, or deposits
    seq    bigint      NOT NULL, -- its seq on that stream
    time   timestamptz NOT NULL
);

-- What each transaction moves. In every asset, a transaction's postings add up to zero.
CREATE TABLE postings (
    transaction_id bigint NOT NULL REFERENCES transactions,
    account        text   NOT NULL, -- alice, bank, exchange:fees
    asset          text   NOT NULL, -- USD in cents, or a symbol in shares
    amount         bigint NOT NULL CHECK (amount <> 0)
);
