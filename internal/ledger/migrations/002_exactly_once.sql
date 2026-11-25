-- An execution can be booked only once.
ALTER TABLE transactions ADD UNIQUE (stream, seq);

-- How far the ledger got in each stream, written in the same transaction as what it booked.
CREATE TABLE applied (
    stream text   PRIMARY KEY,
    seq    bigint NOT NULL
);
