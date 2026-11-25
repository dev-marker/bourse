-- How far the accounts got in each stream, written in the same transaction as what they applied.
CREATE TABLE applied (
    stream text   PRIMARY KEY,
    seq    bigint NOT NULL
);

-- Every event names its order's account and client order ID, and that's how its hold is found now.
ALTER TABLE holds DROP COLUMN order_id;
