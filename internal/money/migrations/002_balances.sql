-- What every account owns: cash in cents under USD, and shares under their symbol.
CREATE TABLE balances (
    account   text   NOT NULL,
    asset     text   NOT NULL,
    available bigint NOT NULL DEFAULT 0 CHECK (available >= 0),
    held      bigint NOT NULL DEFAULT 0 CHECK (held >= 0),
    PRIMARY KEY (account, asset)
);

-- What each order still holds, until it trades or leaves the book.
CREATE TABLE holds (
    account   text   NOT NULL,
    client_id text   NOT NULL,
    symbol    text   NOT NULL,
    order_id  bigint,
    asset     text   NOT NULL,
    price     bigint NOT NULL,
    amount    bigint NOT NULL CHECK (amount >= 0),
    PRIMARY KEY (account, client_id),
    UNIQUE (symbol, order_id)
);
