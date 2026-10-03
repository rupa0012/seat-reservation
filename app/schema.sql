-- Idempotent schema: safe to run on every start.

CREATE TABLE IF NOT EXISTS shows (
    id             uuid PRIMARY KEY,
    name           text   NOT NULL,
    price_paise    bigint NOT NULL CHECK (price_paise >= 0),
    per_user_limit int    NOT NULL DEFAULT 4 CHECK (per_user_limit > 0),
    total_seats    int    NOT NULL CHECK (total_seats > 0),
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reservations (
    id           uuid PRIMARY KEY,
    show_id      uuid   NOT NULL REFERENCES shows(id),
    user_id      text   NOT NULL,
    seats        text[] NOT NULL,
    amount_paise bigint NOT NULL CHECK (amount_paise >= 0),
    status       text   NOT NULL CHECK (status IN ('confirmed', 'cancelled')),
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- One row per (show, seat). The CHECK makes a half-owned seat unrepresentable:
-- a seat is either free (no owner) or owned by exactly one user + reservation.
CREATE TABLE IF NOT EXISTS seats (
    show_id        uuid NOT NULL REFERENCES shows(id),
    label          text NOT NULL,
    pos            int  NOT NULL,          -- creation order, for stable listing
    status         text NOT NULL DEFAULT 'available'
                   CHECK (status IN ('available', 'held', 'confirmed')),
    user_id        text,
    reservation_id uuid REFERENCES reservations(id),
    PRIMARY KEY (show_id, label),
    CHECK (
        (status = 'available' AND user_id IS NULL AND reservation_id IS NULL)
        OR
        (status <> 'available' AND user_id IS NOT NULL AND reservation_id IS NOT NULL)
    )
);

-- Speeds up the per-user-limit count.
CREATE INDEX IF NOT EXISTS seats_owner_idx
    ON seats (show_id, user_id) WHERE user_id IS NOT NULL;

-- Exactly-once store: the first outcome (success OR domain decline) for a
-- (user, key) is saved here in the same transaction as the work it describes.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    user_id        text NOT NULL,
    key            text NOT NULL,
    request_hash   text NOT NULL,
    status_code    int  NOT NULL,
    response       jsonb NOT NULL,
    reservation_id uuid,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, key)
);
