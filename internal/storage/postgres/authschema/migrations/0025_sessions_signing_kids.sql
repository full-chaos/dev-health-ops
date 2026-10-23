-- 0025 the kids that signed an access token of a session. ADDITIVE ONLY. See
-- 0001's header.
--
-- signing_kids is the append-only set of signing-key ids whose tokens the
-- session carried. NULL = never recorded (a row from before this migration):
-- a key compromise falls back to the time-window rule for it. '{}' = minted
-- with no EdDSA kid (the HS256 compatibility class): an EdDSA key compromise
-- leaves it alone. No backfill, no index.
ALTER TABLE sessions ADD COLUMN signing_kids text[];
