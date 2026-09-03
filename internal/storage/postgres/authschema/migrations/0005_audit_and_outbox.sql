-- 0005 security audit trail and the transactional outbox (TRD §9, G-53).
-- ADDITIVE ONLY. See 0001's header.

-- security_audit_events is the append-only record of security-relevant
-- decisions and mutations. It has no updated_at and nothing in the runtime
-- role's posture will permit UPDATE or DELETE on it: an audit row that can be
-- edited is not an audit row.
CREATE TABLE security_audit_events (
    id              bigserial   PRIMARY KEY,
    occurred_at     timestamptz NOT NULL DEFAULT now(),
    -- event_type is the closed vocabulary of what happened. It is text rather
    -- than an enum type so adding one is an INSERT-compatible change instead
    -- of an ALTER TYPE that locks the table.
    event_type      text        NOT NULL,
    -- The actor is who did it; the subject is who it was done to. They differ
    -- for administrative actions, and collapsing them loses exactly the
    -- distinction an investigation needs. Both are nullable because some
    -- events (a failed login for an unknown identity) genuinely have neither.
    actor_principal_id   uuid   REFERENCES principals (id) ON DELETE SET NULL,
    subject_principal_id uuid   REFERENCES principals (id) ON DELETE SET NULL,
    organization_id uuid        REFERENCES organizations (id) ON DELETE SET NULL,
    -- outcome is closed: an audit trail where "what happened" is free text
    -- cannot be aggregated or alerted on.
    outcome         text        NOT NULL CHECK (outcome IN ('allowed', 'denied', 'error')),
    -- policy_revision records WHICH policy produced a decision, so the
    -- decision stays explicable after the policy changes (ACP-ADR-05).
    policy_revision bigint,
    -- attributes carries event-specific detail. It must never hold credential
    -- material; the naming guard over this lineage does not inspect jsonb
    -- contents, so that rule is the writer's to keep.
    attributes      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    request_id      text,
    client_ip       inet
);

CREATE INDEX security_audit_events_occurred_idx ON security_audit_events (occurred_at DESC);
CREATE INDEX security_audit_events_actor_idx
    ON security_audit_events (actor_principal_id, occurred_at DESC);
CREATE INDEX security_audit_events_subject_idx
    ON security_audit_events (subject_principal_id, occurred_at DESC);
CREATE INDEX security_audit_events_type_idx ON security_audit_events (event_type, occurred_at DESC);

-- auth_outbox_events is the transactional outbox G-53 requires: a state change
-- and its event are written in ONE transaction, so an event can never describe
-- a mutation that did not commit, and a committed mutation can never be
-- missing its event. This is the specific guarantee ACP-ADR-04 §1 cites as the
-- reason the auth schema shares an instance with ops rather than living in a
-- separate cluster -- a cross-cluster split would break exactly this write.
CREATE TABLE auth_outbox_events (
    id             bigserial   PRIMARY KEY,
    -- aggregate_type and aggregate_id name what changed, so a consumer can
    -- order events per aggregate without parsing the payload.
    aggregate_type text        NOT NULL,
    aggregate_id   text        NOT NULL,
    event_type     text        NOT NULL,
    payload        jsonb       NOT NULL,
    -- idempotency_key is what makes redelivery safe. A consumer that has
    -- already applied this key must be able to recognise it without
    -- inspecting the payload -- duplicate or out-of-order delivery must not
    -- double-apply or revert state, which is CHAOS-4885's executed proof.
    idempotency_key text       NOT NULL,
    -- available_at supports delayed and retried delivery without a separate
    -- scheduling table.
    available_at   timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz,
    attempts       integer     NOT NULL DEFAULT 0,
    last_error     text,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX auth_outbox_events_idempotency_key ON auth_outbox_events (idempotency_key);
-- The publisher's hot query is "unpublished, due now, oldest first". A partial
-- index over exactly that predicate keeps it constant-cost as the published
-- backlog grows, which it does forever.
CREATE INDEX auth_outbox_events_pending_idx
    ON auth_outbox_events (available_at, id) WHERE published_at IS NULL;
CREATE INDEX auth_outbox_events_aggregate_idx
    ON auth_outbox_events (aggregate_type, aggregate_id, id);
