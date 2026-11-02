-- 0046 aggregate_revisions admits the signing_key aggregate.
-- NOT ADDITIVE in one place, on purpose (additiveExemptions): it REDEFINES the
-- aggregate_type CHECK and only WIDENS it (nothing is removed from the 0044
-- list), so no stored row can violate it and no pre-flight count is needed.
-- The aggregate_id CHECK is untouched: ses_ and key_ are three letters and an
-- underscore, the shape the 0013 pattern admits.
--
-- signing_key is the aggregate of signing_key.compromised, a revocation event
-- of provisioning-event.v1 (CHAOS-11816). The outbox table has no
-- aggregate_type CHECK; the per-aggregate revision counter does. Session scope
-- revocations ride on the principal and organization aggregates and need no new
-- type. session.revoked needs none either: session is a one-shot aggregate
-- (revision 1, terminal) and keeps no counter row at all (acp, answers-11816-2).
--
-- The ALTER takes ACCESS EXCLUSIVE on aggregate_revisions for as long as the
-- scan to re-validate the CHECK takes.
ALTER TABLE aggregate_revisions
    DROP CONSTRAINT aggregate_revisions_aggregate_type_check,
    ADD CONSTRAINT aggregate_revisions_aggregate_type_check
    CHECK (aggregate_type IN ('principal', 'user', 'external_identity',
        'organization', 'membership', 'platform_role_assignment', 'service_account', 'workload', 'resource_grant', 'invite',
        'external_push_credential', 'signing_key'));
