-- 0038 aggregate_revisions admits the external_push_credential aggregate.
-- NOT ADDITIVE in two places, on purpose (additiveExemptions): it REDEFINES the
-- aggregate_type CHECK and the aggregate_id CHECK, and only WIDENS them
-- (nothing is removed from the 0033 type list; every id the 0013 pattern
-- admitted is still admitted), so no stored row can violate either and no
-- pre-flight count is needed.
--
-- external_push_credential is the aggregate of the External Push credential
-- lifecycle events (external-push-lifecycle-event.v1, CHAOS-10894): the
-- customer-push token writes append them to auth_outbox_events in their own
-- transaction (CHAOS-10895). The outbox table has no aggregate_type CHECK;
-- the per-aggregate revision counter does.
--
-- The credential's wire id is the External Push domain's own id, a UUID in
-- canonical text (the registry's external_reference), not a prefixed id like
-- the provisioning aggregates, so the id CHECK admits that shape for this one
-- aggregate type only.
--
-- The ALTER takes ACCESS EXCLUSIVE on aggregate_revisions for as long as the
-- scan to re-validate the CHECK takes.
ALTER TABLE aggregate_revisions
    DROP CONSTRAINT aggregate_revisions_aggregate_id_check,
    ADD CONSTRAINT aggregate_revisions_aggregate_id_check
    CHECK (aggregate_id ~ '^[a-z]{3}_[A-Za-z0-9_-]{1,64}$'
        OR (aggregate_type = 'external_push_credential'
            AND aggregate_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')),
    DROP CONSTRAINT aggregate_revisions_aggregate_type_check,
    ADD CONSTRAINT aggregate_revisions_aggregate_type_check
    CHECK (aggregate_type IN ('principal', 'user', 'external_identity',
        'organization', 'membership', 'platform_role_assignment', 'service_account', 'workload', 'resource_grant', 'invite',
        'external_push_credential'));
