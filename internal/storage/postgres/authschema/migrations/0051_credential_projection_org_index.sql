-- 0051 organization read of the credential projection (CHAOS-11199).
-- The daily acr reconcile reads one organization of one domain in reference
-- order. The primary key (domain_owner, external_reference) cannot serve the
-- organization filter; this index leads with both filters and ends on the
-- keyset column, so the read is one range scan with no sort. Additive: one
-- index on a table the projection engine fills, no row moves.
CREATE INDEX credential_projection_org_idx
    ON credential_projection (domain_owner, organization_id, external_reference);
