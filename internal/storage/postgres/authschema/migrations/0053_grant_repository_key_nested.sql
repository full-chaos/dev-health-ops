-- 0053 resource_grants_repository_key_canonical admits a nested repository key
-- (CHAOS-12785, part of CHAOS-10708). A GitLab project path nests up to 20
-- groups deep, so the key is "*" or a lowercase path of 2-20 segments in at
-- most 255 bytes: the pattern internal/auth/repokey.SchemaPattern and the
-- authorization_request.v1 key hold. Every segment ends in [a-z0-9]; only the
-- last may start with '.' (.github).
--
-- The constraint is dropped and added again NOT VALID, then validated, as 0020
-- and 0024 did. Every key of 0020 that stays valid is still valid; a key that
-- ended in '.', '_' or '-' was valid under 0020 and is not now, so the
-- migration counts such rows first and refuses with the count, never a raw
-- VALIDATE error. Production held no repository grant when the rule was ruled.

ALTER TABLE resource_grants DROP CONSTRAINT resource_grants_repository_key_canonical;

ALTER TABLE resource_grants
    ADD CONSTRAINT resource_grants_repository_key_canonical
    CHECK (resource_kind <> 'repository' OR (resource_id ~ '^(?:[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?)?)?)?)?)?)?)?)?)?)?)?)?)?)?)?)?)?)?/(?:[a-z0-9]|[a-z0-9.][a-z0-9._-]{0,98}[a-z0-9])|\*)$' AND octet_length(resource_id) <= 255)) NOT VALID;

DO $$
DECLARE
    violating bigint;
BEGIN
    SELECT count(*) INTO violating FROM resource_grants
    WHERE NOT (resource_kind <> 'repository' OR (resource_id ~ '^(?:[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?(?:/[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?)?)?)?)?)?)?)?)?)?)?)?)?)?)?)?)?)?)?/(?:[a-z0-9]|[a-z0-9.][a-z0-9._-]{0,98}[a-z0-9])|\*)$' AND octet_length(resource_id) <= 255));
    IF violating > 0 THEN
        RAISE EXCEPTION 'migration 0053 refused: resource_grants_repository_key_canonical has % violating row(s); fix or remove them, then migrate again', violating
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
END
$$;

ALTER TABLE resource_grants VALIDATE CONSTRAINT resource_grants_repository_key_canonical;
