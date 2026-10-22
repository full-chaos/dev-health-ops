-- 0018 an increment-only way to raise go-api's users.token_version (CHAOS-10705).
-- ADDITIVE ONLY. See 0001's header.
--
-- A password reset or change ends the access tokens go-api minted without a
-- session id by raising public.users.token_version, as go-api's own reset does.
-- The runtime role must not hold UPDATE on public.users: a column grant would
-- let it write ANY value, a lower one included, and a lower value makes old
-- tokens valid again. This function is the only write path, and it can only add
-- one.
--
-- It runs as its owner, the migration role, which owns public.users, so the
-- runtime role gets EXECUTE (granted by the runtime grants step, not here) and
-- nothing on the table. search_path is pinned to pg_catalog and pg_temp and
-- every name below is qualified, so a caller's search_path changes nothing.
-- EXECUTE is revoked from PUBLIC. Removed by CHAOS-10630 (the users cutover).
--
-- The body is plpgsql: its table reference is resolved at the first call, so
-- the migration also applies on a database that has no public.users yet.
CREATE FUNCTION bump_legacy_token_version(p_user_id uuid) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    v_version bigint;
BEGIN
    UPDATE public.users SET token_version = coalesce(token_version, 0) + 1
    WHERE id = p_user_id
    RETURNING token_version INTO v_version;
    RETURN v_version;
END;
$$;

REVOKE ALL ON FUNCTION bump_legacy_token_version(uuid) FROM PUBLIC;
