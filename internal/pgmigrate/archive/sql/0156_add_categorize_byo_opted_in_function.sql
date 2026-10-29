-- Revision 0156 (down_revision 0155), written by hand.
-- The heavy worker connects as the domain runtime role, which holds nothing on settings: that table also holds
-- the organizations' encrypted provider keys. The categorization opt-in is the one settings fact the worker needs
-- for every org, so it reads it through this function only. It runs with its owner's privileges, reads only the
-- llm categorize_byo_enabled row of one org and returns only whether that row says "true" (no row, NULL or any
-- other value is false). go-river-migrate grants EXECUTE to the domain role (domainPosture RequiredFunctions).

CREATE FUNCTION public.categorize_byo_opted_in(p_org_id text)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT COALESCE((
        SELECT setting.value OPERATOR(pg_catalog.=) 'true'
        FROM public.settings AS setting
        WHERE setting.org_id OPERATOR(pg_catalog.=) p_org_id
            AND setting.category OPERATOR(pg_catalog.=) 'llm'
            AND setting.key OPERATOR(pg_catalog.=) 'categorize_byo_enabled'
    ), false)
$$;

REVOKE ALL ON FUNCTION public.categorize_byo_opted_in(text) FROM PUBLIC;
