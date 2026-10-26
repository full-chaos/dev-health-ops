-- Revision 0158 (down_revision 0157), written by hand.
-- The heavy worker connects as the domain runtime role, which holds nothing on settings. To build an opted-in
-- org's categorization backend it reads that org's own-model configuration and its budget limit through this
-- function only. It runs with its owner's privileges and returns rows of one org: the llm rows of a pinned key
-- list (the default and the categorize override configuration, the provider row of each purpose, the own-model
-- toggle and the prompt-cache opt-in) only while the org is opted in to categorization on its own model (its
-- categorize_byo_enabled row is exactly "true"); the other purposes' api_key rows on the same condition and only
-- while the org stores a non-empty base_url the worker can serve (the default or the categorize override one),
-- because the worker refuses a base_url that carries any key the org stores; and the llm_budget limit row always.
-- No other key, category or org is returned. llmorgsettings.CategorizeBYODefinerKeys,
-- CategorizeBYOBaseURLCheckKeys and CategorizeBYOServedBaseURLKeys pin the same lists.
-- go-river-migrate grants EXECUTE to the domain role (domainPosture RequiredFunctions).

CREATE FUNCTION public.categorize_byo_settings(p_org_id text)
RETURNS TABLE (org_id text, category text, key text, value text, is_encrypted boolean, updated_at timestamp with time zone)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT setting.org_id, setting.category, setting.key, setting.value, setting.is_encrypted, setting.updated_at
    FROM public.settings AS setting
    WHERE setting.org_id OPERATOR(pg_catalog.=) p_org_id
        AND (
            (
                setting.category OPERATOR(pg_catalog.=) 'llm'
                AND (
                    setting.key OPERATOR(pg_catalog.=) ANY (ARRAY[
                        'provider',
                        'model',
                        'api_key',
                        'base_url',
                        'purpose:categorize:provider',
                        'purpose:categorize:model',
                        'purpose:categorize:api_key',
                        'purpose:categorize:base_url',
                        'purpose:investigate:provider',
                        'purpose:explain:provider',
                        'own_model_enabled',
                        'prompt_cache'
                    ]::pg_catalog.text[])
                    OR (
                        setting.key OPERATOR(pg_catalog.=) ANY (ARRAY[
                            'purpose:investigate:api_key',
                            'purpose:explain:api_key'
                        ]::pg_catalog.text[])
                        AND EXISTS (
                            SELECT 1
                            FROM public.settings AS served_url
                            WHERE served_url.org_id OPERATOR(pg_catalog.=) p_org_id
                                AND served_url.category OPERATOR(pg_catalog.=) 'llm'
                                AND served_url.key OPERATOR(pg_catalog.=) ANY (ARRAY[
                                    'base_url',
                                    'purpose:categorize:base_url'
                                ]::pg_catalog.text[])
                                AND COALESCE(served_url.value, '') OPERATOR(pg_catalog.<>) ''
                        )
                    )
                )
                AND EXISTS (
                    SELECT 1
                    FROM public.settings AS opt_in
                    WHERE opt_in.org_id OPERATOR(pg_catalog.=) p_org_id
                        AND opt_in.category OPERATOR(pg_catalog.=) 'llm'
                        AND opt_in.key OPERATOR(pg_catalog.=) 'categorize_byo_enabled'
                        AND opt_in.value OPERATOR(pg_catalog.=) 'true'
                )
            )
            OR (
                setting.category OPERATOR(pg_catalog.=) 'llm_budget'
                AND setting.key OPERATOR(pg_catalog.=) 'limit_micro_usd'
            )
        )
$$;

REVOKE ALL ON FUNCTION public.categorize_byo_settings(text) FROM PUBLIC;
