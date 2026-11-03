-- One human with an active admin membership in one organization: the principal the PDP load asks about. The service
-- projects the principal and the organization into the auth schema when the session is issued. Variables: user_id, org_id.
INSERT INTO public.users (id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES (:'user_id', 'pdp-load@example.invalid', NULL, 'local', true, true, false, 0, now(), now());
INSERT INTO public.organizations (id, slug, name) VALUES (:'org_id', 'pdp-load', 'pdp-load');
INSERT INTO public.memberships (id, user_id, org_id, role, joined_at) VALUES (gen_random_uuid(), :'user_id', :'org_id', 'admin', now());
