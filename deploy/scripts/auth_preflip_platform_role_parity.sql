-- Pre-flip read for the go-api and query-api enforcement cells (CHAOS-13209, F7/F8/F9).
-- Run it read-only, as one point-in-time read, before a cell turns on:
--   BEGIN TRANSACTION READ ONLY; \i deploy/scripts/auth_preflip_platform_role_parity.sql ROLLBACK;
-- Each row is (check, count). The flip needs every count but superusers to be 0:
--   superusers_without_platform_admin: a public superuser go-api admits, with no active auth platform_admin row.
--   platform_admin_not_superuser: an active auth platform_admin row on a user that is no public superuser.
--   platform_admin_on_disabled_copy_of_active_user: an active platform_admin row whose auth principal is disabled (or no
--   user) while public.users is active; go-api's rule refuses it and the PDP's snapshot admits it.
-- docs/operate/runbooks/auth-pdp-outage-and-rollback.md names this file; internal/api/policy reads it in a cell.
SELECT 'superusers' AS check_name, count(*) AS n FROM public.users WHERE is_superuser
UNION ALL
SELECT 'superusers_without_platform_admin', count(*) FROM public.users u
 WHERE u.is_superuser AND NOT EXISTS (SELECT 1 FROM auth.platform_role_assignments r
   WHERE r.principal_id = u.id AND r.role_key = 'platform_admin'
     AND (r.expires_at IS NULL OR r.expires_at > statement_timestamp()))
UNION ALL
SELECT 'platform_admin_not_superuser', count(*) FROM auth.platform_role_assignments r
 WHERE r.role_key = 'platform_admin' AND (r.expires_at IS NULL OR r.expires_at > statement_timestamp())
   AND NOT EXISTS (SELECT 1 FROM public.users u WHERE u.id = r.principal_id AND u.is_superuser)
UNION ALL
SELECT 'platform_admin_on_disabled_copy_of_active_user', count(*) FROM auth.platform_role_assignments r
 JOIN auth.principals p ON p.id = r.principal_id JOIN public.users u ON u.id = r.principal_id
 WHERE r.role_key = 'platform_admin' AND (r.expires_at IS NULL OR r.expires_at > statement_timestamp())
   AND u.is_active IS TRUE AND (p.kind <> 'user' OR p.disabled_at IS NOT NULL);
