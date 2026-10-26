package authflowvenue_test

import "fmt"

// userAndSessionStateSQL is the content of every table a sign-up or a session could write: one hash of all the row texts of
// each, so an in-place update changes it where a row count would not. It is the state of the legs where auth-service runs.
const userAndSessionStateSQL = `SELECT
	(SELECT md5(coalesce(string_agg(t::text, E'\n' ORDER BY t::text), '')) FROM public.users t),
	(SELECT md5(coalesce(string_agg(t::text, E'\n' ORDER BY t::text), '')) FROM public.refresh_tokens t),
	(SELECT md5(coalesce(string_agg(t::text, E'\n' ORDER BY t::text), '')) FROM auth.users t),
	(SELECT md5(coalesce(string_agg(t::text, E'\n' ORDER BY t::text), '')) FROM auth.principals t),
	(SELECT md5(coalesce(string_agg(t::text, E'\n' ORDER BY t::text), '')) FROM auth.refresh_credentials t)`

// goAPIStateSQL is the state of leg go-api, which runs without auth-service: its database has no auth schema.
const goAPIStateSQL = `SELECT
	(SELECT md5(coalesce(string_agg(t::text, E'\n' ORDER BY t::text), '')) FROM public.users t),
	(SELECT md5(coalesce(string_agg(t::text, E'\n' ORDER BY t::text), '')) FROM public.refresh_tokens t)`

// stateSQLForLeg is the state SQL of a corpus leg (AUTH_CORPUS_LEG, ci/auth_corpus_external.sh). On leg acp, owned or
// not, an auth table that is missing fails the query.
func stateSQLForLeg(leg string) (string, error) {
	switch leg {
	case "go-api":
		return goAPIStateSQL, nil
	case "acp":
		return userAndSessionStateSQL, nil
	}
	return "", fmt.Errorf("AUTH_CORPUS_LEG is %q: want go-api or acp", leg)
}
