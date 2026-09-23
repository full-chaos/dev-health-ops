package teamsidentity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Routes returns this area's routes: the 7 pure-CRUD team+identity admin
// routes (list/create/get/patch/delete teams, list/create identities) plus
// GET /teams/discover (dispatched from inside getTeam -- see its own
// comment below for why). Import and drift-review routes are separate,
// later PRs under CHAOS-6251 (see the package doc comment). pool/decryptor
// resolve a stored provider credential for discover; both may be nil,
// mirroring the rest of this package's Deps.ClickHouse-nil convention --
// discover then answers CodeInternal rather than dereferencing either.
func Routes(conn driver.Conn, guard *policy.Guard, logger *slog.Logger, pool *pgxpool.Pool, decryptor providerfoundation.CredentialDecryptor) []httpapi.Route {
	if logger == nil {
		logger = slog.Default()
	}
	h := handlers{store: Store{Conn: conn}, credentials: discoverCredentials{Pool: pool, Decryptor: decryptor}, logger: logger}
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: "/api/v1/admin/teams",
			Handler: guard.Wrap(policy.AdminOrg, http.HandlerFunc(h.listTeams))},
		{Method: http.MethodPost, Pattern: "/api/v1/admin/teams",
			Handler: h.decodeFirst(guard, http.HandlerFunc(h.createOrUpdateTeam))},
		// CHAOS-6310 r1 finding #7: a literal `/teams/discover` stub was
		// tried here to keep it from ever falling through to the
		// {team_id} wildcard below. REVERTED -- it cannot be registered
		// safely in this framework. httpapi's server (server.go) adds a
		// method-less catch-all registration for EVERY pattern that has at
		// least one route (its own 405 responder), and Go's http.ServeMux
		// panics at construction whenever a more-specific-PATH pattern's
		// registered method set is not a subset (or superset covering the
		// same requests) of an overlapping less-specific-path pattern's --
		// which a method-less catch-all always violates, no matter which
		// or how many explicit methods the literal also carries (proved
		// live across three attempts: GET-only, GET+HEAD, and all of
		// GET/HEAD/DELETE/PATCH each panicked differently).
		// TestTeamsDiscoverStaticRouteWins still documents the underlying
		// net/http precedence rule (a literal beats a wildcard at the same
		// position, order-independent) for a mux with NO overlapping
		// partial-method registrations -- true and still relevant once
		// CHAOS-6311 adds a real /teams/discover route, but that PR's
		// implementer must be aware of the catch-all conflict above:
		// registering a real handler here needs either a framework fix in
		// httpapi/server.go, or a method set proven not to trigger it.
		// TestRoutesRegisterWithoutPanicking proves the ACTUAL route table
		// below builds cleanly today, and will catch any future addition
		// (this one included) that reintroduces the same conflict.
		// r2 (CHAOS-6310) finding #4: the wildcard capturing "discover" was
		// proved live to answer Go's own 404 "Team not found" where Python
		// answers 422 "Field required" for a missing `provider` query
		// parameter -- getTeam below now intercepts exactly that one case
		// (the only one this PR can prove without CHAOS-6311's business
		// logic) before it ever reaches a team lookup.
		{Method: http.MethodDelete, Pattern: "/api/v1/admin/teams/{team_id}", Allow: "DELETE",
			Handler: guard.Wrap(policy.AdminOrg, http.HandlerFunc(h.deleteTeam))},
		{Method: http.MethodGet, Pattern: "/api/v1/admin/teams/{team_id}",
			Handler: guard.Wrap(policy.AdminOrg, http.HandlerFunc(h.getTeam))},
		{Method: http.MethodPatch, Pattern: "/api/v1/admin/teams/{team_id}",
			Handler: h.decodeFirst(guard, http.HandlerFunc(h.updateTeam))},
		// POST /teams/import is dispatched from inside postTeamWildcard the
		// same way GET /teams/discover is dispatched from inside getTeam
		// -- a second literal registration for it panics construction for
		// the identical reason (see the comment above).
		{Method: http.MethodPost, Pattern: "/api/v1/admin/teams/{team_id}",
			Handler: h.decodeFirst(guard, http.HandlerFunc(h.postTeamWildcard))},
		{Method: http.MethodGet, Pattern: "/api/v1/admin/identities",
			Handler: guard.Wrap(policy.AdminOrg, http.HandlerFunc(h.listIdentities))},
		{Method: http.MethodPost, Pattern: "/api/v1/admin/identities",
			Handler: h.decodeFirst(guard, http.HandlerFunc(h.createOrUpdateIdentity))},
	}
}

type handlers struct {
	store       Store
	credentials discoverCredentials
	logger      *slog.Logger
}

func (h handlers) internal(w http.ResponseWriter, r *http.Request, what string, err error) {
	h.logger.ErrorContext(r.Context(), "api route failed", slog.String("path", r.URL.Path),
		slog.String("step", what), slog.String("error", err.Error()))
	policy.WriteInternal(w)
}

// orgIDOf is get_admin_org_id's own resolution: the CURRENT authenticated
// user's own org_id (current_user.org_id), never the Scope-resolved
// impersonation-aware org policy.OrgIDFrom reads for other routes --
// guard.Wrap(policy.AdminOrg, ...) already checked it is non-empty.
func orgIDOf(ctx context.Context) string {
	return policy.UserFrom(ctx).OrgID
}

type bodyKey struct{}

// decodeFirst reads the body before authentication, exactly as
// internal/api/orgs's own decodeFirst does (FastAPI decodes the body
// before any Depends() runs). A local, package-private context key rather
// than the shared policy.Guard.BodyFirst/policy.BodyFrom pair: that pair's
// context key is unexported to internal/api/policy, so a handler-level
// unit test outside that package cannot inject a body into it without
// driving a full HTTP round trip through a real Authenticator (a real
// edgetoken.Verifier + Postgres-backed Store) -- disproportionate for
// testing this package's own decode/decision logic, which
// internal/api/policy's own tests already cover for Guard/BodyFirst
// itself.
func (h handlers) decodeFirst(guard *policy.Guard, next http.Handler) http.Handler {
	guarded := guard.Wrap(policy.AdminOrg, next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, outcome, failure, err := pybody.Read(r)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				policy.WriteDetail(w, http.StatusRequestEntityTooLarge, "Request Entity Too Large", nil)
				return
			}
			h.internal(w, r, "read body", err)
			return
		}
		switch outcome {
		case pybody.DecodeFailed:
			policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail([]pybody.Error{*failure}), nil)
			return
		case pybody.ParseFailed:
			policy.WriteDetail(w, http.StatusBadRequest, "There was an error parsing the body", nil)
			return
		}
		guarded.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), bodyKey{}, body)))
	})
}

func bodyFromContext(ctx context.Context) pybody.Body {
	body, _ := ctx.Value(bodyKey{}).(pybody.Body)
	return body
}

func pathParam(r *http.Request, name string) string {
	return r.PathValue(name)
}

// --- response builders -----------------------------------------------------

func teamJSON(team Team) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", team.ID)
	out.Set("team_id", team.TeamID)
	out.Set("name", team.Name)
	setOptionalString(out, "description", team.Description)
	out.Set("repo_patterns", stringsToValues(team.RepoPatterns))
	out.Set("project_keys", stringsToValues(team.ProjectKeys))
	out.Set("extra_data", pyjson.NewObject())
	out.Set("managed_fields", []pyjson.Value{})
	out.Set("sync_policy", int64(2))
	out.Set("flagged_changes", nil)
	out.Set("last_drift_sync_at", nil)
	out.Set("is_active", team.IsActive)
	out.Set("created_at", pytimeRFC3339(team.UpdatedAt))
	out.Set("updated_at", pytimeRFC3339(team.UpdatedAt))
	return out
}

func identityJSON(identity Identity) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", identity.ID)
	out.Set("canonical_id", identity.CanonicalID)
	setOptionalString(out, "display_name", identity.DisplayName)
	setOptionalString(out, "email", identity.Email)
	providers := pyjson.NewObject()
	if identity.ProviderIdentities != nil {
		for _, key := range identity.ProviderIdentities.Keys {
			values, _ := identity.ProviderIdentities.Get(key)
			providers.Set(key, stringsToValues(values))
		}
	}
	out.Set("provider_identities", providers)
	out.Set("team_ids", stringsToValues(identity.TeamIDs))
	out.Set("is_active", identity.IsActive)
	out.Set("created_at", pytimeRFC3339(identity.UpdatedAt))
	out.Set("updated_at", pytimeRFC3339(identity.UpdatedAt))
	return out
}

func setOptionalString(out *pyjson.Object, key string, value *string) {
	if value != nil {
		out.Set(key, *value)
	} else {
		out.Set(key, nil)
	}
}

func stringsToValues(values []string) []pyjson.Value {
	out := make([]pyjson.Value, len(values))
	for index, value := range values {
		out[index] = value
	}
	return out
}

// --- handlers ----------------------------------------------------------

func (h handlers) listTeams(w http.ResponseWriter, r *http.Request) {
	activeOnly, badBool := queryBoolDefaultTrue(r, "active_only")
	if badBool != nil {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail([]pybody.Error{*badBool}), nil)
		return
	}
	teams, err := h.store.ListTeams(r.Context(), orgIDOf(r.Context()), activeOnly)
	if err != nil {
		h.internal(w, r, "list teams", err)
		return
	}
	out := make([]pyjson.Value, len(teams))
	for index, team := range teams {
		out[index] = teamJSON(team)
	}
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

func (h handlers) createOrUpdateTeam(w http.ResponseWriter, r *http.Request) {
	body := bodyFromContext(r.Context())
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	teamID, _ := problems.RequiredString(object, "team_id", 1, 0)
	name, _ := problems.RequiredString(object, "name", 1, 0)
	description, hasDescription := problems.OptionalString(object, "description", 0, 0)
	// TeamMappingCreate declares repo_patterns/project_keys/extra_data/
	// managed_fields/sync_policy WITHOUT `| None` (Field(default_factory=...)
	// or Field(default=1, ...)): absent uses the default, but an explicit
	// JSON null is itself a validation error -- unlike TeamMappingUpdate,
	// whose fields all ARE `| None`. Defaulted* (not Optional*) matches that.
	repoPatterns, _ := problems.DefaultedStringList(object, "repo_patterns")
	projectKeys, _ := problems.DefaultedStringList(object, "project_keys")
	// extra_data/managed_fields/sync_policy are accepted (validated) like
	// Python's schema, but never forwarded to the ClickHouse write -- the
	// CH-backed create_or_update never took them (see teams.py's
	// create_or_update_team, which passes only team_id/name/description/
	// repo_patterns/project_keys).
	problems.DefaultedAnyDict(object, "extra_data")
	problems.DefaultedStringList(object, "managed_fields")
	problems.DefaultedBoundedInt(object, "sync_policy", 0, 2)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}

	write := TeamWrite{TeamID: teamID, Name: name, RepoPatterns: &repoPatterns, ProjectKeys: &projectKeys}
	if hasDescription {
		write.Description = &description
	}
	team, err := h.store.CreateOrUpdateTeam(r.Context(), orgIDOf(r.Context()), write)
	if err != nil {
		h.internal(w, r, "create or update team", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, teamJSON(team), nil)
}

func (h handlers) deleteTeam(w http.ResponseWriter, r *http.Request) {
	teamID := pathParam(r, "team_id")
	deleted, err := h.store.DeleteTeam(r.Context(), orgIDOf(r.Context()), teamID)
	if err != nil {
		h.internal(w, r, "delete team", err)
		return
	}
	if !deleted {
		policy.WriteDetail(w, http.StatusNotFound, "Team not found", nil)
		return
	}
	out := pyjson.NewObject()
	out.Set("deleted", true)
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

// discoverProviderPattern is FastAPI's own Query(..., pattern=
// "^(github|gitlab|jira|linear)$") for GET /teams/discover's `provider`
// parameter (teams.py:159-161).
var discoverProviderPattern = regexp.MustCompile(`^(github|gitlab|jira|linear)$`)

// discoverMissingProviderDetail is the exact 422 body FastAPI's own
// Query(...) dependency raises for GET /api/v1/admin/teams/discover
// without its required `provider` query parameter (captured against the
// live venue).
func discoverMissingProviderDetail() *pyjson.Object {
	return pybody.Detail([]pybody.Error{{
		Type: "missing", Loc: []pyjson.Value{"query", "provider"}, Msg: "Field required", Input: nil,
	}})
}

// discoverInvalidProviderDetail mirrors FastAPI's pattern-mismatch 422 for
// a `provider` value outside github|gitlab|jira|linear.
func discoverInvalidProviderDetail(provider string) *pyjson.Object {
	ctx := pyjson.NewObject()
	ctx.Set("pattern", discoverProviderPattern.String())
	return pybody.Detail([]pybody.Error{{
		Type: "string_pattern_mismatch", Loc: []pyjson.Value{"query", "provider"},
		Msg:   fmt.Sprintf("String should match pattern '%s'", discoverProviderPattern.String()),
		Input: provider, Ctx: ctx,
	}})
}

// discoverTeams is GET /api/v1/admin/teams/discover, reached from inside
// getTeam (see the route-table comment on Routes for why this cannot be a
// second registered pattern). Resolves a stored, encrypted provider
// credential (providerfoundation, the same integration_credentials table
// and Fernet decryptor internal/apiservice already builds for
// webhookintake), then calls the live provider API.
func (h handlers) discoverTeams(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if !query.Has("provider") {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, discoverMissingProviderDetail(), nil)
		return
	}
	provider := query.Get("provider")
	if !discoverProviderPattern.MatchString(provider) {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, discoverInvalidProviderDetail(provider), nil)
		return
	}
	ctx := r.Context()
	orgID := orgIDOf(ctx)
	credential, err := h.credentials.resolve(ctx, orgID, provider, query.Get("credential_id"), query.Get("credential_name"))
	if err != nil {
		var ambiguous *providerfoundation.CredentialAmbiguousError
		switch {
		case errors.As(err, &ambiguous):
			policy.WriteDetail(w, http.StatusConflict, ambiguous.Error(), nil)
		case errors.Is(err, providerfoundation.ErrCredentialNotFound):
			policy.WriteDetail(w, http.StatusNotFound, fmt.Sprintf("No credentials found for provider '%s'", provider), nil)
		default:
			h.internal(w, r, "resolve discover credential", err)
		}
		return
	}
	// Python validates the credential's shape (400/401) BEFORE it resolves
	// an org/group or calls a provider, and never sends a stored base URL
	// to github (PAT) or linear -- see prepareDiscoveryCredential.
	prepared, err := prepareDiscoveryCredential(ctx, provider, credential)
	if err != nil {
		var statusErr *discoveryStatusError
		if errors.As(err, &statusErr) {
			policy.WriteDetail(w, statusErr.Status, statusErr.Detail, nil)
		} else {
			h.internal(w, r, "prepare discover credential", err)
		}
		return
	}
	var (
		teams     []discoveredTeam
		truncated bool
		warnings  []string
	)
	switch provider {
	case "jira":
		teams, err = discoverJira(ctx, prepared)
	case "linear":
		teams, err = discoverLinear(ctx, prepared)
	case "github":
		// Resolution order mirrors teams.py:231-249 exactly: explicit
		// ?org= -> credential.config["org"] -> owners derived from this
		// org's existing repo sync configurations
		// (_derive_owners_from_sync_configs, teams.py:55-79). The last
		// step can resolve MORE THAN ONE org (an org with several active
		// GitHub sync configs pointed at different owners), so discovery
		// loops over every resolved name and the combined results are
		// deduped by provider_team_id (_dedupe_teams, teams.py:82-92).
		var orgNames []string
		switch {
		case query.Get("org") != "":
			orgNames = []string{query.Get("org")}
		case credential.Config["org"] != "":
			orgNames = []string{credential.Config["org"]}
		default:
			orgNames, err = deriveOwnersFromSyncConfigs(ctx, h.credentials.Pool, orgID, "github", []string{"owner", "org"})
			if err != nil {
				h.internal(w, r, "derive github owners from sync configs", err)
				return
			}
		}
		if len(orgNames) == 0 {
			policy.WriteDetail(w, http.StatusBadRequest,
				"Could not determine a GitHub organization for team discovery. Pass ?org=<org-name>, set config.org on the credential, or configure a GitHub repository sync first.", nil)
			return
		}
		var discovered []discoveredTeam
		for _, orgName := range orgNames {
			orgTeams, discoverErr := discoverGitHub(ctx, prepared, orgName)
			if discoverErr != nil {
				err = discoverErr
				break
			}
			discovered = append(discovered, orgTeams...)
		}
		teams = dedupeDiscoveredTeams(discovered)
	case "gitlab":
		// Resolution order mirrors teams.py:266-283 -- same
		// query-param -> credential.config -> sync-config-derived
		// fallback shape as github above, option keys ("group", "owner").
		var groupPaths []string
		switch {
		case query.Get("group") != "":
			groupPaths = []string{query.Get("group")}
		case credential.Config["group"] != "":
			groupPaths = []string{credential.Config["group"]}
		default:
			groupPaths, err = deriveOwnersFromSyncConfigs(ctx, h.credentials.Pool, orgID, "gitlab", []string{"group", "owner"})
			if err != nil {
				h.internal(w, r, "derive gitlab owners from sync configs", err)
				return
			}
		}
		if len(groupPaths) == 0 {
			policy.WriteDetail(w, http.StatusBadRequest,
				"Could not determine a GitLab group for team discovery. Pass ?group=<group-path>, set config.group on the credential, or configure a GitLab repository sync first.", nil)
			return
		}
		var discovered []discoveredTeam
		for _, groupPath := range groupPaths {
			groupTeams, groupTruncated, groupWarnings, discoverErr := discoverGitLab(ctx, prepared, groupPath)
			if discoverErr != nil {
				err = discoverErr
				break
			}
			discovered = append(discovered, groupTeams...)
			truncated = truncated || groupTruncated
			warnings = append(warnings, groupWarnings...)
		}
		teams = dedupeDiscoveredTeams(discovered)
	default:
		h.internal(w, r, "discover teams", fmt.Errorf("provider %q discovery not yet ported", provider))
		return
	}
	if err != nil {
		h.internal(w, r, "discover teams", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, teamDiscoverResponseJSON(provider, teams, truncated, warnings), nil)
}

func (h handlers) getTeam(w http.ResponseWriter, r *http.Request) {
	teamID := pathParam(r, "team_id")
	if teamID == "discover" {
		h.discoverTeams(w, r)
		return
	}
	team, err := h.store.GetTeam(r.Context(), orgIDOf(r.Context()), teamID)
	if err != nil {
		h.internal(w, r, "get team", err)
		return
	}
	if team == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Team not found", nil)
		return
	}
	policy.WriteJSON(w, http.StatusOK, teamJSON(*team), nil)
}

// postTeamWildcard is POST /api/v1/admin/teams/{team_id}: Python has no
// real route at this shape (POST /teams is create, a different, already-
// registered literal path; there is no POST /teams/{team_id} at all) --
// the ONLY POST path under /teams/ that exists is the literal
// /teams/import. team_id=="import" dispatches to it; anything else
// answers Starlette's own generic "no route matched" 404 shape, the same
// thing Python would answer for a POST to any other /teams/<literal>
// path it has no route for.
//
// KNOWN, DOCUMENTED GAP: this route (like the create/update routes) is
// wrapped by decodeFirst, which reads and decodes the request body BEFORE
// this dispatch ever runs -- matching FastAPI's own before-routing-
// resolves-vs-after ordering for the "import" case (Python decodes the
// body as part of resolving that ONE endpoint), but NOT for a POST to any
// OTHER team_id: real Starlette never attempts body decoding for a path
// it has no route for at all (routing fails before any dependency runs),
// while this implementation always decodes first regardless of team_id --
// a malformed body on a POST to a random team_id could answer 422/400
// here where Python answers a clean 404. Narrow, low-likelihood (no
// legitimate client sends this), not reproduced.
func (h handlers) postTeamWildcard(w http.ResponseWriter, r *http.Request) {
	if pathParam(r, "team_id") == "import" {
		h.importTeams(w, r)
		return
	}
	// Python has no POST route on /teams/{team_id}: FastAPI answers 405
	// with the Allow header of the pattern's first route (DELETE), not a
	// 404 -- CHAOS-6311's venue run caught this once the wildcard gained a
	// POST handler for /teams/import.
	w.Header().Set("Allow", "DELETE")
	policy.WriteDetail(w, http.StatusMethodNotAllowed, "Method Not Allowed", nil)
}

func (h handlers) updateTeam(w http.ResponseWriter, r *http.Request) {
	teamID := pathParam(r, "team_id")
	body := bodyFromContext(r.Context())
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	name, hasName := problems.OptionalString(object, "name", 0, 0)
	description, hasDescription := problems.OptionalString(object, "description", 0, 0)
	repoPatterns, hasRepoPatterns := problems.OptionalStringList(object, "repo_patterns")
	projectKeys, hasProjectKeys := problems.OptionalStringList(object, "project_keys")
	// extra_data/managed_fields/sync_policy: TeamMappingUpdate declares all
	// three (validated the same as every other field FastAPI decodes the
	// body into), even though ClickHouse's create_or_update never takes
	// them -- same "validated, never forwarded" note as createOrUpdateTeam.
	problems.OptionalAnyDict(object, "extra_data")
	problems.OptionalStringList(object, "managed_fields")
	problems.OptionalBoundedInt(object, "sync_policy", 0, 2)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}

	existing, err := h.store.GetTeam(r.Context(), orgIDOf(r.Context()), teamID)
	if err != nil {
		h.internal(w, r, "get team", err)
		return
	}
	if existing == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Team not found", nil)
		return
	}

	write := TeamWrite{TeamID: teamID, Name: existing.Name, Description: existing.Description}
	if hasName {
		write.Name = name
	}
	if hasDescription {
		write.Description = &description
	}
	if hasRepoPatterns {
		write.RepoPatterns = &repoPatterns
	} else {
		write.RepoPatterns = ptrSlice(existing.RepoPatterns)
	}
	if hasProjectKeys {
		write.ProjectKeys = &projectKeys
	} else {
		write.ProjectKeys = ptrSlice(existing.ProjectKeys)
	}
	updated, err := h.store.CreateOrUpdateTeam(r.Context(), orgIDOf(r.Context()), write)
	if err != nil {
		h.internal(w, r, "update team", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, teamJSON(updated), nil)
}

func (h handlers) listIdentities(w http.ResponseWriter, r *http.Request) {
	activeOnly, badBool := queryBoolDefaultTrue(r, "active_only")
	if badBool != nil {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail([]pybody.Error{*badBool}), nil)
		return
	}
	identities, err := h.store.ListIdentities(r.Context(), orgIDOf(r.Context()), activeOnly)
	if err != nil {
		h.internal(w, r, "list identities", err)
		return
	}
	out := make([]pyjson.Value, len(identities))
	for index, identity := range identities {
		out[index] = identityJSON(identity)
	}
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

// storedFacets is member_facets applied to a STORED record -- email union
// canonical_id union every provider_identities value, plus display_name
// only when there is no email. Always derived from the resolved/stored
// record, never the raw request payload (see identities.py's own comment
// on _stored_facets: an omitted-but-preserved facet must not look removed).
func storedFacets(identity Identity) map[string]bool {
	facets := map[string]bool{}
	if identity.Email != nil && *identity.Email != "" {
		facets[*identity.Email] = true
	}
	if identity.CanonicalID != "" {
		facets[identity.CanonicalID] = true
	}
	if identity.ProviderIdentities != nil {
		for _, key := range identity.ProviderIdentities.Keys {
			values, _ := identity.ProviderIdentities.Get(key)
			for _, value := range values {
				if value != "" {
					facets[value] = true
				}
			}
		}
	}
	if (identity.Email == nil || *identity.Email == "") && identity.DisplayName != nil && *identity.DisplayName != "" {
		facets[*identity.DisplayName] = true
	}
	return facets
}

func (h handlers) createOrUpdateIdentity(w http.ResponseWriter, r *http.Request) {
	body := bodyFromContext(r.Context())
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	canonicalID, _ := problems.RequiredString(object, "canonical_id", 1, 0)
	displayName, hasDisplayName := problems.OptionalString(object, "display_name", 0, 0)
	email, hasEmail := problems.OptionalString(object, "email", 0, 0)
	// IdentityMappingCreate declares provider_identities/team_ids WITHOUT
	// `| None` (Field(default_factory=...)): absent uses the default, but an
	// explicit JSON null is itself a validation error -- this route is
	// create-only (there is no PATCH /identities/{id} in this PR's scope),
	// so it always uses the create-shaped (Defaulted*) validators, never the
	// nullable (Optional*) ones updateTeam uses.
	providerIdentities, hasProviders := problems.DefaultedStringArrayDict(object, "provider_identities")
	teamIDs, hasTeamIDs := problems.DefaultedStringList(object, "team_ids")
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	if !hasProviders {
		providerIdentities = pybody.NewOrderedStringListDict()
	}
	if !hasTeamIDs {
		teamIDs = []string{}
	}

	ctx := r.Context()
	orgID := orgIDOf(ctx)

	// Atomic validate-then-write: every team_id must already exist, 404
	// before any mutation, so the write is all-or-nothing.
	var missing []string
	for _, teamID := range teamIDs {
		team, err := h.store.GetTeam(ctx, orgID, teamID)
		if err != nil {
			h.internal(w, r, "check team_id", err)
			return
		}
		if team == nil {
			missing = append(missing, teamID)
		}
	}
	if len(missing) > 0 {
		policy.WriteDetail(w, http.StatusNotFound, unknownTeamIDsDetail(missing), nil)
		return
	}

	// A submitted provider identity must not already belong to a DIFFERENT
	// canonical identity. Iterated in the request's own INSERTION order,
	// matching Python's `for provider, identities in
	// provider_identities.items()` exactly -- r1 used a sorted order here
	// (Go's map iteration is randomized and no insertion-order-preserving
	// type existed yet), which was deterministic but could still report a
	// DIFFERENT conflict first than Python when a request names more than
	// one; r2's OrderedStringListDict removes that gap too.
	for _, provider := range providerIdentities.Keys {
		values, _ := providerIdentities.Get(provider)
		for _, identityValue := range values {
			owner, err := h.store.FindIdentityByProviderIdentity(ctx, orgID, provider, identityValue)
			if err != nil {
				h.internal(w, r, "check provider identity ownership", err)
				return
			}
			if owner != nil && owner.CanonicalID != canonicalID {
				policy.WriteDetail(w, http.StatusConflict, providerIdentityConflictDetail(provider, identityValue, owner.CanonicalID), nil)
				return
			}
		}
	}

	old, err := h.store.GetIdentity(ctx, orgID, canonicalID)
	if err != nil {
		h.internal(w, r, "get identity", err)
		return
	}
	var oldFacets map[string]bool
	var oldTeamIDs map[string]bool
	if old != nil {
		oldFacets = storedFacets(*old)
		oldTeamIDs = toSet(old.TeamIDs)
	} else {
		oldFacets = map[string]bool{}
		oldTeamIDs = map[string]bool{}
	}
	newTeamIDs := toSet(teamIDs)

	write := IdentityWrite{CanonicalID: canonicalID, ProviderIdentities: providerIdentities, TeamIDs: &teamIDs}
	if hasDisplayName {
		write.DisplayName = &displayName
	}
	if hasEmail {
		write.Email = &email
	}
	stored, err := h.store.CreateOrUpdateIdentity(ctx, orgID, write)
	if err != nil {
		h.internal(w, r, "create or update identity", err)
		return
	}
	newFacets := storedFacets(stored)
	staleFacets := map[string]bool{}
	for facet := range oldFacets {
		if !newFacets[facet] {
			staleFacets[facet] = true
		}
	}

	// Teams the identity left entirely: drop ALL of its old facets.
	for teamID := range oldTeamIDs {
		if !newTeamIDs[teamID] {
			if _, err := h.store.RemoveMembers(ctx, orgID, teamID, oldFacets); err != nil {
				h.internal(w, r, "remove members from left team", err)
				return
			}
		}
	}
	// Teams retained or newly joined: drop changed-away facets, then add
	// the current (complete, resolved) facet set.
	for teamID := range newTeamIDs {
		if len(staleFacets) > 0 {
			if _, err := h.store.RemoveMembers(ctx, orgID, teamID, staleFacets); err != nil {
				h.internal(w, r, "remove stale members", err)
				return
			}
		}
		if _, err := h.store.AddMembers(ctx, orgID, teamID, sortedKeysBool(newFacets)); err != nil {
			h.internal(w, r, "add members", err)
			return
		}
	}

	policy.WriteJSON(w, http.StatusOK, identityJSON(stored), nil)
}
