package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Provider source (repo/project) discovery, native Go (CHAOS-4602).
//
// Governing requirement: go-worker-migration-implementation-plan.md L653
// ("Port pagination, incremental windows, backfills, discovery, and cost
// classification.") and L691-695/CHAOS-3047 ("discovery" is one of the named
// sync-dispatch responsibilities). Before this, the ONLY source-discovery
// mechanism in the system was Python, API-plane, one-shot at sync-config
// creation (discovery/repos.py::discover_repos_for_config) -- github/gitlab
// only, jira fell through to []. That one-shot never re-runs, so a
// provider's projects/repos changing after config creation was invisible,
// and Jira (no branch at all, pre-CHAOS-4584) never got sources whatsoever.
//
// This file adds the step INSIDE the native Go scheduled-sync route, run
// once per occurrence, BEFORE unit planning reads sources
// (loadMaterializationPlan -> loadPlanSources), modeled on native reference
// discovery's shape (own executor interface, own domain-pool transaction,
// idempotent, per-outcome telemetry) but without reference discovery's
// claim/lease ledger -- this step has no cross-run identity to protect: it
// is a read-then-idempotent-upsert with no partial-effect window a retry
// needs to recover from, so the added machinery would buy nothing.
//
// Explicit scope (a sync config pinned to one already-known IntegrationSource
// via config.source_id, exactly the sourceID loadPlanSources already
// branches on) skips discovery entirely -- broadening the visible set is
// meaningless for a config that only ever wants that one source. Every
// other config for a source-type-scoped provider always discovers: existing
// integration_sources rows are refreshed (name/full_name/metadata) but their
// is_enabled and discovered_at are NEVER touched, matching
// discover_sources_for_integration's own preserved-fields contract exactly.

// Source-discovery outcomes, and the provider_source_discovery_total label
// vocabulary. "skipped" covers both an explicit-scope config and a provider
// outside sourceDiscoveryProviders; "error" is a provider-API or credential
// failure -- discovery failing never fails the occurrence (see the call
// site in materializer.go), it only means this pass didn't widen coverage.
const (
	SourceDiscoveryOutcomeCreated  = "created"
	SourceDiscoveryOutcomeExisting = "existing"
	SourceDiscoveryOutcomeSkipped  = "skipped"
	SourceDiscoveryOutcomeError    = "error"
)

// sourceDiscoveryProviders is the CLOSED set of providers whose sources are
// enumerated from the provider's own API and upserted into
// integration_sources: github repos, gitlab projects, jira projects.
//
// sourceDiscoveryExemptProviders is every OTHER provider registered in
// supportedProviderDatasets (planner.go), each with its own reason this step
// does not apply: pagerduty sources itself via preparePagerDutyRepair (a
// single "account" source, repaired in place from the credential, never
// enumerated from an API list); linear's per-run scope comes from the
// Go-native team-catalog collectors (CHAOS-4431), not integration_sources;
// launchdarkly has no per-source scope at all (org-level only).
//
// Both maps exist so a NEW provider dataset family is a compile-visible,
// test-visible choice (TestEveryProviderDatasetFamilyHasADecidedSourceDiscoveryStance
// below) rather than a silent gap -- the same shape CHAOS-4433's ledger
// drift gate uses for River kinds and bridge routes.
var sourceDiscoveryProviders = map[string]bool{
	"github": true,
	"gitlab": true,
	"jira":   true,
}

var sourceDiscoveryExemptProviders = map[string]bool{
	"pagerduty":    true,
	"linear":       true,
	"launchdarkly": true,
}

const (
	sourceDiscoveryMaxPages = 50
	sourceDiscoveryPerPage  = 100
)

var ErrSourceDiscoveryUnavailable = errors.New("native source discovery is unavailable")

// SourceDiscoveryArgs is the secret-free (beyond CredentialID, an opaque
// reference) input to one Discover call: one occurrence's integration.
type SourceDiscoveryArgs struct {
	OrgID         string
	IntegrationID string
	CredentialID  *string
	Provider      string
	// SyncOptions is the decoded sync_configurations.sync_options JSONB for
	// this occurrence's config -- the same provider-specific scope fields
	// (owner/search/all_repos for github; group/owner/search/all_repos for
	// gitlab) discover_repos_for_config already reads on the Python side.
	SyncOptions map[string]any
	// ExplicitScope is true when the sync config is pinned to one specific,
	// already-known IntegrationSource (config.source_id IS NOT NULL) --
	// exactly the sourceID loadPlanSources already branches on.
	ExplicitScope bool
}

// SourceDiscoveryReport summarizes one Discover call for the caller's own
// logging; the per-outcome counts are also what drove the telemetry.
type SourceDiscoveryReport struct {
	Outcome  string
	Created  int
	Existing int
}

// SourceDiscoveryExecutor is the native-Go per-occurrence source-discovery
// step, modeled on NativeReferenceDiscoveryService's DiscoveryExecutor shape.
type SourceDiscoveryExecutor interface {
	Discover(ctx context.Context, args SourceDiscoveryArgs) (SourceDiscoveryReport, error)
}

// sourceDiscoveryTelemetry backs provider_source_discovery_total{provider,
// outcome}. Pre-seeded to zero for every (provider,outcome) pair in
// sourceDiscoveryProviders x the four outcomes, so an operator can alert on
// this series before the first occurrence ever runs -- the same
// pre-seeding rationale executedProofRefreshFailuresTotal and the
// reconciler's per-stage counters already follow.
type sourceDiscoveryTelemetry struct {
	mu     sync.Mutex
	counts map[[2]string]uint64
}

func newSourceDiscoveryTelemetry() *sourceDiscoveryTelemetry {
	telemetry := &sourceDiscoveryTelemetry{counts: make(map[[2]string]uint64)}
	outcomes := []string{
		SourceDiscoveryOutcomeCreated, SourceDiscoveryOutcomeExisting,
		SourceDiscoveryOutcomeSkipped, SourceDiscoveryOutcomeError,
	}
	for provider := range sourceDiscoveryProviders {
		for _, outcome := range outcomes {
			telemetry.counts[[2]string{provider, outcome}] = 0
		}
	}
	return telemetry
}

func (telemetry *sourceDiscoveryTelemetry) observe(provider, outcome string) {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.counts[[2]string{provider, outcome}]++
}

// WritePrometheus satisfies the same health.MetricsSource shape
// NativeMaterializer.WritePrometheus and the reconciler's pipeline already
// implement.
func (telemetry *sourceDiscoveryTelemetry) WritePrometheus(output io.Writer) error {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	var text strings.Builder
	text.WriteString("# HELP provider_source_discovery_total Native Go per-occurrence provider source (repo/project) discovery outcomes, run before unit planning (CHAOS-4602). existing rows are never flipped by discovery (is_enabled untouched); skipped means the sync config has an explicit single-source scope.\n")
	text.WriteString("# TYPE provider_source_discovery_total counter\n")
	keys := make([][2]string, 0, len(telemetry.counts))
	for key := range telemetry.counts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	for _, key := range keys {
		fmt.Fprintf(&text, "provider_source_discovery_total{provider=%q,outcome=%q} %d\n", key[0], key[1], telemetry.counts[key])
	}
	_, err := io.WriteString(output, text.String())
	return err
}

// sourceDiscoveryLease is a trivial, ctx-bound LeaseGuard -- source discovery
// runs once per occurrence with no claimed lease behind it (there is no
// cross-run identity to protect, see the file doc comment above), mirroring
// teamCatalogLease's identical reasoning for reference discovery.
type sourceDiscoveryLease struct{}

func (sourceDiscoveryLease) Assert(ctx context.Context) error { return ctx.Err() }

// discoveredSource is one provider-API result, already mapped to
// IntegrationSource field shape.
type discoveredSource struct {
	ExternalID string
	SourceType string
	Name       string
	FullName   string
	Metadata   map[string]any
}

// NativeSourceDiscoveryService is the production SourceDiscoveryExecutor.
type NativeSourceDiscoveryService struct {
	domainPool  *pgxpool.Pool
	credentials providerfoundation.CredentialResolver
	doer        providerfoundation.HTTPDoer
	retry       providerfoundation.RetryPolicy
	logger      *slog.Logger
	telemetry   *sourceDiscoveryTelemetry
	now         func() time.Time
}

// NewNativeSourceDiscoveryService constructs the discovery step. domainPool
// must be authenticated as the domain role: every write this service makes
// goes to public.integration_sources on that pool, in its own transaction,
// never on the caller's coordinator transaction (see the file doc comment).
func NewNativeSourceDiscoveryService(
	domainPool *pgxpool.Pool,
	credentials providerfoundation.CredentialResolver,
	doer providerfoundation.HTTPDoer,
	logger *slog.Logger,
) (*NativeSourceDiscoveryService, error) {
	if domainPool == nil || credentials.Repository == nil || credentials.Decryptor == nil || doer == nil {
		return nil, ErrSourceDiscoveryUnavailable
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &NativeSourceDiscoveryService{
		domainPool: domainPool, credentials: credentials, doer: doer,
		retry: providerfoundation.DefaultRetryPolicy(), logger: logger,
		telemetry: newSourceDiscoveryTelemetry(), now: time.Now,
	}, nil
}

// WritePrometheus exposes provider_source_discovery_total.
func (service *NativeSourceDiscoveryService) WritePrometheus(output io.Writer) error {
	return service.telemetry.WritePrometheus(output)
}

func (service *NativeSourceDiscoveryService) Discover(ctx context.Context, args SourceDiscoveryArgs) (SourceDiscoveryReport, error) {
	provider := strings.ToLower(strings.TrimSpace(args.Provider))
	if !sourceDiscoveryProviders[provider] {
		return SourceDiscoveryReport{Outcome: SourceDiscoveryOutcomeSkipped}, nil
	}
	if args.ExplicitScope {
		service.telemetry.observe(provider, SourceDiscoveryOutcomeSkipped)
		return SourceDiscoveryReport{Outcome: SourceDiscoveryOutcomeSkipped}, nil
	}
	credentialID := ""
	if args.CredentialID != nil {
		credentialID = *args.CredentialID
	}
	credential, err := service.credentials.Resolve(ctx, sourceDiscoveryLease{}, providerfoundation.TenantScope{
		OrgID: args.OrgID, Provider: provider, IntegrationID: args.IntegrationID, CredentialID: credentialID,
	})
	if err != nil {
		service.telemetry.observe(provider, SourceDiscoveryOutcomeError)
		return SourceDiscoveryReport{}, fmt.Errorf("resolve %s credential for source discovery: %w", provider, err)
	}
	var discovered []discoveredSource
	switch provider {
	case "github":
		discovered, err = service.discoverGitHub(ctx, credential, args.SyncOptions)
	case "gitlab":
		discovered, err = service.discoverGitLab(ctx, credential, args.SyncOptions)
	case "jira":
		discovered, err = service.discoverJira(ctx, credential)
	}
	if err != nil {
		service.telemetry.observe(provider, SourceDiscoveryOutcomeError)
		return SourceDiscoveryReport{}, fmt.Errorf("discover %s sources: %w", provider, err)
	}
	created, existing, err := service.upsertSources(ctx, args.OrgID, args.IntegrationID, provider, discovered, service.nowUTC())
	if err != nil {
		service.telemetry.observe(provider, SourceDiscoveryOutcomeError)
		return SourceDiscoveryReport{}, fmt.Errorf("upsert %s sources: %w", provider, err)
	}
	for i := 0; i < created; i++ {
		service.telemetry.observe(provider, SourceDiscoveryOutcomeCreated)
	}
	for i := 0; i < existing; i++ {
		service.telemetry.observe(provider, SourceDiscoveryOutcomeExisting)
	}
	outcome := SourceDiscoveryOutcomeExisting
	if created > 0 {
		outcome = SourceDiscoveryOutcomeCreated
	}
	return SourceDiscoveryReport{Outcome: outcome, Created: created, Existing: existing}, nil
}

func (service *NativeSourceDiscoveryService) nowUTC() time.Time {
	if service.now == nil {
		return time.Now().UTC()
	}
	return service.now().UTC()
}

// githubDiscoveryScope mirrors discover_github_repos' owner/search
// resolution for the common PAT case: an explicit owner always wins; a
// "owner/pattern"-shaped search fills in whichever half is missing; a bare
// search with no owner becomes the owner with an unbounded pattern.
//
// Deliberately NOT ported: the all_repos GitHub-App installation-listing
// branch (discover_github_repos' _discover_github_app_installation_repos).
// NewGitHubClient already mints and applies an App installation token
// transparently for an App-auth credential, so an App-authenticated config
// still discovers via the same /orgs or /user listing below -- only the
// installation-repository-list SHORTCUT (bypassing the owner/search scope
// entirely) is out of scope for this step. Tracked as a follow-up, not a
// silent gap: see this PR's ticket.
func githubDiscoveryScope(options map[string]any) (owner, pattern string) {
	owner = stringOption(options, "owner")
	search := stringOption(options, "search")
	if search == "" {
		return owner, "*"
	}
	if idx := strings.Index(search, "/"); idx >= 0 {
		if owner == "" {
			owner = search[:idx]
		}
		return owner, search[idx+1:]
	}
	if owner == "" {
		return search, "*"
	}
	return owner, search
}

func (service *NativeSourceDiscoveryService) discoverGitHub(ctx context.Context, credential providerfoundation.Credential, options map[string]any) ([]discoveredSource, error) {
	client, err := providerfoundation.NewGitHubClient(credential, service.doer, service.retry, sourceDiscoveryLease{})
	if err != nil {
		return nil, err
	}
	owner, pattern := githubDiscoveryScope(options)
	requestPath := "/user/repos"
	query := url.Values{"per_page": {strconv.Itoa(sourceDiscoveryPerPage)}, "affiliation": {"owner,collaborator,organization_member"}}
	if owner != "" {
		requestPath = "/orgs/" + url.PathEscape(owner) + "/repos"
		query = url.Values{"per_page": {strconv.Itoa(sourceDiscoveryPerPage)}, "type": {"all"}}
	}
	page, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
		Path: requestPath, Query: query, MaxPages: sourceDiscoveryMaxPages,
	})
	if err != nil && owner != "" {
		// Python's discover_github_repos falls back from org to user lookup
		// (an owner that is a user account, not an org, 404s under
		// /orgs/{owner}/repos). Retry once under /users/ before giving up.
		userPage, userErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path: "/users/" + url.PathEscape(owner) + "/repos", Query: query, MaxPages: sourceDiscoveryMaxPages,
		})
		if userErr == nil {
			page, err = userPage, nil
		}
	}
	if err != nil {
		return nil, err
	}
	result := make([]discoveredSource, 0, len(page.Items))
	for _, raw := range page.Items {
		var repo struct {
			Name     string `json:"name"`
			FullName string `json:"full_name"`
			Owner    struct {
				Login string `json:"login"`
			} `json:"owner"`
		}
		if err := json.Unmarshal(raw, &repo); err != nil {
			continue
		}
		if repo.Name == "" || repo.Owner.Login == "" {
			continue
		}
		if pattern != "" && pattern != "*" {
			if matched, matchErr := path.Match(pattern, repo.Name); matchErr != nil || !matched {
				continue
			}
		}
		fullName := repo.FullName
		if fullName == "" {
			fullName = repo.Owner.Login + "/" + repo.Name
		}
		result = append(result, discoveredSource{
			ExternalID: fullName, SourceType: "repository", Name: repo.Name, FullName: fullName,
			Metadata: map[string]any{"owner": repo.Owner.Login},
		})
	}
	return result, nil
}

// gitlabDiscoveryScope mirrors discover_gitlab_repos' group/owner/search
// resolution: an explicit group (or owner, used as a group path fallback)
// always wins; a "group/pattern"-shaped search (split on the LAST slash, so
// nested GitLab subgroups resolve correctly) fills in whichever half is
// missing; a bare search with no group becomes the group with an unbounded
// pattern.
func gitlabDiscoveryScope(options map[string]any) (group, pattern string) {
	group = stringOption(options, "group")
	if group == "" {
		group = stringOption(options, "owner")
	}
	search := stringOption(options, "search")
	if search == "" {
		return group, "*"
	}
	if idx := strings.LastIndex(search, "/"); idx >= 0 {
		if group == "" {
			group = search[:idx]
		}
		return group, search[idx+1:]
	}
	if group == "" {
		return search, "*"
	}
	return group, search
}

func (service *NativeSourceDiscoveryService) discoverGitLab(ctx context.Context, credential providerfoundation.Credential, options map[string]any) ([]discoveredSource, error) {
	client, err := providerfoundation.NewGitLabClient(credential, service.doer, service.retry, sourceDiscoveryLease{})
	if err != nil {
		return nil, err
	}
	group, pattern := gitlabDiscoveryScope(options)
	requestPath := "/api/v4/projects"
	query := url.Values{"membership": {"true"}}
	if group != "" {
		requestPath = "/api/v4/groups/" + url.PathEscape(group) + "/projects"
		query = url.Values{"include_subgroups": {"true"}}
	}
	page, err := providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
		Path: requestPath, Query: query, PerPage: sourceDiscoveryPerPage, MaxPages: sourceDiscoveryMaxPages,
	})
	if err != nil {
		return nil, err
	}
	normalizedGroup := strings.ToLower(group)
	result := make([]discoveredSource, 0, len(page.Items))
	for _, raw := range page.Items {
		var project struct {
			ID                json.Number `json:"id"`
			Name              string      `json:"name"`
			PathWithNamespace string      `json:"path_with_namespace"`
		}
		if err := json.Unmarshal(raw, &project); err != nil {
			continue
		}
		if project.ID.String() == "" || project.PathWithNamespace == "" {
			continue
		}
		if normalizedGroup != "" {
			candidate := strings.ToLower(project.PathWithNamespace)
			if candidate != normalizedGroup && !strings.HasPrefix(candidate, normalizedGroup+"/") {
				continue
			}
		}
		if pattern != "" && pattern != "*" {
			if matched, matchErr := path.Match(pattern, project.Name); matchErr != nil || !matched {
				continue
			}
		}
		name := project.Name
		if name == "" {
			if idx := strings.LastIndex(project.PathWithNamespace, "/"); idx >= 0 {
				name = project.PathWithNamespace[idx+1:]
			} else {
				name = project.PathWithNamespace
			}
		}
		result = append(result, discoveredSource{
			ExternalID: project.ID.String(), SourceType: "project", Name: name, FullName: project.PathWithNamespace,
			Metadata: map[string]any{"path_with_namespace": project.PathWithNamespace},
		})
	}
	return result, nil
}

// discoverJira lists every Jira project visible to the credential via
// GET /rest/api/3/project/search. There is no Python precedent to match
// byte-for-byte (discover_repos_for_config's jira branch does not exist on
// main as of this PR -- CHAOS-4584 is still in flight): external_id is the
// project KEY, matching the "project_key" scope vocabulary this ticket and
// team_autoimport_jira.py's own ownership resolution already use, not the
// numeric project id (which fetchJiraProject/resolveJiraProjectCatalog in
// jira_work_items_route.go resolve FROM the key, the reverse direction).
func (service *NativeSourceDiscoveryService) discoverJira(ctx context.Context, credential providerfoundation.Credential) ([]discoveredSource, error) {
	client, err := providerfoundation.NewJiraClient(credential, service.doer, service.retry, sourceDiscoveryLease{})
	if err != nil {
		return nil, err
	}
	page, err := providerfoundation.CollectJiraTokenOffsetPages(ctx, client, providerfoundation.JiraPageOptions{
		Path: "/rest/api/3/project/search", DataKey: "values",
		MaxResults: sourceDiscoveryPerPage, MaxPages: sourceDiscoveryMaxPages,
	})
	if err != nil {
		return nil, err
	}
	result := make([]discoveredSource, 0, len(page.Items))
	for _, raw := range page.Items {
		var project struct {
			ID   string `json:"id"`
			Key  string `json:"key"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &project); err != nil {
			continue
		}
		if project.Key == "" {
			continue
		}
		fullName := project.Name
		if fullName == "" {
			fullName = project.Key
		}
		result = append(result, discoveredSource{
			ExternalID: project.Key, SourceType: "project", Name: fullName, FullName: fullName,
			Metadata: map[string]any{"project_id": project.ID},
		})
	}
	return result, nil
}

// upsertSources idempotently upserts every discovered row keyed on the
// EXACT unique constraint discover_sources_for_integration already uses
// (org_id,integration_id,provider,external_id). is_enabled and
// discovered_at are deliberately absent from the UPDATE SET list: an
// existing row's operator-set enablement and original discovery timestamp
// are never touched, matching the Python function's documented contract.
// (xmax = 0) is the standard Postgres idiom for "this row was inserted, not
// updated, by this statement" -- xmax is 0 on a freshly inserted tuple and
// set to the current transaction id when an UPDATE creates a new tuple
// version for an existing row.
func (service *NativeSourceDiscoveryService) upsertSources(
	ctx context.Context, orgID, integrationID, provider string, sources []discoveredSource, now time.Time,
) (created, existing int, err error) {
	if len(sources) == 0 {
		return 0, 0, nil
	}
	tx, err := service.domainPool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("begin source discovery domain transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	for _, source := range sources {
		metadataJSON, marshalErr := json.Marshal(source.Metadata)
		if marshalErr != nil {
			return 0, 0, fmt.Errorf("encode %s source metadata: %w", provider, marshalErr)
		}
		// integration_sources.metadata is a plain `json` column (alembic 0015
		// `sa.JSON()`, not JSONB) -- the `||` merge operator only exists for
		// jsonb, so both sides are cast to jsonb for the merge and the result
		// is cast back to json for storage. Fresh discovery keys win, keys
		// the fresh payload lacks (e.g. a manually-added
		// planner_managed_sync_config_id) are preserved, matching
		// discover_sources_for_integration's own merge contract exactly.
		var inserted bool
		scanErr := tx.QueryRow(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,metadata,is_enabled,discovered_at,last_seen_at)
VALUES ($1::uuid,$2,$3::uuid,$4,$5,$6,$7,$8,$9::json,TRUE,$10,$10)
ON CONFLICT (org_id,integration_id,provider,external_id) DO UPDATE
SET name=EXCLUDED.name, full_name=EXCLUDED.full_name,
    metadata=(public.integration_sources.metadata::jsonb || EXCLUDED.metadata::jsonb)::json,
    last_seen_at=EXCLUDED.last_seen_at
RETURNING (xmax = 0)`,
			uuid.New().String(), orgID, integrationID, provider, source.SourceType,
			source.ExternalID, source.Name, source.FullName, metadataJSON, now,
		).Scan(&inserted)
		if scanErr != nil {
			return 0, 0, fmt.Errorf("upsert %s source %s: %w", provider, source.ExternalID, scanErr)
		}
		if inserted {
			created++
		} else {
			existing++
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("commit source discovery domain transaction: %w", err)
	}
	committed = true
	return created, existing, nil
}

func stringOption(options map[string]any, key string) string {
	value, _ := options[key].(string)
	return strings.TrimSpace(value)
}
