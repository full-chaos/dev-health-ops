package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

// ErrSourceDiscoveryTruncated marks a discovery pass that hit the page
// budget (sourceDiscoveryMaxPages) before the provider ran out of pages.
// Codex review (round 1, P2): silently upserting a partial listing as if it
// were complete would leave the remainder invisible with no signal at all
// -- an integration with more than sourceDiscoveryMaxPages*sourceDiscoveryPerPage
// (5,000) repos/projects would have the tail silently omitted forever.
// Surfaced as an ordinary discovery error (outcome=error, logged, never
// fails the occurrence) rather than a partial commit.
var ErrSourceDiscoveryTruncated = errors.New("provider source discovery hit its page budget before completing")

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
	// ConfigID and PlannerManaged identify the sync config whose occurrence
	// is running discovery. Codex review (round 1, P1): loadPlanSources only
	// admits a planner-managed parent's sources whose
	// metadata.planner_managed_sync_config_id equals ConfigID
	// (materializer.go's loadPlanSources SQL) -- a newly discovered source
	// with no such tag is invisible to unit planning even though the row
	// exists. Discover stamps this tag on every upserted row when
	// PlannerManaged is true; it is a no-op (and ConfigID may be empty) for
	// a non-planner-managed config, which loadPlanSources never gates on
	// this key at all.
	ConfigID       string
	PlannerManaged bool
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
	if args.CredentialID == nil {
		// Codex review (round 1, P1): a NULL Integration.credential_id means
		// this config uses environment-variable auth (the planner stamps
		// AUTH_SOURCE_ENVIRONMENT for it, resolveCredentialStamp in
		// materializer.go) -- resolving an empty CredentialID against
		// PostgresCredentialRepository does NOT mean "environment", it means
		// "the org's default stored credential", which can be a wrong,
		// unrelated account. There is no environment-credential resolution
		// path in this Go resolver, so discovery is skipped rather than
		// risk discovering against the wrong account; already-existing
		// sources still plan normally.
		service.telemetry.observe(provider, SourceDiscoveryOutcomeSkipped)
		return SourceDiscoveryReport{Outcome: SourceDiscoveryOutcomeSkipped}, nil
	}
	credential, err := service.credentials.Resolve(ctx, sourceDiscoveryLease{}, providerfoundation.TenantScope{
		OrgID: args.OrgID, Provider: provider, IntegrationID: args.IntegrationID, CredentialID: *args.CredentialID,
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
	created, existing, err := service.upsertSources(ctx, args.OrgID, args.IntegrationID, provider, discovered, service.nowUTC(), args.ConfigID, args.PlannerManaged)
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

// githubDiscoveryOptions mirrors discover_github_repos' owner/search/all_repos
// resolution exactly (src/dev_health_ops/discovery/repos.py):
//
//   - all_repos + a "/"-shaped search: the half before "/" becomes the
//     namespace filter (unless owner already set it), the half after is the
//     name pattern.
//   - all_repos + a bare search: the whole thing is the name pattern; the
//     namespace filter stays whatever owner already resolved to (may be "").
//   - NOT all_repos, "/" in search (regardless of any existing owner): the
//     search is split on the FIRST "/" into owner/pattern, overwriting owner.
//   - NOT all_repos, bare search, no owner: owner becomes the search, pattern
//     is unbounded.
//   - otherwise: pattern is unbounded, owner/namespace as already resolved.
//
// Codex review (round 1, P1) caught two bugs in the prior version: all_repos
// was never read at all, and the NOT-all_repos / no-owner case listed
// EVERYTHING instead of Python's explicit "return []" (discoverGitHub turns
// an empty owner in the non-all_repos case into no discovery at all, not an
// unbounded /user/repos listing).
func githubDiscoveryOptions(options map[string]any) (allRepos bool, owner, pattern, namespace string) {
	owner = stringOption(options, "owner")
	search := stringOption(options, "search")
	allRepos = boolOption(options, "all_repos")
	if allRepos {
		namespace = owner
	}
	switch {
	case allRepos && search != "":
		if idx := strings.Index(search, "/"); idx >= 0 {
			if namespace == "" {
				namespace = search[:idx]
			}
			pattern = search[idx+1:]
		} else {
			pattern = search
		}
	case strings.Contains(search, "/"):
		idx := strings.Index(search, "/")
		owner = search[:idx]
		pattern = search[idx+1:]
	case search != "" && owner == "":
		owner = search
		pattern = "*"
	default:
		pattern = "*"
	}
	return allRepos, owner, pattern, namespace
}

// Deliberately NOT ported: the all_repos GitHub-App installation-listing
// branch (discover_github_repos' _discover_github_app_installation_repos).
// NewGitHubClient already mints and applies an App installation token
// transparently for an App-auth credential, so an App-authenticated config
// still discovers via the same /user or /orgs listing below -- only the
// installation-repository-list SHORTCUT (bypassing the owner/search scope
// entirely) is out of scope for this step. Tracked as a follow-up, not a
// silent gap: see this PR's ticket.
func (service *NativeSourceDiscoveryService) discoverGitHub(ctx context.Context, credential providerfoundation.Credential, options map[string]any) ([]discoveredSource, error) {
	client, err := providerfoundation.NewGitHubClient(credential, service.doer, service.retry, sourceDiscoveryLease{})
	if err != nil {
		return nil, err
	}
	allRepos, owner, pattern, namespace := githubDiscoveryOptions(options)
	var page providerfoundation.PageCollection
	if allRepos && githubCredentialIsAppAuth(credential) {
		// Codex review (round 2, P2): a GitHub App installation token has
		// no authenticated-USER surface at all -- /user/repos 401s for it.
		// The canonical App-compatible listing is /installation/repositories
		// (paginated the same Link-header way, response wrapped in a
		// "repositories" key instead of a bare array).
		page, err = providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path:     "/installation/repositories",
			Query:    url.Values{"per_page": {strconv.Itoa(sourceDiscoveryPerPage)}},
			DataKey:  "repositories",
			MaxPages: sourceDiscoveryMaxPages,
		})
	} else if allRepos {
		// Python: g.get_user().get_repos() -- the AUTHENTICATED USER's own
		// accessible repos (not org-scoped), filtered client-side below by
		// namespace (exact owner match) and name pattern.
		page, err = providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path: "/user/repos",
			Query: url.Values{
				"per_page":    {strconv.Itoa(sourceDiscoveryPerPage)},
				"affiliation": {"owner,collaborator,organization_member"},
			},
			MaxPages: sourceDiscoveryMaxPages,
		})
	} else if owner == "" {
		// Python: `if not owner: return []` -- no scope resolved at all, no
		// API call, no sources. NOT the same as all_repos with no namespace.
		return nil, nil
	} else {
		query := url.Values{"per_page": {strconv.Itoa(sourceDiscoveryPerPage)}, "type": {"all"}}
		page, err = providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path: "/orgs/" + url.PathEscape(owner) + "/repos", Query: query, MaxPages: sourceDiscoveryMaxPages,
		})
		if err != nil {
			// Python's discover_github_repos falls back from org to user
			// lookup (an owner that is a user account, not an org, 404s
			// under /orgs/{owner}/repos). Retry once under /users/ before
			// giving up.
			userPage, userErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
				Path: "/users/" + url.PathEscape(owner) + "/repos", Query: query, MaxPages: sourceDiscoveryMaxPages,
			})
			if userErr == nil {
				page, err = userPage, nil
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if page.PageBudgetExhausted {
		return nil, fmt.Errorf("github: %w", ErrSourceDiscoveryTruncated)
	}
	normalizedNamespace := strings.ToLower(namespace)
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
		if !fnmatchLike(pattern, repo.Name) {
			continue
		}
		if normalizedNamespace != "" && strings.ToLower(repo.Owner.Login) != normalizedNamespace {
			continue
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

// gitlabDiscoveryOptions mirrors discover_gitlab_repos' group/owner/search/
// all_repos resolution exactly, including the ONE deliberate asymmetry
// Python itself has: the all_repos branch splits a "/"-shaped search on the
// LAST slash (nested-subgroup-safe), while the non-all_repos branch splits
// on the FIRST slash. Codex review (round 1, P1) caught this file using
// LastIndex unconditionally, which is wrong for the non-all_repos case.
func gitlabDiscoveryOptions(options map[string]any) (allRepos bool, groupPath, pattern, namespace string) {
	groupPath = stringOption(options, "group")
	owner := stringOption(options, "owner")
	allRepos = boolOption(options, "all_repos")
	if allRepos {
		if groupPath != "" {
			namespace = groupPath
		} else if owner != "" {
			namespace = owner
		}
	}
	search := stringOption(options, "search")
	switch {
	case allRepos && search != "":
		if idx := strings.LastIndex(search, "/"); idx >= 0 {
			if namespace == "" {
				namespace = search[:idx]
			}
			pattern = search[idx+1:]
		} else {
			pattern = search
		}
	case strings.Contains(search, "/"):
		idx := strings.Index(search, "/")
		groupPath = search[:idx]
		pattern = search[idx+1:]
	case search != "" && groupPath == "":
		groupPath = search
		pattern = "*"
	default:
		pattern = "*"
	}
	return allRepos, groupPath, pattern, namespace
}

func (service *NativeSourceDiscoveryService) discoverGitLab(ctx context.Context, credential providerfoundation.Credential, options map[string]any) ([]discoveredSource, error) {
	// Codex review (round 1, P1): sync_options.gitlab_url names a self-hosted
	// instance (persisted by the batch admin API,
	// api/admin/routers/sync.py:1846-1865) and must be honored over the
	// credential's own base URL -- otherwise self-hosted discovery silently
	// queries gitlab.com (or wherever the credential itself points) instead.
	var client *providerfoundation.HTTPClient
	var err error
	if gitlabURL := stringOption(options, "gitlab_url"); gitlabURL != "" {
		token, _ := credential.Secret("token")
		client, err = providerfoundation.NewHTTPClient(
			"gitlab", gitlabURL, service.doer,
			providerfoundation.TokenAuth("PRIVATE-TOKEN", "", token),
			service.retry, sourceDiscoveryLease{},
		)
	} else {
		client, err = providerfoundation.NewGitLabClient(credential, service.doer, service.retry, sourceDiscoveryLease{})
	}
	if err != nil {
		return nil, err
	}
	allRepos, groupPath, pattern, namespace := gitlabDiscoveryOptions(options)
	var page providerfoundation.PageCollection
	if allRepos {
		page, err = providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
			Path: "/api/v4/projects", Query: url.Values{"membership": {"true"}},
			PerPage: sourceDiscoveryPerPage, MaxPages: sourceDiscoveryMaxPages,
		})
	} else if groupPath == "" {
		// Python: `if not group_path: return []`.
		return nil, nil
	} else {
		// Python's non-all_repos branch does NOT pass include_subgroups --
		// it lists exactly one group's own projects.
		page, err = providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
			Path: "/api/v4/groups/" + url.PathEscape(groupPath) + "/projects", Query: url.Values{},
			PerPage: sourceDiscoveryPerPage, MaxPages: sourceDiscoveryMaxPages,
		})
	}
	if err != nil {
		return nil, err
	}
	if page.PageBudgetExhausted {
		return nil, fmt.Errorf("gitlab: %w", ErrSourceDiscoveryTruncated)
	}
	normalizedNamespace := strings.ToLower(namespace)
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
		if normalizedNamespace != "" {
			candidate := strings.ToLower(project.PathWithNamespace)
			if candidate != normalizedNamespace && !strings.HasPrefix(candidate, normalizedNamespace+"/") {
				continue
			}
		}
		if !fnmatchLike(pattern, project.Name) {
			continue
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
// GET /rest/api/3/project/search, falling back to the legacy unpaginated
// GET /rest/api/3/project when the enhanced search endpoint 404/405/410s
// (some older/JSM-only Jira deployments don't serve it) -- codex review
// (round 1, P2), mirroring the existing Python client's own fallback
// (src/dev_health_ops/providers/jira/client.py:552-579).
//
// There is no Python precedent to match byte-for-byte for the primary path
// (discover_repos_for_config's jira branch does not exist on main as of this
// PR -- CHAOS-4584 is still in flight): external_id is the project KEY,
// matching the "project_key" scope vocabulary this ticket and
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
		// Codex review (round 2, P2): falling back unconditionally would
		// hide a 401/403/429/5xx or a malformed response behind a legacy
		// retry that might mask it entirely (or, worse, succeed against a
		// DIFFERENT, wrong project list). Inspect the classified
		// *providerfoundation.ProviderError this same call already
		// produced and only fall back for 404/405/410 -- exactly the
		// canonical Python client's own trigger
		// (providers/jira/client.py:552-579). Every other status/transport
		// failure surfaces as a real error.
		if !jiraProjectSearchEndpointUnsupported(err) {
			return nil, err
		}
		legacyItems, legacyErr := discoverJiraLegacyProjects(ctx, client)
		if legacyErr != nil {
			// codex review finding: this used to return the ORIGINAL err
			// (the 404/405/410 that triggered the fallback in the first
			// place), masking whatever the legacy endpoint itself actually
			// failed with (a 401/500/decode error) behind an already-
			// explained "endpoint unsupported" message. Wrap both so
			// neither failure is silently dropped.
			return nil, fmt.Errorf("jira legacy project fallback also failed (search endpoint error: %v): %w", err, legacyErr)
		}
		page = providerfoundation.PageCollection{Items: legacyItems}
	}
	if page.PageBudgetExhausted {
		return nil, fmt.Errorf("jira: %w", ErrSourceDiscoveryTruncated)
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

// jiraProjectSearchEndpointUnsupported inspects the classified
// *providerfoundation.ProviderError CollectJiraTokenOffsetPages' own
// client.Do call already produced (see http.go's ClassifyHTTPWithMessage):
// only a 404/405/410 status means the enhanced-search endpoint itself is
// unsupported on this Jira deployment (the legacy-fallback trigger, same
// as the canonical Python client at providers/jira/client.py:552-579);
// 401/403/429 (ErrorAuthentication/ErrorRateLimited), a 5xx
// (ErrorTransient), or any other 4xx (ErrorPermanent) is a real failure the
// caller must see, never silently retried against a different endpoint.
func jiraProjectSearchEndpointUnsupported(err error) bool {
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) {
		return false
	}
	switch providerErr.StatusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusGone:
		return true
	default:
		return false
	}
}

// discoverJiraLegacyProjects is the unpaginated GET /rest/api/3/project
// fallback: a plain JSON array of every project, no query params, no
// pagination envelope.
func discoverJiraLegacyProjects(ctx context.Context, client *providerfoundation.HTTPClient) ([]json.RawMessage, error) {
	response, err := client.Do(ctx, http.MethodGet, "/rest/api/3/project", nil)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("legacy jira project listing: unexpected status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	var items []json.RawMessage
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	return items, nil
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
	configID string, plannerManaged bool,
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
	// Codex review (round 1, P1): loadPlanSources only admits a
	// planner-managed parent's sources whose own
	// metadata.planner_managed_sync_config_id equals the config currently
	// planning -- a freshly discovered row with no such tag is invisible to
	// unit planning even though it exists.
	//
	// Codex review (round 2, P1) on the naive fix: tagging EVERY discovered
	// source unconditionally WIDENS a config that already has an explicit
	// selection (batch-created with specific repos/projects an operator
	// chose, tagged by a mechanism outside this file entirely) into
	// "everything visible" -- a config that selected one repo would
	// suddenly plan all of them. The fix only auto-tags when this
	// integration+provider has NO existing tagged row at all: that is
	// "truly unbounded discovery", CHAOS-4602's own reported case (a config
	// that has never had a single source selected, planning zero units
	// because nothing exists to select from). Once at least one row is
	// already tagged, this pass never adds the tag to a newly discovered
	// row -- it only refreshes name/full_name/metadata on rows the merge
	// below finds already tagged, via EXCLUDED.metadata simply lacking the
	// key so the jsonb merge preserves whatever the existing row already
	// had.
	stampNewRows := plannerManaged && configID != ""
	if stampNewRows {
		var alreadySelected bool
		if err := tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM public.integration_sources
  WHERE org_id=$1 AND integration_id=$2::uuid AND provider=$3
    AND metadata::jsonb ? 'planner_managed_sync_config_id'
)`, orgID, integrationID, provider).Scan(&alreadySelected); err != nil {
			return 0, 0, fmt.Errorf("check existing %s source selection: %w", provider, err)
		}
		stampNewRows = !alreadySelected
	}
	for _, source := range sources {
		metadata := source.Metadata
		if stampNewRows {
			tagged := make(map[string]any, len(metadata)+1)
			for key, value := range metadata {
				tagged[key] = value
			}
			tagged["planner_managed_sync_config_id"] = configID
			metadata = tagged
		}
		metadataJSON, marshalErr := json.Marshal(metadata)
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

func boolOption(options map[string]any, key string) bool {
	value, _ := options[key].(bool)
	return value
}

// githubCredentialIsAppAuth mirrors NewGitHubClient's own PAT-vs-App-auth
// branch (providerfoundation/clients.go:41): a configured "token" secret
// means PAT auth; its absence means App auth (NewGitHubClient's own else
// branch mints an installation token via NewGitHubAppAuth). Codex review
// round 2, P2: all_repos discovery needs to know this to pick a listing
// endpoint an App installation token can actually call.
func githubCredentialIsAppAuth(credential providerfoundation.Credential) bool {
	token, ok := credential.Secret("token")
	return !(ok && token.Configured())
}

// fnmatchLike evaluates a discovery scope pattern the way Python's fnmatch
// does for the one case that would otherwise silently diverge from Go's
// path.Match: bracket negation is written "[!abc]" in fnmatch and "[^abc]"
// in path.Match -- translated below. Every other fnmatch/glob construct
// (*, ?, a non-negated [seq], and an EMPTY pattern -- which path.Match
// already treats as "matches only an empty string", exactly fnmatch's own
// behavior) matches path.Match's grammar directly, so no special-casing is
// needed for those. Codex review round 2, P2 (both the negation mismatch
// and an earlier version of this function's own empty-pattern special case,
// which incorrectly treated "" as "no filter").
func fnmatchLike(pattern, name string) bool {
	if pattern == "*" {
		return true
	}
	matched, err := path.Match(translateFnmatchBracketNegation(pattern), name)
	return err == nil && matched
}

func translateFnmatchBracketNegation(pattern string) string {
	var builder strings.Builder
	builder.Grow(len(pattern))
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '[' && i+1 < len(pattern) && pattern[i+1] == '!' {
			builder.WriteByte('[')
			builder.WriteByte('^')
			i++
			continue
		}
		builder.WriteByte(pattern[i])
	}
	return builder.String()
}
