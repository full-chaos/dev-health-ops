package sync

import (
	"context"
	"encoding/binary"
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
	"github.com/jackc/pgx/v5"
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
	// The three below are Jira-only parity outcomes with Python's
	// discover_sources_for_integration (#2036/CHAOS-4584), ported under
	// CHAOS-4629: github/gitlab cannot structurally emit them.
	SourceDiscoveryOutcomeCapped     = "capped_by_repo_limit"
	SourceDiscoveryOutcomeRecovered  = "recovered_from_repo_limit_cap"
	SourceDiscoveryOutcomeSuperseded = "superseded_by_scope_change"
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
	// Jira-only parity outcomes (CHAOS-4629): pre-seeded only for jira,
	// since github/gitlab cannot emit them.
	for _, outcome := range []string{SourceDiscoveryOutcomeCapped, SourceDiscoveryOutcomeRecovered, SourceDiscoveryOutcomeSuperseded} {
		telemetry.counts[[2]string{"jira", outcome}] = 0
	}
	return telemetry
}

func (telemetry *sourceDiscoveryTelemetry) observe(provider, outcome string) {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.counts[[2]string{provider, outcome}]++
}

func (telemetry *sourceDiscoveryTelemetry) observeN(provider, outcome string, n int) {
	if n <= 0 {
		return
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.counts[[2]string{provider, outcome}] += uint64(n)
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

// isUnboundedDiscovery decides whether upsertSources should tag every
// discovered row for this config (codex review, gate round, P1): "any
// tagged row already exists" is not a durable signal of "this config's
// selection is explicit and must never widen" -- it also fires,
// permanently, for a genuinely unbounded config the FIRST time discovery
// tags whatever happens to be visible then. A later-discovered new
// repo/project for that same unbounded config would come back untagged
// and stay invisible to loadPlanSources forever. Use the config's OWN
// durable scope instead, the exact field sync.py's own validation already
// treats as authoritative for this distinction: github/gitlab require
// either sync_options.all_repos=true or an explicit POST
// /sync-configs/batch selection (a plain create with neither is rejected
// there); jira has no all_repos option, but a planner-managed (parent)
// config CAN be scoped to one project via sync_options.project_key/
// project_id (_non_git_source_rows, sync.py) with source_id left NULL --
// only legacy per-source CHILD configs ever get one, so the Discover
// caller's own source_id-based ExplicitScope check never catches this
// case (codex review, gate round 8, P1: treating every jira config as
// unconditionally unbounded silently widened such a config to every
// accessible project the credential can see).
func isUnboundedDiscovery(provider string, syncOptions map[string]any) bool {
	if provider == "jira" {
		return firstTruthyString(syncOptions, "project_id", "project_key") == ""
	}
	return boolOption(syncOptions, "all_repos")
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
		discovered, err = service.discoverJira(ctx, credential, args.SyncOptions)
	}
	if err != nil {
		service.telemetry.observe(provider, SourceDiscoveryOutcomeError)
		return SourceDiscoveryReport{}, fmt.Errorf("discover %s sources: %w", provider, err)
	}
	unbounded := isUnboundedDiscovery(provider, args.SyncOptions)

	var created, existing, superseded int
	if provider == "jira" {
		// CHAOS-4629: Jira gets the case-insensitive matching / repo-limit
		// capping / rescope-supersede path that parities #2036/CHAOS-4584's
		// hardened Python behaviors. github/gitlab keep the plain exact-match
		// upsertSources path below (Python's own out-of-scope decision).
		//
		// upsertJiraSources and rebalanceJiraSourceRepoLimit share ONE
		// transaction (codex review P1): committing the upsert separately
		// from the cap/recovery step left a window where a rebalance
		// failure after a successful over-limit upsert left the org over
		// its max_repos entitlement with no automatic retry. One atomic
		// transaction means either both succeed or neither does -- a
		// failure here is retried wholesale on the next discovery pass,
		// the same "log loud, never fails the occurrence, nothing half-done"
		// contract every other discovery failure in this file already has.
		tx, txErr := service.domainPool.Begin(ctx)
		if txErr != nil {
			err = fmt.Errorf("begin jira source discovery domain transaction: %w", txErr)
		} else {
			txCommitted := false
			defer func() {
				if !txCommitted {
					_ = tx.Rollback(context.WithoutCancel(ctx))
				}
			}()
			var createdLower, discoveredLower map[string]struct{}
			created, existing, createdLower, discoveredLower, superseded, err = service.upsertJiraSources(
				ctx, tx, args.OrgID, args.IntegrationID, discovered, service.nowUTC(), args.ConfigID, args.PlannerManaged, unbounded,
			)
			if err == nil {
				err = service.rebalanceJiraSourceRepoLimit(ctx, tx, args.OrgID, args.IntegrationID, createdLower, discoveredLower)
			}
			if err == nil {
				if commitErr := tx.Commit(ctx); commitErr != nil {
					err = fmt.Errorf("commit jira source discovery domain transaction: %w", commitErr)
				} else {
					txCommitted = true
				}
			}
		}
	} else {
		created, existing, err = service.upsertSources(ctx, args.OrgID, args.IntegrationID, provider, discovered, service.nowUTC(), args.ConfigID, args.PlannerManaged, unbounded)
	}
	if err != nil {
		service.telemetry.observe(provider, SourceDiscoveryOutcomeError)
		return SourceDiscoveryReport{}, fmt.Errorf("upsert %s sources: %w", provider, err)
	}
	service.telemetry.observeN(provider, SourceDiscoveryOutcomeSuperseded, superseded)
	recordSourceDiscoveryOutcome(service.telemetry, provider, created, existing)
	outcome := SourceDiscoveryOutcomeExisting
	if created > 0 {
		outcome = SourceDiscoveryOutcomeCreated
	}
	return SourceDiscoveryReport{Outcome: outcome, Created: created, Existing: existing}, nil
}

// recordSourceDiscoveryOutcome observes one telemetry point per created/
// existing row, plus (codex review, gate round, P3) exactly one more when
// BOTH counts are zero. A successful listing that finds zero sources
// (created==0, existing==0 -- a valid, legitimate outcome, e.g. a brand-new
// all_repos config whose owner genuinely has no repos yet) would otherwise
// leave both loops empty and observe NOTHING at all, making a
// successful-but-empty run indistinguishable, in telemetry, from "this
// occurrence's discovery step never executed" -- Discover's caller
// (materializer.go) only logs on error, so nothing else would surface it
// either. The zero-count observation uses the same outcome the returned
// report itself claims (SourceDiscoveryOutcomeExisting, see Discover's own
// outcome selection), so the pre-seeded zero baseline actually moves.
func recordSourceDiscoveryOutcome(telemetry *sourceDiscoveryTelemetry, provider string, created, existing int) {
	for i := 0; i < created; i++ {
		telemetry.observe(provider, SourceDiscoveryOutcomeCreated)
	}
	for i := 0; i < existing; i++ {
		telemetry.observe(provider, SourceDiscoveryOutcomeExisting)
	}
	if created == 0 && existing == 0 {
		telemetry.observe(provider, SourceDiscoveryOutcomeExisting)
	}
}

func (service *NativeSourceDiscoveryService) nowUTC() time.Time {
	if service.now == nil {
		return time.Now().UTC()
	}
	return service.now().UTC()
}

// log falls back to slog.Default() for a service built via a bare struct
// literal (several existing tests construct NativeSourceDiscoveryService
// this way, skipping NewNativeSourceDiscoveryService's own nil check) --
// without this, a nil service.logger would panic the first time CHAOS-4629's
// rebalance logging runs.
func (service *NativeSourceDiscoveryService) log() *slog.Logger {
	if service.logger == nil {
		return slog.Default()
	}
	return service.logger
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

// NewGitHubClient already mints and applies an App installation token
// transparently for an App-auth credential. The all_repos GitHub-App
// installation-listing branch (discover_github_repos'
// _discover_github_app_installation_repos) IS ported below via
// /installation/repositories (codex review round 2, P2) -- App tokens have
// no /user/repos surface at all. It is still client-side filtered by the
// same owner/search/namespace scope as every other branch here, never a
// bypass-everything shortcut.
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
// external_id is the project KEY, matching the "project_key" scope
// vocabulary this ticket and team_autoimport_jira.py's own ownership
// resolution already use, not the numeric project id (which
// fetchJiraProject/resolveJiraProjectCatalog in jira_work_items_route.go
// resolve FROM the key, the reverse direction) -- UNLESS syncOptions carries
// an explicit project_id, in which case the result is filtered to that one
// project and identified BY that id (see the filtering block below,
// CHAOS-4629 parity with discovery/repos.py::discover_jira_projects, whose
// docstring names the exact precedence: project_id, once present, is the
// ENTIRE scope; project_key is ignored, not additionally enforced).
//
// This filtering is a CHAOS-4629 prerequisite, not cosmetic: without it, a
// bounded config's discovery pass would still list every visible project
// (Jira has no server-side "list one project" filter this client used
// before), which would put the OLD (soon-to-be-superseded) project back into
// discoveredLower on every single pass -- silently defeating
// supersedeStaleScopedJiraSources, which trusts discoveredLower to mean "the
// scope's CURRENT membership", not "everything the credential can see".
func (service *NativeSourceDiscoveryService) discoverJira(ctx context.Context, credential providerfoundation.Credential, syncOptions map[string]any) ([]discoveredSource, error) {
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
	explicitID := strings.TrimSpace(stringOption(syncOptions, "project_id"))
	explicitKey := normalizeSourceKey(stringOption(syncOptions, "project_key"))
	identityIsProjectID := explicitID != ""
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
		// Filter precedence mirrors discover_jira_projects exactly:
		// project_id, once present, is the ENTIRE scope (project_key is
		// ignored, not additionally enforced -- a stale key alongside a
		// freshly-PATCHed id must not filter every project out).
		if identityIsProjectID {
			if project.ID != explicitID {
				continue
			}
		} else if explicitKey != "" && normalizeSourceKey(project.Key) != explicitKey {
			continue
		}
		identity := project.Key
		if identityIsProjectID && project.ID != "" {
			identity = project.ID
		}
		fullName := project.Name
		if fullName == "" {
			fullName = project.Key
		}
		result = append(result, discoveredSource{
			ExternalID: identity, SourceType: "project", Name: fullName, FullName: fullName,
			// Codex review (gate round 10, P2): "discovered_project" mirrors
			// Python's own real-discovery marker (_map_jira_tuple,
			// CHAOS-4584 round 5) -- set on EVERY row this discovers,
			// regardless of what its key happens to be. Without it, a real
			// project whose key is literally "JIRA" (a live edge case) has
			// no explicit_project_scope (it wasn't explicitly configured,
			// it was discovered) and no project_key/project_id in an
			// unbounded config's sync_options, so isNonProjectJiraSource
			// falls through to the external_id=="jira" legacy-placeholder
			// check and wrongly classifies a real, validly-discovered
			// project as the known-bad shape, silently planning zero units
			// for it.
			//
			// project_id is ALWAYS carried, even when the KEY is the
			// identity: it is how a project-key RENAME is detected as the
			// same project rather than a new one (discovery/repos.py's own
			// rationale, ported verbatim; Go has no rename-migration path of
			// its own yet, out of CHAOS-4629's scope, but keeping the field
			// present costs nothing and keeps this row's shape aligned with
			// Python's for whenever that lands).
			Metadata: map[string]any{"project_id": project.ID, "discovered_project": true},
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
	configID string, plannerManaged, unbounded bool,
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
	// suddenly plan all of them.
	//
	// Codex review (gate round, P1) on THAT fix: gating on "does any
	// tagged row already exist" instead of on the config's own declared
	// scope broke a genuinely unbounded config the same way -- the first
	// discovery pass tags whatever is visible then, and every later pass
	// (a new repo/project becomes visible) sees an already-tagged row and
	// permanently stops tagging, hiding the new source from planning
	// forever. `unbounded` (the caller's provider-aware read of the
	// config's own sync_options.all_repos, or "always" for jira, which has
	// no such option) is a durable, config-level signal instead: it never
	// changes based on what has or hasn't been tagged before, so it can't
	// regress once any row happens to already carry the tag.
	stampNewRows := plannerManaged && configID != "" && unbounded
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

// --- CHAOS-4629: Jira parity with #2036/CHAOS-4584's hardened Python
// discover_sources_for_integration behaviors ---------------------------
//
// Three behaviors below (case-insensitive matching, repo-limit capping,
// rescope-supersede) are Jira-only, exactly matching Python's own scope:
// github/gitlab provider casing has never been shown to drift, and neither
// has an equivalent entitlement/rescope concern for them.

const (
	sourceCapMarkerKey        = "capped_by_repo_limit"
	sourceSupersededMarkerKey = "superseded_by_scope_change"
	sourceDuplicateOfKey      = "duplicate_of_external_id"
)

// errSourceInsertConflict marks a concurrent exact-external_id insert
// winning the race between our existing-rows fetch and our INSERT. The
// caller treats the loss the same as "existing" -- the row is there.
var errSourceInsertConflict = errors.New("integration source insert lost a concurrent race")

// normalizeSourceKey mirrors discovery/repos.py::jira_key_norm exactly:
// .strip().lower(), the ONE normalization implementation for every
// provider/external_id comparison here. Comparison happens only in Go, never
// mirrored in SQL -- chris's ruling (CHAOS-4584 gate round 7, reaffirmed for
// CHAOS-4629) after two independent SQL-side normalization mirrors drifted
// from Python's Unicode-aware jira_key_norm on real inputs.
func normalizeSourceKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

type existingSourceRow struct {
	ID           string
	Provider     string
	ExternalID   string
	IsEnabled    bool
	DiscoveredAt time.Time
	Metadata     map[string]any
}

func cloneMetadata(metadata map[string]any) map[string]any {
	clone := make(map[string]any, len(metadata)+1)
	for key, value := range metadata {
		clone[key] = value
	}
	return clone
}

func fetchExistingSourceRows(ctx context.Context, tx pgx.Tx, orgID, integrationID string) ([]*existingSourceRow, error) {
	rows, err := tx.Query(ctx, `
SELECT id::text, provider, external_id, is_enabled, discovered_at, metadata::jsonb
FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid`, orgID, integrationID)
	if err != nil {
		return nil, fmt.Errorf("load existing integration sources: %w", err)
	}
	defer rows.Close()
	var result []*existingSourceRow
	for rows.Next() {
		row := &existingSourceRow{}
		var metadataJSON []byte
		if err := rows.Scan(&row.ID, &row.Provider, &row.ExternalID, &row.IsEnabled, &row.DiscoveredAt, &metadataJSON); err != nil {
			return nil, fmt.Errorf("scan existing integration source: %w", err)
		}
		_ = json.Unmarshal(metadataJSON, &row.Metadata)
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func disableSourceRow(ctx context.Context, tx pgx.Tx, id string, metadata map[string]any) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("encode source metadata: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE public.integration_sources SET is_enabled=FALSE, metadata=$2::json WHERE id=$1::uuid`, id, metadataJSON); err != nil {
		return fmt.Errorf("disable integration source %s: %w", id, err)
	}
	return nil
}

func enableSourceRow(ctx context.Context, tx pgx.Tx, id string, metadata map[string]any) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("encode source metadata: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE public.integration_sources SET is_enabled=TRUE, metadata=$2::json WHERE id=$1::uuid`, id, metadataJSON); err != nil {
		return fmt.Errorf("enable integration source %s: %w", id, err)
	}
	return nil
}

func updateExistingSourceRow(ctx context.Context, tx pgx.Tx, id, name, fullName string, metadata map[string]any, now time.Time, reenable bool) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("encode source metadata: %w", err)
	}
	sql := `UPDATE public.integration_sources SET name=$2, full_name=$3, metadata=$4::json, last_seen_at=$5`
	if reenable {
		sql += `, is_enabled=TRUE`
	}
	sql += ` WHERE id=$1::uuid`
	if _, err := tx.Exec(ctx, sql, id, name, fullName, metadataJSON, now); err != nil {
		return fmt.Errorf("update integration source %s: %w", id, err)
	}
	return nil
}

func insertNewSourceRow(
	ctx context.Context, tx pgx.Tx, orgID, integrationID, provider, sourceType, externalID, name, fullName string,
	metadata map[string]any, now time.Time,
) (id string, discoveredAt time.Time, err error) {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("encode source metadata: %w", err)
	}
	var returnedID string
	err = tx.QueryRow(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,metadata,is_enabled,discovered_at,last_seen_at)
VALUES ($1::uuid,$2,$3::uuid,$4,$5,$6,$7,$8,$9::json,TRUE,$10,$10)
ON CONFLICT (org_id,integration_id,provider,external_id) DO NOTHING
RETURNING id::text`,
		uuid.New().String(), orgID, integrationID, provider, sourceType, externalID, name, fullName, metadataJSON, now,
	).Scan(&returnedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", time.Time{}, errSourceInsertConflict
	}
	if err != nil {
		return "", time.Time{}, fmt.Errorf("insert integration source: %w", err)
	}
	return returnedID, now, nil
}

// upsertJiraSources ports discover_sources_for_integration's jira branch
// (CHAOS-4584 rounds 2-4). Jira project keys are case-insensitive
// server-side but an operator-typed scope, or a pre-existing row, can carry
// any casing -- matching must fold case BEFORE deciding insert-vs-update, or
// the plain exact-match upsertSources path inserts a case-variant duplicate
// that double-schedules the same project. A pre-existing case-variant PAIR
// self-repairs: every extra candidate folds into one surviving row (an
// already-enabled candidate wins ties, so an existing sync in progress is
// never silently stopped).
//
// Takes an already-open tx (never begins/commits its own): the caller
// (Discover) runs this and rebalanceJiraSourceRepoLimit in ONE transaction
// (codex review P1: two separate commits left a window where a rebalance
// failure after a successful over-limit upsert committed left the org over
// its entitlement with no automatic retry -- one atomic transaction means
// either both succeed or neither does, and a failure here is retried
// wholesale on the next discovery pass, exactly like every other discovery
// failure this file already treats as "log loud, never fails the
// occurrence, nothing was silently left half-done").
//
// Returns the created/discovered external_id sets (normalized) the caller
// needs for repo-limit capping/recovery, and the count of sources this run
// superseded for a rescoped explicit config.
func (service *NativeSourceDiscoveryService) upsertJiraSources(
	ctx context.Context, tx pgx.Tx, orgID, integrationID string, sources []discoveredSource, now time.Time,
	configID string, plannerManaged, unbounded bool,
) (created, existingCount int, createdLower, discoveredLower map[string]struct{}, superseded int, err error) {
	discoveredLower = make(map[string]struct{}, len(sources))
	createdLower = make(map[string]struct{})
	for _, source := range sources {
		discoveredLower[normalizeSourceKey(source.ExternalID)] = struct{}{}
	}
	if len(sources) == 0 {
		return 0, 0, createdLower, discoveredLower, 0, nil
	}

	existingRows, err := fetchExistingSourceRows(ctx, tx, orgID, integrationID)
	if err != nil {
		return 0, 0, nil, nil, 0, err
	}

	// stampNewRows (codex review P1): tag EVERY discovered row whenever this
	// integration has a planner-managed parent, bounded or unbounded --
	// UNLIKE upsertSources' own isUnboundedDiscovery-gated tagging (used for
	// github/gitlab, which have no listing-time scope filter of their own).
	// discoverJira now filters its OWN listing to the config's explicit
	// project_key/project_id scope (see that function's doc comment) BEFORE
	// this function ever sees the results, so a bounded config's discovered
	// set can only ever contain its own current scope -- there is no "other
	// visible project" left to accidentally over-tag, which is exactly what
	// upsertSources' unbounded gate exists to prevent for providers that
	// have no such listing-time filter. Gating jira's OWN tag on unbounded
	// was the bug: a bounded config's rescope (OLD -> NEW) inserted NEW
	// untagged, so loadPlanSources' tag-filtered SELECT never saw it -- the
	// rescoped project silently planned zero units forever.
	stampNewRows := plannerManaged && configID != ""

	for _, source := range sources {
		normalizedKey := normalizeSourceKey(source.ExternalID)
		var candidates []*existingSourceRow
		for _, row := range existingRows {
			if normalizeSourceKey(row.Provider) == "jira" && normalizeSourceKey(row.ExternalID) == normalizedKey {
				candidates = append(candidates, row)
			}
		}
		sort.Slice(candidates, func(i, j int) bool {
			if !candidates[i].DiscoveredAt.Equal(candidates[j].DiscoveredAt) {
				return candidates[i].DiscoveredAt.Before(candidates[j].DiscoveredAt)
			}
			return candidates[i].ID < candidates[j].ID
		})

		var survivor *existingSourceRow
		if len(candidates) > 0 {
			pool := make([]*existingSourceRow, 0, len(candidates))
			for _, candidate := range candidates {
				if candidate.IsEnabled {
					pool = append(pool, candidate)
				}
			}
			if len(pool) == 0 {
				pool = candidates
			}
			survivor = pool[0]
			for _, candidate := range pool {
				if candidate.ExternalID == source.ExternalID {
					survivor = candidate
					break
				}
			}
			for _, dup := range candidates {
				if dup == survivor {
					continue
				}
				metadata := cloneMetadata(dup.Metadata)
				metadata[sourceDuplicateOfKey] = survivor.ExternalID
				if err := disableSourceRow(ctx, tx, dup.ID, metadata); err != nil {
					return 0, 0, nil, nil, 0, err
				}
				dup.IsEnabled = false
				dup.Metadata = metadata
			}
		}

		metadata := source.Metadata
		if stampNewRows {
			metadata = cloneMetadata(metadata)
			metadata["planner_managed_sync_config_id"] = configID
		}

		if survivor != nil {
			merged := cloneMetadata(survivor.Metadata)
			for key, value := range metadata {
				merged[key] = value
			}
			reenable := false
			if _, ok := merged[sourceSupersededMarkerKey]; ok {
				// The project this row was superseded for is reconfirmed by
				// this discovery run -- undo a system-driven (never an
				// operator's own) rescope disable.
				delete(merged, sourceSupersededMarkerKey)
				reenable = true
			}
			if err := updateExistingSourceRow(ctx, tx, survivor.ID, source.Name, source.FullName, merged, now, reenable); err != nil {
				return 0, 0, nil, nil, 0, err
			}
			survivor.Metadata = merged
			if reenable {
				survivor.IsEnabled = true
			}
			existingCount++
			continue
		}

		newID, discoveredAt, insertErr := insertNewSourceRow(ctx, tx, orgID, integrationID, "jira", "project", source.ExternalID, source.Name, source.FullName, metadata, now)
		if errors.Is(insertErr, errSourceInsertConflict) {
			existingCount++
			continue
		}
		if insertErr != nil {
			return 0, 0, nil, nil, 0, insertErr
		}
		created++
		createdLower[normalizedKey] = struct{}{}
		existingRows = append(existingRows, &existingSourceRow{
			ID: newID, Provider: "jira", ExternalID: source.ExternalID,
			IsEnabled: true, DiscoveredAt: discoveredAt, Metadata: metadata,
		})
	}

	if plannerManaged && configID != "" && !unbounded {
		count, err := supersedeStaleScopedJiraSources(ctx, tx, orgID, integrationID, configID, discoveredLower)
		if err != nil {
			return 0, 0, nil, nil, 0, err
		}
		superseded = count
	}

	return created, existingCount, createdLower, discoveredLower, superseded, nil
}

// supersedeStaleScopedJiraSources ports
// discovery.py::_supersede_stale_scoped_jira_sources (CHAOS-4584 round 3):
// when a planner-managed config is explicitly scoped to one project
// (unbounded==false), disable any OTHER enabled source THIS config tagged
// that discovery did not just return -- otherwise moving an explicitly
// scoped config's project_key/project_id leaves the OLD project enabled
// forever. Never applied to an unbounded config (a project transiently
// missing from one run is never auto-disabled -- this file's documented
// stale-handling policy), and never on an empty discoveredLower
// (indistinguishable from a transient credential/API failure -- the caller
// already guarantees this by only calling with unbounded==false, len(sources)>0).
func supersedeStaleScopedJiraSources(
	ctx context.Context, tx pgx.Tx, orgID, integrationID, configID string, discoveredLower map[string]struct{},
) (int, error) {
	if len(discoveredLower) == 0 {
		return 0, nil
	}
	rows, err := tx.Query(ctx, `
SELECT id::text, external_id, metadata::jsonb FROM public.integration_sources
WHERE org_id=$1 AND integration_id=$2::uuid AND provider='jira' AND is_enabled`, orgID, integrationID)
	if err != nil {
		return 0, fmt.Errorf("load enabled jira sources for supersede: %w", err)
	}
	type candidateRow struct {
		id         string
		externalID string
		metadata   map[string]any
	}
	var superseded []candidateRow
	for rows.Next() {
		var row candidateRow
		var metadataJSON []byte
		if err := rows.Scan(&row.id, &row.externalID, &metadataJSON); err != nil {
			rows.Close()
			return 0, err
		}
		_ = json.Unmarshal(metadataJSON, &row.metadata)
		tag, _ := row.metadata["planner_managed_sync_config_id"].(string)
		if tag != configID {
			continue
		}
		if _, ok := discoveredLower[normalizeSourceKey(row.externalID)]; ok {
			continue
		}
		superseded = append(superseded, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	for _, row := range superseded {
		metadata := cloneMetadata(row.metadata)
		metadata[sourceSupersededMarkerKey] = true
		if err := disableSourceRow(ctx, tx, row.id, metadata); err != nil {
			return 0, err
		}
	}
	return len(superseded), nil
}

// jiraMaxReposTierDefaults mirrors models/licensing.py::TIER_LIMITS_DEFAULTS'
// max_repos entries exactly: community=3, team=10, enterprise=unlimited.
var jiraMaxReposTierDefaults = map[string]*int{
	"community":  intPtr(3),
	"team":       intPtr(10),
	"enterprise": nil,
}

func intPtr(value int) *int { return &value }

// resolveMaxReposLimit mirrors
// api/services/licensing.py::TierLimitService.get_limit(org_id,"max_repos")'s
// exact precedence: org_licenses.limits_override (highest) -> tier_limits DB
// table -> hardcoded jiraMaxReposTierDefaults. Reuses the same
// organizations/org_licenses join loadPlanLimits already uses for
// backfill_days/max_sync_units, so a tier resolution divergence between the
// two would need a change in only one of these two spots to fix.
func resolveMaxReposLimit(ctx context.Context, tx pgx.Tx, orgID string) (*int, error) {
	var orgTier, licenseTier *string
	var overridesJSON []byte
	err := tx.QueryRow(ctx, `
SELECT coalesce(organization.tier,'community'),license.tier,license.limits_override::jsonb
FROM public.organizations AS organization
LEFT JOIN public.org_licenses AS license ON license.org_id=organization.id
WHERE organization.id=$1::uuid`, orgID).Scan(&orgTier, &licenseTier, &overridesJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return intPtr(3), nil
	}
	if err != nil {
		return nil, fmt.Errorf("load org tier for max_repos limit: %w", err)
	}
	resolvedTier := "community"
	if orgTier != nil {
		resolvedTier = *orgTier
	}
	if licenseTier != nil {
		resolvedTier = *licenseTier
	}
	if resolvedTier != "community" && resolvedTier != "team" && resolvedTier != "enterprise" {
		resolvedTier = "community"
	}
	var overrides map[string]json.RawMessage
	_ = json.Unmarshal(overridesJSON, &overrides)
	if raw, ok := overrides["max_repos"]; ok {
		if string(raw) == "null" {
			return nil, nil
		}
		var value int
		if json.Unmarshal(raw, &value) == nil {
			return &value, nil
		}
	}
	var limitValueText *string
	err = tx.QueryRow(ctx, `SELECT limit_value FROM public.tier_limits WHERE tier=$1 AND limit_key='max_repos'`, resolvedTier).Scan(&limitValueText)
	if err == nil {
		if limitValueText == nil {
			return nil, nil
		}
		var value int
		if _, scanErr := fmt.Sscanf(*limitValueText, "%d", &value); scanErr == nil {
			return &value, nil
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("load max_repos tier limit: %w", err)
	}
	return jiraMaxReposTierDefaults[resolvedTier], nil
}

// activeRepoUsageCountForLimit mirrors
// discovery.py::_active_repo_usage_count_for_limit: org-wide "legacy active
// configs (not a planner-managed parent) + enabled sources on every
// planner-managed integration" count.
func activeRepoUsageCountForLimit(ctx context.Context, tx pgx.Tx, orgID string) (int, error) {
	var legacyCount int
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM public.sync_configurations
WHERE org_id=$1 AND is_active
  AND NOT (parent_id IS NULL AND planner_managed AND integration_id IS NOT NULL)`, orgID).Scan(&legacyCount); err != nil {
		return 0, fmt.Errorf("count legacy active sync configs: %w", err)
	}
	var sourceCount int
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM public.integration_sources AS source
WHERE source.org_id=$1 AND source.is_enabled
  AND source.integration_id IN (
    SELECT integration_id FROM public.sync_configurations
    WHERE org_id=$1 AND is_active AND parent_id IS NULL AND planner_managed AND integration_id IS NOT NULL
  )`, orgID).Scan(&sourceCount); err != nil {
		return 0, fmt.Errorf("count enabled planner-managed sources: %w", err)
	}
	return legacyCount + sourceCount, nil
}

// repoLimitAdvisoryLockKey mirrors
// discovery.py::_acquire_repo_limit_lock's key formula EXACTLY --
// uuid.UUID(org_id).int & ((1<<63)-1), falling back to
// uuid5(NAMESPACE_URL, org_id) for a non-UUID org_id -- because Python's own
// create_sync_config repo-limit preflight and THIS Go rebalance step must
// serialize against each other on the SAME advisory-lock key during the
// coexistence window (an org can get a new Jira config created via Python
// at the same moment an occurrence's Go discovery rebalances it). A
// same-process-only key (e.g. Postgres's own hashtextextended) would not
// coordinate across languages at all.
func repoLimitAdvisoryLockKey(orgID string) int64 {
	parsed, err := uuid.Parse(orgID)
	if err != nil {
		// RFC 4122 NAMESPACE_URL, matching Python's uuid.NAMESPACE_URL.
		namespace := uuid.MustParse("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
		parsed = uuid.NewSHA1(namespace, []byte(orgID))
	}
	// Python's uuid.UUID.int is the big-endian 128-bit unsigned integer of
	// the 16 raw bytes; the low 64 bits of that integer are exactly the
	// last 8 bytes of the (big-endian) byte array. Mask off the sign bit to
	// match Python's `& ((1 << 63) - 1)`.
	low64 := binary.BigEndian.Uint64(parsed[8:16])
	return int64(low64 & 0x7FFFFFFFFFFFFFFF)
}

type repoLimitCandidateRow struct {
	id         string
	externalID string
	metadata   map[string]any
}

// rebalanceJiraSourceRepoLimit ports
// discovery.py::_rebalance_jira_sources_against_repo_limit (CHAOS-4584):
// keeps this integration's enabled Jira source count within the org's
// max_repos entitlement after a discovery run. Prefers capping sources
// CREATED by THIS run over already-enabled pre-existing ones (an org already
// at its limit must not lose real, relied-upon sources just because this run
// also discovered new ones); recovers previously cap-disabled rows once
// headroom returns, but only ones discovery just reconfirmed exist.
//
// Takes an already-open tx, shared with upsertJiraSources -- see that
// function's doc comment for why capping/recovery must commit atomically
// with the upsert (codex review P1) rather than in its own transaction.
func (service *NativeSourceDiscoveryService) rebalanceJiraSourceRepoLimit(
	ctx context.Context, tx pgx.Tx, orgID, integrationID string, createdLower, discoveredLower map[string]struct{},
) error {
	var plannerConfigActive *bool
	err := tx.QueryRow(ctx, `
SELECT is_active FROM public.sync_configurations
WHERE integration_id=$1::uuid AND org_id=$2 AND planner_managed AND parent_id IS NULL
LIMIT 1`, integrationID, orgID).Scan(&plannerConfigActive)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("load planner config active state: %w", err)
	}
	if plannerConfigActive != nil && !*plannerConfigActive {
		// A paused integration's own usage would read as zero (active-only
		// count), and recovery could wrongly re-enable every capped source
		// for it -- skip cap/recovery entirely while paused (CHAOS-4584
		// round 2 P1). The PATCH reactivation handler re-runs discovery.
		return nil
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, repoLimitAdvisoryLockKey(orgID)); err != nil {
		return fmt.Errorf("acquire repo-limit lock: %w", err)
	}

	maxRepos, err := resolveMaxReposLimit(ctx, tx, orgID)
	if err != nil {
		return err
	}

	if maxRepos != nil {
		usage, err := activeRepoUsageCountForLimit(ctx, tx, orgID)
		if err != nil {
			return err
		}
		overflow := usage - *maxRepos
		if overflow > 0 {
			rows, err := tx.Query(ctx, `
SELECT id::text, external_id, metadata::jsonb FROM public.integration_sources
WHERE org_id=$1 AND integration_id=$2::uuid AND provider='jira' AND is_enabled
ORDER BY external_id DESC`, orgID, integrationID)
			if err != nil {
				return fmt.Errorf("load enabled jira sources for capping: %w", err)
			}
			var createdRows, preExistingRows []repoLimitCandidateRow
			for rows.Next() {
				var row repoLimitCandidateRow
				var metadataJSON []byte
				if err := rows.Scan(&row.id, &row.externalID, &metadataJSON); err != nil {
					rows.Close()
					return err
				}
				_ = json.Unmarshal(metadataJSON, &row.metadata)
				if _, ok := createdLower[normalizeSourceKey(row.externalID)]; ok {
					createdRows = append(createdRows, row)
				} else {
					preExistingRows = append(preExistingRows, row)
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
			capped := append(createdRows, preExistingRows...)
			if len(capped) > overflow {
				capped = capped[:overflow]
			}
			for _, row := range capped {
				metadata := cloneMetadata(row.metadata)
				metadata[sourceCapMarkerKey] = true
				if err := disableSourceRow(ctx, tx, row.id, metadata); err != nil {
					return err
				}
			}
			if len(capped) > 0 {
				service.telemetry.observeN("jira", SourceDiscoveryOutcomeCapped, len(capped))
				service.log().Warn("jira_project_discovery_capped_by_repo_limit",
					"org_id", orgID, "integration_id", integrationID, "capped_count", len(capped), "max_repos", *maxRepos)
			}
			return nil
		}
	}

	rows, err := tx.Query(ctx, `
SELECT id::text, external_id, metadata::jsonb FROM public.integration_sources
WHERE org_id=$1 AND integration_id=$2::uuid AND provider='jira' AND NOT is_enabled
ORDER BY external_id ASC`, orgID, integrationID)
	if err != nil {
		return fmt.Errorf("load disabled jira sources for recovery: %w", err)
	}
	var recoverable []repoLimitCandidateRow
	for rows.Next() {
		var row repoLimitCandidateRow
		var metadataJSON []byte
		if err := rows.Scan(&row.id, &row.externalID, &metadataJSON); err != nil {
			rows.Close()
			return err
		}
		_ = json.Unmarshal(metadataJSON, &row.metadata)
		capped, _ := row.metadata[sourceCapMarkerKey].(bool)
		_, discoveredNow := discoveredLower[normalizeSourceKey(row.externalID)]
		if capped && discoveredNow {
			recoverable = append(recoverable, row)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(recoverable) == 0 {
		return nil
	}
	if maxRepos != nil {
		usage, err := activeRepoUsageCountForLimit(ctx, tx, orgID)
		if err != nil {
			return err
		}
		headroom := *maxRepos - usage
		if headroom < 0 {
			headroom = 0
		}
		if len(recoverable) > headroom {
			recoverable = recoverable[:headroom]
		}
	}
	for _, row := range recoverable {
		metadata := cloneMetadata(row.metadata)
		delete(metadata, sourceCapMarkerKey)
		if err := enableSourceRow(ctx, tx, row.id, metadata); err != nil {
			return err
		}
	}
	if len(recoverable) > 0 {
		service.telemetry.observeN("jira", SourceDiscoveryOutcomeRecovered, len(recoverable))
		service.log().Info("jira_project_discovery_recovered_from_repo_limit_cap",
			"org_id", orgID, "integration_id", integrationID, "recovered_count", len(recoverable))
	}
	return nil
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
