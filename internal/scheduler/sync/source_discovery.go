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

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/synclimits"
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
	// SourceIDs are the ids of the integration_sources rows this run upserted
	// (new and updated), one per discovered source in discovery order, as
	// discover_sources_for_integration returns its `upserted` list: a row
	// the run matched twice appears twice. The integration admin discover
	// route renders them.
	SourceIDs []string
}

// sourceIDCollector records the row each discovered source landed on, for
// Discover's report. It rides the context so the upsert functions keep their
// signatures.
type sourceIDCollector struct{ ids []string }

type sourceIDCollectorKey struct{}

func withSourceIDCollector(ctx context.Context) (context.Context, *sourceIDCollector) {
	collector := &sourceIDCollector{}
	return context.WithValue(ctx, sourceIDCollectorKey{}, collector), collector
}

func recordUpsertedSource(ctx context.Context, id string) {
	if collector, ok := ctx.Value(sourceIDCollectorKey{}).(*sourceIDCollector); ok {
		collector.ids = append(collector.ids, id)
	}
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
	Metadata   *pyjson.Object
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

// WithClock sets the instant discovery stamps on the rows it writes
// (discovered_at, last_seen_at): a caller serving a request passes the
// request's clock, as Python's discovery reads the same datetime.now the
// rest of the request does. nil keeps the wall clock.
func (service *NativeSourceDiscoveryService) WithClock(now func() time.Time) *NativeSourceDiscoveryService {
	if now != nil {
		service.now = now
	}
	return service
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
		if provider == "jira" {
			// Python reads the JIRA_* environment for such an integration and,
			// with none set, discovers nothing and counts a zero discovery
			// (sync/discovery.py); this service discovers nothing here too.
			recordJiraProjectDiscovery(ctx, 0, 0, 0, 0, 0, args.PlannerManaged)
		}
		return SourceDiscoveryReport{Outcome: SourceDiscoveryOutcomeSkipped}, nil
	}
	credential, err := service.credentials.Resolve(ctx, sourceDiscoveryLease{}, providerfoundation.TenantScope{
		OrgID: args.OrgID, Provider: provider, IntegrationID: args.IntegrationID, CredentialID: *args.CredentialID,
	})
	if err != nil {
		service.telemetry.observe(provider, SourceDiscoveryOutcomeError)
		return SourceDiscoveryReport{}, fmt.Errorf("resolve %s credential for source discovery: %w", provider, err)
	}
	ctx, collector := withSourceIDCollector(ctx)
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

	var created, existing, superseded, capped, recovered int
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
				capped, recovered, err = service.rebalanceJiraSourceRepoLimit(ctx, tx, args.OrgID, args.IntegrationID, createdLower, discoveredLower)
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
	// codex review round-3 P3: capped/recovered telemetry and logging are
	// recorded ONLY here, after the shared transaction has actually
	// committed (mirrors materializer.go's RecordMaterialized, which
	// defers plan telemetry past its caller's own commit for the identical
	// reason) -- rebalanceJiraSourceRepoLimit itself never logs or observes,
	// so a rolled-back transaction can never be reported as a successful cap.
	if capped > 0 {
		service.telemetry.observeN("jira", SourceDiscoveryOutcomeCapped, capped)
		service.log().Warn("jira_project_discovery_capped_by_repo_limit",
			"org_id", args.OrgID, "integration_id", args.IntegrationID, "capped_count", capped)
	}
	if recovered > 0 {
		service.telemetry.observeN("jira", SourceDiscoveryOutcomeRecovered, recovered)
		service.log().Info("jira_project_discovery_recovered_from_repo_limit_cap",
			"org_id", args.OrgID, "integration_id", args.IntegrationID, "recovered_count", recovered)
	}
	service.telemetry.observeN(provider, SourceDiscoveryOutcomeSuperseded, superseded)
	recordSourceDiscoveryOutcome(service.telemetry, provider, created, existing)
	if provider == "jira" {
		recordJiraProjectDiscovery(ctx, len(discovered), created, superseded, capped, recovered, args.PlannerManaged)
	}
	outcome := SourceDiscoveryOutcomeExisting
	if created > 0 {
		outcome = SourceDiscoveryOutcomeCreated
	}
	return SourceDiscoveryReport{Outcome: outcome, Created: created, Existing: existing, SourceIDs: collector.ids}, nil
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

// recordJiraProjectDiscovery is sync/discovery.py's jira_project_discovery_total
// emission for one committed Jira discovery: _record_jira_project_discovery's
// discovered/created/existing (or discovered_zero) and
// skipped_no_planner_parent, plus the superseded, capped and recovered counts
// of the steps that ran in the same pass. existing is Python's
// `len(source_dicts) - created_count`.
func recordJiraProjectDiscovery(ctx context.Context, discovered, created, superseded, capped, recovered int, plannerManaged bool) {
	if superseded > 0 {
		providerfoundation.RecordJiraProjectDiscovery(ctx, providerfoundation.JiraDiscoverySuperseded, superseded)
	}
	if capped > 0 {
		providerfoundation.RecordJiraProjectDiscovery(ctx, providerfoundation.JiraDiscoveryCapped, capped)
	}
	if recovered > 0 {
		providerfoundation.RecordJiraProjectDiscovery(ctx, providerfoundation.JiraDiscoveryRecoveredFromCap, recovered)
	}
	if discovered == 0 {
		providerfoundation.RecordJiraProjectDiscovery(ctx, providerfoundation.JiraDiscoveryZero, 1)
	} else {
		providerfoundation.RecordJiraProjectDiscovery(ctx, providerfoundation.JiraDiscoveryDiscovered, discovered)
		providerfoundation.RecordJiraProjectDiscovery(ctx, providerfoundation.JiraDiscoveryCreated, created)
		if existing := discovered - created; existing > 0 {
			providerfoundation.RecordJiraProjectDiscovery(ctx, providerfoundation.JiraDiscoveryExisting, existing)
		}
	}
	if !plannerManaged {
		providerfoundation.RecordJiraProjectDiscovery(ctx, providerfoundation.JiraDiscoverySkippedNoPlanner, 1)
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
			Metadata: metadataOf("owner", repo.Owner.Login),
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
			Metadata: metadataOf("path_with_namespace", project.PathWithNamespace),
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
		decoded, err := pyjson.DecodeString(string(raw))
		if err != nil {
			continue
		}
		project, ok := decoded.(*pyjson.Object)
		if !ok {
			continue
		}
		// discover_jira_projects: key = str(project.get("key") or "").strip(),
		// project_id likewise, name = str(project.get("name") or key),
		// project_type_key stripped and lower-cased.
		field := func(name string) string {
			value, _ := project.Get(name)
			if !pyjson.Truthy(value) {
				return ""
			}
			return pyjson.Str(value)
		}
		key := pythonparity.Strip(field("key"))
		if key == "" {
			continue
		}
		projectID := pythonparity.Strip(field("id"))
		// Filter precedence mirrors discover_jira_projects exactly:
		// project_id, once present, is the ENTIRE scope (project_key is
		// ignored, not additionally enforced -- a stale key alongside a
		// freshly-PATCHed id must not filter every project out).
		if identityIsProjectID {
			if projectID != explicitID {
				continue
			}
		} else if explicitKey != "" && normalizeSourceKey(key) != explicitKey {
			continue
		}
		name := field("name")
		if name == "" {
			name = key
		}
		projectTypeKey := pythonparity.Lower(pythonparity.Strip(field("projectTypeKey")))
		identity := key
		if identityIsProjectID && projectID != "" {
			identity = projectID
		}
		// _map_jira_tuple(identity, name, project_type_key, project_id):
		// external_id and full_name are the identity; the metadata is
		// {project_type_key, discovered_project: True, jira_project_id
		// (when there is one)}, in that order, and the planner tag follows.
		//
		// discovered_project marks every row a real discovery run created:
		// without it, a real project whose key is literally "JIRA" is
		// classified as the legacy placeholder shape and plans zero units.
		// jira_project_id is Jira's immutable numeric id, carried even when
		// the key is the identity, so a key rename is recognisable as the
		// same project.
		metadata := pyjson.NewObject()
		metadata.Set("project_type_key", projectTypeKey)
		metadata.Set("discovered_project", true)
		if projectID != "" {
			metadata.Set("jira_project_id", projectID)
		}
		result = append(result, discoveredSource{
			ExternalID: identity, SourceType: "project", Name: name, FullName: identity, Metadata: metadata,
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
		return nil, logging.TransportFailure(err)
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
	// Read, merge and write as discover_sources_for_integration does: an
	// existing row keeps its own metadata keys (their order and spelling) and
	// the fresh keys win, and the column holds json.dumps text. A single
	// jsonb-merging upsert would reorder the keys and write non-ASCII
	// unescaped, so the stored text would differ from Python's.
	existingRows, err := fetchExistingSourceRows(ctx, tx, orgID, integrationID)
	if err != nil {
		return 0, 0, err
	}
	for _, source := range sources {
		metadata := source.Metadata
		if metadata == nil {
			metadata = pyjson.NewObject()
		}
		if stampNewRows {
			metadata = cloneMetadata(metadata)
			metadata.Set("planner_managed_sync_config_id", configID)
		}
		var row *existingSourceRow
		for _, candidate := range existingRows {
			if candidate.Provider == provider && candidate.ExternalID == source.ExternalID {
				row = candidate
				break
			}
		}
		if row != nil {
			merged := cloneMetadata(row.Metadata)
			for _, key := range metadata.Keys() {
				value, _ := metadata.Get(key)
				merged.Set(key, value)
			}
			if err := updateExistingSourceRow(ctx, tx, row.ID, source.Name, source.FullName, merged, now, false); err != nil {
				return 0, 0, err
			}
			row.Metadata = merged
			recordUpsertedSource(ctx, row.ID)
			existing++
			continue
		}
		newID, discoveredAt, insertErr := insertNewSourceRow(ctx, tx, orgID, integrationID, provider, source.SourceType, source.ExternalID, source.Name, source.FullName, metadata, now)
		if errors.Is(insertErr, errSourceInsertConflict) {
			// A concurrent discovery inserted this exact key first: the row is there.
			var winnerID string
			if err := tx.QueryRow(ctx, `SELECT id::text FROM public.integration_sources
WHERE org_id=$1 AND integration_id=$2::uuid AND provider=$3 AND external_id=$4`, orgID, integrationID, provider, source.ExternalID).Scan(&winnerID); err != nil {
				return 0, 0, fmt.Errorf("read the source a concurrent discovery inserted: %w", err)
			}
			recordUpsertedSource(ctx, winnerID)
			existing++
			continue
		}
		if insertErr != nil {
			return 0, 0, insertErr
		}
		recordUpsertedSource(ctx, newID)
		created++
		existingRows = append(existingRows, &existingSourceRow{
			ID: newID, Provider: provider, ExternalID: source.ExternalID,
			IsEnabled: true, DiscoveredAt: discoveredAt, Metadata: metadata,
		})
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

// normalizeSourceKey mirrors discovery/repos.py::jira_key_norm's shape
// (.strip().lower()) as the ONE normalization implementation for every
// provider/external_id comparison here. Comparison happens only in Go, never
// mirrored in SQL -- chris's ruling (CHAOS-4584 gate round 7, reaffirmed for
// CHAOS-4629) after two independent SQL-side normalization mirrors drifted
// from Python's jira_key_norm on real inputs.
//
// NOT Unicode-equivalent to Python's jira_key_norm for every input (CHAOS-4679,
// codex CHAOS-4629 gate round 6, executed): Python's str.lower() applies full
// Unicode SpecialCasing (e.g. U+0130 LATIN CAPITAL LETTER I WITH DOT ABOVE ->
// two codepoints, "i"+U+0307); Go's strings.ToLower does simple 1:1 rune
// mapping and collapses the same input to plain "i". Accepted as a narrow,
// documented limitation (chris/team-lead ruling) rather than pulling in
// golang.org/x/text/cases: exploiting it needs a literal U+0130 in a
// provider/external_id column, which no current write path produces, and
// the Python comparator this could diverge from is on a retiring plane --
// CHAOS-4679 tracks it and dissolves at Python retirement, when Go's own
// self-consistency (which already holds) is the only property that matters.
func normalizeSourceKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

type existingSourceRow struct {
	ID           string
	Provider     string
	ExternalID   string
	IsEnabled    bool
	DiscoveredAt time.Time
	Metadata     *pyjson.Object
}

// metadataOf is a one-key metadata dict.
func metadataOf(key string, value pyjson.Value) *pyjson.Object {
	metadata := pyjson.NewObject()
	metadata.Set(key, value)
	return metadata
}

// decodeSourceMetadata reads a stored metadata column as Python's ORM does:
// the JSON text's own key order; anything that is not a JSON object reads
// as {} (the "metadata_ or {}" the Python merges start from).
func decodeSourceMetadata(text string) *pyjson.Object {
	value, err := pyjson.DecodeString(text)
	if err != nil {
		return pyjson.NewObject()
	}
	if object, ok := value.(*pyjson.Object); ok {
		return object
	}
	return pyjson.NewObject()
}

// cloneMetadata is dict(metadata): the same keys in the same order.
func cloneMetadata(metadata *pyjson.Object) *pyjson.Object {
	clone := pyjson.NewObject()
	if metadata == nil {
		return clone
	}
	for _, key := range metadata.Keys() {
		value, _ := metadata.Get(key)
		clone.Set(key, value)
	}
	return clone
}

// withoutMetadataKey is the dict with key removed (dict.pop).
func withoutMetadataKey(metadata *pyjson.Object, key string) *pyjson.Object {
	out := pyjson.NewObject()
	for _, name := range metadata.Keys() {
		if name == key {
			continue
		}
		value, _ := metadata.Get(name)
		out.Set(name, value)
	}
	return out
}

// metadataString is metadata.get(key) when it is a str, else "".
func metadataString(metadata *pyjson.Object, key string) string {
	value, _ := metadata.Get(key)
	text, _ := value.(string)
	return text
}

// encodeSourceMetadata is the ORM's json.dumps of the metadata column.
func encodeSourceMetadata(metadata *pyjson.Object) (string, error) {
	if metadata == nil {
		metadata = pyjson.NewObject()
	}
	text, err := pyjson.Dumps(metadata)
	if err != nil {
		return "", fmt.Errorf("encode source metadata: %w", err)
	}
	return text, nil
}

func fetchExistingSourceRows(ctx context.Context, tx pgx.Tx, orgID, integrationID string) ([]*existingSourceRow, error) {
	rows, err := tx.Query(ctx, `
SELECT id::text, provider, external_id, is_enabled, discovered_at, metadata::text
FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid`, orgID, integrationID)
	if err != nil {
		return nil, fmt.Errorf("load existing integration sources: %w", err)
	}
	defer rows.Close()
	var result []*existingSourceRow
	for rows.Next() {
		row := &existingSourceRow{}
		var metadataText string
		if err := rows.Scan(&row.ID, &row.Provider, &row.ExternalID, &row.IsEnabled, &row.DiscoveredAt, &metadataText); err != nil {
			return nil, fmt.Errorf("scan existing integration source: %w", err)
		}
		row.Metadata = decodeSourceMetadata(metadataText)
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func disableSourceRow(ctx context.Context, tx pgx.Tx, id string, metadata *pyjson.Object) error {
	metadataJSON, err := encodeSourceMetadata(metadata)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE public.integration_sources SET is_enabled=FALSE, metadata=$2::json WHERE id=$1::uuid`, id, metadataJSON); err != nil {
		return fmt.Errorf("disable integration source %s: %w", id, err)
	}
	return nil
}

func enableSourceRow(ctx context.Context, tx pgx.Tx, id string, metadata *pyjson.Object) error {
	metadataJSON, err := encodeSourceMetadata(metadata)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE public.integration_sources SET is_enabled=TRUE, metadata=$2::json WHERE id=$1::uuid`, id, metadataJSON); err != nil {
		return fmt.Errorf("enable integration source %s: %w", id, err)
	}
	return nil
}

func updateExistingSourceRow(ctx context.Context, tx pgx.Tx, id, name, fullName string, metadata *pyjson.Object, now time.Time, reenable bool) error {
	metadataJSON, err := encodeSourceMetadata(metadata)
	if err != nil {
		return err
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
	metadata *pyjson.Object, now time.Time,
) (id string, discoveredAt time.Time, err error) {
	metadataJSON, err := encodeSourceMetadata(metadata)
	if err != nil {
		return "", time.Time{}, err
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
				if dup.ExternalID != survivor.ExternalID {
					// The losing case-variant may hold the project's sync
					// watermarks; move them onto the survivor's key.
					if err := migrateJiraWatermarksOnRename(ctx, tx, orgID, dup.ExternalID, survivor.ExternalID, now); err != nil {
						return 0, 0, nil, nil, 0, err
					}
				}
				metadata := cloneMetadata(dup.Metadata)
				metadata.Set(sourceDuplicateOfKey, survivor.ExternalID)
				if err := disableSourceRow(ctx, tx, dup.ID, metadata); err != nil {
					return 0, 0, nil, nil, 0, err
				}
				dup.IsEnabled = false
				dup.Metadata = metadata
			}
		}

		renamedTo := ""
		if survivor == nil {
			// No row carries the discovered key: before treating it as a new
			// project, look for the SAME project under an older key. Jira's
			// numeric project id survives a rename, so a row carrying it is
			// reused (its watermarks moved to the new key), never duplicated.
			if projectID := metadataString(source.Metadata, "jira_project_id"); projectID != "" {
				var renameCandidates []*existingSourceRow
				for _, row := range existingRows {
					if normalizeSourceKey(row.Provider) != "jira" {
						continue
					}
					if stored, ok := row.Metadata.Get("jira_project_id"); ok {
						if text, isString := stored.(string); isString && text == projectID {
							renameCandidates = append(renameCandidates, row)
						}
					}
				}
				sort.Slice(renameCandidates, func(i, j int) bool {
					if !renameCandidates[i].DiscoveredAt.Equal(renameCandidates[j].DiscoveredAt) {
						return renameCandidates[i].DiscoveredAt.Before(renameCandidates[j].DiscoveredAt)
					}
					return renameCandidates[i].ID < renameCandidates[j].ID
				})
				if len(renameCandidates) > 0 {
					renamed := renameCandidates[0]
					if normalizeSourceKey(renamed.ExternalID) != normalizedKey {
						if err := migrateJiraWatermarksOnRename(ctx, tx, orgID, renamed.ExternalID, source.ExternalID, now); err != nil {
							return 0, 0, nil, nil, 0, err
						}
						renamed.ExternalID = source.ExternalID
						renamedTo = source.ExternalID
					}
					survivor = renamed
				}
			}
		}

		metadata := source.Metadata
		if stampNewRows {
			metadata = cloneMetadata(metadata)
			metadata.Set("planner_managed_sync_config_id", configID)
		}

		if survivor != nil {
			merged := cloneMetadata(survivor.Metadata)
			for _, key := range metadata.Keys() {
				value, _ := metadata.Get(key)
				merged.Set(key, value)
			}
			reenable := false
			if _, ok := merged.Get(sourceSupersededMarkerKey); ok {
				// The project this row was superseded for is reconfirmed by
				// this discovery run -- undo a system-driven (never an
				// operator's own) rescope disable.
				merged = withoutMetadataKey(merged, sourceSupersededMarkerKey)
				reenable = true
			}
			if renamedTo != "" {
				if _, err := tx.Exec(ctx, `UPDATE public.integration_sources SET external_id=$2 WHERE id=$1::uuid`, survivor.ID, renamedTo); err != nil {
					return 0, 0, nil, nil, 0, fmt.Errorf("rename integration source %s: %w", survivor.ID, err)
				}
			}
			if err := updateExistingSourceRow(ctx, tx, survivor.ID, source.Name, source.FullName, merged, now, reenable); err != nil {
				return 0, 0, nil, nil, 0, err
			}
			survivor.Metadata = merged
			if reenable {
				survivor.IsEnabled = true
			}
			recordUpsertedSource(ctx, survivor.ID)
			existingCount++
			continue
		}

		newID, discoveredAt, insertErr := insertNewSourceRow(ctx, tx, orgID, integrationID, "jira", "project", source.ExternalID, source.Name, source.FullName, metadata, now)
		if errors.Is(insertErr, errSourceInsertConflict) {
			// A concurrent insert of this exact key won: the row is there.
			var winnerID string
			if err := tx.QueryRow(ctx, `SELECT id::text FROM public.integration_sources
WHERE org_id=$1 AND integration_id=$2::uuid AND provider=$3 AND external_id=$4`, orgID, integrationID, "jira", source.ExternalID).Scan(&winnerID); err != nil {
				return 0, 0, nil, nil, 0, fmt.Errorf("read the source a concurrent discovery inserted: %w", err)
			}
			recordUpsertedSource(ctx, winnerID)
			existingCount++
			continue
		}
		if insertErr != nil {
			return 0, 0, nil, nil, 0, insertErr
		}
		recordUpsertedSource(ctx, newID)
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
	// No SQL-side provider filter (codex review round-5 P1, ARGUED): a
	// second attempt at this same normalization mismatch, this time
	// whitespace rather than case -- lower(provider)='jira' still would not
	// match ' JIRA ' (integration_sources.provider is unconstrained text,
	// no write-time trim). Rather than patch the SQL predicate a third
	// time, drop it entirely and filter with normalizeSourceKey in Go, the
	// SAME function every other comparison in this file already goes
	// through -- chris's ruling (CHAOS-4584 gate round 7): one normalization
	// implementation, no SQL counterpart left to fall out of sync with it.
	rows, err := tx.Query(ctx, `
SELECT id::text, provider, external_id, metadata::text FROM public.integration_sources
WHERE org_id=$1 AND integration_id=$2::uuid AND is_enabled`, orgID, integrationID)
	if err != nil {
		return 0, fmt.Errorf("load enabled sources for supersede: %w", err)
	}
	type candidateRow struct {
		id         string
		externalID string
		metadata   *pyjson.Object
	}
	var superseded []candidateRow
	for rows.Next() {
		var row candidateRow
		var provider string
		var metadataText string
		if err := rows.Scan(&row.id, &provider, &row.externalID, &metadataText); err != nil {
			rows.Close()
			return 0, err
		}
		if normalizeSourceKey(provider) != "jira" {
			continue
		}
		row.metadata = decodeSourceMetadata(metadataText)
		tag := metadataString(row.metadata, "planner_managed_sync_config_id")
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
		metadata.Set(sourceSupersededMarkerKey, true)
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

type repoLimitCandidateRow struct {
	id         string
	externalID string
	metadata   *pyjson.Object
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
//
// Returns the capped/recovered counts rather than recording telemetry or
// logging itself (codex review round-3 P3): this function runs BEFORE the
// caller's commit, so a log line or counter emitted here would report a
// successful outcome even if the shared transaction is later rolled back
// or fails to commit. Mirrors materializer.go's RecordMaterialized, which
// documents the identical rule for plan-telemetry: "anything incremented
// at this point would fire before the caller's own commit makes this ...
// durable." The caller (Discover) records both only after its own commit
// succeeds.
func (service *NativeSourceDiscoveryService) rebalanceJiraSourceRepoLimit(
	ctx context.Context, tx pgx.Tx, orgID, integrationID string, createdLower, discoveredLower map[string]struct{},
) (capped, recovered int, err error) {
	var plannerConfigActive *bool
	err = tx.QueryRow(ctx, `
SELECT is_active FROM public.sync_configurations
WHERE integration_id=$1::uuid AND org_id=$2 AND planner_managed AND parent_id IS NULL
LIMIT 1`, integrationID, orgID).Scan(&plannerConfigActive)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, fmt.Errorf("load planner config active state: %w", err)
	}
	if plannerConfigActive != nil && !*plannerConfigActive {
		// A paused integration's own usage would read as zero (active-only
		// count), and recovery could wrongly re-enable every capped source
		// for it -- skip cap/recovery entirely while paused (CHAOS-4584
		// round 2 P1). The PATCH reactivation handler re-runs discovery.
		return 0, 0, nil
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, synclimits.AdvisoryLockKey(orgID)); err != nil {
		return 0, 0, fmt.Errorf("acquire repo-limit lock: %w", err)
	}

	maxRepos, err := resolveMaxReposLimit(ctx, tx, orgID)
	if err != nil {
		return 0, 0, err
	}

	if maxRepos != nil {
		usage, err := synclimits.ActiveRepoUsageCount(ctx, tx, orgID)
		if err != nil {
			return 0, 0, err
		}
		overflow := usage - *maxRepos
		if overflow > 0 {
			// No SQL-side provider filter (codex review rounds 3 and 5,
			// P1 both times: case, then whitespace): provider is
			// unconstrained text, not write-time validated anywhere in
			// this codebase, and no SQL predicate stayed in sync with
			// normalizeSourceKey through two attempts. Filter in Go
			// instead, the SAME function every other comparison in this
			// file already goes through. activeRepoUsageCountForLimit
			// below counts EVERY enabled source on this integration with
			// no provider filter at all, so a mixed-case/whitespace row
			// already counts toward usage; this SELECT must find it too,
			// or it becomes permanently invisible to capping -- over
			// max_repos with no candidate left to cap.
			rows, err := tx.Query(ctx, `
SELECT id::text, provider, external_id, metadata::text FROM public.integration_sources
WHERE org_id=$1 AND integration_id=$2::uuid AND is_enabled
ORDER BY external_id DESC`, orgID, integrationID)
			if err != nil {
				return 0, 0, fmt.Errorf("load enabled sources for capping: %w", err)
			}
			var createdRows, preExistingRows []repoLimitCandidateRow
			for rows.Next() {
				var row repoLimitCandidateRow
				var provider string
				var metadataText string
				if err := rows.Scan(&row.id, &provider, &row.externalID, &metadataText); err != nil {
					rows.Close()
					return 0, 0, err
				}
				if normalizeSourceKey(provider) != "jira" {
					continue
				}
				row.metadata = decodeSourceMetadata(metadataText)
				if _, ok := createdLower[normalizeSourceKey(row.externalID)]; ok {
					createdRows = append(createdRows, row)
				} else {
					preExistingRows = append(preExistingRows, row)
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return 0, 0, err
			}
			rows.Close()
			cappedRows := append(createdRows, preExistingRows...)
			if len(cappedRows) > overflow {
				cappedRows = cappedRows[:overflow]
			}
			for _, row := range cappedRows {
				metadata := cloneMetadata(row.metadata)
				metadata.Set(sourceCapMarkerKey, true)
				if err := disableSourceRow(ctx, tx, row.id, metadata); err != nil {
					return 0, 0, err
				}
			}
			return len(cappedRows), 0, nil
		}
	}

	// No SQL-side provider filter, same reasoning as the capping query above.
	rows, err := tx.Query(ctx, `
SELECT id::text, provider, external_id, metadata::text FROM public.integration_sources
WHERE org_id=$1 AND integration_id=$2::uuid AND NOT is_enabled
ORDER BY external_id ASC`, orgID, integrationID)
	if err != nil {
		return 0, 0, fmt.Errorf("load disabled sources for recovery: %w", err)
	}
	var recoverable []repoLimitCandidateRow
	for rows.Next() {
		var row repoLimitCandidateRow
		var provider string
		var metadataText string
		if err := rows.Scan(&row.id, &provider, &row.externalID, &metadataText); err != nil {
			rows.Close()
			return 0, 0, err
		}
		if normalizeSourceKey(provider) != "jira" {
			continue
		}
		row.metadata = decodeSourceMetadata(metadataText)
		cappedValue, _ := row.metadata.Get(sourceCapMarkerKey)
		capped, _ := cappedValue.(bool)
		_, discoveredNow := discoveredLower[normalizeSourceKey(row.externalID)]
		if capped && discoveredNow {
			recoverable = append(recoverable, row)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, 0, err
	}
	rows.Close()
	if len(recoverable) == 0 {
		return 0, 0, nil
	}
	if maxRepos != nil {
		usage, err := synclimits.ActiveRepoUsageCount(ctx, tx, orgID)
		if err != nil {
			return 0, 0, err
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
		metadata := withoutMetadataKey(row.metadata, sourceCapMarkerKey)
		if err := enableSourceRow(ctx, tx, row.id, metadata); err != nil {
			return 0, 0, err
		}
	}
	return 0, len(recoverable), nil
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

// migrateJiraWatermarksOnRename ports
// discovery.py::_migrate_jira_watermarks_on_rename: every sync watermark of
// the org under the old source key moves to the new key. When the new key
// already has a watermark for the same dataset, the later last_synced_at is
// kept on that row and the old row is deleted, so no cursor goes backwards
// and uq_sync_watermark_org_source_dataset holds. updated_at is stamped on
// every row written, as the model's onupdate does.
func migrateJiraWatermarksOnRename(ctx context.Context, tx pgx.Tx, orgID, oldExternalID, newExternalID string, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT id::text, dataset_key, last_synced_at FROM public.sync_watermarks WHERE org_id=$1 AND source_id=$2`,
		orgID, oldExternalID)
	if err != nil {
		return fmt.Errorf("load watermarks to migrate: %w", err)
	}
	type watermark struct {
		id, datasetKey string
		lastSyncedAt   *time.Time
	}
	var old []watermark
	for rows.Next() {
		var row watermark
		if err := rows.Scan(&row.id, &row.datasetKey, &row.lastSyncedAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan watermark to migrate: %w", err)
		}
		old = append(old, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load watermarks to migrate: %w", err)
	}
	for _, row := range old {
		var conflictID string
		var conflictAt *time.Time
		err := tx.QueryRow(ctx, `SELECT id::text, last_synced_at FROM public.sync_watermarks WHERE org_id=$1 AND source_id=$2 AND dataset_key=$3`,
			orgID, newExternalID, row.datasetKey).Scan(&conflictID, &conflictAt)
		if errors.Is(err, pgx.ErrNoRows) {
			if _, err := tx.Exec(ctx, `UPDATE public.sync_watermarks SET source_id=$2, updated_at=$3 WHERE id=$1::uuid`, row.id, newExternalID, now); err != nil {
				return fmt.Errorf("move watermark %s: %w", row.id, err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("load conflicting watermark: %w", err)
		}
		if row.lastSyncedAt != nil && (conflictAt == nil || row.lastSyncedAt.After(*conflictAt)) {
			if _, err := tx.Exec(ctx, `UPDATE public.sync_watermarks SET last_synced_at=$2, updated_at=$3 WHERE id=$1::uuid`, conflictID, *row.lastSyncedAt, now); err != nil {
				return fmt.Errorf("advance watermark %s: %w", conflictID, err)
			}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM public.sync_watermarks WHERE id=$1::uuid`, row.id); err != nil {
			return fmt.Errorf("delete superseded watermark %s: %w", row.id, err)
		}
	}
	return nil
}
