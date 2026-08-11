package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/providerunit"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	valkeygo "github.com/valkey-io/valkey-go"
)

const (
	// providerUnitQueue and its worker budget must match the deployment manifest
	// entry for the sync process; exact startup validation compares the two.
	providerUnitQueue         = "sync_provider"
	providerUnitQueueWorkers  = 2
	providerUnitLeaseDuration = 2 * time.Minute
	providerUnitHeartbeat     = 30 * time.Second
	providerUnitBudgetTTL     = 15 * time.Minute
)

type providerSyncWorkerComponent struct {
	client     *river.Client[pgx.Tx]
	clickhouse driver.Conn
	valkey     valkeygo.Client
}

func (component *providerSyncWorkerComponent) Name() string {
	return "river-provider-sync-worker"
}

func (component *providerSyncWorkerComponent) Start(ctx context.Context) error {
	if component == nil || component.client == nil {
		return errWorkerDependencyUnavailable
	}
	return component.client.Start(ctx)
}

func (component *providerSyncWorkerComponent) Shutdown(ctx context.Context) error {
	if component == nil {
		return nil
	}
	var result error
	if component.client != nil {
		result = component.client.Stop(ctx)
	}
	if component.valkey != nil {
		component.valkey.Close()
	}
	if component.clickhouse != nil {
		if err := component.clickhouse.Close(); result == nil {
			result = err
		}
	}
	return result
}

// budgetWaitObserver bridges providerfoundation's credential-free
// BudgetWaitObserver to the process's shared MetricsCollector. It is a value
// type specifically so a nil collector never becomes a non-nil, panicking
// interface value: ObserveProviderBudgetWait is a no-op until a real
// collector is attached.
type budgetWaitObserver struct {
	collector *jobruntime.MetricsCollector
}

func (o budgetWaitObserver) ObserveProviderBudgetWait(provider, costClass string, wait time.Duration) error {
	if o.collector == nil {
		return nil
	}
	return o.collector.ObserveProviderBudgetWait(jobruntime.BudgetLabels{Provider: provider, CostClass: costClass}, wait)
}

var _ providerfoundation.BudgetWaitObserver = budgetWaitObserver{}

// leaseRecoveryObserver bridges providerunit's expired-lease recovery
// signal to the shared MetricsCollector, for the same nil-safety reason as
// budgetWaitObserver above.
type leaseRecoveryObserver struct {
	collector *jobruntime.MetricsCollector
}

func (o leaseRecoveryObserver) ObserveSyncLeaseExpired(labels jobruntime.SyncLeaseLabels, result jobruntime.SyncLeaseResult) error {
	if o.collector == nil {
		return nil
	}
	return o.collector.ObserveSyncLeaseExpired(labels, result)
}

var _ jobruntime.SyncLeaseObserver = leaseRecoveryObserver{}

// providerSyncRepository is the exact capability buildProviderSyncHandler
// needs from its repository: everything providerunit.Handler requires (the
// lease/claim lifecycle) plus the effect ledger EffectCommitter requires.
// *providersync.PostgresRepository implements both in production.
type providerSyncRepository interface {
	providerunit.UnitRepository
	providersync.EffectLedger
}

type providerSyncWorkerConstructor func(
	context.Context,
	config.Config,
	workerDatabase,
	*jobruntime.Registry,
	jobruntime.Observer,
	*slog.Logger,
) (workerFamily, error)

var constructProviderSyncWorker providerSyncWorkerConstructor = constructProviderSyncWorkerWithDependencies

func pagerDutyEffectsFactory(
	dataset string,
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
) providersync.CompleteRouteEffectsFactory {
	return func(
		credential providerfoundation.Credential,
	) (providersync.EffectSink, providersync.EffectReadback, error) {
		providerInstance, err := providersync.PagerDutyProviderInstance(credential)
		if err != nil {
			return nil, nil, err
		}
		var effects interface {
			providersync.EffectSink
			providersync.EffectReadback
		}
		switch dataset {
		case "services":
			effects = providersync.PagerDutyServicesClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: providerInstance,
			}
		case "business-services":
			effects = providersync.PagerDutyBusinessServicesClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: providerInstance,
			}
		case "escalation-policies":
			effects = providersync.PagerDutyEscalationPoliciesClickHouseEffects{
				Conn: conn, Lease: lease,
			}
		case "schedules":
			effects = providersync.PagerDutySchedulesClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: providerInstance,
			}
		case "on-calls":
			effects = providersync.PagerDutyOnCallsClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: providerInstance,
			}
		case "users":
			effects = providersync.PagerDutyUsersClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: providerInstance,
			}
		case "teams":
			effects = providersync.PagerDutyTeamsClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: providerInstance,
			}
		case "incidents", "incident-alerts", "incident-log-entries", "incident-notes":
			effects = providersync.PagerDutyIncidentFamilyClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: providerInstance,
			}
		default:
			return nil, nil, providersync.ErrInvalidConfiguration
		}
		return effects, effects, nil
	}
}

// buildProviderSyncHandler constructs the provider-unit handler and the one
// providerfoundation.Metrics instance its executor is wired to reference.
//
// It performs no I/O of its own — every connection/client/pool parameter is
// only stored as a closure capture here, never dialed or dereferenced — and
// is extracted from buildProviderSyncWorker specifically so a test can pin
// the identity between the Metrics instance BuildExecutor hands each claim's
// executor and the instance the caller registers as workerFamily.metricsSource,
// without needing a live ClickHouse/Valkey/Postgres connection. See
// provider_sync_test.go's TestBuildProviderSyncHandlerSharesOneMetricsInstance:
// this is a mutation-tested seam (CHAOS-3118) — reintroducing a second
// providerfoundation.NewMetrics() call inside BuildExecutor, the exact defect
// this ticket exists to eliminate, must fail that test.
func buildProviderSyncHandler(
	repository providerSyncRepository,
	switches providersync.CompleteRouteSwitches,
	decryptor providerfoundation.CredentialDecryptor,
	clickhouseConnection driver.Conn,
	valkeyClient valkeygo.Client,
	domainPool *pgxpool.Pool,
	jiraIncidentEntitlement providersync.JiraIncidentEntitlement,
	collector *jobruntime.MetricsCollector,
	logger *slog.Logger,
) (*providerunit.Handler, *providerfoundation.Metrics) {
	return buildProviderSyncHandlerWithWorkItemsRuntimeConfig(
		repository, switches, decryptor, clickhouseConnection, valkeyClient,
		domainPool, jiraIncidentEntitlement, collector, logger,
		workItemsRuntimeConfig{},
	)
}

// buildProviderSyncHandlerWithGitHubWorkItemsRuntimeConfig remains a narrow
// compatibility seam for existing focused tests. Production construction uses
// the provider-neutral work-item runtime below.
func buildProviderSyncHandlerWithGitHubWorkItemsRuntimeConfig(
	repository providerSyncRepository,
	switches providersync.CompleteRouteSwitches,
	decryptor providerfoundation.CredentialDecryptor,
	clickhouseConnection driver.Conn,
	valkeyClient valkeygo.Client,
	domainPool *pgxpool.Pool,
	jiraIncidentEntitlement providersync.JiraIncidentEntitlement,
	collector *jobruntime.MetricsCollector,
	logger *slog.Logger,
	githubWorkItemsRuntime githubWorkItemsRuntimeConfig,
) (*providerunit.Handler, *providerfoundation.Metrics) {
	return buildProviderSyncHandlerWithWorkItemsRuntimeConfig(
		repository, switches, decryptor, clickhouseConnection, valkeyClient,
		domainPool, jiraIncidentEntitlement, collector, logger,
		githubWorkItemsRuntime,
	)
}

func buildProviderSyncHandlerWithWorkItemsRuntimeConfig(
	repository providerSyncRepository,
	switches providersync.CompleteRouteSwitches,
	decryptor providerfoundation.CredentialDecryptor,
	clickhouseConnection driver.Conn,
	valkeyClient valkeygo.Client,
	domainPool *pgxpool.Pool,
	jiraIncidentEntitlement providersync.JiraIncidentEntitlement,
	collector *jobruntime.MetricsCollector,
	logger *slog.Logger,
	workItemsRuntime workItemsRuntimeConfig,
) (*providerunit.Handler, *providerfoundation.Metrics) {
	// providerMetrics is constructed exactly once per worker process and
	// referenced by every claim's executor, so dev_health_provider_* actually
	// accumulates across dispatches instead of being built and discarded per
	// unit. The caller registers this same pointer with the health.Registry
	// via workerFamily.metricsSource, which is what makes it a live, scraped
	// series rather than a constructed-but-never-registered family.
	providerMetrics := providerfoundation.NewMetrics()
	handler := &providerunit.Handler{
		Repository: repository,
		// Derived from process configuration, not hardcoded. A hardcoded
		// switch set makes the handler serve a route the readiness check says
		// is off (or refuse one it says is on); both sides now read
		// workerRouteSwitches (CHAOS-3123).
		Switches:      switches,
		LeaseDuration: providerUnitLeaseDuration,
		Heartbeat:     providerUnitHeartbeat,
		// LeaseMetrics observes worker_sync_lease_expired_total. Only claims
		// this handler itself resolved after Repository.Claim reported
		// claim.Recovered (an expired lease was recovered into this attempt)
		// are ever observed; see providerunit.Handler.observeLeaseRecovery.
		LeaseMetrics: leaseRecoveryObserver{collector: collector},
		// A route fault means the Python producer gate routed a scope the Go
		// capability system does not serve. The unit is never terminalized
		// (TRD non-negotiable #3), so this log is the operator's only signal
		// that a producer gate needs reconciliation. Fields are bounded: no
		// credentials, URLs, or provider payloads.
		OnRouteFault: func(fault providerunit.RouteFault) {
			logger.Error(
				"provider unit route reconciliation required",
				"provider", fault.Provider,
				"dataset", fault.Dataset,
				"descriptor_present", fault.DescriptorPresent,
				"route_ready", fault.RouteReady,
				"route_enabled", fault.RouteEnabled,
				"attempt", fault.Attempt,
				"max_attempts", fault.MaxAttempts,
				"released_for_retry", fault.Released,
				"terminal_reconciliation_required", fault.Terminal,
			)
		},
		BuildExecutor: func(
			session *providersync.LeaseSession,
		) (providersync.CompleteRouteExecutor, error) {
			if session == nil {
				return providersync.CompleteRouteExecutor{},
					errWorkerDependencyUnavailable
			}
			// Twenty-one canonical route-ready claims can reach this closure today:
			// launchdarkly/feature-flags plus github/repo-metadata, cicd,
			// tests, commits, deployments, security, files, commit-stats, and
			// blame, work-items, plus jira/incidents and gitlab/repo-metadata,
			// commits, commit-stats, cicd, and tests. The four direct GitHub
			// work-item aliases are matrix-ready identities but are rejected by
			// providerunit before BuildExecutor, so they never become partial
			// writers. The GitHub cicd/tests aliases and the GitLab cicd/tests
			// aliases intentionally share one complete handler and effect sink;
			// every other route has its own.
			// session.Claim
			// is already known here — providerunit.Handler.Work only calls
			// BuildExecutor after its own descriptor gate passed for THIS
			// claim's provider/dataset — so select by claim rather than
			// hardcoding one pair. A hardcoded Handler was exactly the
			// CHAOS-3123 gap: it would compile, satisfy every other test, and
			// still fail every github/repo-metadata claim
			// (LaunchDarklyRouteHandler.Collect fails closed on
			// claim.Provider != "launchdarkly") the moment the switch was
			// flipped on.
			var (
				routeHandler   providersync.CompleteRouteHandler
				sink           providersync.EffectSink
				readback       providersync.EffectReadback
				effectsFactory providersync.CompleteRouteEffectsFactory
			)
			switch {
			case session.Claim.Provider == "launchdarkly" &&
				session.Claim.Dataset == "feature-flags":
				ldSink := providersync.LaunchDarklyClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.LaunchDarklyRouteHandler{
					CodeReferences: providersync.LaunchDarklyClickHouseReferences{
						Conn: clickhouseConnection, Lease: session,
					},
				}
				sink, readback = ldSink, ldSink
			case session.Claim.Provider == "github" &&
				session.Claim.Dataset == "repo-metadata":
				ghSink := providersync.GitHubRepositoryClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitHubRepositoryRouteHandler{}
				sink, readback = ghSink, ghSink
			case session.Claim.Provider == "github" &&
				session.Claim.Dataset == "work-items":
				if !workItemsRuntime.configured() {
					return providersync.CompleteRouteExecutor{}, providersync.ErrInvalidConfiguration
				}
				ghSink, err := providersync.NewGitHubWorkItemClickHouseEffects(
					clickhouseConnection, session,
				)
				if err != nil {
					return providersync.CompleteRouteExecutor{}, err
				}
				ghDeriver, err := providersync.NewGitHubWorkItemDeriver(
					clickhouseConnection, session,
					workItemsRuntime.statusMappingPath,
					workItemsRuntime.investmentConfigPath,
				)
				if err != nil {
					return providersync.CompleteRouteExecutor{}, err
				}
				routeHandler = providersync.GitHubWorkItemsRouteHandler{
					Projects: providersync.GitHubProjectV2Fetcher{},
					Deriver:  ghDeriver,
				}
				sink, readback = ghSink, ghSink
			case session.Claim.Provider == "gitlab" &&
				session.Claim.Dataset == "work-items":
				if !workItemsRuntime.configured() {
					return providersync.CompleteRouteExecutor{}, providersync.ErrInvalidConfiguration
				}
				glSink, err := providersync.NewGitLabWorkItemFamilyClickHouseEffects(
					clickhouseConnection, session,
				)
				if err != nil {
					return providersync.CompleteRouteExecutor{}, err
				}
				glDeriver, err := providersync.NewGitLabWorkItemDeriver(
					clickhouseConnection, session,
					workItemsRuntime.statusMappingPath,
					workItemsRuntime.investmentConfigPath,
				)
				if err != nil {
					return providersync.CompleteRouteExecutor{}, err
				}
				routeHandler = providersync.GitLabWorkItemsRouteHandler{
					StatusMapping: workItemsRuntime.statusMapping,
					Derived:       glDeriver,
				}
				sink, readback = glSink, glSink
			case session.Claim.Provider == "jira" &&
				session.Claim.Dataset == "work-items":
				if !workItemsRuntime.configured() {
					return providersync.CompleteRouteExecutor{}, providersync.ErrInvalidConfiguration
				}
				jiraSink, err := providersync.NewJiraWorkItemCompositeClickHouseEffects(
					clickhouseConnection, session,
				)
				if err != nil {
					return providersync.CompleteRouteExecutor{}, err
				}
				jiraDeriver, err := providersync.NewJiraWorkItemDeriver(
					clickhouseConnection, session,
					workItemsRuntime.statusMappingPath,
					workItemsRuntime.investmentConfigPath,
				)
				if err != nil {
					return providersync.CompleteRouteExecutor{}, err
				}
				routeHandler = providersync.JiraAtlassianRouteHandler{
					StatusMapping: workItemsRuntime.statusMapping,
					Derived:       jiraDeriver,
				}
				sink, readback = jiraSink, jiraSink
			case session.Claim.Provider == "linear" &&
				session.Claim.Dataset == "work-items":
				if !workItemsRuntime.configured() {
					return providersync.CompleteRouteExecutor{}, providersync.ErrInvalidConfiguration
				}
				linearSink, err := providersync.NewLinearWorkItemFamilyClickHouseEffects(
					clickhouseConnection, session,
				)
				if err != nil {
					return providersync.CompleteRouteExecutor{}, err
				}
				linearDeriver, err := providersync.NewLinearWorkItemDeriver(
					clickhouseConnection, session,
					workItemsRuntime.statusMappingPath,
					workItemsRuntime.investmentConfigPath,
				)
				if err != nil {
					return providersync.CompleteRouteExecutor{}, err
				}
				routeHandler = providersync.LinearWorkItemFamilyRouteHandler{
					Direct:  providersync.LinearWorkItemsRouteHandler{},
					Derived: linearDeriver,
				}
				sink, readback = linearSink, linearSink
			case session.Claim.Provider == "gitlab" &&
				session.Claim.Dataset == "repo-metadata":
				glSink := providersync.GitLabRepositoryClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitLabRepositoryRouteHandler{}
				sink, readback = glSink, glSink
			case session.Claim.Provider == "gitlab" &&
				session.Claim.Dataset == "commits":
				glCommitsSink := providersync.GitLabCommitsClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitLabCommitsRouteHandler{}
				sink, readback = glCommitsSink, glCommitsSink
			case session.Claim.Provider == "gitlab" &&
				session.Claim.Dataset == "commit-stats":
				glCommitStatsSink := providersync.GitLabCommitStatsClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitLabCommitStatsRouteHandler{}
				sink, readback = glCommitStatsSink, glCommitStatsSink
			case session.Claim.Provider == "gitlab" &&
				(session.Claim.Dataset == "cicd" || session.Claim.Dataset == "tests"):
				glTestsSink := providersync.TestOpsClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitLabTestsRouteHandler{}
				sink, readback = glTestsSink, glTestsSink
			case session.Claim.Provider == "gitlab" &&
				session.Claim.Dataset == "incidents":
				glIncidentsSink := providersync.GitLabIncidentsClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitLabIncidentsRouteHandler{}
				sink, readback = glIncidentsSink, glIncidentsSink
			case session.Claim.Provider == "gitlab" &&
				session.Claim.Dataset == "deployments":
				glDeploymentsSink := providersync.GitLabDeploymentsClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitLabDeploymentsRouteHandler{}
				sink, readback = glDeploymentsSink, glDeploymentsSink
			case session.Claim.Provider == "gitlab" &&
				session.Claim.Dataset == "feature-flags":
				glFeatureFlagsSink := providersync.GitLabFeatureFlagsClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitLabFeatureFlagsRouteHandler{}
				sink, readback = glFeatureFlagsSink, glFeatureFlagsSink
			case session.Claim.Provider == "gitlab" &&
				session.Claim.Dataset == "files":
				glFilesSink := providersync.GitLabFilesClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitLabFilesRouteHandler{}
				sink, readback = glFilesSink, glFilesSink
			case session.Claim.Provider == "gitlab" &&
				session.Claim.Dataset == "blame":
				glBlameSink := providersync.GitLabBlameClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitLabBlameRouteHandler{
					Coverage: providersync.GitLabBlameClickHouseCoverage{
						Conn: clickhouseConnection, Lease: session,
					},
				}
				sink, readback = glBlameSink, glBlameSink
			case session.Claim.Provider == "gitlab" &&
				(session.Claim.Dataset == "prs" ||
					session.Claim.Dataset == "pr-reviews" ||
					session.Claim.Dataset == "pr-comments"):
				glPRSink := providersync.GitLabPullRequestSocialClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitLabPullRequestRouteHandler{}
				sink, readback = glPRSink, glPRSink
			case session.Claim.Provider == "gitlab" &&
				session.Claim.Dataset == "security":
				glSecuritySink := providersync.GitLabSecurityClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitLabSecurityRouteHandler{}
				sink, readback = glSecuritySink, glSecuritySink
			case session.Claim.Provider == "github" &&
				(session.Claim.Dataset == "prs" ||
					session.Claim.Dataset == "pr-reviews" ||
					session.Claim.Dataset == "pr-comments"):
				ghPRSink := providersync.GitHubPullRequestSocialClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitHubPullRequestSocialRouteHandler{}
				sink, readback = ghPRSink, ghPRSink
			case session.Claim.Provider == "github" &&
				session.Claim.Dataset == "cicd":
				ghCICDSink := providersync.GitHubTestsClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitHubTestsRouteHandler{}
				sink, readback = ghCICDSink, ghCICDSink
			case session.Claim.Provider == "github" &&
				session.Claim.Dataset == "commits":
				ghCommitsSink := providersync.GitHubCommitsClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitHubCommitsRouteHandler{}
				sink, readback = ghCommitsSink, ghCommitsSink
			case session.Claim.Provider == "github" &&
				session.Claim.Dataset == "deployments":
				ghDeploymentsSink := providersync.GitHubDeploymentsClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitHubDeploymentsRouteHandler{}
				sink, readback = ghDeploymentsSink, ghDeploymentsSink
			case session.Claim.Provider == "github" &&
				session.Claim.Dataset == "security":
				ghSecuritySink := providersync.GitHubSecurityClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitHubSecurityRouteHandler{}
				sink, readback = ghSecuritySink, ghSecuritySink
			case session.Claim.Provider == "github" &&
				session.Claim.Dataset == "files":
				ghFilesSink := providersync.GitHubFilesClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitHubFilesRouteHandler{}
				sink, readback = ghFilesSink, ghFilesSink
			case session.Claim.Provider == "github" &&
				session.Claim.Dataset == "commit-stats":
				ghCommitStatsSink := providersync.GitHubCommitStatsClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitHubCommitStatsRouteHandler{}
				sink, readback = ghCommitStatsSink, ghCommitStatsSink
			case session.Claim.Provider == "jira" &&
				session.Claim.Dataset == "incidents":
				if jiraIncidentEntitlement == nil {
					return providersync.CompleteRouteExecutor{},
						errWorkerDependencyUnavailable
				}
				jiraSink := providersync.JiraIncidentClickHouseEffects{
					Writer: clickhouseConnection, Lease: session,
					Entitlement: jiraIncidentEntitlement,
				}
				jiraReadback := providersync.JiraIncidentClickHouseReadback{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.JiraIncidentRouteHandler{
					Entitlement: jiraIncidentEntitlement,
				}
				sink, readback = jiraSink, jiraReadback
			case session.Claim.Provider == "github" &&
				session.Claim.Dataset == "blame":
				ghBlameSink := providersync.GitHubBlameClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitHubBlameRouteHandler{
					Coverage: providersync.GitHubBlameClickHouseCoverage{
						Conn: clickhouseConnection, Lease: session,
					},
				}
				sink, readback = ghBlameSink, ghBlameSink
			case session.Claim.Provider == "github" &&
				session.Claim.Dataset == "tests":
				ghTestsSink := providersync.GitHubTestsClickHouseEffects{
					Conn: clickhouseConnection, Lease: session,
				}
				routeHandler = providersync.GitHubTestsRouteHandler{}
				sink, readback = ghTestsSink, ghTestsSink
			case session.Claim.Provider == "pagerduty":
				switch session.Claim.Dataset {
				case "services":
					routeHandler = providersync.PagerDutyServicesRouteHandler{}
				case "business-services":
					routeHandler = providersync.PagerDutyBusinessServicesRouteHandler{}
				case "escalation-policies":
					routeHandler = providersync.PagerDutyEscalationPoliciesRouteHandler{}
				case "schedules":
					routeHandler = providersync.PagerDutySchedulesRouteHandler{}
				case "on-calls":
					routeHandler = providersync.PagerDutyOnCallsRouteHandler{}
				case "users":
					routeHandler = providersync.PagerDutyUsersRouteHandler{}
				case "teams":
					routeHandler = providersync.PagerDutyTeamsRouteHandler{}
				case "incidents", "incident-alerts", "incident-log-entries", "incident-notes":
					routeHandler = providersync.PagerDutyIncidentFamilyRouteHandler{}
				default:
					return providersync.CompleteRouteExecutor{}, errWorkerDependencyUnavailable
				}
				effectsFactory = pagerDutyEffectsFactory(
					session.Claim.Dataset, clickhouseConnection, session,
				)
			default:
				// Unreachable in production: providerunit.Handler.Work only
				// invokes BuildExecutor for a claim whose descriptor already
				// reported RouteReady && RouteEnabled, and the cases
				// above are the only pairs CompleteRouteSwitches.Descriptor
				// ever marks RouteReady. Fail closed rather than construct an
				// executor with a nil Handler.
				return providersync.CompleteRouteExecutor{},
					errWorkerDependencyUnavailable
			}
			return providersync.CompleteRouteExecutor{
				Credentials: providerfoundation.CredentialResolver{
					Repository: providerfoundation.PostgresCredentialRepository{
						Pool: domainPool,
					},
					Decryptor: decryptor,
				},
				Doer: &http.Client{
					Timeout: 45 * time.Second,
					CheckRedirect: func(*http.Request, []*http.Request) error {
						return http.ErrUseLastResponse
					},
				},
				Retry: providerfoundation.DefaultRetryPolicy(),
				Budget: providerfoundation.ValkeyBudgetStore{
					Client:   valkeyClient,
					Observer: budgetWaitObserver{collector: collector},
				},
				BudgetLimits: map[providersync.CostClass]int{
					providersync.CostLight:  4,
					providersync.CostMedium: 2,
					providersync.CostHeavy:  1,
				},
				BudgetTTL: providerUnitBudgetTTL,
				Gate: func(
					claim providersync.Claim,
					client *providerfoundation.HTTPClient,
				) providerfoundation.BackoffGate {
					if client == nil || client.BaseURL == nil {
						return nil
					}
					return providerfoundation.ValkeyBackoffGate{
						Client: valkeyClient, Provider: claim.Provider,
						OrgID: claim.OrgID, Host: client.BaseURL.Hostname(),
						MaxBackoff: 5 * time.Minute,
						CostClass:  string(claim.CostClass),
						Observer:   budgetWaitObserver{collector: collector},
					}
				},
				// This is the exact seam the mutation test targets: Metrics
				// must stay the shared providerMetrics closed over above, not
				// a fresh providerfoundation.NewMetrics() built per call.
				Metrics:    providerMetrics,
				Handler:    routeHandler,
				Comparator: providersync.ProductionContractComparator{},
				Committer: providersync.EffectCommitter{
					Ledger: repository, Sink: sink, Readback: readback,
				},
				EffectsFactory:    effectsFactory,
				HeartbeatInterval: providerUnitHeartbeat,
			}, nil
		},
	}
	return handler, providerMetrics
}

func buildProviderSyncWorker(
	ctx context.Context,
	cfg config.Config,
	database workerDatabase,
	registry *jobruntime.Registry,
	observer jobruntime.Observer,
	logger *slog.Logger,
) (workerFamily, error) {
	// Construct the family when ANY route switch is on, not launchdarkly's
	// alone: (github, repo-metadata) became routable in CHAOS-3123,
	// (github, prs) in CHAOS-3122, and (gitlab, repo-metadata) in CHAOS-3342.
	// A process that dispatches either provider's units while refusing to build
	// the handler for them would strand every unit at a worker with nothing
	// registered.
	if cfg.Profile != "sync" || !providerSyncWorkerEnabled(cfg) {
		return workerFamily{}, nil
	}
	return constructProviderSyncWorker(ctx, cfg, database, registry, observer, logger)
}

func constructProviderSyncWorkerWithDependencies(
	ctx context.Context,
	cfg config.Config,
	database workerDatabase,
	registry *jobruntime.Registry,
	observer jobruntime.Observer,
	logger *slog.Logger,
) (workerFamily, error) {
	if registry == nil || observer == nil || logger == nil ||
		!cfg.SettingsEncryptionKey.Configured() {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	spec, ok := registry.Descriptor(jobcontract.KindSyncProviderUnit)
	// Executable() already restricts this to shadow, river_canary, and river.
	// Pinning the literal canary route here would make the handler refuse to
	// register the moment the kind was promoted past canary, which is the one
	// transition the pin was meant to protect.
	if !ok || !spec.Executable() || spec.RollbackRoute != "celery" {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	// Validate the exact config paths before opening any outbound dependency.
	// This value is then captured by BuildExecutor, so startup/readiness and
	// claim construction cannot drift onto different StatusMapping/classifier
	// artifacts (D19).
	workItemsRuntime, err := workItemsRuntimeConfigFrom(cfg)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	repository, err := providersync.NewPostgresRepository(
		postgresDatabase.pools.Domain,
	)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(
		cfg.SettingsEncryptionKey, "",
	)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	clickhouseConnection, err := clickhousestore.Open(
		ctx, clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
	)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	valkeyClient, err := valkeystore.Open(
		ctx, valkeystore.DefaultConfig(cfg.ValkeyURI.Reveal()),
	)
	if err != nil {
		_ = clickhouseConnection.Close()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	closeDependencies := func() {
		valkeyClient.Close()
		_ = clickhouseConnection.Close()
	}
	// The observer passed in is always the process's one *jobruntime.MetricsCollector
	// in production; the assertion fails closed to a nil collector (both bridge
	// types below are then safe no-ops) for any other Observer implementation,
	// such as a test double.
	collector, _ := observer.(*jobruntime.MetricsCollector)
	handler, providerMetrics := buildProviderSyncHandlerWithWorkItemsRuntimeConfig(
		repository, workerRouteSwitches(cfg), decryptor, clickhouseConnection,
		valkeyClient, postgresDatabase.pools.Domain,
		providersync.PostgresJiraIncidentEntitlement{Pool: postgresDatabase.pools.Domain},
		collector, logger, workItemsRuntime,
	)
	adapter, err := jobruntime.NewAdapter[jobruntime.ProviderUnitArgs](
		registry, spec, handler, jobruntime.Dependencies{
			Logger: logger, Observer: observer,
			TenantScope: providerUnitTenantScope{},
			Budget:      newOperationalBudget(),
			Idempotency: providerunit.AuthoritativeIdempotency{},
		},
	)
	if err != nil {
		closeDependencies()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, adapter); err != nil {
		closeDependencies()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	client, err := river.NewClient(
		riverpgxv5.New(postgresDatabase.pools.QueueControl),
		providerSyncRiverConfig(
			logger, workers, cfg.RiverDatabaseSchema,
		),
	)
	if err != nil {
		closeDependencies()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	return workerFamily{
		component: &providerSyncWorkerComponent{
			client: client, clickhouse: clickhouseConnection, valkey: valkeyClient,
		},
		handlers: []jobruntime.HandlerSpec{adapter.Spec()},
		queues: []jobruntime.QueueBudget{
			{Queue: providerUnitQueue, MaxWorkers: providerUnitQueueWorkers},
		},
		metricsSource: providerMetrics,
	}, nil
}

func providerSyncWorkerEnabled(cfg config.Config) bool {
	return cfg.WorkerLaunchDarklyFeatureFlagsEnabled || cfg.WorkerGithubRepoMetadataEnabled ||
		cfg.WorkerGitlabRepoMetadataEnabled ||
		cfg.WorkerGitlabCommitsEnabled ||
		cfg.WorkerGitlabCommitStatsEnabled ||
		cfg.WorkerGitlabCICDEnabled ||
		cfg.WorkerGitlabTestsEnabled ||
		cfg.WorkerGitlabIncidentsEnabled ||
		cfg.WorkerGitlabDeploymentsEnabled ||
		cfg.WorkerGitlabFeatureFlagsEnabled ||
		cfg.WorkerGitlabFilesEnabled ||
		cfg.WorkerGitlabBlameEnabled ||
		cfg.WorkerGitlabPRsEnabled ||
		cfg.WorkerGitlabPRReviewsEnabled ||
		cfg.WorkerGitlabPRCommentsEnabled ||
		cfg.WorkerGitlabSecurityEnabled ||
		cfg.WorkerGitlabWorkItemsEnabled ||
		cfg.WorkerGithubPRsEnabled || cfg.WorkerGithubPRReviewsEnabled ||
		cfg.WorkerGithubPRCommentsEnabled || cfg.WorkerGithubCICDEnabled ||
		cfg.WorkerGithubCommitsEnabled || cfg.WorkerGithubDeploymentsEnabled ||
		cfg.WorkerGithubSecurityEnabled || cfg.WorkerGithubFilesEnabled ||
		cfg.WorkerGithubCommitStatsEnabled || cfg.WorkerJiraIncidentsEnabled ||
		cfg.WorkerJiraWorkItemsEnabled || cfg.WorkerLinearWorkItemsEnabled ||
		cfg.WorkerGithubBlameEnabled || cfg.WorkerGithubTestsEnabled ||
		cfg.WorkerGithubWorkItemsEnabled ||
		cfg.WorkerPagerDutyServicesEnabled ||
		cfg.WorkerPagerDutyBusinessServicesEnabled ||
		cfg.WorkerPagerDutyEscalationPoliciesEnabled ||
		cfg.WorkerPagerDutySchedulesEnabled ||
		cfg.WorkerPagerDutyOnCallsEnabled || cfg.WorkerPagerDutyUsersEnabled ||
		cfg.WorkerPagerDutyTeamsEnabled || cfg.WorkerPagerDutyIncidentsEnabled
}

func providerSyncRiverConfig(
	logger *slog.Logger,
	workers *river.Workers,
	schema string,
) *river.Config {
	return &river.Config{
		Logger: logger,
		Queues: map[string]river.QueueConfig{
			providerUnitQueue: {MaxWorkers: providerUnitQueueWorkers},
		},
		Schema:  schema,
		Workers: workers,
	}
}

type providerUnitTenantScope struct{}

func (providerUnitTenantScope) Supports(scope string) bool {
	return scope == "tenant"
}

func (providerUnitTenantScope) Resolve(
	ctx context.Context,
	request jobruntime.ScopeRequest,
) (context.Context, error) {
	if ctx == nil || request.OrganizationScope != "tenant" ||
		request.OrganizationID == nil ||
		request.Domain.Type != "sync_run_unit" {
		return nil, jobruntime.DomainMismatch(errWorkerDependencyUnavailable)
	}
	return ctx, nil
}
