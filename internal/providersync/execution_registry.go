package providersync

import (
	"strings"
)

// ExecutorKind names the fixed provider/dataset execution ownership required
// by CUT-09. It answers "which code would run this pair", never "may this pair
// route" — routing is decided only by RouteReady and Plannable. Capability
// metadata is not execution evidence (TRD §10.1).
type ExecutorKind string

const (
	// ExecutorNativeGo means a Go CompleteRouteHandler for the pair is
	// compiled into this module and owns fetch, normalization, and effects.
	ExecutorNativeGo ExecutorKind = "native_go"
	// ExecutorPythonCompatibility means the pair is served by the bounded
	// API-side compatibility endpoint described in TRD §10.2: Go keeps lease,
	// status, completion, and watermark ownership and sends only bounded
	// identifiers plus its claim context — never credentials, URLs, module
	// names, or provider payloads. No pair is registered under this kind yet
	// because that endpoint does not exist; a later lane owns it.
	ExecutorPythonCompatibility ExecutorKind = "python_compatibility"
	// ExecutorNone means no Go-owned executor exists for the pair.
	ExecutorNone ExecutorKind = "none"
)

// providerExecutorRegistry is the fixed provider/dataset executor registry.
// Absent keys resolve to ExecutorNone. Membership here is a statement about
// compiled code only: a pair may be native_go while RouteReady is still false
// because live parity evidence has not been captured.
var providerExecutorRegistry = map[string]ExecutorKind{
	"launchdarkly/feature-flags":     ExecutorNativeGo,
	"github/repo-metadata":           ExecutorNativeGo,
	"github/prs":                     ExecutorNativeGo,
	"github/pr-reviews":              ExecutorNativeGo,
	"github/pr-comments":             ExecutorNativeGo,
	"github/cicd":                    ExecutorNativeGo,
	"github/commits":                 ExecutorNativeGo,
	"github/deployments":             ExecutorNativeGo,
	"github/security":                ExecutorNativeGo,
	"github/files":                   ExecutorNativeGo,
	"github/commit-stats":            ExecutorNativeGo,
	"jira/incidents":                 ExecutorNativeGo,
	"gitlab/repo-metadata":           ExecutorNativeGo,
	"gitlab/commits":                 ExecutorNativeGo,
	"gitlab/commit-stats":            ExecutorNativeGo,
	"gitlab/cicd":                    ExecutorNativeGo,
	"gitlab/tests":                   ExecutorNativeGo,
	"gitlab/incidents":               ExecutorNativeGo,
	"gitlab/deployments":             ExecutorNativeGo,
	"gitlab/feature-flags":           ExecutorNativeGo,
	"gitlab/files":                   ExecutorNativeGo,
	"gitlab/blame":                   ExecutorNativeGo,
	"gitlab/prs":                     ExecutorNativeGo,
	"gitlab/pr-reviews":              ExecutorNativeGo,
	"gitlab/pr-comments":             ExecutorNativeGo,
	"gitlab/security":                ExecutorNativeGo,
	"gitlab/work-items":              ExecutorNativeGo,
	"gitlab/work-item-labels":        ExecutorNativeGo,
	"gitlab/work-item-projects":      ExecutorNativeGo,
	"gitlab/work-item-history":       ExecutorNativeGo,
	"gitlab/work-item-comments":      ExecutorNativeGo,
	"jira/work-items":                ExecutorNativeGo,
	"jira/work-item-labels":          ExecutorNativeGo,
	"jira/work-item-projects":        ExecutorNativeGo,
	"jira/work-item-history":         ExecutorNativeGo,
	"jira/work-item-comments":        ExecutorNativeGo,
	"linear/work-items":              ExecutorNativeGo,
	"linear/work-item-labels":        ExecutorNativeGo,
	"linear/work-item-projects":      ExecutorNativeGo,
	"linear/work-item-history":       ExecutorNativeGo,
	"linear/work-item-comments":      ExecutorNativeGo,
	"pagerduty/services":             ExecutorNativeGo,
	"pagerduty/business-services":    ExecutorNativeGo,
	"pagerduty/escalation-policies":  ExecutorNativeGo,
	"pagerduty/schedules":            ExecutorNativeGo,
	"pagerduty/on-calls":             ExecutorNativeGo,
	"pagerduty/users":                ExecutorNativeGo,
	"pagerduty/teams":                ExecutorNativeGo,
	"pagerduty/incidents":            ExecutorNativeGo,
	"pagerduty/incident-alerts":      ExecutorNativeGo,
	"pagerduty/incident-log-entries": ExecutorNativeGo,
	"pagerduty/incident-notes":       ExecutorNativeGo,
	"github/blame":                   ExecutorNativeGo,
	"github/tests":                   ExecutorNativeGo,
	// GitHub's five work-item dataset identities are one complete native
	// execution family. The planner emits only a canonical `work-items` claim;
	// the four sibling identities remain in the capability contract so their
	// per-alias watermark/audit completion stays visible.
	"github/work-items":         ExecutorNativeGo,
	"github/work-item-labels":   ExecutorNativeGo,
	"github/work-item-projects": ExecutorNativeGo,
	"github/work-item-history":  ExecutorNativeGo,
	"github/work-item-comments": ExecutorNativeGo,
}

// ProviderExecutor reports the fixed executor kind for a provider/dataset pair.
func ProviderExecutor(provider, dataset string) ExecutorKind {
	kind, ok := providerExecutorRegistry[matrixKey(provider, dataset)]
	if !ok {
		return ExecutorNone
	}
	return kind
}

func matrixKey(provider, dataset string) string {
	return strings.ToLower(strings.TrimSpace(provider)) + "/" +
		strings.ToLower(strings.TrimSpace(dataset))
}

// nativeShadowReady is deliberately narrower than the REST fixture collectors.
// Production maps every work-item* dataset unit to one full work-item job; the
// labels/projects/history/comments names are not independent sink semantics.
// Until a native handler emits that complete batch and has canary evidence,
// only repository metadata is eligible for auditable shadow execution.
func nativeShadowReady(provider, dataset string) bool {
	return (provider == "github" || provider == "gitlab") && dataset == "repo-metadata"
}

// CompleteRouteDescriptor is the single canonical provider capability
// descriptor. The legacy parallel ExecutionDescriptor/RouteSwitches registry
// was removed in CUT-08: every consumer — production routing, the shadow
// parity harness, and the frozen provider matrix contract — now reads this
// type so no future slice can be wired to a dead descriptor path.
type CompleteRouteDescriptor struct {
	Provider         string
	RequestedDataset string
	RouteDataset     string
	Destinations     []string
	// Executor is the fixed executor kind for the requested pair.
	Executor ExecutorKind
	// NativeShadow marks pairs whose native fetch output may be compared
	// against the Python-owned sink for parity evidence. Shadow eligibility
	// never implies routing.
	NativeShadow bool
	// PreparedManifestRecovery requires an exact Postgres sidecar snapshot
	// before the first sink effect. It is currently reserved for GitHub's
	// mutable, multi-source work-items composition and does not imply routing.
	PreparedManifestRecovery bool
	// Chunked opts a route into the additive durable checkpoint/sidecar path.
	// It is independent from PreparedManifestRecovery: the former persists a
	// sequence of bounded normalized chunks, while the latter persists one
	// complete mutable-provider manifest.
	Chunked     bool
	ChunkPolicy ChunkPolicy
	// RouteReady is the sole capability gate (CHAOS-4054). It is a code fact —
	// the route is shipped, reviewed, and registered — never a runtime read of
	// deployment configuration. There is no route enablement plane: user sync
	// config decides intent and -Q queue topology decides serving.
	RouteReady bool
	// Plannable reports whether this identity may be planned and claimed on its
	// own. Exactly one identity per writer family is plannable: the canonical
	// writer (`prs`, `cicd`, `work-items`). Alias identities stay RouteReady so
	// the capability matrix, audit, and per-alias watermarks keep telling the
	// truth, but they are never independently planned or executed — that is
	// where alias mutual-exclusivity lives now, as writer topology rather than
	// as a pair of booleans nobody may set at once.
	Plannable bool
}

// ShadowDescriptor is a fixture/parity-only projection of the canonical
// descriptor. It exists so the generation-block parity harness cannot become a
// second capability registry: it can only be derived from a descriptor the
// canonical system already recognises as shadow-eligible.
type ShadowDescriptor struct {
	Provider string
	Dataset  string
	// Write enables generation-block persistence inside the parity harness.
	// It is never derived from configuration and never set by a binary.
	Write bool
}

// Shadow projects a canonical descriptor onto the parity harness. It fails
// closed for any pair that is not shadow-eligible.
func (descriptor CompleteRouteDescriptor) Shadow(write bool) (ShadowDescriptor, bool) {
	if !descriptor.NativeShadow || descriptor.Provider == "" ||
		descriptor.RequestedDataset == "" {
		return ShadowDescriptor{}, false
	}
	return ShadowDescriptor{
		Provider: descriptor.Provider,
		Dataset:  descriptor.RequestedDataset,
		Write:    write,
	}, true
}

// Descriptor resolves the canonical capability descriptor for a claimed
// provider/dataset pair. Recognition is driven by the dataset capability
// registry, so the descriptor surface and the frozen matrix contract cover the
// same set by construction.
//
// It takes no configuration. Capability is always on in the binary
// (CHAOS-4054): a shipped, registered route is executable, and nothing about
// its functionality is hidden behind an environment switch.
//
// Descriptor collapses the alias identities onto their one canonical writer:
// Python's five work-item dataset aliases onto `work-items`, `pr-reviews` and
// `pr-comments` onto `prs`, and `tests` onto `cicd`. Alias identities remain
// visible for audit and watermark compatibility, but they can never be
// activated as partial routes.
func Descriptor(
	provider string,
	dataset string,
) (CompleteRouteDescriptor, bool) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	dataset = strings.ToLower(strings.TrimSpace(dataset))
	if _, known := Capability(provider, dataset); !known {
		return CompleteRouteDescriptor{}, false
	}
	descriptor := CompleteRouteDescriptor{
		Provider: provider, RequestedDataset: dataset, RouteDataset: dataset,
		Executor:     ProviderExecutor(provider, dataset),
		NativeShadow: nativeShadowReady(provider, dataset),
	}
	// The GitHub work-item implementation owns this family identity. Do not
	// derive route activation from Linear's expired-lease recovery oracle: both
	// happen to name the five planner aliases, but their evolution is separate.
	workItemAlias := isWorkItemFamilyDataset(dataset)
	switch {
	case provider == "github" && workItemAlias:
		// The five Python dataset identities describe one indivisible work-item
		// crawl. The capability matrix deliberately reports all five as native
		// and ready so producer/matrix drift cannot re-open one partial alias.
		// Only the canonical claim can execute, however: planner.py collapses a
		// family into dataset="work-items" plus per-alias processor flags. A
		// direct sibling alias is malformed persisted state and must trip the
		// providerunit route-reconciliation guard before construction/I/O.
		descriptor.Destinations = workItemRouteDestinations()
		descriptor.RouteReady = true
		if dataset == "work-items" {
			descriptor.Plannable = true
			// Mutable provider selection requires exact prepared-snapshot recovery
			// before the first sink effect. It is a canonical-claim property, not
			// a reason to pretend a direct sibling alias is executable.
			descriptor.PreparedManifestRecovery = true
		}
	case provider == "linear" && workItemAlias:
		descriptor.RouteDataset = "work-items"
		descriptor.Destinations = workItemRouteDestinations()
		descriptor.RouteReady = true
		if dataset == "work-items" {
			descriptor.Plannable = true
		}
	case provider == "gitlab" && workItemAlias:
		descriptor.Destinations = workItemRouteDestinations()
		descriptor.RouteReady = true
		if dataset == "work-items" {
			descriptor.Plannable = true
		}
	case provider == "jira" && workItemAlias:
		descriptor.RouteDataset = "work-items"
		descriptor.Destinations = append(workItemRouteDestinations(), "worklogs")
		descriptor.RouteReady = true
		if dataset == "work-items" {
			descriptor.Plannable = true
		}
	case provider == "jira" && dataset == "incidents":
		descriptor.Destinations = []string{"operational_incidents"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "launchdarkly" && dataset == "feature-flags":
		descriptor.Destinations = launchDarklyRouteDestinations()
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "github" && dataset == "repo-metadata":
		// GitHub has a native complete-route handler
		// (GitHubRepositoryRouteHandler) and a repos effect sink
		// (GitHubRepositoryClickHouseEffects). CHAOS-3123 cleared this to
		// RouteReady=true on fixture-level field parity evidence against the
		// production Python collector (_repo_from_item /
		// get_repo_uuid_from_repo / normalized_operational_provider_instance
		// / processors/github.py's settings-dict construction) — canary
		// staging and live-traffic parity are waived for this program (no
		// production users yet; see plan).
		descriptor.Destinations = []string{"repos"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "gitlab" && dataset == "repo-metadata":
		descriptor.Destinations = []string{"repos"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "gitlab" && dataset == "commits":
		descriptor.Destinations = []string{"git_commits"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "gitlab" && dataset == "commit-stats":
		descriptor.Destinations = []string{"git_commit_stats"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "gitlab" && dataset == "cicd":
		descriptor.Destinations = []string{
			"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
			"test_suite_results", "test_case_results", "coverage_snapshots",
		}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "gitlab" && dataset == "tests":
		descriptor.Destinations = []string{
			"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
			"test_suite_results", "test_case_results", "coverage_snapshots",
		}
		descriptor.RouteReady = true
	case provider == "gitlab" && dataset == "incidents":
		descriptor.Destinations = []string{
			"operational_services", "operational_service_repository_mappings",
			"operational_incidents",
		}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "gitlab" && dataset == "deployments":
		descriptor.Destinations = []string{"deployments"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "gitlab" && dataset == "feature-flags":
		descriptor.Destinations = []string{"feature_flag", "feature_flag_event", "work_graph_edges"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "gitlab" && dataset == "files":
		descriptor.Destinations = []string{"git_files"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "gitlab" && dataset == "blame":
		descriptor.Destinations = []string{"git_blame"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "gitlab" && dataset == "prs":
		descriptor.Destinations = githubPRSocialRouteDestinations()
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "gitlab" && dataset == "pr-reviews":
		descriptor.Destinations = githubPRSocialRouteDestinations()
		descriptor.RouteReady = true
	case provider == "gitlab" && dataset == "pr-comments":
		descriptor.Destinations = githubPRSocialRouteDestinations()
		descriptor.RouteReady = true
	case provider == "gitlab" && dataset == "security":
		descriptor.Destinations = []string{"security_alerts"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "pagerduty" && dataset == "services":
		descriptor.Destinations = []string{"operational_services", "operational_service_repository_mappings"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "pagerduty" && dataset == "business-services":
		descriptor.Destinations = []string{"operational_services"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "pagerduty" && dataset == "escalation-policies":
		descriptor.Destinations = []string{"operational_escalation_policies"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "pagerduty" && dataset == "schedules":
		descriptor.Destinations = []string{"operational_on_call_schedules"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "pagerduty" && dataset == "on-calls":
		descriptor.Destinations = []string{"operational_on_call_assignments"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "pagerduty" && dataset == "users":
		descriptor.Destinations = []string{"operational_users"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "pagerduty" && dataset == "teams":
		descriptor.Destinations = []string{"operational_teams"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "pagerduty" && dataset == "incidents":
		descriptor.Destinations = []string{"operational_incidents"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "pagerduty" && dataset == "incident-alerts":
		descriptor.Destinations = []string{"operational_alerts"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "pagerduty" && dataset == "incident-log-entries":
		descriptor.Destinations = []string{"operational_incident_timeline_events"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "pagerduty" && dataset == "incident-notes":
		descriptor.Destinations = []string{"operational_incident_notes"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "github" && dataset == "prs":
		// The PR-social route mirrors Python's one
		// _sync_github_prs_to_store_async boundary: REST PR detail (including
		// comments_count), GraphQL review enrichment, the complete
		// git_pull_requests row, and raw git_pull_request_reviews. D16 keeps
		// the three dataset identities independent while their effects stay
		// byte-identical. Every switch defaults off, so readiness alone moves
		// no traffic.
		descriptor.Destinations = githubPRSocialRouteDestinations()
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "github" && dataset == "pr-reviews":
		descriptor.Destinations = githubPRSocialRouteDestinations()
		descriptor.RouteReady = true
	case provider == "github" && dataset == "pr-comments":
		descriptor.Destinations = githubPRSocialRouteDestinations()
		descriptor.RouteReady = true
	case provider == "github" && dataset == "cicd":
		// cicd and tests delegate to one complete-row unit. Startup rejects both
		// switches enabled together, so ci_pipeline_runs has one active writer.
		descriptor.Destinations = []string{
			"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
			"test_suite_results", "test_case_results", "coverage_snapshots",
		}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "github" && dataset == "commits":
		descriptor.Destinations = []string{"git_commits"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "github" && dataset == "deployments":
		descriptor.Destinations = []string{"deployments"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "github" && dataset == "security":
		descriptor.Destinations = []string{"security_alerts"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "github" && dataset == "files":
		descriptor.Destinations = []string{"git_files"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "github" && dataset == "commit-stats":
		descriptor.Destinations = []string{"git_commit_stats"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "github" && dataset == "blame":
		// The path-progress effect sorts before git_blame in EffectCommitter.
		// That ordering is the crash-safety contract: accepted blame rows must
		// always have a durable selection identity available for recovery.
		descriptor.Destinations = []string{"github_blame_path_progress", "git_blame"}
		descriptor.RouteReady = true
		descriptor.Plannable = true
	case provider == "github" && dataset == "tests":
		descriptor.Destinations = []string{
			"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
			"test_suite_results", "test_case_results", "coverage_snapshots",
		}
		descriptor.RouteReady = true
	}
	// TestOps is the first opt-in chunked route family. The policy is fixed in
	// code so all workers and recovery attempts agree on the same bounds; every
	// other route retains the legacy one-batch persistence contract.
	if (provider == "github" || provider == "gitlab") &&
		(dataset == "cicd" || dataset == "tests") {
		descriptor.Chunked = true
		descriptor.ChunkPolicy = DefaultChunkPolicy()
	}
	return descriptor, true
}

func launchDarklyRouteDestinations() []string {
	return []string{
		"feature_flag",
		"feature_flag_event",
		"feature_flag_link",
		"work_graph_edges",
	}
}

func githubPRSocialRouteDestinations() []string {
	return []string{"git_pull_requests", "git_pull_request_reviews"}
}

func workItemRouteDestinations() []string {
	// The effect construction manifest is the neutral semantic owner of the
	// GitHub family. Linear expired-lease recovery retains an independent retry
	// eligibility policy, even where it currently names the same destinations.
	return githubWorkItemRouteDestinations()
}
