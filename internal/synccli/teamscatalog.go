package synccli

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// `dho sync teams --provider github|...` triggers the provider's team catalog: the same
// producer the worker's post-sync team autoimport runs (internal/providersync,
// CHAOS-4434 and siblings), written to the ClickHouse team dimensions the way the
// team-attribution contract prescribes (ops/docs/architecture/team-attribution.md §0):
// `teams` (provider_access rows), `team_memberships` and, for GitHub, `team_repo_ownership`.
//
// It is NOT the legacy `dev-hops sync teams` writer. That verb wrote `teams` rows carrying
// a `members` array of provider logins and nothing else; this one does not extend it
// (CHAOS-2600: no new legacy team attribution). The rows differ from Python's by design,
// column by column: teamsOracleColumns in the differential test names each one.

// teamCatalogLease is the in-process lease: the run holds none, so the only thing to
// assert before a provider call is that the caller's context is still live.
type teamCatalogLease struct{}

func (teamCatalogLease) Assert(ctx context.Context) error { return ctx.Err() }

// catalogProviders are the `--provider` values that run a catalog (jira runs Atlassian Teams).
var catalogProviders = []string{"github", "gitlab", "linear"}

func isCatalogProvider(name string) bool {
	for _, provider := range catalogProviders {
		if provider == name {
			return true
		}
	}
	return false
}

// catalogProviderSpec is the per-provider text Python's refusals use
// (providers/teams.py): "--owner is required for <provider> provider
// (<ownerNoun>)." and "<label> token required. Use --auth or set <tokenEnv>
// env var." -- verified against the github and gitlab branches, which share
// this exact shape with only the provider name, the owner's noun and the
// token env var spelled differently. Linear's branch (providers.teams.py,
// LinearClient.from_env) needs neither an --owner (the token scopes the
// whole workspace) nor an --auth override -- ownerNoun empty skips the owner
// refusal, tokenMissingMsg overrides the generic token message with Python's
// own wording exactly (LinearClient.from_env's ValueError text).
type catalogProviderSpec struct {
	label     string
	ownerNoun string
	tokenEnv  string
	// tokenMissingMsg, when set, replaces the generic "<label> token
	// required. Use --auth or set <tokenEnv> env var." message verbatim.
	tokenMissingMsg string
}

var catalogProviderSpecs = map[string]catalogProviderSpec{
	"github": {label: "GitHub", ownerNoun: "org name", tokenEnv: "GITHUB_TOKEN"},
	"gitlab": {label: "GitLab", ownerNoun: "group path", tokenEnv: "GITLAB_TOKEN"},
	// No --owner: python's linear branch scopes to the whole workspace via
	// LinearClient.from_env(), never a group/org argument. --auth is accepted
	// here as a Go-only convenience override of LINEAR_API_KEY (python's verb
	// reads only the env var, a named divergence -- RISK-NOTES).
	"linear": {label: "Linear", tokenEnv: "LINEAR_API_KEY", tokenMissingMsg: "Linear configuration error: Linear API key required (set LINEAR_API_KEY)"},
}

// catalogRequest is what the verb read from its flags and environment.
type catalogRequest struct {
	provider   string
	orgID      string
	owner      string
	auth       string
	allowEmpty bool
	structure  bool
	members    bool
	projects   bool
	dsn        string
}

// buildCatalogCollector resolves the provider's credential, HTTP client and
// TeamCatalogCollector -- everything that differs between providers. The
// caller (runCatalogTeams) owns the refusals shared by every provider
// (owner/token/ClickHouse) and the shared collection/reporting flow.
func buildCatalogCollector(env cli.Env, d deps, request catalogRequest, owner, token string, conn driver.Conn) (providersync.TeamCatalogCollector, providerfoundation.Credential, *providerfoundation.HTTPClient, int) {
	doer := d.doer
	if doer == nil {
		doer = productionDoer()
	}
	switch request.provider {
	case "github":
		config := map[string]string{"org": owner}
		// The GitHub Enterprise base URL, spelled as `dho sync <target>` reads it (Python's
		// team verb does not read one: a named difference).
		for _, key := range []string{"GITHUB_URL", "GITHUB_BASE_URL"} {
			if value, _ := env.Lookup(key); strings.TrimSpace(value) != "" {
				config["base_url"] = strings.TrimSpace(value)
				break
			}
		}
		credential := providerfoundation.NewCredential("github", "cli", config,
			map[string]secrets.Value{"token": secrets.NewValue(token)})
		client, err := providerfoundation.NewGitHubClient(credential, doer, providerfoundation.DefaultRetryPolicy(), teamCatalogLease{})
		if err != nil {
			return nil, providerfoundation.Credential{}, nil, writeError(env.Stderr, cli.ExitFailure, "client_invalid", clientBuildDetail("GitHub", err))
		}
		collector := providersync.GitHubTeamCatalogCollector{
			Client: providersync.GitHubTeamCatalogRouteHandler{ResolveEmail: true},
			Sink:   providersync.GitHubTeamCatalogClickHouseEffects{Conn: conn},
		}
		return collector, credential, client, 0
	case "gitlab":
		// group_path outranks group outranks owner (gitlabTeamCatalogGroupPath's
		// precedence) -- "owner" is Python's own flag name for the group path, so
		// it is carried under all three keys the collector's route handler reads.
		config := map[string]string{"group_path": owner, "group": owner, "owner": owner}
		// GITLAB_URL, the same spelling `dho sync <target>` and the Python verb
		// both read (default https://gitlab.com, applied by NewGitLabClient/
		// gitLabCredentialBaseURL when unset).
		if value, _ := env.Lookup("GITLAB_URL"); strings.TrimSpace(value) != "" {
			config["gitlab_url"] = strings.TrimSpace(value)
		}
		credential := providerfoundation.NewCredential("gitlab", "cli", config,
			map[string]secrets.Value{"token": secrets.NewValue(token)})
		client, err := providerfoundation.NewGitLabClient(credential, doer, providerfoundation.DefaultRetryPolicy(), teamCatalogLease{})
		if err != nil {
			return nil, providerfoundation.Credential{}, nil, writeError(env.Stderr, cli.ExitFailure, "client_invalid", clientBuildDetail("GitLab", err))
		}
		collector := providersync.GitLabTeamCatalogCollector{
			Handler: providersync.GitLabTeamCatalogRouteHandler{},
			Sink:    providersync.GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: teamCatalogLease{}},
		}
		return collector, credential, client, 0
	case "linear":
		// No group/org scoping: the API key's workspace is the whole scope,
		// exactly as LinearClient.from_env() reads it.
		config := map[string]string{}
		// LINEAR_URL, the same "PROVIDER_URL" spelling GITHUB_URL/GITLAB_URL use.
		// Python's client hardcodes LINEAR_API_URL with no override at all: a
		// named difference, needed here only so a fake API is reachable in tests.
		if value, _ := env.Lookup("LINEAR_URL"); strings.TrimSpace(value) != "" {
			config["base_url"] = strings.TrimSpace(value)
		}
		credential := providerfoundation.NewCredential("linear", "cli", config,
			map[string]secrets.Value{"api_key": secrets.NewValue(token)})
		client, err := providerfoundation.NewLinearClient(credential, doer, providerfoundation.DefaultRetryPolicy(), teamCatalogLease{})
		if err != nil {
			return nil, providerfoundation.Credential{}, nil, writeError(env.Stderr, cli.ExitFailure, "client_invalid", clientBuildDetail("Linear", err))
		}
		collector := providersync.LinearTeamCatalogCollector{
			Sink: providersync.LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: teamCatalogLease{}},
		}
		return collector, credential, client, 0
	default:
		return nil, providerfoundation.Credential{}, nil, writeError(env.Stderr, cli.ExitUsage, "unsupported_provider", "provider "+request.provider+" has no team catalog verb")
	}
}

// clientBuildDetail is the refusal's detail with its fixed, value-free cause (a typed credential refusal's
// reason), so an operator sees why the provider client could not be built.
func clientBuildDetail(provider string, err error) string {
	detail := "the " + provider + " client could not be built"
	if reason := providerfoundation.FailureReason(err); reason != "" {
		detail += ": " + reason
	}
	return detail
}

// runCatalogTeams runs one provider's catalog into the ClickHouse the DSN names.
func runCatalogTeams(ctx context.Context, env cli.Env, d deps, request catalogRequest) int {
	boundary := secrets.NewBoundary(request.dsn)
	redact := func(err error) string { return boundary.Redact(err).Error() }
	// What the collector logs (the roster-preservation warnings carry the
	// driver's error) is redacted with the same boundary.
	defer redactProcessLogger(boundary.RedactText)()
	spec, ok := catalogProviderSpecs[request.provider]
	if !ok {
		return writeError(env.Stderr, cli.ExitUsage, "unsupported_provider", "provider "+request.provider+" has no team catalog verb")
	}

	// Python's own refusals, in its order (providers/teams.py, github/gitlab/linear branches): all exit 1.
	owner := strings.TrimSpace(request.owner)
	if spec.ownerNoun != "" && owner == "" {
		return writeError(env.Stderr, cli.ExitFailure, "owner_required", "--owner is required for "+request.provider+" provider ("+spec.ownerNoun+").")
	}
	token := request.auth
	if token == "" {
		token, _ = env.Lookup(spec.tokenEnv)
	}
	if token == "" {
		message := spec.tokenMissingMsg
		if message == "" {
			message = spec.label + " token required. Use --auth or set " + spec.tokenEnv + " env var."
		}
		return writeError(env.Stderr, cli.ExitFailure, "token_required", message)
	}

	conn, err := d.openStore(ctx, request.dsn)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "clickhouse_unavailable", redact(err))
	}
	defer func() { _ = conn.Close() }()

	collector, credential, client, exitCode := buildCatalogCollector(env, d, request, owner, token, conn)
	if collector == nil {
		return exitCode
	}

	selections := providersync.TeamCatalogSelections{Teams: request.structure, Members: request.members, Projects: request.projects}
	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	started := time.Now()
	now := d.now()
	result, err := collector.CollectTeamCatalog(ctx, providersync.TeamCatalogReference{OrgID: request.orgID, SyncRunID: uuid.NewString(), Strict: true},
		credential, client, selections, now)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "sync_failed", redact(err))
	}
	// An empty catalog is a provider that returned no teams AND wrote nothing else. A team the
	// catalog found but did not write (its sync policy leaves it untouched, or its changes were
	// staged for review) is not empty; teams the catalog could not confirm the rosters of are a
	// failure, not an empty answer. A selective run (e.g. --members alone, with --structure off)
	// can commit real membership/ownership/project rows while TeamsWritten stays zero (no Teams
	// row was ever in scope) -- counting only team-shaped outcomes here reported that write as an
	// empty sync (codex r1, CHAOS-6907: an executed `--members`-only run committed a membership
	// row and still exited 1 with "No teams found/generated").
	found := result.TeamsWritten + result.TeamsSkippedPolicy + result.TeamsStagedForReview +
		result.MembershipsWritten + result.OwnershipWritten + result.ProjectsWritten
	if result.RosterPreservationFailed && result.TeamsWritten == 0 {
		return writeError(env.Stderr, cli.ExitFailure, "roster_unconfirmed",
			"the current team rosters could not be confirmed, so no team row was written this run")
	}
	if found == 0 && !request.allowEmpty {
		return writeError(env.Stderr, cli.ExitFailure, "empty_result",
			"No teams found/generated. Pass --allow-empty to exit successfully on an empty sync.")
	}
	logger.Info(request.provider+" team catalog synced", "org_id", request.orgID, "teams", result.TeamsWritten,
		"teams_skipped_policy", result.TeamsSkippedPolicy, "teams_staged_for_review", result.TeamsStagedForReview,
		"memberships", result.MembershipsWritten, "ownership", result.OwnershipWritten, "repo_ownership", result.RepoOwnershipWritten,
		"projects", result.ProjectsWritten, "roster_preservation_failed", result.RosterPreservationFailed, "duration_ms", time.Since(started).Milliseconds())
	if _, err := fmt.Fprintf(env.Stdout, "provider=%s teams=%d teams_skipped_policy=%d teams_staged_for_review=%d memberships=%d ownership=%d repo_ownership=%d projects=%d\n",
		request.provider, result.TeamsWritten, result.TeamsSkippedPolicy, result.TeamsStagedForReview, result.MembershipsWritten, result.OwnershipWritten, result.RepoOwnershipWritten, result.ProjectsWritten); err != nil {
		return cli.ExitFailure
	}
	return cli.ExitOK
}
