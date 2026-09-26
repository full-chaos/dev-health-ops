package synccli

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

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
var catalogProviders = []string{"github"}

func isCatalogProvider(name string) bool {
	for _, provider := range catalogProviders {
		if provider == name {
			return true
		}
	}
	return false
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
	dsn        string
}

// runCatalogTeams runs one provider's catalog into the ClickHouse the DSN names.
func runCatalogTeams(ctx context.Context, env cli.Env, d deps, request catalogRequest) int {
	boundary := secrets.NewBoundary(request.dsn)
	redact := func(err error) string { return boundary.Redact(err).Error() }
	switch request.provider {
	case "github":
	default:
		return writeError(env.Stderr, cli.ExitUsage, "unsupported_provider", "provider "+request.provider+" has no team catalog verb")
	}

	// Python's own refusals, in its order (providers/teams.py, github branch): all exit 1.
	owner := strings.TrimSpace(request.owner)
	if owner == "" {
		return writeError(env.Stderr, cli.ExitFailure, "owner_required", "--owner is required for github provider (org name).")
	}
	token := request.auth
	if token == "" {
		token, _ = env.Lookup("GITHUB_TOKEN")
	}
	if token == "" {
		return writeError(env.Stderr, cli.ExitFailure, "token_required", "GitHub token required. Use --auth or set GITHUB_TOKEN env var.")
	}

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
	doer := d.doer
	if doer == nil {
		doer = &http.Client{Timeout: 45 * time.Second}
	}
	client, err := providerfoundation.NewGitHubClient(credential, doer, providerfoundation.DefaultRetryPolicy(), teamCatalogLease{})
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "client_invalid", "the GitHub client could not be built")
	}
	conn, err := d.openStore(ctx, request.dsn)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "clickhouse_unavailable", redact(err))
	}
	defer func() { _ = conn.Close() }()

	selections := providersync.TeamCatalogSelections{Teams: request.structure, Members: request.members}
	collector := providersync.GitHubTeamCatalogCollector{
		Client: providersync.GitHubTeamCatalogRouteHandler{ResolveEmail: true},
		Sink:   providersync.GitHubTeamCatalogClickHouseEffects{Conn: conn},
	}
	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	started := time.Now()
	now := d.now()
	result, err := collector.CollectTeamCatalog(ctx, providersync.TeamCatalogReference{OrgID: request.orgID, SyncRunID: uuid.NewString(), Strict: true},
		credential, client, selections, now)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "sync_failed", redact(err))
	}
	// An empty catalog is a provider that returned no teams. A team the catalog found but did not
	// write (its sync policy leaves it untouched, or its changes were staged for review) is not
	// empty; teams the catalog could not confirm the rosters of are a failure, not an empty answer.
	found := result.TeamsWritten + result.TeamsSkippedPolicy + result.TeamsStagedForReview
	if result.RosterPreservationFailed && result.TeamsWritten == 0 {
		return writeError(env.Stderr, cli.ExitFailure, "roster_unconfirmed",
			"the current team rosters could not be confirmed, so no team row was written this run")
	}
	if found == 0 && !request.allowEmpty {
		return writeError(env.Stderr, cli.ExitFailure, "empty_result",
			"No teams found/generated. Pass --allow-empty to exit successfully on an empty sync.")
	}
	logger.Info("github team catalog synced", "org_id", request.orgID, "teams", result.TeamsWritten,
		"teams_skipped_policy", result.TeamsSkippedPolicy, "teams_staged_for_review", result.TeamsStagedForReview,
		"memberships", result.MembershipsWritten, "repo_ownership", result.RepoOwnershipWritten,
		"roster_preservation_failed", result.RosterPreservationFailed, "duration_ms", time.Since(started).Milliseconds())
	if _, err := fmt.Fprintf(env.Stdout, "provider=github teams=%d teams_skipped_policy=%d teams_staged_for_review=%d memberships=%d repo_ownership=%d\n",
		result.TeamsWritten, result.TeamsSkippedPolicy, result.TeamsStagedForReview, result.MembershipsWritten, result.RepoOwnershipWritten); err != nil {
		return cli.ExitFailure
	}
	return cli.ExitOK
}
