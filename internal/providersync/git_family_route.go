package providersync

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitFamilyRoute is the (handler, sink, readback) triple of one GitHub or
// GitLab route that needs nothing but a ClickHouse connection and a lease
// guard: the git family (repo metadata, commits, commit stats, files, blame,
// pull requests, CI/CD and tests, deployments, security, GitLab incidents and
// feature flags).
type GitFamilyRoute struct {
	Handler  CompleteRouteHandler
	Sink     EffectSink
	Readback EffectReadback
}

// GitFamilyDeps are the collaborators a git-family route is built from.
type GitFamilyDeps struct {
	Conn driver.Conn
	// Lease fences every write and coverage read. The worker passes the unit's
	// LeaseSession; an in-process run (dho sync <target>) passes a guard that
	// is valid while the process lives.
	Lease providerfoundation.LeaseGuard
	// GitHubTestsMaxArtifactBytes overrides the github cicd/tests route's
	// per-artifact download cap; zero keeps the route's own default.
	GitHubTestsMaxArtifactBytes int64
}

// SelectGitFamilyRoute is the ONE place the git-family (provider, dataset)
// pairs are mapped to their route handler, effect sink and readback. The
// worker's BuildExecutor and the in-process `dho sync <target>` executor both
// call it, so a route added or changed here changes both, and the two can
// never disagree about which sink a dataset writes through. ok=false means the
// pair is not a git-family route (work items, Jira incidents, PagerDuty and
// LaunchDarkly need collaborators only the worker has).
func SelectGitFamilyRoute(provider, dataset string, deps GitFamilyDeps) (route GitFamilyRoute, ok bool) {
	conn, lease := deps.Conn, deps.Lease
	switch provider {
	case "gitlab":
		identity := GitLabRepoIdentityClickHouseReader{Conn: conn, Lease: lease}
		switch dataset {
		case "repo-metadata":
			sink := GitLabRepositoryClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitLabRepositoryRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "commits":
			sink := GitLabCommitsClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitLabCommitsRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "commit-stats":
			sink := GitLabCommitStatsClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitLabCommitStatsRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "cicd", "tests":
			sink := TestOpsClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitLabTestsRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "incidents":
			sink := GitLabIncidentsClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitLabIncidentsRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "deployments":
			sink := GitLabDeploymentsClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitLabDeploymentsRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "feature-flags":
			sink := GitLabFeatureFlagsClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitLabFeatureFlagsRouteHandler{}, sink, sink}, true
		case "files":
			sink := GitLabFilesClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitLabFilesRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "blame":
			sink := GitLabBlameClickHouseEffects{Conn: conn, Lease: lease}
			handler := GitLabBlameRouteHandler{Coverage: GitLabBlameClickHouseCoverage{Conn: conn, Lease: lease}, RepoIdentity: identity}
			return GitFamilyRoute{handler, sink, sink}, true
		case "prs", "pr-reviews", "pr-comments":
			sink := GitLabPullRequestSocialClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitLabPullRequestRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "security":
			sink := GitLabSecurityClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitLabSecurityRouteHandler{RepoIdentity: identity}, sink, sink}, true
		}
	case "github":
		identity := GitLabRepoIdentityClickHouseReader{Conn: conn, Lease: lease, Provider: "github"}
		switch dataset {
		case "repo-metadata":
			sink := GitHubRepositoryClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitHubRepositoryRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "prs", "pr-reviews", "pr-comments":
			sink := GitHubPullRequestSocialClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitHubPullRequestSocialRouteHandler{PullRequests: GitHubPullRequestRouteHandler{RepoIdentity: identity}}, sink, sink}, true
		case "cicd", "tests":
			sink := GitHubTestsClickHouseEffects{Conn: conn, Lease: lease}
			handler := GitHubTestsRouteHandler{MaxArtifactBytes: deps.GitHubTestsMaxArtifactBytes, RepoIdentity: identity}
			return GitFamilyRoute{handler, sink, sink}, true
		case "commits":
			sink := GitHubCommitsClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitHubCommitsRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "deployments":
			sink := GitHubDeploymentsClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitHubDeploymentsRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "security":
			sink := GitHubSecurityClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitHubSecurityRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "files":
			sink := GitHubFilesClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitHubFilesRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "commit-stats":
			sink := GitHubCommitStatsClickHouseEffects{Conn: conn, Lease: lease}
			return GitFamilyRoute{GitHubCommitStatsRouteHandler{RepoIdentity: identity}, sink, sink}, true
		case "blame":
			sink := GitHubBlameClickHouseEffects{Conn: conn, Lease: lease}
			handler := GitHubBlameRouteHandler{Coverage: GitHubBlameClickHouseCoverage{Conn: conn, Lease: lease}, RepoIdentity: identity}
			return GitFamilyRoute{handler, sink, sink}, true
		}
	}
	return GitFamilyRoute{}, false
}
