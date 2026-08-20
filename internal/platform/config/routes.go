package config

import (
	"slices"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Route names one provider-dataset pair the worker can be told to serve.
//
// These forty switches are ENVIRONMENT ONLY and deliberately have no flag.
// Selection-by-subscription is the model: what a worker executes follows from
// the queues it consumes (-Q/--queues), not from a parallel forty-switch
// enablement surface. The switches remain because the Python planner reads the
// identical WORKER_*_ENABLED names through ProviderUnitRouteSwitches to decide
// what to plan, and a producer and executor that disagree about a route
// dispatch units to a worker with no handler.
//
// This registry exists so that agreement is greppable in one place rather than
// spread through a 182-line literal inside Load. It is the deprecation map for
// the eventual move to subscription.
type Route struct {
	// Env is the switch name, shared verbatim with the Python producer.
	Env string
	// target locates this route's switch inside a Config.
	target func(*Config) *bool
}

// routeRegistry is the one place a provider route switch is declared.
var routeRegistry = []Route{
	{Env: "WORKER_GITHUB_BLAME_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubBlameEnabled }},
	{Env: "WORKER_GITHUB_CICD_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubCICDEnabled }},
	{Env: "WORKER_GITHUB_COMMIT_STATS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubCommitStatsEnabled }},
	{Env: "WORKER_GITHUB_COMMITS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubCommitsEnabled }},
	{Env: "WORKER_GITHUB_DEPLOYMENTS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubDeploymentsEnabled }},
	{Env: "WORKER_GITHUB_FILES_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubFilesEnabled }},
	{Env: "WORKER_GITHUB_PR_COMMENTS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubPRCommentsEnabled }},
	{Env: "WORKER_GITHUB_PR_REVIEWS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubPRReviewsEnabled }},
	{Env: "WORKER_GITHUB_PRS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubPRsEnabled }},
	{Env: "WORKER_GITHUB_REPO_METADATA_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubRepoMetadataEnabled }},
	{Env: "WORKER_GITHUB_SECURITY_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubSecurityEnabled }},
	{Env: "WORKER_GITHUB_TESTS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubTestsEnabled }},
	{Env: "WORKER_GITHUB_WORK_ITEMS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGithubWorkItemsEnabled }},
	{Env: "WORKER_GITLAB_BLAME_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabBlameEnabled }},
	{Env: "WORKER_GITLAB_CICD_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabCICDEnabled }},
	{Env: "WORKER_GITLAB_COMMIT_STATS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabCommitStatsEnabled }},
	{Env: "WORKER_GITLAB_COMMITS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabCommitsEnabled }},
	{Env: "WORKER_GITLAB_DEPLOYMENTS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabDeploymentsEnabled }},
	{Env: "WORKER_GITLAB_FEATURE_FLAGS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabFeatureFlagsEnabled }},
	{Env: "WORKER_GITLAB_FILES_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabFilesEnabled }},
	{Env: "WORKER_GITLAB_INCIDENTS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabIncidentsEnabled }},
	{Env: "WORKER_GITLAB_PR_COMMENTS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabPRCommentsEnabled }},
	{Env: "WORKER_GITLAB_PR_REVIEWS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabPRReviewsEnabled }},
	{Env: "WORKER_GITLAB_PRS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabPRsEnabled }},
	{Env: "WORKER_GITLAB_REPO_METADATA_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabRepoMetadataEnabled }},
	{Env: "WORKER_GITLAB_SECURITY_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabSecurityEnabled }},
	{Env: "WORKER_GITLAB_TESTS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabTestsEnabled }},
	{Env: "WORKER_GITLAB_WORK_ITEMS_ENABLED", target: func(c *Config) *bool { return &c.WorkerGitlabWorkItemsEnabled }},
	{Env: "WORKER_JIRA_INCIDENTS_ENABLED", target: func(c *Config) *bool { return &c.WorkerJiraIncidentsEnabled }},
	{Env: "WORKER_JIRA_WORK_ITEMS_ENABLED", target: func(c *Config) *bool { return &c.WorkerJiraWorkItemsEnabled }},
	{Env: "WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED", target: func(c *Config) *bool { return &c.WorkerLaunchDarklyFeatureFlagsEnabled }},
	{Env: "WORKER_LINEAR_WORK_ITEMS_ENABLED", target: func(c *Config) *bool { return &c.WorkerLinearWorkItemsEnabled }},
	{Env: "WORKER_PAGERDUTY_BUSINESS_SERVICES_ENABLED", target: func(c *Config) *bool { return &c.WorkerPagerDutyBusinessServicesEnabled }},
	{Env: "WORKER_PAGERDUTY_ESCALATION_POLICIES_ENABLED", target: func(c *Config) *bool { return &c.WorkerPagerDutyEscalationPoliciesEnabled }},
	{Env: "WORKER_PAGERDUTY_INCIDENTS_ENABLED", target: func(c *Config) *bool { return &c.WorkerPagerDutyIncidentsEnabled }},
	{Env: "WORKER_PAGERDUTY_ON_CALLS_ENABLED", target: func(c *Config) *bool { return &c.WorkerPagerDutyOnCallsEnabled }},
	{Env: "WORKER_PAGERDUTY_SCHEDULES_ENABLED", target: func(c *Config) *bool { return &c.WorkerPagerDutySchedulesEnabled }},
	{Env: "WORKER_PAGERDUTY_SERVICES_ENABLED", target: func(c *Config) *bool { return &c.WorkerPagerDutyServicesEnabled }},
	{Env: "WORKER_PAGERDUTY_TEAMS_ENABLED", target: func(c *Config) *bool { return &c.WorkerPagerDutyTeamsEnabled }},
	{Env: "WORKER_PAGERDUTY_USERS_ENABLED", target: func(c *Config) *bool { return &c.WorkerPagerDutyUsersEnabled }},
}

// Routes returns the declared provider route switches.
func Routes() []Route {
	return slices.Clone(routeRegistry)
}

// applyRoutes resolves every route switch from the environment.
func applyRoutes(cfg *Config, lookup secrets.LookupEnv, allProviderRoutes bool) error {
	for _, route := range routeRegistry {
		fallback := false
		if allProviderRoutes {
			presetDefault, err := providerRoutePresetDefault(lookup, route.Env)
			if err != nil {
				return err
			}
			fallback = presetDefault
		}
		value, err := boolEnv(lookup, route.Env, fallback)
		if err != nil {
			return err
		}
		*route.target(cfg) = value
	}
	return nil
}

// EnabledRouteNames reports the route switches this configuration enables, for
// startup evidence.
func (c Config) EnabledRouteNames() []string {
	enabled := make([]string, 0, len(routeRegistry))
	for _, route := range routeRegistry {
		if *route.target(&c) {
			enabled = append(enabled, route.Env)
		}
	}
	slices.Sort(enabled)
	return enabled
}

// environmentIsLocal reports whether this process is configured as a local
// deployment, the one place the all-routes preset is permitted.
func environmentIsLocal(lookup secrets.LookupEnv) bool {
	environment, _ := lookup(devHealthEnv)
	return strings.ToLower(strings.TrimSpace(environment)) == "local"
}
