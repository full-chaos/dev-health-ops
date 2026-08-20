package providersync

import "github.com/full-chaos/dev-health-ops/internal/platform/config"

// RouteSwitchesFromConfig maps deployment configuration onto the route
// enablement flags.
//
// It lives here, beside CompleteRouteSwitches itself, because more than one
// binary needs the same answer: the worker decides what it may execute, and
// the reconciler's unreclaimable-dispatching sweep decides whether a stranded
// unit has any runtime at all (CHAOS-4005). Two hand-maintained copies of a
// forty-field mapping is precisely the producer/consumer drift that stranded
// units in the first place, so there is exactly one.

func RouteSwitchesFromConfig(cfg config.Config) CompleteRouteSwitches {
	return CompleteRouteSwitches{
		LocalAllRoutes:              cfg.LocalAllProviderRoutes,
		LinearWorkItems:             cfg.WorkerLinearWorkItemsEnabled,
		JiraWorkItems:               cfg.WorkerJiraWorkItemsEnabled,
		JiraIncidents:               cfg.WorkerJiraIncidentsEnabled,
		LaunchDarklyFeatureFlags:    cfg.WorkerLaunchDarklyFeatureFlagsEnabled,
		GithubRepoMetadata:          cfg.WorkerGithubRepoMetadataEnabled,
		GitlabRepoMetadata:          cfg.WorkerGitlabRepoMetadataEnabled,
		GitlabCommits:               cfg.WorkerGitlabCommitsEnabled,
		GitlabCommitStats:           cfg.WorkerGitlabCommitStatsEnabled,
		GitlabCICD:                  cfg.WorkerGitlabCICDEnabled,
		GitlabTests:                 cfg.WorkerGitlabTestsEnabled,
		GitlabIncidents:             cfg.WorkerGitlabIncidentsEnabled,
		GitlabDeployments:           cfg.WorkerGitlabDeploymentsEnabled,
		GitlabFeatureFlags:          cfg.WorkerGitlabFeatureFlagsEnabled,
		GitlabFiles:                 cfg.WorkerGitlabFilesEnabled,
		GitlabBlame:                 cfg.WorkerGitlabBlameEnabled,
		GitlabPRs:                   cfg.WorkerGitlabPRsEnabled,
		GitlabPRReviews:             cfg.WorkerGitlabPRReviewsEnabled,
		GitlabPRComments:            cfg.WorkerGitlabPRCommentsEnabled,
		GitlabSecurity:              cfg.WorkerGitlabSecurityEnabled,
		GitlabWorkItems:             cfg.WorkerGitlabWorkItemsEnabled,
		PagerDutyServices:           cfg.WorkerPagerDutyServicesEnabled,
		PagerDutyBusinessServices:   cfg.WorkerPagerDutyBusinessServicesEnabled,
		PagerDutyEscalationPolicies: cfg.WorkerPagerDutyEscalationPoliciesEnabled,
		PagerDutySchedules:          cfg.WorkerPagerDutySchedulesEnabled,
		PagerDutyOnCalls:            cfg.WorkerPagerDutyOnCallsEnabled,
		PagerDutyUsers:              cfg.WorkerPagerDutyUsersEnabled,
		PagerDutyTeams:              cfg.WorkerPagerDutyTeamsEnabled,
		PagerDutyIncidents:          cfg.WorkerPagerDutyIncidentsEnabled,
		PagerDutyIncidentAlerts:     cfg.WorkerPagerDutyIncidentsEnabled,
		PagerDutyIncidentLogEntries: cfg.WorkerPagerDutyIncidentsEnabled,
		PagerDutyIncidentNotes:      cfg.WorkerPagerDutyIncidentsEnabled,
		GithubPRs:                   cfg.WorkerGithubPRsEnabled,
		GithubPRReviews:             cfg.WorkerGithubPRReviewsEnabled,
		GithubPRComments:            cfg.WorkerGithubPRCommentsEnabled,
		GithubCICD:                  cfg.WorkerGithubCICDEnabled,
		GithubCommits:               cfg.WorkerGithubCommitsEnabled,
		GithubDeployments:           cfg.WorkerGithubDeploymentsEnabled,
		GithubSecurity:              cfg.WorkerGithubSecurityEnabled,
		GithubFiles:                 cfg.WorkerGithubFilesEnabled,
		GithubCommitStats:           cfg.WorkerGithubCommitStatsEnabled,
		GithubBlame:                 cfg.WorkerGithubBlameEnabled,
		GithubTests:                 cfg.WorkerGithubTestsEnabled,
		GithubWorkItems:             cfg.WorkerGithubWorkItemsEnabled,
	}
}
