package providersync

// Static route templates for provider routes whose request path carries an
// owner, repository, project or run name. A failure names the template (see
// providerfoundation.ProviderError.Route) and never the path. Constants only:
// a template is never built from a request value.
const (
	githubRepoRoute = "/repos/{owner}/{repo}"

	githubPullsRoute        = githubRepoRoute + "/pulls"
	githubPullRoute         = githubRepoRoute + "/pulls/{number}"
	githubCommitsRoute      = githubRepoRoute + "/commits"
	githubCommitPullsRoute  = githubRepoRoute + "/commits/{sha}/pulls"
	githubIssuesRoute       = githubRepoRoute + "/issues"
	githubIssueEventsRoute  = githubRepoRoute + "/issues/{number}/events"
	githubIssueCommentRoute = githubRepoRoute + "/issues/{number}/comments"
	githubLabelsRoute       = githubRepoRoute + "/labels"
	githubMilestonesRoute   = githubRepoRoute + "/milestones"
	githubDeploymentsRoute  = githubRepoRoute + "/deployments"
	githubStatusesRoute     = githubRepoRoute + "/deployments/{id}/statuses"
	githubReleasesRoute     = githubRepoRoute + "/releases"
	githubRunsRoute         = githubRepoRoute + "/actions/runs"
	githubRunJobsRoute      = githubRepoRoute + "/actions/runs/{run}/jobs"
	githubRunArtifactsRoute = githubRepoRoute + "/actions/runs/{run}/artifacts"

	githubTreeRoute         = githubRepoRoute + "/git/trees/{ref}"
	githubBranchRoute       = githubRepoRoute + "/branches/{branch}"
	githubCommitRoute       = githubRepoRoute + "/commits/{sha}"
	githubDependabotRoute   = githubRepoRoute + "/dependabot/alerts"
	githubCodeScanningRoute = githubRepoRoute + "/code-scanning/alerts"
	githubAdvisoriesRoute   = githubRepoRoute + "/security-advisories"

	graphQLRoute = "/graphql"

	gitlabProjectRoute = "/projects/{project}"
	// The artifact download routes also name a failure of the redirected
	// download (the artifact store), whose own path is never printed.
	githubArtifactZipRoute  = githubRepoRoute + "/actions/artifacts/{artifact}/zip"
	gitlabJobArtifactsRoute = gitlabProjectRoute + "/jobs/{job}/artifacts"
	gitlabGroupEpicsRoute   = "/groups/{group}/epics"

	gitlabCommitsRoute       = gitlabProjectRoute + "/repository/commits"
	gitlabMergeRequestsRoute = gitlabProjectRoute + "/merge_requests"
	gitlabPipelinesRoute     = gitlabProjectRoute + "/pipelines"
	gitlabPipelineJobsRoute  = gitlabProjectRoute + "/pipelines/{pipeline}/jobs"
	gitlabIssuesRoute        = gitlabProjectRoute + "/issues"
	gitlabDeploymentsRoute   = gitlabProjectRoute + "/deployments"
	gitlabReleasesRoute      = gitlabProjectRoute + "/releases"
	gitlabLabelsRoute        = gitlabProjectRoute + "/labels"
	gitlabTreeRoute          = gitlabProjectRoute + "/repository/tree"
	gitlabBlameRoute         = gitlabProjectRoute + "/repository/files/{file}/blame"
	gitlabCommitRoute        = gitlabProjectRoute + "/repository/commits/{sha}"
	gitlabFeatureFlagsRoute  = gitlabProjectRoute + "/feature_flags"
	gitlabFindingsRoute      = gitlabProjectRoute + "/vulnerability_findings"
	gitlabDependenciesRoute  = gitlabProjectRoute + "/dependencies"
	gitlabMilestonesRoute    = gitlabProjectRoute + "/milestones"
)
