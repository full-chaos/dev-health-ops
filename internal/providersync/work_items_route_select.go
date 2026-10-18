package providersync

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// WorkItemsRouteDeps are the collaborators a work-items route is built from.
type WorkItemsRouteDeps struct {
	Conn driver.Conn
	// Lease fences every write and coverage read (the worker's LeaseSession).
	Lease providerfoundation.LeaseGuard
	// Metrics receives the sinks' provider metrics.
	Metrics *providerfoundation.Metrics
	// StatusMapping is the loaded status mapping of the worker's runtime
	// config; GitLab and Jira normalize statuses through it. A nil mapping
	// selects no route.
	StatusMapping *StatusMapping
}

// SelectWorkItemsRoute is the ONE place the four work-items routes (github,
// gitlab, jira, linear) are mapped to their route handler, effect sink and
// readback. The worker's BuildExecutor calls it, and so does any in-process
// run of the same route (the fixture world producer), so the two can never
// disagree about the sink a provider's work items are written through.
// ok=false means the pair is not a work-items route, or the status mapping is
// missing.
func SelectWorkItemsRoute(provider, dataset string, deps WorkItemsRouteDeps) (route GitFamilyRoute, ok bool, err error) {
	handler, ok := SelectWorkItemsRouteHandler(provider, dataset, deps)
	if !ok {
		return GitFamilyRoute{}, false, nil
	}
	var sink interface {
		EffectSink
		EffectReadback
	}
	switch provider {
	case "github":
		sink, err = NewGitHubWorkItemClickHouseEffects(deps.Conn, deps.Lease, deps.Metrics)
	case "gitlab":
		sink, err = NewGitLabWorkItemFamilyClickHouseEffects(deps.Conn, deps.Lease, deps.Metrics)
	case "jira":
		sink, err = NewJiraWorkItemCompositeClickHouseEffects(deps.Conn, deps.Lease, deps.Metrics)
	case "linear":
		sink, err = NewLinearWorkItemFamilyClickHouseEffects(deps.Conn, deps.Lease, deps.Metrics)
	}
	if err != nil {
		return GitFamilyRoute{}, false, err
	}
	return GitFamilyRoute{Handler: handler, Sink: sink, Readback: sink}, true, nil
}

// SelectWorkItemsRouteHandler is the route handler half of SelectWorkItemsRoute.
func SelectWorkItemsRouteHandler(provider, dataset string, deps WorkItemsRouteDeps) (CompleteRouteHandler, bool) {
	if dataset != "work-items" || deps.StatusMapping == nil {
		return nil, false
	}
	switch provider {
	case "github":
		return GitHubWorkItemsRouteHandler{
			REST:     GitHubWorkItemsRESTCollector{RepoIdentity: GitLabRepoIdentityClickHouseReader{Conn: deps.Conn, Lease: deps.Lease, Provider: "github"}},
			Projects: GitHubProjectV2Fetcher{},
			ProjectMembershipSnapshotDiff: GitHubProjectV2SnapshotDiffClickHouseReader{
				Conn: deps.Conn,
			},
		}, true
	case "gitlab":
		// NO page limits on purpose: the route's own defaults apply, and for every
		// nested list that default is the 100-page hard ceiling
		// (gitLabWorkItemsNestedHardMaxPages, CHAOS-8777). A limit set here could
		// only lower the ceiling (the route clamps it), never raise it.
		return GitLabWorkItemsRouteHandler{
			StatusMapping: deps.StatusMapping,
			FetchEpics:    gitLabFetchEpicsFromEnv(),
			RepoIdentity:  GitLabRepoIdentityClickHouseReader{Conn: deps.Conn, Lease: deps.Lease},
		}, true
	case "jira":
		return JiraAtlassianRouteHandler{StatusMapping: deps.StatusMapping}, true
	case "linear":
		return LinearWorkItemFamilyRouteHandler{
			Direct: LinearWorkItemsRouteHandler{},
		}, true
	}
	return nil, false
}
