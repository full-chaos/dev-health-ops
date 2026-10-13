package projectmembership

// Project state categories (CHAOS-9438). The closed set is the one Linear's
// project status type already writes, lowercase, so every provider's `projects`
// row answers the same status filter. A category is never empty and never
// "unknown": a provider with no lifecycle fact maps to started with a source
// that names the fact it lacked.
const (
	StateBacklog   = "backlog"
	StatePlanned   = "planned"
	StateStarted   = "started"
	StatePaused    = "paused"
	StateCompleted = "completed"
	StateCanceled  = "canceled"
)

// ProjectState is a category plus the provider fact that decided it.
type ProjectState struct {
	State  string
	Source string
}

// Lead decision (2026-10-13, chris may refine): a project that is archived or
// closed at the provider appears completed; a Jira project in the deleted
// state appears canceled; every other project is started.

// GitLabProjectState maps GitLab's project `archived` flag.
func GitLabProjectState(archived bool) ProjectState {
	if archived {
		return ProjectState{StateCompleted, "gitlab.project.archived=true"}
	}
	return ProjectState{StateStarted, "gitlab.project.archived=false"}
}

// GitHubProjectV2State maps a Projects v2 board's `closed` flag.
func GitHubProjectV2State(closed *bool) ProjectState {
	switch {
	case closed == nil:
		return ProjectState{StateStarted, "github.projectv2.closed=unread"}
	case *closed:
		return ProjectState{StateCompleted, "github.projectv2.closed=true"}
	}
	return ProjectState{StateStarted, "github.projectv2.closed=false"}
}

// JiraProjectStatus values are Jira Cloud's project search `status` filter.
const (
	JiraStatusLive     = "live"
	JiraStatusArchived = "archived"
	JiraStatusDeleted  = "deleted"
)

// JiraProjectState maps a Jira project status (live, archived, deleted). An
// empty or unrecognised status means the project's status was not read: it maps
// to started and the source says so.
func JiraProjectState(status string) ProjectState {
	switch status {
	case JiraStatusArchived:
		return ProjectState{StateCompleted, "jira.project.status=archived"}
	case JiraStatusDeleted:
		return ProjectState{StateCanceled, "jira.project.status=deleted"}
	case JiraStatusLive:
		return ProjectState{StateStarted, "jira.project.status=live"}
	}
	return ProjectState{StateStarted, "jira.project.status=unread"}
}

// LinearProjectState keeps Linear's own status type (already in the set).
func LinearProjectState(statusType string) ProjectState {
	switch statusType {
	case StateBacklog, StatePlanned, StateStarted, StatePaused, StateCompleted, StateCanceled:
		return ProjectState{statusType, "linear.project.status.type=" + statusType}
	}
	return ProjectState{StateStarted, "linear.project.status.type=unread"}
}

// WithState returns the row carrying the given state.
func (row CatalogRow) WithState(state ProjectState) CatalogRow {
	row.State, row.StateSource = state.State, state.Source
	return row
}
