// Package linkscope narrows a work-item read to the items linked to the repositories of a request's ceiling
// (CHAOS-13085). The work_item_* tables carry no repository of their own that may be used, so an item reaches a
// repository only through its linked pull request rows in work_graph_issue_pr (all three provenance tiers, the
// definition of the Home repository view in linkeditems), never through work_items.repo_id.
package linkscope

import (
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkeditems"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// BindingLinkRepoIDs is the binding of linkeditems.Subquery that holds the repository ids.
const BindingLinkRepoIDs = "link_repo_ids"

// ItemCondition is the " AND ..." fragment that holds column (a work item id column, a code constant) to the items
// linked to the ceiling's repositories. An unrestricted ceiling returns ("", nil): the statement stays today's. A
// restricted ceiling with no repository returns " AND 1 = 0". The statement that embeds it must bind org_id.
func ItemCondition(ceiling teamscope.Ceiling, column string) (string, []dhclickhouse.Binding) {
	if !ceiling.Restricted() {
		return "", nil
	}
	return ItemConditionIDs(ceiling.IDs(), column)
}

// ItemConditionIDs is the condition of an explicit repository id list, already held to a ceiling by the caller: no id
// returns " AND 1 = 0".
func ItemConditionIDs(ids []string, column string) (string, []dhclickhouse.Binding) {
	if len(ids) == 0 {
		return " AND 1 = 0", nil
	}
	return " AND " + column + " IN " + linkeditems.Subquery, []dhclickhouse.Binding{{Name: BindingLinkRepoIDs, Value: ids}}
}

// ScopeRepositoryCeiling is the scope of a count held to the repositories of a ceiling.
const ScopeRepositoryCeiling = "repository ceiling"

// ReasonNotScopedToRepositories is the reason of a signal whose rows carry no repository.
const ReasonNotScopedToRepositories = "not scoped to repositories"

// OmittedSignal says one signal was left out of an answer for a caller held to a ceiling, and the reason.
type OmittedSignal struct {
	Kind   string `json:"kind"`
	Reason string `json:"reason"`
}

// NarrowedCount says one count of an answer is held to a scope narrower than the organization.
type NarrowedCount struct {
	Kind  string `json:"kind"`
	Scope string `json:"scope"`
}
