package units

import (
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// WorkItemLabelFields carries the two fields materialize._resolve_work_unit_label
// reads off a work item -- title and type.
type WorkItemLabelFields struct {
	Title string
	Type  string
}

// PRLabelFields carries the one field _resolve_work_unit_label reads off a
// pull request -- title.
type PRLabelFields struct {
	Title string
}

// CommitLabelFields carries the one field _resolve_work_unit_label reads off
// a commit -- message.
type CommitLabelFields struct {
	Message string
}

// ResolveWorkUnitLabelInput carries what _resolve_work_unit_label reads.
// WorkItems/PRs/Commits are keyed the same way materialize.py's own
// work_item_map/pr_map/commit_map are: WorkItems by work_item_id (the plain
// issue id), PRs and Commits by the canonical composite id
// ("{repo}#pr{number}"/"{repo}@{hash}", units/ids.go's own output format).
// A missing key reads as the zero value, matching Python's `.get(id) or {}`.
type ResolveWorkUnitLabelInput struct {
	IssueIDs  []string
	PRIDs     []string
	CommitIDs []string
	WorkItems map[string]WorkItemLabelFields
	PRs       map[string]PRLabelFields
	Commits   map[string]CommitLabelFields
}

// ResolveWorkUnitLabel ports materialize._resolve_work_unit_label
// (:1115-1153) exactly: the first NON-EMPTY title/message wins, checked in
// a fixed priority order (issues, then PRs, then commits) and, WITHIN each
// tier, in sorted id order -- not component/edge order. A component whose
// only labelled node is its third-sorted issue still returns that issue's
// title, not the first one encountered while walking edges.
//
// If NOTHING in any tier carries a non-empty title/message, the type-only
// fallback below returns a type with no name, in the same issues-then-PRs-
// then-commits priority, using the SMALLEST sorted issue id's own type
// (falling back to "issue" only if that specific field is empty -- not
// "issue" unconditionally).
func ResolveWorkUnitLabel(input ResolveWorkUnitLabelInput) (labelType *string, name *string) {
	issueIDs := sortedCopy(input.IssueIDs)
	prIDs := sortedCopy(input.PRIDs)
	commitIDs := sortedCopy(input.CommitIDs)

	for _, issueID := range issueIDs {
		item := input.WorkItems[issueID]
		title := pythonparity.Strip(item.Title)
		if title != "" {
			itemType := pythonparity.Strip(item.Type)
			if itemType == "" {
				itemType = "issue"
			}
			return &itemType, &title
		}
	}
	for _, prID := range prIDs {
		pr := input.PRs[prID]
		title := pythonparity.Strip(pr.Title)
		if title != "" {
			return stringPointer("pr"), &title
		}
	}
	for _, commitID := range commitIDs {
		commit := input.Commits[commitID]
		message := pythonparity.Strip(commit.Message)
		if message != "" {
			line := firstLine(message)
			return stringPointer("commit"), &line
		}
	}

	if len(issueIDs) > 0 {
		item := input.WorkItems[issueIDs[0]]
		itemType := pythonparity.Strip(item.Type)
		if itemType == "" {
			itemType = "issue"
		}
		return &itemType, nil
	}
	if len(prIDs) > 0 {
		return stringPointer("pr"), nil
	}
	if len(commitIDs) > 0 {
		return stringPointer("commit"), nil
	}
	return nil, nil
}

func stringPointer(value string) *string { return &value }

// sortedCopy returns a sorted COPY of ids -- Python's sorted() never mutates
// its argument, and issue_ids/pr_ids/commit_ids are shared with other
// callers in the same materialize run.
func sortedCopy(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	copied := make([]string, len(ids))
	copy(copied, ids)
	sort.Strings(copied)
	return copied
}

// firstLine ports materialize._first_line: the first line (per Python's
// str.splitlines(), a wider line-boundary set than "\n") that is non-empty
// after stripping, or the whole stripped text if every line is blank.
func firstLine(text string) string {
	for _, line := range pythonparity.SplitLines(text) {
		if stripped := pythonparity.Strip(line); stripped != "" {
			return stripped
		}
	}
	return pythonparity.Strip(text)
}
