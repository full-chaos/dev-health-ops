// Package plant is a builder that bypasses the work item base, for
// TestWorkItemRowBaseCensusCatchesABypass. Line numbers are asserted.
package plant

type githubWorkItemRow struct {
	WorkItemID  string
	ParentID    *string
	EpicID      *string
	DueAtAbsent bool
}

type linearWorkItemRow struct{ WorkItemID string }

func bypassLiteral() githubWorkItemRow {
	empty := githubWorkItemRow{}
	_ = empty
	return githubWorkItemRow{WorkItemID: "jira:OPS-1"}
}

func bypassFieldWrites(row *githubWorkItemRow, parent string) {
	row.DueAtAbsent = true
	row.ParentID = &parent
	row.EpicID = &parent
}

func bypassLinear() []linearWorkItemRow {
	return []linearWorkItemRow{
		{WorkItemID: "linear:ENG-1"},
	}
}
