package plant

// buildWorkItemRow is allowed: the census skips it.
func buildWorkItemRow() githubWorkItemRow {
	row := githubWorkItemRow{WorkItemID: "jira:OPS-1"}
	row.ParentID = nil
	return row
}

// helperBypass sits in the allowed file name but is not an allowed function.
func helperBypass() githubWorkItemRow {
	return githubWorkItemRow{WorkItemID: "jira:OPS-2"}
}
