package streamhandlers

import "strings"

func unstated(column string, fields ...string) columnContract {
	return columnContract{column: column, rule: columnUnstated, fields: fields}
}

func from(column string, rule columnRule, fields ...string) columnContract {
	return columnContract{column: column, rule: rule, fields: fields}
}

var (
	externalPullRequestContract = writerContract{writer: "external_ingest", table: "git_pull_requests", columns: []columnContract{
		from("repo_id", columnIdentity, "repositoryExternalId"), from("number", columnIdentity, "number"),
		unstated("title", "title"), unstated("body", "body"), from("state", columnStated, "state"),
		unstated("author_name", "authorName"), unstated("author_email", "authorEmail"),
		from("created_at", columnStated, "createdAt"),
		{column: "merged_at", rule: columnUnstated, fields: []string{"mergedAt"}, terminal: true},
		from("closed_at", columnStateCoupled, "closedAt"),
		unstated("head_branch", "headBranch"), unstated("base_branch", "baseBranch"),
		unstated("additions", "additions"), unstated("deletions", "deletions"), unstated("changed_files", "changedFiles"),
		{column: "first_review_at", rule: columnUnstated, fields: []string{"firstReviewAt"}, terminal: true},
		unstated("first_comment_at", "firstCommentAt"),
		unstated("changes_requested_count", "changesRequestedCount"),
		unstated("reviews_count", "reviewsCount"), unstated("comments_count", "commentsCount"),
		idLastSynced, idSource, idOrg,
	}}
	externalReviewContract = writerContract{writer: "external_ingest", table: "git_pull_request_reviews", columns: []columnContract{
		from("repo_id", columnIdentity, "repositoryExternalId"), from("number", columnIdentity, "pullRequestNumber"),
		from("review_id", columnIdentity, "reviewId"), from("reviewer", columnStated, "reviewer"),
		from("state", columnStated, "state"), from("submitted_at", columnStated, "submittedAt"),
		idLastSynced, idSource, idOrg,
	}}
	externalCommitContract = writerContract{writer: "external_ingest", table: "git_commits", columns: []columnContract{
		from("repo_id", columnIdentity, "repositoryExternalId"), from("hash", columnIdentity, "hash"),
		unstated("message", "message"), unstated("author_name", "authorName"), unstated("author_email", "authorEmail"),
		from("author_when", columnStated, "authorWhen"),
		unstated("committer_name", "committerName"), unstated("committer_email", "committerEmail"),
		unstated("committer_when", "committerWhen"), unstated("parents", "parents"),
		idLastSynced, idSource, idOrg,
	}}
)

// externalUnstoredFields names each declared payload key that reaches no
// column, with the reason. Keys are "<kind>" or "<kind>/<system>".
var externalUnstoredFields = map[string]map[string]string{
	"pull_request.v1": {"url": "git_pull_requests has no url column"},
	"work_item.v1":    {"provider": "the provider column is the ingestion's source system"},
	"work_item.v1/jira": {
		"nativeTeamKey": "a jira project is its projectKey", "projectId": "a jira project is its projectKey",
		"projectName": "a jira project is its projectKey",
	},
	"work_item.v1/github": {
		"nativeTeamKey": "a github project is its repository", "projectKey": "a github project is its repository",
		"projectId": "a github project is its repository", "projectName": "a github project is its repository",
	},
	"work_item.v1/gitlab": {
		"nativeTeamKey": "a gitlab project is its repository", "projectKey": "a gitlab project is its repository",
		"projectId": "a gitlab project is its repository", "projectName": "a gitlab project is its repository",
	},
	"work_item.v1/linear": {"projectKey": "a linear project is its projectId, projectName or team"},
}

// externalWorkItemContract follows externalProjectScope: a project column is
// stated only by the payload keys that system derives it from.
// description, priority_raw, service_class and due_at are not produced by
// the Python-parity translation, so this writer appends them from the payload.
func externalWorkItemContract(system string) writerContract {
	project := noField("project_key", "project_id", "native_team_key", "project_name")
	switch system {
	case "jira":
		project[0] = unstated("project_key", "projectKey")
	case "github", "gitlab":
		project[1] = unstated("project_id", "repositoryExternalId")
	case "linear":
		project[1] = unstated("project_id", "projectId", "projectName", "nativeTeamKey")
		project[2] = unstated("native_team_key", "nativeTeamKey")
		project[3] = unstated("project_name", "projectName")
	}
	return writerContract{writer: "external_ingest", table: "work_items", columns: contractColumns(
		[]columnContract{
			from("repo_id", columnIdentity, "repositoryExternalId"),
			from("work_item_id", columnIdentity, "externalKey", "type"),
			{column: "provider", rule: columnWriter}, from("title", columnStated, "title"),
			unstated("type", "type"), from("status", columnStated, "status"), unstated("status_raw", "statusRaw"),
		},
		project,
		[]columnContract{
			unstated("assignees", "assignees"), unstated("reporter", "reporter"),
			from("created_at", columnStated, "createdAt"), unstated("updated_at", "updatedAt"),
			from("started_at", columnStateCoupled, "startedAt"), from("completed_at", columnStateCoupled, "completedAt"),
			from("closed_at", columnStateCoupled, "closedAt"),
			unstated("labels", "labels"), unstated("story_points", "storyPoints"),
			unstated("sprint_id", "sprintId"), unstated("sprint_name", "sprintName"),
			unstated("parent_id", "parentId"), unstated("epic_id", "epicId"), unstated("url", "url"),
			idLastSynced, idOrg, idSource,
			appended("description", "description", externalNullableString),
			appended("priority_raw", "priorityRaw", externalNullableString),
			appended("service_class", "serviceClass", externalNullableString),
			appended("due_at", "dueAt", externalNullableTime),
		},
	)}
}

func appended(column, field string, translate func(map[string]any, string) any) columnContract {
	return columnContract{
		column: column, rule: columnUnstated, fields: []string{field},
		value: func(payload map[string]any) any { return translate(payload, field) },
	}
}

// externalContract returns the contract table for a kind whose table is in
// scope; other kinds write as their translation produces.
func externalContract(kind, system string) (writerContract, bool) {
	switch kind {
	case "pull_request.v1":
		return externalPullRequestContract, true
	case "review.v1":
		return externalReviewContract, true
	case "commit.v1":
		return externalCommitContract, true
	case "work_item.v1":
		return externalWorkItemContract(system), true
	}
	return writerContract{}, false
}

// withContractColumns appends to an insert the contract columns its column
// list does not name, in contract order, and returns their names.
func withContractColumns(insert string, contract writerContract) (string, []string, error) {
	positions, err := insertColumnPositions(insert)
	if err != nil {
		return "", nil, err
	}
	var missing []string
	for _, column := range contract.columns {
		if _, ok := positions[column.column]; !ok {
			missing = append(missing, column.column)
		}
	}
	if len(missing) == 0 {
		return insert, nil, nil
	}
	end := strings.LastIndexByte(insert, ')')
	return insert[:end] + "," + strings.Join(missing, ",") + insert[end:], missing, nil
}

// appendedValues translates the appended columns of one record; a column
// without a translation starts as nil and is kept by its R1 rule.
func (c writerContract) appendedValues(columns []string, payload map[string]any) []any {
	values := make([]any, len(columns))
	for i, name := range columns {
		for _, column := range c.columns {
			if column.column == name && column.value != nil {
				values[i] = column.value(payload)
			}
		}
	}
	return values
}

// externalCarry applies the contract to one record: the stored payload keeps
// the client's JSON as sent, so an absent key is distinguishable from null.
func externalCarry(contract writerContract, payload map[string]any) map[string]bool {
	return contract.carry(func(field string) bool {
		_, present := payload[field]
		return !present
	})
}
