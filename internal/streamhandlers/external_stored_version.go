package streamhandlers

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

func unstated(column string, fields ...string) storedversion.Column {
	return storedversion.Column{Name: column, Rule: storedversion.Unstated, Fields: fields}
}

func from(column string, rule storedversion.Rule, fields ...string) storedversion.Column {
	return storedversion.Column{Name: column, Rule: rule, Fields: fields}
}

var (
	externalPullRequestContract = storedversion.Contract{Writer: "external_ingest", Table: "git_pull_requests", Columns: []storedversion.Column{
		from("repo_id", storedversion.Identity, "repositoryExternalId"), from("number", storedversion.Identity, "number"),
		unstated("title", "title"), unstated("body", "body"), from("state", storedversion.Stated, "state"),
		unstated("author_name", "authorName"), unstated("author_email", "authorEmail"),
		from("created_at", storedversion.Stated, "createdAt"),
		{Name: "merged_at", Rule: storedversion.Unstated, Fields: []string{"mergedAt"}, Terminal: true},
		from("closed_at", storedversion.StateCoupled, "closedAt"),
		unstated("head_branch", "headBranch"), unstated("base_branch", "baseBranch"),
		unstated("additions", "additions"), unstated("deletions", "deletions"), unstated("changed_files", "changedFiles"),
		{Name: "first_review_at", Rule: storedversion.Unstated, Fields: []string{"firstReviewAt"}, Terminal: true},
		unstated("first_comment_at", "firstCommentAt"),
		unstated("changes_requested_count", "changesRequestedCount"),
		unstated("reviews_count", "reviewsCount"), unstated("comments_count", "commentsCount"),
		idLastSynced, idSource, idOrg,
	}}
	externalReviewContract = storedversion.Contract{Writer: "external_ingest", Table: "git_pull_request_reviews", Columns: []storedversion.Column{
		from("repo_id", storedversion.Identity, "repositoryExternalId"), from("number", storedversion.Identity, "pullRequestNumber"),
		from("review_id", storedversion.Identity, "reviewId"), from("reviewer", storedversion.Stated, "reviewer"),
		from("state", storedversion.Stated, "state"), from("submitted_at", storedversion.Stated, "submittedAt"),
		idLastSynced, idSource, idOrg,
	}}
	externalCommitContract = storedversion.Contract{Writer: "external_ingest", Table: "git_commits", Columns: []storedversion.Column{
		from("repo_id", storedversion.Identity, "repositoryExternalId"), from("hash", storedversion.Identity, "hash"),
		unstated("message", "message"), unstated("author_name", "authorName"), unstated("author_email", "authorEmail"),
		from("author_when", storedversion.Stated, "authorWhen"),
		unstated("committer_name", "committerName"), unstated("committer_email", "committerEmail"),
		unstated("committer_when", "committerWhen"), unstated("parents", "parents"),
		idLastSynced, idSource, idOrg,
	}}
)

var (
	externalRepositoryContract = storedversion.Contract{Writer: "external_ingest", Table: "repos", Columns: []storedversion.Column{
		from("id", storedversion.Identity, "sourceSystem", "externalId"), from("repo", storedversion.Stated, "externalId"),
		unstated("ref", "defaultRef"),
		{Name: "created_at", Rule: storedversion.NoField},
		unstated("settings", "settings"), unstated("tags", "tags"),
		from("provider", storedversion.Stated, "sourceSystem"),
		idLastSynced, idSource, idOrg,
	}}
	externalIdentityContract = storedversion.Contract{Writer: "external_ingest", Table: "identities", Columns: []storedversion.Column{
		idOrg, from("canonical_id", storedversion.Identity, "canonicalId"),
		{Name: "identity_uuid", Rule: storedversion.Writer},
		unstated("display_name", "displayName"), unstated("email", "email"),
		unstated("provider_identities", "providerIdentities"), unstated("team_ids", "teamIds"),
		unstated("is_active", "isActive"), from("updated_at", storedversion.Stated, "updatedAt"), idSource,
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
func externalWorkItemContract(system string) storedversion.Contract {
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
	return storedversion.Contract{Writer: "external_ingest", Table: "work_items", Columns: contractColumns(
		[]storedversion.Column{
			from("repo_id", storedversion.Identity, "repositoryExternalId"),
			from("work_item_id", storedversion.Identity, "externalKey", "type"),
			{Name: "provider", Rule: storedversion.Writer}, from("title", storedversion.Stated, "title"),
			unstated("type", "type"), from("status", storedversion.Stated, "status"), unstated("status_raw", "statusRaw"),
		},
		project,
		[]storedversion.Column{
			unstated("assignees", "assignees"), unstated("reporter", "reporter"),
			from("created_at", storedversion.Stated, "createdAt"), unstated("updated_at", "updatedAt"),
			from("started_at", storedversion.StateCoupled, "startedAt"), from("completed_at", storedversion.StateCoupled, "completedAt"),
			from("closed_at", storedversion.StateCoupled, "closedAt"),
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

func appended(column, field string, translate func(map[string]any, string) any) storedversion.Column {
	return storedversion.Column{
		Name: column, Rule: storedversion.Unstated, Fields: []string{field},
		Value: func(payload map[string]any) any { return translate(payload, field) },
	}
}

// externalContract returns the contract table for a kind whose table is in
// scope; other kinds write as their translation produces.
func externalContract(kind, system string) (storedversion.Contract, bool) {
	switch kind {
	case "pull_request.v1":
		return externalPullRequestContract, true
	case "review.v1":
		return externalReviewContract, true
	case "commit.v1":
		return externalCommitContract, true
	case "work_item.v1":
		return externalWorkItemContract(system), true
	case "repository.v1":
		return externalRepositoryContract, true
	case "identity.v1":
		return externalIdentityContract, true
	}
	return storedversion.Contract{}, false
}

// withContractColumns appends to an insert the contract columns its column
// list does not name, in contract order, and returns their names.
func withContractColumns(insert string, contract storedversion.Contract) (string, []string, error) {
	positions, err := storedversion.Positions(insert)
	if err != nil {
		return "", nil, err
	}
	var missing []string
	for _, column := range contract.Columns {
		if _, ok := positions[column.Name]; !ok {
			missing = append(missing, column.Name)
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
func appendedValues(c storedversion.Contract, columns []string, payload map[string]any) []any {
	values := make([]any, len(columns))
	for i, name := range columns {
		for _, column := range c.Columns {
			if column.Name == name && column.Value != nil {
				values[i] = column.Value(payload)
			}
		}
	}
	return values
}

// externalCarry applies the contract to one record: the stored payload keeps
// the client's JSON as sent, so an absent key is distinguishable from null.
func externalCarry(contract storedversion.Contract, payload map[string]any) map[string]bool {
	return contract.Carry(func(field string) bool {
		_, present := payload[field]
		return !present
	})
}
