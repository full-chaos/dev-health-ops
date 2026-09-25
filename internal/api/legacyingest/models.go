package legacyingest

import (
	"math/big"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// The request models of api/ingest/schemas.py, validated field by field in
// declaration order so the 422 lists errors in pydantic's order, and dumped
// as model_dump_json does (every field in declaration order, defaults
// included; an extra key the model ignores is not written).

// maxItems is the `items` bound of every request but telemetry.
const maxItems = 1000

// optional is a `T | None = None` field's dump value: null unless present.
func optional[T any](field pybody.Field[T], convert func(T) pyjson.Value) pyjson.Value {
	if !field.Set || field.Null {
		return nil
	}
	return convert(field.Value)
}

func text(value string) pyjson.Value            { return value }
func moment(value pytime.DateTime) pyjson.Value { return pytime.Pydantic(value) }

// batch is a validated request: its dump, its item count, its org.
type batch struct {
	OrgID string
	Dump  *pyjson.Object
	Items int
}

// parser validates one request body.
type parser func(body pybody.Body) (batch, pybody.Errors)

// batchParser builds the parser of an IngestBatchRequest subclass:
// org_id, repo_url (when withRepo), then items.
func batchParser(withRepo bool, item func(pybody.Model) (*pyjson.Object, bool)) parser {
	items := pybody.SizedModelList(1, maxItems, item)
	return func(body pybody.Body) (batch, pybody.Errors) {
		var errs pybody.Errors
		object, ok := errs.Object(body)
		if !ok {
			return batch{}, errs
		}
		m := pybody.Model{Errors: &errs, Object: object, Loc: []pyjson.Value{"body"}}
		orgID := pybody.Get(m, "org_id", pybody.Required, pybody.Str)
		repoURL := pybody.Field[string]{OK: true}
		if withRepo {
			repoURL = pybody.Get(m, "repo_url", pybody.Required, pybody.Str)
		}
		list := pybody.Get(m, "items", pybody.Required, items)
		if !(orgID.OK && repoURL.OK && list.OK) || len(errs) > 0 {
			return batch{}, errs
		}
		dump := pyjson.NewObject()
		dump.Set("org_id", orgID.Value)
		if withRepo {
			dump.Set("repo_url", repoURL.Value)
		}
		values := make([]pyjson.Value, len(list.Value))
		for index, item := range list.Value {
			values[index] = item
		}
		dump.Set("items", values)
		return batch{OrgID: orgID.Value, Dump: dump, Items: len(values)}, nil
	}
}

// parseCommit is IngestCommit.
func parseCommit(m pybody.Model) (*pyjson.Object, bool) {
	hash := pybody.Get(m, "hash", pybody.Required, pybody.Str)
	message := pybody.Get(m, "message", pybody.Required, pybody.Str)
	authorName := pybody.Get(m, "author_name", pybody.Required, pybody.Str)
	authorEmail := pybody.Get(m, "author_email", pybody.Required, pybody.Str)
	authorWhen := pybody.Get(m, "author_when", pybody.Required, pybody.Datetime)
	committerName := pybody.Get(m, "committer_name", pybody.Nullable, pybody.Str)
	committerEmail := pybody.Get(m, "committer_email", pybody.Nullable, pybody.Str)
	committerWhen := pybody.Get(m, "committer_when", pybody.Nullable, pybody.Datetime)
	parents := pybody.Get(m, "parents", pybody.Defaulted, pybody.Int)
	ok := hash.OK && message.OK && authorName.OK && authorEmail.OK && authorWhen.OK &&
		committerName.OK && committerEmail.OK && committerWhen.OK && parents.OK
	if !ok {
		return nil, false
	}
	out := pyjson.NewObject()
	out.Set("hash", hash.Value)
	out.Set("message", message.Value)
	out.Set("author_name", authorName.Value)
	out.Set("author_email", authorEmail.Value)
	out.Set("author_when", moment(authorWhen.Value))
	out.Set("committer_name", optional(committerName, text))
	out.Set("committer_email", optional(committerEmail, text))
	out.Set("committer_when", optional(committerWhen, moment))
	if parents.Set {
		out.Set("parents", pyjson.Int{Int: parents.Value})
	} else {
		out.Set("parents", pyjson.IntOf(1))
	}
	return out, true
}

// parseDeployment is IngestDeployment.
func parseDeployment(m pybody.Model) (*pyjson.Object, bool) {
	id := pybody.Get(m, "deployment_id", pybody.Required, pybody.Str)
	status := pybody.Get(m, "status", pybody.Required, pybody.Str)
	environment := pybody.Get(m, "environment", pybody.Required, pybody.Str)
	startedAt := pybody.Get(m, "started_at", pybody.Nullable, pybody.Datetime)
	finishedAt := pybody.Get(m, "finished_at", pybody.Nullable, pybody.Datetime)
	deployedAt := pybody.Get(m, "deployed_at", pybody.Nullable, pybody.Datetime)
	pullRequest := pybody.Get(m, "pull_request_number", pybody.Nullable, pybody.Int)
	releaseRef := pybody.Get(m, "release_ref", pybody.Nullable, pybody.Str)
	confidence := pybody.Get(m, "release_ref_confidence", pybody.Nullable, pybody.Float)
	ok := id.OK && status.OK && environment.OK && startedAt.OK && finishedAt.OK && deployedAt.OK &&
		pullRequest.OK && releaseRef.OK && confidence.OK
	if !ok {
		return nil, false
	}
	out := pyjson.NewObject()
	out.Set("deployment_id", id.Value)
	out.Set("status", status.Value)
	out.Set("environment", environment.Value)
	out.Set("started_at", optional(startedAt, moment))
	out.Set("finished_at", optional(finishedAt, moment))
	out.Set("deployed_at", optional(deployedAt, moment))
	out.Set("pull_request_number", optional(pullRequest, func(value *big.Int) pyjson.Value { return pyjson.Int{Int: value} }))
	out.Set("release_ref", optional(releaseRef, text))
	out.Set("release_ref_confidence", optional(confidence, func(value float64) pyjson.Value { return pyjson.Float(value) }))
	return out, true
}

// parseIncident is IngestIncident.
func parseIncident(m pybody.Model) (*pyjson.Object, bool) {
	id := pybody.Get(m, "incident_id", pybody.Required, pybody.Str)
	status := pybody.Get(m, "status", pybody.Required, pybody.Str)
	startedAt := pybody.Get(m, "started_at", pybody.Required, pybody.Datetime)
	resolvedAt := pybody.Get(m, "resolved_at", pybody.Nullable, pybody.Datetime)
	if !(id.OK && status.OK && startedAt.OK && resolvedAt.OK) {
		return nil, false
	}
	out := pyjson.NewObject()
	out.Set("incident_id", id.Value)
	out.Set("status", status.Value)
	out.Set("started_at", moment(startedAt.Value))
	out.Set("resolved_at", optional(resolvedAt, moment))
	return out, true
}

// intValue is an `int | None` field's dump value.
func intValue(value *big.Int) pyjson.Value { return pyjson.Int{Int: value} }

// parseReview is IngestPullRequestReview: every field required.
func parseReview(m pybody.Model) (*pyjson.Object, bool) {
	id := pybody.Get(m, "review_id", pybody.Required, pybody.Str)
	reviewer := pybody.Get(m, "reviewer", pybody.Required, pybody.Str)
	state := pybody.Get(m, "state", pybody.Required, pybody.Str)
	submittedAt := pybody.Get(m, "submitted_at", pybody.Required, pybody.Datetime)
	if !(id.OK && reviewer.OK && state.OK && submittedAt.OK) {
		return nil, false
	}
	out := pyjson.NewObject()
	out.Set("review_id", id.Value)
	out.Set("reviewer", reviewer.Value)
	out.Set("state", state.Value)
	out.Set("submitted_at", moment(submittedAt.Value))
	return out, true
}

var reviewList = pybody.ModelList(parseReview)

// parsePullRequest is IngestPullRequest (`reviews` defaults to a new empty
// list; a null is refused).
func parsePullRequest(m pybody.Model) (*pyjson.Object, bool) {
	number := pybody.Get(m, "number", pybody.Required, pybody.Int)
	title := pybody.Get(m, "title", pybody.Required, pybody.Str)
	body := pybody.Get(m, "body", pybody.Nullable, pybody.Str)
	state := pybody.Get(m, "state", pybody.Required, pybody.Str)
	authorName := pybody.Get(m, "author_name", pybody.Required, pybody.Str)
	authorEmail := pybody.Get(m, "author_email", pybody.Nullable, pybody.Str)
	createdAt := pybody.Get(m, "created_at", pybody.Required, pybody.Datetime)
	mergedAt := pybody.Get(m, "merged_at", pybody.Nullable, pybody.Datetime)
	closedAt := pybody.Get(m, "closed_at", pybody.Nullable, pybody.Datetime)
	headBranch := pybody.Get(m, "head_branch", pybody.Nullable, pybody.Str)
	baseBranch := pybody.Get(m, "base_branch", pybody.Nullable, pybody.Str)
	additions := pybody.Get(m, "additions", pybody.Nullable, pybody.Int)
	deletions := pybody.Get(m, "deletions", pybody.Nullable, pybody.Int)
	changedFiles := pybody.Get(m, "changed_files", pybody.Nullable, pybody.Int)
	reviews := pybody.Get(m, "reviews", pybody.Defaulted, reviewList)
	ok := number.OK && title.OK && body.OK && state.OK && authorName.OK && authorEmail.OK && createdAt.OK &&
		mergedAt.OK && closedAt.OK && headBranch.OK && baseBranch.OK && additions.OK && deletions.OK &&
		changedFiles.OK && reviews.OK
	if !ok {
		return nil, false
	}
	out := pyjson.NewObject()
	out.Set("number", pyjson.Int{Int: number.Value})
	out.Set("title", title.Value)
	out.Set("body", optional(body, text))
	out.Set("state", state.Value)
	out.Set("author_name", authorName.Value)
	out.Set("author_email", optional(authorEmail, text))
	out.Set("created_at", moment(createdAt.Value))
	out.Set("merged_at", optional(mergedAt, moment))
	out.Set("closed_at", optional(closedAt, moment))
	out.Set("head_branch", optional(headBranch, text))
	out.Set("base_branch", optional(baseBranch, text))
	out.Set("additions", optional(additions, intValue))
	out.Set("deletions", optional(deletions, intValue))
	out.Set("changed_files", optional(changedFiles, intValue))
	list := []pyjson.Value{}
	if reviews.Set {
		for _, review := range reviews.Value {
			list = append(list, review)
		}
	}
	out.Set("reviews", list)
	return out, true
}

var (
	workItemProvider = pybody.Literal("jira", "github", "gitlab", "linear")
	workItemType     = pybody.Literal("story", "task", "bug", "epic", "issue", "incident", "chore", "unknown")
	workItemStatus   = pybody.Literal("backlog", "todo", "in_progress", "in_review", "blocked", "done", "canceled", "unknown")
)

// stringList is a `list[str]` field's dump value.
func stringList(values []string) []pyjson.Value {
	out := make([]pyjson.Value, len(values))
	for index, value := range values {
		out[index] = value
	}
	return out
}

// parseWorkItem is IngestWorkItem (`type` and `status` default to
// "unknown", `assignees` and `labels` to new empty lists).
func parseWorkItem(m pybody.Model) (*pyjson.Object, bool) {
	id := pybody.Get(m, "work_item_id", pybody.Required, pybody.Str)
	provider := pybody.Get(m, "provider", pybody.Required, workItemProvider)
	title := pybody.Get(m, "title", pybody.Required, pybody.Str)
	kind := pybody.Get(m, "type", pybody.Defaulted, workItemType)
	status := pybody.Get(m, "status", pybody.Defaulted, workItemStatus)
	statusRaw := pybody.Get(m, "status_raw", pybody.Nullable, pybody.Str)
	description := pybody.Get(m, "description", pybody.Nullable, pybody.Str)
	projectKey := pybody.Get(m, "project_key", pybody.Nullable, pybody.Str)
	assignees := pybody.Get(m, "assignees", pybody.Defaulted, pybody.StrList)
	reporter := pybody.Get(m, "reporter", pybody.Nullable, pybody.Str)
	createdAt := pybody.Get(m, "created_at", pybody.Required, pybody.Datetime)
	updatedAt := pybody.Get(m, "updated_at", pybody.Nullable, pybody.Datetime)
	startedAt := pybody.Get(m, "started_at", pybody.Nullable, pybody.Datetime)
	completedAt := pybody.Get(m, "completed_at", pybody.Nullable, pybody.Datetime)
	labels := pybody.Get(m, "labels", pybody.Defaulted, pybody.StrList)
	storyPoints := pybody.Get(m, "story_points", pybody.Nullable, pybody.Float)
	priorityRaw := pybody.Get(m, "priority_raw", pybody.Nullable, pybody.Str)
	url := pybody.Get(m, "url", pybody.Nullable, pybody.Str)
	ok := id.OK && provider.OK && title.OK && kind.OK && status.OK && statusRaw.OK && description.OK &&
		projectKey.OK && assignees.OK && reporter.OK && createdAt.OK && updatedAt.OK && startedAt.OK &&
		completedAt.OK && labels.OK && storyPoints.OK && priorityRaw.OK && url.OK
	if !ok {
		return nil, false
	}
	defaulted := func(field pybody.Field[string]) string {
		if field.Set {
			return field.Value
		}
		return "unknown"
	}
	out := pyjson.NewObject()
	out.Set("work_item_id", id.Value)
	out.Set("provider", provider.Value)
	out.Set("title", title.Value)
	out.Set("type", defaulted(kind))
	out.Set("status", defaulted(status))
	out.Set("status_raw", optional(statusRaw, text))
	out.Set("description", optional(description, text))
	out.Set("project_key", optional(projectKey, text))
	out.Set("assignees", stringList(assignees.Value))
	out.Set("reporter", optional(reporter, text))
	out.Set("created_at", moment(createdAt.Value))
	out.Set("updated_at", optional(updatedAt, moment))
	out.Set("started_at", optional(startedAt, moment))
	out.Set("completed_at", optional(completedAt, moment))
	out.Set("labels", stringList(labels.Value))
	out.Set("story_points", optional(storyPoints, func(value float64) pyjson.Value { return pyjson.Float(value) }))
	out.Set("priority_raw", optional(priorityRaw, text))
	out.Set("url", optional(url, text))
	return out, true
}
