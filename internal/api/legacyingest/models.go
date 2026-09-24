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
