package streamhandlers

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/externalingest"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

type externalFieldType uint8

const (
	externalString externalFieldType = iota
	externalInteger
	externalNumber
	externalBoolean
	externalArray
	externalObject
	externalDateTime
)

type externalFieldRule struct {
	kind     externalFieldType
	required bool
	enum     []string
}

// externalRecordSchemas is a field-NAME reference per record kind, read by
// stored_version_test.go's contract-completeness checks (does every
// declared field reach a sink column or a named "unstored" reason) --
// NOT a validation rule engine any more (CHAOS-6345, no second
// implementation of the shape rules): normalizeExternalRecords
// decides accept/reject through externalingest.ValidateRecords alone, the
// one exact validator. The per-field required/enum metadata below is
// UNUSED for validation decisions now; kept because it costs nothing to
// leave in a name/metadata table and rewriting every literal into a bare
// name set is a mechanical change with no behavioral upside.
var externalRecordSchemas = buildExternalRecordSchemas()

func buildExternalRecordSchemas() map[string]map[string]externalFieldRule {
	requiredString := externalFieldRule{kind: externalString, required: true}
	optionalString := externalFieldRule{kind: externalString}
	requiredDate := externalFieldRule{kind: externalDateTime, required: true}
	optionalDate := externalFieldRule{kind: externalDateTime}
	optionalBool := externalFieldRule{kind: externalBoolean}
	optionalArray := externalFieldRule{kind: externalArray}
	optionalNumber := externalFieldRule{kind: externalNumber}
	optionalInt := externalFieldRule{kind: externalInteger}
	operational := map[string]externalFieldRule{
		"externalId": requiredString, "sourceVersionAt": requiredDate,
		"sourceUrl": optionalString, "sourceEventAt": optionalDate, "sourceEventId": optionalString,
		"rawStatus": optionalString, "rawSeverity": optionalString, "rawPriority": optionalString,
		"normalizedStatus":       {kind: externalString, enum: []string{"active", "open", "acknowledged", "resolved", "closed", "suppressed"}},
		"normalizedSeverity":     {kind: externalString, enum: []string{"critical", "high", "medium", "low", "info"}},
		"normalizedPriority":     {kind: externalString, enum: []string{"critical", "high", "medium", "low"}},
		"relationshipProvenance": optionalString, "relationshipConfidence": optionalNumber,
	}
	withOperational := func(extra map[string]externalFieldRule) map[string]externalFieldRule {
		fields := cloneRules(operational)
		for key, rule := range extra {
			fields[key] = rule
		}
		return fields
	}
	schemas := map[string]map[string]externalFieldRule{
		"repository.v1": {
			"externalId": requiredString, "sourceSystem": {kind: externalString, required: true, enum: []string{"github", "gitlab", "custom"}},
			"defaultRef": optionalString, "tags": optionalArray, "settings": {kind: externalObject},
		},
		"identity.v1": {
			"canonicalId": requiredString, "displayName": optionalString, "email": optionalString,
			"providerIdentities": {kind: externalObject}, "teamIds": optionalArray,
			"isActive": optionalBool, "updatedAt": requiredDate,
		},
		"team.v1": {
			"id": requiredString, "name": requiredString, "description": optionalString,
			"members": optionalArray, "projectKeys": optionalArray, "repoPatterns": optionalArray,
			"isActive": optionalBool, "updatedAt": requiredDate, "nativeTeamKey": optionalString, "parentTeamId": optionalString,
		},
		"work_item.v1": {
			"externalKey": requiredString,
			"provider":    {kind: externalString, required: true, enum: []string{"jira", "github", "gitlab", "linear"}},
			"title":       requiredString,
			"type":        {kind: externalString, enum: []string{"story", "task", "bug", "epic", "pr", "merge_request", "issue", "incident", "chore", "unknown"}},
			"status":      {kind: externalString, required: true, enum: workItemStatuses()},
			"statusRaw":   optionalString, "description": optionalString, "repositoryExternalId": optionalString,
			"nativeTeamKey": optionalString, "projectKey": optionalString, "projectId": optionalString, "projectName": optionalString,
			"assignees": optionalArray, "reporter": optionalString, "createdAt": requiredDate, "updatedAt": optionalDate,
			"startedAt": optionalDate, "completedAt": optionalDate, "closedAt": optionalDate, "labels": optionalArray,
			"storyPoints": optionalNumber, "sprintId": optionalString, "sprintName": optionalString,
			"parentId": optionalString, "epicId": optionalString, "url": optionalString, "priorityRaw": optionalString,
			"serviceClass": optionalString, "dueAt": optionalDate,
		},
		"work_item_transition.v1": {
			"externalKey":  requiredString,
			"provider":     {kind: externalString, required: true, enum: []string{"jira", "github", "gitlab", "linear"}},
			"workItemType": {kind: externalString, enum: []string{"issue", "pr", "merge_request"}},
			"occurredAt":   requiredDate, "fromStatusRaw": optionalString, "toStatusRaw": optionalString,
			"fromStatus": {kind: externalString, required: true, enum: workItemStatuses()},
			"toStatus":   {kind: externalString, required: true, enum: workItemStatuses()},
			"actor":      optionalString,
		},
		// CHAOS-4194 / CHAOS-4193 final shape (Context Fabric, 2026-08-24).
		//
		// `subjectKind` is REQUIRED with a closed two-value enum, and it is the
		// POSITIVE declaration the subject derivation branches on. That shape is
		// deliberate: an earlier build refused pull requests by rejecting the
		// value "pr" on `workItemType`, and a PR payload need only OMIT the field
		// to fall through to the issue-shaped id derivation and be accepted
		// (codex adversarial review, round 1). Requiring the declaration closes
		// that whether or not the value is one this table admits.
		//
		// `workItemType` stays optional in this table and is enforced by
		// refuseProjectMembershipContradiction instead, because what it must say
		// depends on `subjectKind` -- a field rule cannot see a sibling field.
		// A work_item subject must positively declare "issue"; a pull_request
		// subject must not carry one at all.
		//
		// `toProjectId` is OPTIONAL, which reverses the earlier lock. `""` is now
		// the ruled UNASSIGNMENT sentinel -- removed from every project -- and it
		// is only that when `toProjectKey` is empty too. A half-empty destination
		// is a contradiction with no honest reading and is refused in
		// normalization, where both fields are visible at once.
		//
		// `occurredAt` is REQUIRED, which DEVIATES from CHAOS-4194's provisional
		// "else last_synced" default -- deliberately, because that default is
		// incompatible with the locked sorting key. occurred_at is a key member,
		// so a sink-supplied `now` differs on every re-sync of the same provider
		// event, the keys differ, and FINAL returns one row per sync: the exact
		// duplication event_id is in the key to prevent. The sink cannot invent
		// a value stable across re-syncs; only the producer can. Context Fabric
		// ruled A on 2026-08-24: occurred_at stays in the key and producers
		// guarantee a re-sync-stable value.
		//
		// `repositoryExternalId` mirrors work_item.v1 so the subject_id derived
		// here matches the one work_item.v1 derives. Without it, a batch whose
		// source instance is an org while its work items name org/repo produces a
		// DIFFERENT id and the presence edge joins to nothing.
		// work_item_transition.v1 has the same latent hole; this kind does not
		// inherit it, because this is the kind that gets joined. For a
		// pull_request subject it is not merely helpful but REQUIRED -- the PR
		// number alone identifies nothing without its repository.
		//
		// `provider` admits github/jira/linear only. gitlab is absent by ruling:
		// GitLab's own "project" concept IS this schema's repo_id, so a gitlab
		// row would violate the resolve-to-`projects` constraint by construction.
		"project_membership_transition.v1": {
			"subjectKind":          {kind: externalString, required: true, enum: []string{"work_item", "pull_request"}},
			"externalKey":          requiredString,
			"provider":             {kind: externalString, required: true, enum: []string{"jira", "github", "linear"}},
			"eventId":              requiredString,
			"workItemType":         {kind: externalString, enum: []string{"issue"}},
			"occurredAt":           requiredDate,
			"repositoryExternalId": optionalString,
			"fromProjectId":        optionalString, "toProjectId": optionalString,
			"fromProjectKey": optionalString, "toProjectKey": optionalString,
			"actor": optionalString,
		},
		"work_item_dependency.v1": {
			"sourceExternalKey": requiredString, "sourceWorkItemType": {kind: externalString, enum: []string{"issue", "pr", "merge_request"}},
			"targetExternalKey": requiredString, "targetWorkItemType": {kind: externalString, enum: []string{"issue", "pr", "merge_request"}},
			"relationshipType":    {kind: externalString, required: true, enum: []string{"blocks", "blocked_by", "relates_to", "duplicates", "parent_of", "child_of"}},
			"relationshipTypeRaw": optionalString,
		},
		"pull_request.v1": {
			"repositoryExternalId": requiredString, "number": {kind: externalInteger, required: true},
			"title": optionalString, "body": optionalString,
			"state":      {kind: externalString, required: true, enum: []string{"open", "closed", "merged"}},
			"authorName": optionalString, "authorEmail": optionalString, "createdAt": requiredDate,
			"mergedAt": optionalDate, "closedAt": optionalDate, "headBranch": optionalString, "baseBranch": optionalString,
			"additions": optionalInt, "deletions": optionalInt, "changedFiles": optionalInt,
			"firstReviewAt": optionalDate, "firstCommentAt": optionalDate,
			"changesRequestedCount": optionalInt, "reviewsCount": optionalInt, "commentsCount": optionalInt, "url": optionalString,
		},
		"review.v1": {
			"repositoryExternalId": requiredString, "pullRequestNumber": {kind: externalInteger, required: true},
			"reviewId": requiredString, "reviewer": requiredString,
			"state":       {kind: externalString, required: true, enum: []string{"APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED", "PENDING"}},
			"submittedAt": requiredDate,
		},
		"commit.v1": {
			"repositoryExternalId": requiredString, "hash": requiredString, "message": optionalString,
			"authorName": optionalString, "authorEmail": optionalString, "authorWhen": requiredDate,
			"committerName": optionalString, "committerEmail": optionalString, "committerWhen": optionalDate, "parents": optionalInt,
		},
	}
	schemas["operational_service.v1"] = withOperational(map[string]externalFieldRule{
		"name": requiredString, "description": optionalString, "serviceType": optionalString,
		"owningTeamExternalId": optionalString, "escalationPolicyExternalId": optionalString,
		"isDeleted": optionalBool, "deletedAt": optionalDate,
	})
	schemas["operational_incident.v1"] = withOperational(map[string]externalFieldRule{
		"title": requiredString, "description": optionalString, "serviceExternalId": optionalString,
		"escalationPolicyExternalId": optionalString, "startedAt": optionalDate, "resolvedAt": optionalDate,
		"isDeleted": optionalBool, "deletedAt": optionalDate,
	})
	schemas["operational_alert.v1"] = withOperational(map[string]externalFieldRule{
		"title": requiredString, "description": optionalString, "serviceExternalId": optionalString,
		"incidentExternalId": optionalString, "triggeredAt": optionalDate, "acknowledgedAt": optionalDate,
		"resolvedAt": optionalDate, "isDeleted": optionalBool, "deletedAt": optionalDate,
	})
	schemas["incident_timeline_event.v1"] = withOperational(map[string]externalFieldRule{
		"incidentExternalId": requiredString, "eventType": requiredString, "body": optionalString,
		"actorType": optionalString, "actorExternalId": optionalString, "occurredAt": optionalDate,
	})
	schemas["incident_note.v1"] = withOperational(map[string]externalFieldRule{
		"incidentExternalId": requiredString, "body": requiredString, "authorUserExternalId": optionalString, "createdAt": optionalDate,
	})
	schemas["incident_responder.v1"] = withOperational(map[string]externalFieldRule{
		"incidentExternalId": requiredString, "userExternalId": optionalString, "responderName": optionalString,
		"role": optionalString, "responderAssignmentId": optionalString, "requestedAt": optionalDate,
		"assignedAt": optionalDate, "acknowledgedAt": optionalDate, "completedAt": optionalDate,
	})
	for _, kind := range []string{"escalation_policy.v1", "operational_team.v1"} {
		schemas[kind] = withOperational(map[string]externalFieldRule{
			"name": requiredString, "description": optionalString, "isDeleted": optionalBool, "deletedAt": optionalDate,
		})
	}
	schemas["on_call_schedule.v1"] = withOperational(map[string]externalFieldRule{
		"name": requiredString, "description": optionalString, "timezone": optionalString,
		"isDeleted": optionalBool, "deletedAt": optionalDate,
	})
	schemas["on_call_assignment.v1"] = withOperational(map[string]externalFieldRule{
		"scheduleExternalId": optionalString, "userExternalId": optionalString, "escalationPolicyExternalId": optionalString,
		"escalationLevel": optionalInt, "startsAt": optionalDate, "endsAt": optionalDate,
	})
	schemas["operational_user.v1"] = withOperational(map[string]externalFieldRule{
		"displayName": requiredString, "email": optionalString, "isDeleted": optionalBool, "deletedAt": optionalDate,
	})
	schemas["service_repository_mapping.v1"] = withOperational(map[string]externalFieldRule{
		"serviceExternalId": requiredString, "repoFullName": requiredString,
		"repoProvider": {kind: externalString, required: true, enum: []string{"github", "gitlab", "custom"}},
		"mappingKind":  optionalString, "ruleId": optionalString, "validFrom": optionalDate, "validTo": optionalDate, "isActive": optionalBool,
	})
	return schemas
}

// externalKindsWithoutPythonModel names record kinds this worker accepts
// that externalingest.ValidateRecords cannot validate at all, because
// RECORD_KIND_MODELS (schemas.py) has no entry for them -- confirmed by
// reading schemas.py's RECORD_KIND_MODELS dict directly (21 kinds) against
// this package's 22, and normalize.py's ALLOWED_KINDS_BY_SYSTEM (every
// frozenset it is built from), not inferred: the Python worker's
// normalize_batch has never validated or accepted a
// project_membership_transition.v1 record, ever, on any system. Routing
// this kind through ValidateRecords like every other one would report
// unknown_kind for every record of it -- a real behavior change (today's
// production Go worker accepts and processes it via this file's own field
// table + refuseProjectMembershipContradiction) with no Python reference
// to prove correct against. Escalated on the CHAOS-6345 design thread
// rather than silently changed or silently left as a second
// implementation; this is the ONE kind still validated by this package's
// own field-table rules (validateExternalValue below) until that resolves.
var externalKindsWithoutPythonModel = map[string]bool{
	"project_membership_transition.v1": true,
}

// validateExternalRecord reports whether normalize_batch would accept
// kind's payload for every kind BUT externalKindsWithoutPythonModel,
// checking (in Python's order) externalingest.ValidateRecords' shape
// rules, then the one post-shape semantic check normalize.py applies
// outside validate_records itself (service_repository_mapping.v1's
// repository_identity_required -- ServiceRepositoryMappingV1.repo_full_name/
// repo_provider are both Optional at the pydantic model level, so shape
// validation alone never catches either one's absence). A thin
// compatibility wrapper for existing tests that check "would Python accept
// this fixture" as a precondition (project-membership contradiction
// fixtures, the stored-version golden-fixture reference test) -- NOT a
// second validation rule set (CHAOS-6345) for every kind Python
// actually has: the shape RULES for those live only in
// externalingest.ValidateRecords. normalizeExternalRecords (the production
// accept/reject path) never calls this -- it uses
// externalShapeErrorsByIndex, which passes OrderedPayload (input order
// preserved) directly, plus its own inline repository_identity_required
// check with pointer/source context this function does not have. This
// wrapper round-trips payload through JSON to reach pyjson.Decode, so key
// ORDER here is Go map iteration order, not input order -- fine for these
// callers, which check accept/reject on one payload, never which of
// several simultaneous errors ValidateRecords reports first.
func validateExternalRecord(kind string, payload map[string]any) error {
	if externalKindsWithoutPythonModel[kind] {
		return validateExternalRecordFieldTable(kind, payload)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("%s: %w", kind, err)
	}
	decoded, err := pyjson.Decode(raw)
	if err != nil {
		return fmt.Errorf("%s: %w", kind, err)
	}
	ordered, _ := decoded.(*pyjson.Object)
	items := externalingest.ValidateRecords([]externalingest.RecordInput{{Kind: kind, Payload: ordered}})
	if len(items) > 0 {
		return fmt.Errorf("%s: %s", items[0].Path, items[0].Message)
	}
	if kind == "service_repository_mapping.v1" {
		if stringField(payload, "repoFullName") == "" || stringField(payload, "repoProvider") == "" {
			return fmt.Errorf("service_repository_mapping requires repoFullName and repoProvider")
		}
	}
	return nil
}

// validateExternalRecordFieldTable is this file's PRE-CHAOS-6345 validator,
// kept alive ONLY for externalKindsWithoutPythonModel: required/extra/type/
// enum checks against externalRecordSchemas, exactly as this file always
// ran them. Not a second implementation for any kind ValidateRecords can
// reach -- validateExternalRecord only calls this for the one kind it
// cannot.
func validateExternalRecordFieldTable(kind string, payload map[string]any) error {
	schema, ok := externalRecordSchemas[kind]
	if !ok {
		return fmt.Errorf("unsupported record kind")
	}
	for key := range payload {
		if _, allowed := schema[key]; !allowed {
			return fmt.Errorf("unexpected field %q", key)
		}
	}
	for name, rule := range schema {
		value, present := payload[name]
		if !present || value == nil {
			if rule.required {
				return fmt.Errorf("%s is required", name)
			}
			continue
		}
		if err := validateExternalValue(name, value, rule); err != nil {
			return err
		}
	}
	return nil
}

func validateExternalValue(name string, value any, rule externalFieldRule) error {
	valid := false
	switch rule.kind {
	case externalString:
		text, ok := value.(string)
		valid = ok && (!rule.required || strings.TrimSpace(text) != "")
		if valid && len(rule.enum) > 0 {
			valid = slices.Contains(rule.enum, text)
		}
	case externalInteger:
		number, ok := value.(json.Number)
		if ok {
			_, err := number.Int64()
			valid = err == nil
		}
	case externalNumber:
		_, valid = numberField(map[string]any{name: value}, name)
	case externalBoolean:
		_, valid = value.(bool)
	case externalArray:
		_, valid = value.([]any)
	case externalObject:
		_, valid = value.(map[string]any)
	case externalDateTime:
		text, ok := value.(string)
		if ok {
			parsed, err := time.Parse(time.RFC3339Nano, text)
			valid = err == nil && !parsed.IsZero()
		}
	}
	if !valid {
		return fmt.Errorf("%s has invalid type or value", name)
	}
	return nil
}

func cloneRules(source map[string]externalFieldRule) map[string]externalFieldRule {
	result := make(map[string]externalFieldRule, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func workItemStatuses() []string {
	return []string{"backlog", "todo", "in_progress", "in_review", "blocked", "done", "canceled", "unknown"}
}

func stringField(payload map[string]any, name string) string {
	value, _ := payload[name].(string)
	return value
}

func numberField(payload map[string]any, name string) (float64, bool) {
	switch value := payload[name].(type) {
	case json.Number:
		number, err := value.Float64()
		return number, err == nil && !math.IsInf(number, 0) && !math.IsNaN(number)
	case float64:
		return value, !math.IsInf(value, 0) && !math.IsNaN(value)
	default:
		return 0, false
	}
}

func integerField(payload map[string]any, name string) (int64, bool) {
	switch value := payload[name].(type) {
	case json.Number:
		number, err := value.Int64()
		return number, err == nil
	case float64:
		return int64(value), value == math.Trunc(value)
	default:
		return 0, false
	}
}
