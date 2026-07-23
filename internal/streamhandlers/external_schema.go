package streamhandlers

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
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

func validateExternalRecord(kind string, payload map[string]any) error {
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
	if confidence, ok := numberField(payload, "relationshipConfidence"); ok && (confidence < 0 || confidence > 1) {
		return fmt.Errorf("relationshipConfidence must be between 0 and 1")
	}
	for _, name := range []string{"number", "pullRequestNumber", "additions", "deletions", "changedFiles", "changesRequestedCount", "reviewsCount", "commentsCount", "parents", "escalationLevel"} {
		if value, ok := integerField(payload, name); ok && value < 0 {
			return fmt.Errorf("%s must be non-negative", name)
		}
	}
	if value, ok := integerField(payload, "number"); ok && value < 1 {
		return fmt.Errorf("number must be at least 1")
	}
	if value, ok := integerField(payload, "pullRequestNumber"); ok && value < 1 {
		return fmt.Errorf("pullRequestNumber must be at least 1")
	}
	if kind == "commit.v1" {
		hash := stringField(payload, "hash")
		if len(hash) < 7 || len(hash) > 64 {
			return fmt.Errorf("hash length must be between 7 and 64")
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
