package externalingest

// fieldType is the wire type a field's JSON value must have.
type fieldType int

const (
	fString fieldType = iota
	fBool
	fNumber
	fDatetime // an RFC3339 string
	fStringList
	fDict
)

// field is one property of a record-kind payload (schemas.py's per-kind
// Pydantic models, e.g. WorkItemV1), enough to reproduce
// validate.py's validate_records() shape checks: presence, JSON type,
// enum/literal membership, and length/numeric bounds. It intentionally does
// NOT reproduce Pydantic's exact error message text (see doc.go) or resolve
// cross-record references -- validate.py's own docstring scopes deep
// validation the same way ("Shape-only... does not resolve cross-batch
// references").
type field struct {
	name     string // the wire alias, e.g. "externalId"
	required bool
	typ      fieldType
	enum     []string // fString only; empty means unconstrained
	minLen   int
	hasMax   bool
	maxLen   int
	hasGE    bool
	ge       float64
	hasLE    bool
	le       float64
}

// recordSpec is one record kind's full field set (extra="forbid" in Python;
// this package enforces "forbid" too -- see validateRecordPayload).
type recordSpec struct {
	fields []field
}

func req(name string, typ fieldType) field { return field{name: name, required: true, typ: typ} }
func opt(name string, typ fieldType) field { return field{name: name, typ: typ} }

func reqStr(name string, minLen, maxLen int) field {
	return field{name: name, required: true, typ: fString, minLen: minLen, hasMax: true, maxLen: maxLen}
}

func optStr(name string, maxLen int) field {
	return field{name: name, typ: fString, hasMax: maxLen > 0, maxLen: maxLen}
}

func reqEnum(name string, values ...string) field {
	return field{name: name, required: true, typ: fString, enum: values}
}

func optEnum(name string, values ...string) field {
	return field{name: name, typ: fString, enum: values}
}

func numRange(name string, required bool, ge, le float64, hasGE, hasLE bool) field {
	return field{name: name, required: required, typ: fNumber, ge: ge, le: le, hasGE: hasGE, hasLE: hasLE}
}

// operationalCommon is OperationalRecordV1's field set, embedded (Python
// subclassing) by every operational_*.v1 / incident_*.v1 / on_call_*.v1 /
// escalation_policy.v1 / service_repository_mapping.v1 kind.
var operationalCommon = []field{
	reqStr("externalId", 1, 512),
	req("sourceVersionAt", fDatetime),
	optStr("sourceUrl", 2048),
	opt("sourceEventAt", fDatetime),
	optStr("sourceEventId", 512),
	optStr("rawStatus", 255),
	optStr("rawSeverity", 255),
	optStr("rawPriority", 255),
	optEnum("normalizedStatus", "active", "open", "acknowledged", "resolved", "closed", "suppressed"),
	optEnum("normalizedSeverity", "critical", "high", "medium", "low", "info"),
	optEnum("normalizedPriority", "critical", "high", "medium", "low"),
	optStr("relationshipProvenance", 255),
	numRange("relationshipConfidence", false, 0, 1, true, true),
}

func withOperationalCommon(extra ...field) []field {
	return append(append([]field{}, operationalCommon...), extra...)
}

var (
	workItemStatus = []string{"backlog", "todo", "in_progress", "in_review", "blocked", "done", "canceled", "unknown"}
	workItemType   = []string{"issue", "pr", "merge_request"}
)

// recordKindValidators is schemas.py's RECORD_KIND_MODELS, ported field by
// field. Keys are the wire "kind" values POST /batches and POST /validate
// accept; a kind not in this map is unknown_kind.
var recordKindValidators = map[string]recordSpec{
	"repository.v1": {fields: []field{
		reqStr("externalId", 1, 1024),
		reqEnum("sourceSystem", "github", "gitlab", "custom"),
		optStr("defaultRef", 0),
		opt("tags", fStringList),
		opt("settings", fDict),
	}},
	"identity.v1": {fields: []field{
		reqStr("canonicalId", 1, 255),
		optStr("displayName", 0),
		optStr("email", 0),
		opt("providerIdentities", fDict),
		opt("teamIds", fStringList),
		opt("isActive", fBool),
		req("updatedAt", fDatetime),
	}},
	"team.v1": {fields: []field{
		reqStr("id", 1, 255),
		reqStr("name", 0, 0),
		optStr("description", 0),
		opt("members", fStringList),
		opt("projectKeys", fStringList),
		opt("repoPatterns", fStringList),
		opt("isActive", fBool),
		req("updatedAt", fDatetime),
		optStr("nativeTeamKey", 0),
		optStr("parentTeamId", 0),
	}},
	"work_item.v1": {fields: []field{
		reqStr("externalKey", 1, 512),
		reqEnum("provider", "jira", "github", "gitlab", "linear"),
		reqStr("title", 0, 0),
		optEnum("type", "story", "task", "bug", "epic", "pr", "merge_request", "issue", "incident", "chore", "unknown"),
		reqEnum("status", workItemStatus...),
		optStr("statusRaw", 0),
		optStr("description", 0),
		optStr("repositoryExternalId", 0),
		optStr("nativeTeamKey", 0),
		optStr("projectKey", 0),
		optStr("projectId", 0),
		optStr("projectName", 0),
		opt("assignees", fStringList),
		optStr("reporter", 0),
		req("createdAt", fDatetime),
		opt("updatedAt", fDatetime),
		opt("startedAt", fDatetime),
		opt("completedAt", fDatetime),
		opt("closedAt", fDatetime),
		opt("labels", fStringList),
		numRange("storyPoints", false, 0, 0, false, false),
		optStr("sprintId", 0),
		optStr("sprintName", 0),
		optStr("parentId", 0),
		optStr("epicId", 0),
		optStr("url", 0),
		optStr("priorityRaw", 0),
		optStr("serviceClass", 0),
		opt("dueAt", fDatetime),
	}},
	"work_item_transition.v1": {fields: []field{
		reqStr("externalKey", 1, 512),
		reqEnum("provider", "jira", "github", "gitlab", "linear"),
		optEnum("workItemType", workItemType...),
		req("occurredAt", fDatetime),
		optStr("fromStatusRaw", 0),
		optStr("toStatusRaw", 0),
		reqEnum("fromStatus", workItemStatus...),
		reqEnum("toStatus", workItemStatus...),
		optStr("actor", 0),
	}},
	"work_item_dependency.v1": {fields: []field{
		reqStr("sourceExternalKey", 1, 512),
		optEnum("sourceWorkItemType", workItemType...),
		reqStr("targetExternalKey", 1, 512),
		optEnum("targetWorkItemType", workItemType...),
		reqEnum("relationshipType", "blocks", "blocked_by", "relates_to", "duplicates", "parent_of", "child_of"),
		optStr("relationshipTypeRaw", 0),
	}},
	"pull_request.v1": {fields: []field{
		reqStr("repositoryExternalId", 0, 0),
		numRange("number", true, 1, 0, true, false),
		optStr("title", 0),
		optStr("body", 0),
		reqEnum("state", "open", "closed", "merged"),
		optStr("authorName", 0),
		optStr("authorEmail", 0),
		req("createdAt", fDatetime),
		opt("mergedAt", fDatetime),
		opt("closedAt", fDatetime),
		optStr("headBranch", 0),
		optStr("baseBranch", 0),
		numRange("additions", false, 0, 0, true, false),
		numRange("deletions", false, 0, 0, true, false),
		numRange("changedFiles", false, 0, 0, true, false),
		opt("firstReviewAt", fDatetime),
		opt("firstCommentAt", fDatetime),
		numRange("changesRequestedCount", false, 0, 0, true, false),
		numRange("reviewsCount", false, 0, 0, true, false),
		numRange("commentsCount", false, 0, 0, true, false),
		optStr("url", 0),
	}},
	"review.v1": {fields: []field{
		reqStr("repositoryExternalId", 0, 0),
		numRange("pullRequestNumber", true, 1, 0, true, false),
		reqStr("reviewId", 1, 0),
		reqStr("reviewer", 0, 0),
		reqEnum("state", "APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED", "PENDING"),
		req("submittedAt", fDatetime),
	}},
	"commit.v1": {fields: []field{
		reqStr("repositoryExternalId", 0, 0),
		reqStr("hash", 7, 64),
		optStr("message", 0),
		optStr("authorName", 0),
		optStr("authorEmail", 0),
		req("authorWhen", fDatetime),
		optStr("committerName", 0),
		optStr("committerEmail", 0),
		opt("committerWhen", fDatetime),
		numRange("parents", false, 0, 0, true, false),
	}},
	"operational_service.v1": {fields: withOperationalCommon(
		reqStr("name", 1, 512),
		optStr("description", 20_000),
		optStr("serviceType", 255),
		optStr("owningTeamExternalId", 512),
		optStr("escalationPolicyExternalId", 512),
		opt("isDeleted", fBool),
		opt("deletedAt", fDatetime),
	)},
	"operational_incident.v1": {fields: withOperationalCommon(
		reqStr("title", 1, 1024),
		optStr("description", 20_000),
		optStr("serviceExternalId", 512),
		optStr("escalationPolicyExternalId", 512),
		opt("startedAt", fDatetime),
		opt("resolvedAt", fDatetime),
		opt("isDeleted", fBool),
		opt("deletedAt", fDatetime),
	)},
	"operational_alert.v1": {fields: withOperationalCommon(
		reqStr("title", 1, 1024),
		optStr("description", 20_000),
		optStr("serviceExternalId", 512),
		optStr("incidentExternalId", 512),
		opt("triggeredAt", fDatetime),
		opt("acknowledgedAt", fDatetime),
		opt("resolvedAt", fDatetime),
		opt("isDeleted", fBool),
		opt("deletedAt", fDatetime),
	)},
	"incident_timeline_event.v1": {fields: withOperationalCommon(
		reqStr("incidentExternalId", 1, 512),
		reqStr("eventType", 1, 255),
		optStr("body", 20_000),
		optStr("actorType", 255),
		optStr("actorExternalId", 512),
		opt("occurredAt", fDatetime),
	)},
	"incident_note.v1": {fields: withOperationalCommon(
		reqStr("incidentExternalId", 1, 512),
		reqStr("body", 1, 20_000),
		optStr("authorUserExternalId", 512),
		opt("createdAt", fDatetime),
	)},
	"incident_responder.v1": {fields: withOperationalCommon(
		reqStr("incidentExternalId", 1, 512),
		optStr("userExternalId", 512),
		optStr("responderName", 512),
		optStr("role", 255),
		optStr("responderAssignmentId", 512),
		opt("requestedAt", fDatetime),
		opt("assignedAt", fDatetime),
		opt("acknowledgedAt", fDatetime),
		opt("completedAt", fDatetime),
	)},
	"escalation_policy.v1": {fields: withOperationalCommon(
		reqStr("name", 1, 512),
		optStr("description", 20_000),
		opt("isDeleted", fBool),
		opt("deletedAt", fDatetime),
	)},
	"on_call_schedule.v1": {fields: withOperationalCommon(
		reqStr("name", 1, 512),
		optStr("description", 20_000),
		opt("isDeleted", fBool),
		opt("deletedAt", fDatetime),
		optStr("timezone", 255),
	)},
	"on_call_assignment.v1": {fields: withOperationalCommon(
		optStr("scheduleExternalId", 512),
		optStr("userExternalId", 512),
		optStr("escalationPolicyExternalId", 512),
		numRange("escalationLevel", false, 0, 100, true, true),
		opt("startsAt", fDatetime),
		opt("endsAt", fDatetime),
	)},
	"operational_team.v1": {fields: withOperationalCommon(
		reqStr("name", 1, 512),
		optStr("description", 20_000),
		opt("isDeleted", fBool),
		opt("deletedAt", fDatetime),
	)},
	"operational_user.v1": {fields: withOperationalCommon(
		reqStr("displayName", 1, 512),
		optStr("email", 512),
		opt("isDeleted", fBool),
		opt("deletedAt", fDatetime),
	)},
	"service_repository_mapping.v1": {fields: withOperationalCommon(
		reqStr("serviceExternalId", 1, 512),
		optStr("repoFullName", 1024),
		optEnum("repoProvider", "github", "gitlab", "custom"),
		optStr("mappingKind", 255),
		optStr("ruleId", 512),
		opt("validFrom", fDatetime),
		opt("validTo", fDatetime),
		opt("isActive", fBool),
	)},
}

// operationalRecordKinds is schemas.py's OPERATIONAL_RECORD_KINDS: the kinds
// requiring source.entityFamily == "operational" and gated by the
// canonical_incident_ingestion feature.
var operationalRecordKinds = map[string]bool{
	"operational_service.v1":        true,
	"operational_incident.v1":       true,
	"operational_alert.v1":          true,
	"incident_timeline_event.v1":    true,
	"incident_note.v1":              true,
	"incident_responder.v1":         true,
	"escalation_policy.v1":          true,
	"on_call_schedule.v1":           true,
	"on_call_assignment.v1":         true,
	"operational_team.v1":           true,
	"operational_user.v1":           true,
	"service_repository_mapping.v1": true,
}

// entityFamilyForRecordKinds is entity_family_for_record_kinds: the single
// family every submitted kind implies, or "" when the batch mixes families
// (in which case _check_entity_family_or_400 always 400s, since
// source.entityFamily can only equal one of them).
func entityFamilyForRecordKinds(kinds []string) string {
	families := map[string]bool{}
	for _, kind := range kinds {
		if operationalRecordKinds[kind] {
			families["operational"] = true
		} else {
			families["legacy"] = true
		}
	}
	if len(families) != 1 {
		return ""
	}
	for family := range families {
		return family
	}
	return ""
}
