package streamhandlers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/google/uuid"
)

const externalUpdatedAtClampSkew = 5 * time.Minute

type ClickHouseExternalBatchSink struct {
	conn productClickHouse
	now  func() time.Time
}

func NewClickHouseExternalBatchSink(conn productClickHouse) (*ClickHouseExternalBatchSink, error) {
	if conn == nil {
		return nil, streamrunner.ErrInvalidConfig
	}
	return &ClickHouseExternalBatchSink{conn: conn, now: time.Now}, nil
}

func (s *ClickHouseExternalBatchSink) Write(ctx context.Context, source externalSinkBatch) (ExternalRecomputeScope, error) {
	now := s.now().UTC()
	scope := ExternalRecomputeScope{
		OrgID: source.Pointer.OrgID, SourceSystem: source.Pointer.SourceSystem,
		SourceInstance: source.Pointer.SourceInstance, IngestionID: source.Pointer.IngestionID,
	}
	grouped := make(map[string][]externalSinkRecord)
	for _, record := range source.Records {
		grouped[record.Kind] = append(grouped[record.Kind], record)
	}
	kinds := make([]string, 0, len(grouped))
	for kind := range grouped {
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)
	for _, kind := range kinds {
		query, err := externalInsertQuery(kind)
		if err != nil {
			return ExternalRecomputeScope{}, err
		}
		batch, err := s.conn.PrepareBatch(ctx, query)
		if err != nil {
			return ExternalRecomputeScope{}, fmt.Errorf("prepare external %s sink: %w", kind, err)
		}
		for _, record := range grouped[kind] {
			values, err := externalRecordValues(source, record, now, &scope)
			if err != nil {
				return ExternalRecomputeScope{}, fmt.Errorf("translate external %s record %d: %w", kind, record.Index, err)
			}
			if err := batch.Append(values...); err != nil {
				return ExternalRecomputeScope{}, fmt.Errorf("append external %s: %w", kind, err)
			}
		}
		if err := batch.Send(); err != nil {
			return ExternalRecomputeScope{}, fmt.Errorf("persist external %s: %w", kind, err)
		}
		scope.RecordKinds = append(scope.RecordKinds, kind)
	}
	scope.RepoIDs = sortedExternalStrings(scope.RepoIDs)
	scope.TeamIDs = sortedExternalStrings(scope.TeamIDs)
	scope.RecordKinds = sortedExternalStrings(scope.RecordKinds)
	return scope, nil
}

func externalInsertQuery(kind string) (string, error) {
	queries := map[string]string{
		"repository.v1":                 "INSERT INTO repos (id,repo,ref,created_at,settings,tags,provider,last_synced,source_id,org_id)",
		"commit.v1":                     "INSERT INTO git_commits (repo_id,hash,message,author_name,author_email,author_when,committer_name,committer_email,committer_when,parents,last_synced,source_id,org_id)",
		"pull_request.v1":               "INSERT INTO git_pull_requests (repo_id,number,title,body,state,author_name,author_email,created_at,merged_at,closed_at,head_branch,base_branch,additions,deletions,changed_files,first_review_at,first_comment_at,changes_requested_count,reviews_count,comments_count,last_synced,source_id,org_id)",
		"review.v1":                     "INSERT INTO git_pull_request_reviews (repo_id,number,review_id,reviewer,state,submitted_at,last_synced,source_id,org_id)",
		"team.v1":                       "INSERT INTO teams (id,team_uuid,name,description,members,project_keys,repo_patterns,is_active,updated_at,last_synced,org_id,provider,native_team_key,parent_team_id,source_id)",
		"identity.v1":                   "INSERT INTO identities (org_id,canonical_id,identity_uuid,display_name,email,provider_identities,team_ids,is_active,updated_at,source_id)",
		"work_item.v1":                  "INSERT INTO work_items (repo_id,work_item_id,provider,title,type,status,status_raw,project_key,project_id,native_team_key,project_name,assignees,reporter,created_at,updated_at,started_at,completed_at,closed_at,labels,story_points,sprint_id,sprint_name,parent_id,epic_id,url,last_synced,org_id,source_id)",
		"work_item_transition.v1":       "INSERT INTO work_item_transitions (repo_id,work_item_id,occurred_at,from_status,to_status,from_status_raw,to_status_raw,actor,last_synced,org_id,source_id)",
		"work_item_dependency.v1":       "INSERT INTO work_item_dependencies (source_work_item_id,target_work_item_id,relationship_type,relationship_type_raw,last_synced,org_id,source_id)",
		"operational_service.v1":        "INSERT INTO operational_services (" + operationalBaseColumns + ",name,description,service_type,owning_team_id,escalation_policy_id,is_deleted,deleted_at)",
		"operational_incident.v1":       "INSERT INTO operational_incidents (" + operationalBaseColumns + ",service_id,service_external_id,escalation_policy_id,title,description,started_at,resolved_at,is_deleted,deleted_at)",
		"operational_alert.v1":          "INSERT INTO operational_alerts (" + operationalBaseColumns + ",service_id,incident_id,title,description,triggered_at,acknowledged_at,resolved_at,is_deleted,deleted_at)",
		"incident_timeline_event.v1":    "INSERT INTO operational_incident_timeline_events (" + operationalBaseColumns + ",incident_id,event_type,body,actor_type,actor_id,occurred_at)",
		"incident_note.v1":              "INSERT INTO operational_incident_notes (" + operationalBaseColumns + ",incident_id,body,author_user_id,created_at)",
		"incident_responder.v1":         "INSERT INTO operational_incident_responders (" + operationalBaseColumns + ",incident_id,user_id,responder_name,role,responder_assignment_id,requested_at,assigned_at,acknowledged_at,completed_at)",
		"escalation_policy.v1":          "INSERT INTO operational_escalation_policies (" + operationalBaseColumns + ",name,description,is_deleted,deleted_at)",
		"on_call_schedule.v1":           "INSERT INTO operational_on_call_schedules (" + operationalBaseColumns + ",name,description,timezone,is_deleted,deleted_at)",
		"on_call_assignment.v1":         "INSERT INTO operational_on_call_assignments (" + operationalBaseColumns + ",schedule_id,user_id,escalation_policy_id,escalation_level,starts_at,ends_at)",
		"operational_team.v1":           "INSERT INTO operational_teams (" + operationalBaseColumns + ",name,description,is_deleted,deleted_at)",
		"operational_user.v1":           "INSERT INTO operational_users (" + operationalBaseColumns + ",display_name,email,is_deleted,deleted_at)",
		"service_repository_mapping.v1": "INSERT INTO operational_service_repository_mappings (" + operationalBaseColumns + ",service_id,repo_id,repo_full_name,repo_provider,mapping_kind,rule_id,valid_from,valid_to,is_active)",
	}
	query, ok := queries[kind]
	if !ok {
		return "", fmt.Errorf("unsupported external sink kind %q", kind)
	}
	return query, nil
}

func externalRecordValues(
	source externalSinkBatch,
	record externalSinkRecord,
	now time.Time,
	scope *ExternalRecomputeScope,
) ([]any, error) {
	payload := record.Payload
	orgID, system, instance := source.Pointer.OrgID, source.Pointer.SourceSystem, source.Pointer.SourceInstance
	switch record.Kind {
	case "repository.v1":
		repositorySystem := externalStringDefault(payload, "sourceSystem", system)
		repoID := externalRepoUUID(repositorySystem, instance, stringField(payload, "externalId"))
		scope.RepoIDs = append(scope.RepoIDs, repoID.String())
		settings, err := externalPythonJSON(objectField(payload, "settings"))
		if err != nil {
			return nil, err
		}
		tags, err := externalPythonJSON(stringArrayField(payload, "tags"))
		if err != nil {
			return nil, err
		}
		return []any{
			repoID, stringField(payload, "externalId"), externalNullableString(payload, "defaultRef"),
			now, settings, tags, repositorySystem, now, source.SourceID, orgID,
		}, nil
	case "commit.v1":
		repoID := externalRepoUUID(system, instance, stringField(payload, "repositoryExternalId"))
		authorWhen, err := externalTime(payload, "authorWhen")
		if err != nil {
			return nil, err
		}
		committerWhen := authorWhen
		if value, ok, err := externalOptionalTime(payload, "committerWhen"); err != nil {
			return nil, err
		} else if ok {
			committerWhen = value
		}
		scope.RepoIDs = append(scope.RepoIDs, repoID.String())
		trackExternalTime(scope, authorWhen)
		return []any{
			repoID, stringField(payload, "hash"), externalNullableString(payload, "message"),
			externalNullableString(payload, "authorName"), externalNullableString(payload, "authorEmail"), authorWhen,
			externalNullableString(payload, "committerName"), externalNullableString(payload, "committerEmail"), committerWhen,
			uint32(externalIntegerDefault(payload, "parents", 1)), now, source.SourceID, orgID,
		}, nil
	case "pull_request.v1":
		repoID := externalRepoUUID(system, instance, stringField(payload, "repositoryExternalId"))
		createdAt, err := externalTime(payload, "createdAt")
		if err != nil {
			return nil, err
		}
		scope.RepoIDs = append(scope.RepoIDs, repoID.String())
		trackExternalTime(scope, createdAt)
		return []any{
			repoID, uint32(externalIntegerDefault(payload, "number", 0)),
			externalNullableString(payload, "title"), externalNullableString(payload, "body"), stringField(payload, "state"),
			externalNullableString(payload, "authorName"), externalNullableString(payload, "authorEmail"), createdAt,
			externalNullableTime(payload, "mergedAt"), externalNullableTime(payload, "closedAt"),
			externalNullableString(payload, "headBranch"), externalNullableString(payload, "baseBranch"),
			externalNullableUint(payload, "additions"), externalNullableUint(payload, "deletions"), externalNullableUint(payload, "changedFiles"),
			externalNullableTime(payload, "firstReviewAt"), externalNullableTime(payload, "firstCommentAt"),
			uint32(externalIntegerDefault(payload, "changesRequestedCount", 0)),
			uint32(externalIntegerDefault(payload, "reviewsCount", 0)),
			uint32(externalIntegerDefault(payload, "commentsCount", 0)), now, source.SourceID, orgID,
		}, nil
	case "review.v1":
		repoID := externalRepoUUID(system, instance, stringField(payload, "repositoryExternalId"))
		submittedAt, err := externalTime(payload, "submittedAt")
		if err != nil {
			return nil, err
		}
		scope.RepoIDs = append(scope.RepoIDs, repoID.String())
		trackExternalTime(scope, submittedAt)
		return []any{
			repoID, uint32(externalIntegerDefault(payload, "pullRequestNumber", 0)),
			stringField(payload, "reviewId"), stringField(payload, "reviewer"), stringField(payload, "state"),
			submittedAt, now, source.SourceID, orgID,
		}, nil
	case "team.v1":
		teamID := stringField(payload, "id")
		updatedAt, err := externalTime(payload, "updatedAt")
		if err != nil {
			return nil, err
		}
		updatedAt = externalClampUpdatedAt(updatedAt, now)
		scope.TeamIDs = append(scope.TeamIDs, teamID)
		return []any{
			teamID, uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+teamID)), stringField(payload, "name"),
			externalNullableString(payload, "description"), stringArrayField(payload, "members"),
			stringArrayField(payload, "projectKeys"), stringArrayField(payload, "repoPatterns"),
			externalBoolUint(payload, "isActive", true), updatedAt, now, orgID, system,
			externalNullableString(payload, "nativeTeamKey"), externalNullableString(payload, "parentTeamId"), source.SourceID,
		}, nil
	case "identity.v1":
		canonicalID := stringField(payload, "canonicalId")
		updatedAt, err := externalTime(payload, "updatedAt")
		if err != nil {
			return nil, err
		}
		updatedAt = externalClampUpdatedAt(updatedAt, now)
		providerIdentities, err := externalPythonJSON(objectField(payload, "providerIdentities"))
		if err != nil {
			return nil, err
		}
		return []any{
			orgID, canonicalID, uuid.NewSHA1(uuid.NameSpaceURL, []byte("identity:"+orgID+":"+canonicalID)),
			externalNullableString(payload, "displayName"), externalNullableString(payload, "email"),
			providerIdentities, stringArrayField(payload, "teamIds"),
			externalBoolUint(payload, "isActive", true), updatedAt, source.SourceID,
		}, nil
	case "work_item.v1":
		return externalWorkItemValues(source, payload, now, scope)
	case "work_item_transition.v1":
		return externalTransitionValues(source, payload, now, scope)
	case "work_item_dependency.v1":
		sourceID := externalWorkItemID(system, externalWorkItemInstance(system, instance, ""), stringField(payload, "sourceExternalKey"), stringField(payload, "sourceWorkItemType"))
		targetID := externalWorkItemID(system, externalWorkItemInstance(system, instance, ""), stringField(payload, "targetExternalKey"), stringField(payload, "targetWorkItemType"))
		return []any{
			sourceID, targetID, stringField(payload, "relationshipType"),
			stringField(payload, "relationshipTypeRaw"), now, orgID, source.SourceID,
		}, nil
	default:
		return externalOperationalValues(source, record, now, scope)
	}
}

func externalWorkItemValues(source externalSinkBatch, payload map[string]any, now time.Time, scope *ExternalRecomputeScope) ([]any, error) {
	system, sourceInstance := source.Pointer.SourceSystem, source.Pointer.SourceInstance
	repository := stringField(payload, "repositoryExternalId")
	instance := externalWorkItemInstance(system, sourceInstance, repository)
	workItemID := externalWorkItemID(system, instance, stringField(payload, "externalKey"), stringField(payload, "type"))
	repoID := uuid.Nil
	if system == "github" || system == "gitlab" {
		repoID = externalRepoUUID(system, sourceInstance, instance)
		scope.RepoIDs = append(scope.RepoIDs, repoID.String())
	}
	createdAt, err := externalTime(payload, "createdAt")
	if err != nil {
		return nil, err
	}
	updatedAt := createdAt
	if value, ok, err := externalOptionalTime(payload, "updatedAt"); err != nil {
		return nil, err
	} else if ok {
		updatedAt = value
	}
	trackExternalTime(scope, updatedAt)
	projectKey, projectID, projectName := externalProjectScope(system, payload, repository)
	nativeTeamKey := ""
	if system == "linear" {
		nativeTeamKey = stringField(payload, "nativeTeamKey")
	}
	rawAssignees := stringArrayField(payload, "assignees")
	assignees := make([]string, 0, len(rawAssignees))
	for _, raw := range rawAssignees {
		if strings.TrimSpace(raw) != "" {
			assignees = append(assignees, externalIdentity(system, raw))
		}
	}
	reporter := ""
	if raw := stringField(payload, "reporter"); raw != "" {
		reporter = externalIdentity(system, raw)
	}
	return []any{
		repoID, workItemID, system, stringField(payload, "title"),
		externalStringDefault(payload, "type", "unknown"),
		stringField(payload, "status"), stringField(payload, "statusRaw"), projectKey, projectID,
		nativeTeamKey, projectName, assignees, reporter, createdAt, updatedAt,
		externalNullableTime(payload, "startedAt"), externalNullableTime(payload, "completedAt"),
		externalNullableTime(payload, "closedAt"), stringArrayField(payload, "labels"),
		externalNullableNumber(payload, "storyPoints"), stringField(payload, "sprintId"),
		stringField(payload, "sprintName"), stringField(payload, "parentId"), stringField(payload, "epicId"),
		stringField(payload, "url"), now, source.Pointer.OrgID, source.SourceID,
	}, nil
}

func externalTransitionValues(source externalSinkBatch, payload map[string]any, now time.Time, scope *ExternalRecomputeScope) ([]any, error) {
	system, sourceInstance := source.Pointer.SourceSystem, source.Pointer.SourceInstance
	instance := externalWorkItemInstance(system, sourceInstance, "")
	workItemID := externalWorkItemID(system, instance, stringField(payload, "externalKey"), stringField(payload, "workItemType"))
	repoID := uuid.Nil
	if system == "github" || system == "gitlab" {
		repoID = externalRepoUUID(system, sourceInstance, instance)
		scope.RepoIDs = append(scope.RepoIDs, repoID.String())
	}
	occurredAt, err := externalTime(payload, "occurredAt")
	if err != nil {
		return nil, err
	}
	trackExternalTime(scope, occurredAt)
	actor := ""
	if raw := stringField(payload, "actor"); raw != "" {
		actor = externalIdentity(system, raw)
	}
	return []any{
		repoID, workItemID, occurredAt,
		stringField(payload, "fromStatus"), stringField(payload, "toStatus"),
		stringField(payload, "fromStatusRaw"), stringField(payload, "toStatusRaw"),
		actor, now, source.Pointer.OrgID, source.SourceID,
	}, nil
}

type externalOperationalSinkSpec struct {
	table, family string
	fields        []string
}

var externalOperationalSinkSpecs = map[string]externalOperationalSinkSpec{
	"operational_service.v1":        {"operational_services", "operational_service", []string{"name", "description", "serviceType", "owningTeamExternalId", "escalationPolicyExternalId", "isDeleted", "deletedAt"}},
	"operational_incident.v1":       {"operational_incidents", "operational_incident", []string{"serviceExternalId", "serviceExternalId", "escalationPolicyExternalId", "title", "description", "startedAt", "resolvedAt", "isDeleted", "deletedAt"}},
	"operational_alert.v1":          {"operational_alerts", "operational_alert", []string{"serviceExternalId", "incidentExternalId", "title", "description", "triggeredAt", "acknowledgedAt", "resolvedAt", "isDeleted", "deletedAt"}},
	"incident_timeline_event.v1":    {"operational_incident_timeline_events", "operational_incident_timeline_event", []string{"incidentExternalId", "eventType", "body", "actorType", "actorExternalId", "occurredAt"}},
	"incident_note.v1":              {"operational_incident_notes", "operational_incident_note", []string{"incidentExternalId", "body", "authorUserExternalId", "createdAt"}},
	"incident_responder.v1":         {"operational_incident_responders", "operational_incident_responder", []string{"incidentExternalId", "userExternalId", "responderName", "role", "responderAssignmentId", "requestedAt", "assignedAt", "acknowledgedAt", "completedAt"}},
	"escalation_policy.v1":          {"operational_escalation_policies", "operational_escalation_policy", []string{"name", "description", "isDeleted", "deletedAt"}},
	"on_call_schedule.v1":           {"operational_on_call_schedules", "operational_on_call_schedule", []string{"name", "description", "timezone", "isDeleted", "deletedAt"}},
	"on_call_assignment.v1":         {"operational_on_call_assignments", "operational_on_call_assignment", []string{"scheduleExternalId", "userExternalId", "escalationPolicyExternalId", "escalationLevel", "startsAt", "endsAt"}},
	"operational_team.v1":           {"operational_teams", "operational_team", []string{"name", "description", "isDeleted", "deletedAt"}},
	"operational_user.v1":           {"operational_users", "operational_user", []string{"displayName", "email", "isDeleted", "deletedAt"}},
	"service_repository_mapping.v1": {"operational_service_repository_mappings", "operational_service_repository_mapping", []string{"serviceExternalId", "repoFullName", "repoProvider", "mappingKind", "ruleId", "validFrom", "validTo", "isActive"}},
}

func externalOperationalValues(source externalSinkBatch, record externalSinkRecord, now time.Time, scope *ExternalRecomputeScope) ([]any, error) {
	spec, ok := externalOperationalSinkSpecs[record.Kind]
	if !ok {
		return nil, fmt.Errorf("unsupported operational kind %q", record.Kind)
	}
	payload := record.Payload
	sourceVersion, err := externalTime(payload, "sourceVersionAt")
	if err != nil {
		return nil, err
	}
	provider := strings.ToLower(strings.TrimSpace(source.Pointer.SourceSystem))
	instance, err := normalizeExternalOperationalInstance(provider, source.Pointer.SourceInstance)
	if err != nil {
		return nil, err
	}
	sourceID := source.SourceID
	base := operationalBase{
		orgID: source.Pointer.OrgID, provider: provider, providerInstanceID: instance,
		sourceEntityType: "external_push." + strings.TrimSuffix(record.Kind, ".v1"),
		externalID:       strings.TrimSpace(stringField(payload, "externalId")),
		sourceVersionAt:  sourceVersion, sourceID: &sourceID,
		sourceURL: externalStringPointer(payload, "sourceUrl"),
		sourceEventAt: func() *time.Time {
			value, ok, _ := externalOptionalTime(payload, "sourceEventAt")
			if !ok {
				return nil
			}
			return &value
		}(),
		sourceEventID:          externalStringPointer(payload, "sourceEventId"),
		observedAt:             now,
		lastSynced:             now,
		rawStatus:              externalStringPointer(payload, "rawStatus"),
		rawSeverity:            externalStringPointer(payload, "rawSeverity"),
		rawPriority:            externalStringPointer(payload, "rawPriority"),
		normalizedStatus:       externalStringPointer(payload, "normalizedStatus"),
		normalizedSeverity:     externalStringPointer(payload, "normalizedSeverity"),
		normalizedPriority:     externalStringPointer(payload, "normalizedPriority"),
		relationshipProvenance: externalStringPointer(payload, "relationshipProvenance"),
		relationshipConfidence: externalNumberPointer(payload, "relationshipConfidence"),
	}
	entity, err := externalOperationalEntityFields(source, record, provider, instance)
	if err != nil {
		return nil, err
	}
	trackExternalTime(scope, sourceVersion)
	return operationalValues(spec.family, base, entity)
}

func externalOperationalEntityFields(
	source externalSinkBatch,
	record externalSinkRecord,
	provider, instance string,
) ([]operationalField, error) {
	payload := record.Payload
	ref := func(family, key string) any {
		externalID := stringField(payload, key)
		if externalID == "" {
			return nil
		}
		value, err := canonicalOperationalID(source.Pointer.OrgID, provider, instance, family, externalID)
		if err != nil {
			return nil
		}
		return value
	}
	switch record.Kind {
	case "operational_service.v1":
		return []operationalField{
			{"name", stringField(payload, "name")},
			{"description", externalNullableString(payload, "description")},
			{"service_type", externalNullableString(payload, "serviceType")},
			{"owning_team_id", ref("operational_team", "owningTeamExternalId")},
			{"escalation_policy_id", ref("operational_escalation_policy", "escalationPolicyExternalId")},
			{"is_deleted", externalBool(payload, "isDeleted", false)},
			{"deleted_at", externalNullableTime(payload, "deletedAt")},
		}, nil
	case "operational_incident.v1":
		return []operationalField{
			{"service_id", ref("operational_service", "serviceExternalId")},
			{"service_external_id", externalNullableString(payload, "serviceExternalId")},
			{"escalation_policy_id", ref("operational_escalation_policy", "escalationPolicyExternalId")},
			{"title", stringField(payload, "title")},
			{"description", externalNullableString(payload, "description")},
			{"started_at", externalNullableTime(payload, "startedAt")},
			{"resolved_at", externalNullableTime(payload, "resolvedAt")},
			{"is_deleted", externalBool(payload, "isDeleted", false)},
			{"deleted_at", externalNullableTime(payload, "deletedAt")},
		}, nil
	case "operational_alert.v1":
		return []operationalField{
			{"service_id", ref("operational_service", "serviceExternalId")},
			{"incident_id", ref("operational_incident", "incidentExternalId")},
			{"title", stringField(payload, "title")},
			{"description", externalNullableString(payload, "description")},
			{"triggered_at", externalNullableTime(payload, "triggeredAt")},
			{"acknowledged_at", externalNullableTime(payload, "acknowledgedAt")},
			{"resolved_at", externalNullableTime(payload, "resolvedAt")},
			{"is_deleted", externalBool(payload, "isDeleted", false)},
			{"deleted_at", externalNullableTime(payload, "deletedAt")},
		}, nil
	case "incident_timeline_event.v1":
		return []operationalField{
			{"incident_id", ref("operational_incident", "incidentExternalId")},
			{"event_type", stringField(payload, "eventType")},
			{"body", externalNullableString(payload, "body")},
			{"actor_type", externalNullableString(payload, "actorType")},
			{"actor_id", ref("operational_user", "actorExternalId")},
			{"occurred_at", externalNullableTime(payload, "occurredAt")},
		}, nil
	case "incident_note.v1":
		return []operationalField{
			{"incident_id", ref("operational_incident", "incidentExternalId")},
			{"body", stringField(payload, "body")},
			{"author_user_id", ref("operational_user", "authorUserExternalId")},
			{"created_at", externalNullableTime(payload, "createdAt")},
		}, nil
	case "incident_responder.v1":
		return []operationalField{
			{"incident_id", ref("operational_incident", "incidentExternalId")},
			{"user_id", ref("operational_user", "userExternalId")},
			{"responder_name", externalNullableString(payload, "responderName")},
			{"role", externalNullableString(payload, "role")},
			{"responder_assignment_id", externalNullableString(payload, "responderAssignmentId")},
			{"requested_at", externalNullableTime(payload, "requestedAt")},
			{"assigned_at", externalNullableTime(payload, "assignedAt")},
			{"acknowledged_at", externalNullableTime(payload, "acknowledgedAt")},
			{"completed_at", externalNullableTime(payload, "completedAt")},
		}, nil
	case "escalation_policy.v1":
		return externalOperationalNamedFields(payload), nil
	case "on_call_schedule.v1":
		return []operationalField{
			{"name", stringField(payload, "name")},
			{"description", externalNullableString(payload, "description")},
			{"timezone", externalNullableString(payload, "timezone")},
			{"is_deleted", externalBool(payload, "isDeleted", false)},
			{"deleted_at", externalNullableTime(payload, "deletedAt")},
		}, nil
	case "on_call_assignment.v1":
		return []operationalField{
			{"schedule_id", ref("operational_on_call_schedule", "scheduleExternalId")},
			{"user_id", ref("operational_user", "userExternalId")},
			{"escalation_policy_id", ref("operational_escalation_policy", "escalationPolicyExternalId")},
			{"escalation_level", externalNullableInt32(payload, "escalationLevel")},
			{"starts_at", externalNullableTime(payload, "startsAt")},
			{"ends_at", externalNullableTime(payload, "endsAt")},
		}, nil
	case "operational_team.v1":
		return externalOperationalNamedFields(payload), nil
	case "operational_user.v1":
		return []operationalField{
			{"display_name", stringField(payload, "displayName")},
			{"email", externalNullableString(payload, "email")},
			{"is_deleted", externalBool(payload, "isDeleted", false)},
			{"deleted_at", externalNullableTime(payload, "deletedAt")},
		}, nil
	case "service_repository_mapping.v1":
		return []operationalField{
			{"service_id", ref("operational_service", "serviceExternalId")},
			{"repo_id", nil},
			{"repo_full_name", externalNullableString(payload, "repoFullName")},
			{"repo_provider", externalNullableString(payload, "repoProvider")},
			{"mapping_kind", externalNullableString(payload, "mappingKind")},
			{"rule_id", externalNullableString(payload, "ruleId")},
			{"valid_from", externalNullableTime(payload, "validFrom")},
			{"valid_to", externalNullableTime(payload, "validTo")},
			{"is_active", externalBool(payload, "isActive", true)},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported operational record %q", record.Kind)
	}
}

func externalOperationalNamedFields(payload map[string]any) []operationalField {
	return []operationalField{
		{"name", stringField(payload, "name")},
		{"description", externalNullableString(payload, "description")},
		{"is_deleted", externalBool(payload, "isDeleted", false)},
		{"deleted_at", externalNullableTime(payload, "deletedAt")},
	}
}

func externalRepoUUID(system, instance, externalID string) uuid.UUID {
	seed := externalID
	if system == "custom" {
		seed = "custom:" + instance + ":" + externalID
	}
	digest := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(seed))))
	value, _ := uuid.FromBytes(digest[:16])
	return value
}

func externalWorkItemInstance(system, sourceInstance, recordInstance string) string {
	if recordInstance != "" {
		return recordInstance
	}
	if system == "github" || system == "gitlab" {
		return sourceInstance
	}
	return ""
}

func externalWorkItemID(system, instance, externalKey, kind string) string {
	switch system {
	case "jira":
		return "jira:" + externalKey
	case "linear":
		return "linear:" + externalKey
	case "github":
		if kind == "pr" {
			return "ghpr:" + instance + "#" + externalKey
		}
		return "gh:" + instance + "#" + externalKey
	case "gitlab":
		if kind == "merge_request" {
			return "gitlab:" + instance + "!" + externalKey
		}
		return "gitlab:" + instance + "#" + externalKey
	default:
		return "custom:" + instance + ":" + externalKey
	}
}

func externalProjectScope(system string, payload map[string]any, repository string) (string, string, string) {
	switch system {
	case "jira":
		return stringField(payload, "projectKey"), "", ""
	case "github", "gitlab":
		return "", repository, ""
	case "linear":
		projectName := stringField(payload, "projectName")
		projectID := stringField(payload, "projectId")
		if projectID == "" {
			projectID = projectName
		}
		if projectID == "" {
			projectID = stringField(payload, "nativeTeamKey")
		}
		return "", projectID, projectName
	default:
		return stringField(payload, "projectKey"), "", ""
	}
}

func externalIdentity(provider, raw string) string {
	raw = strings.TrimSpace(raw)
	if strings.Contains(raw, "@") {
		return strings.ToLower(raw)
	}
	if raw == "" {
		return "unknown"
	}
	return provider + ":" + raw
}

// externalPythonJSON preserves the current Python sink's json.dumps wire
// shape for String columns (spaces after separators, stable key order). The
// JSON is data in ClickHouse rather than a native JSON column, so equivalent
// but byte-different encodings would otherwise create false parity drift.
func externalPythonJSON(value any) (string, error) {
	var output strings.Builder
	var encode func(any) error
	encode = func(item any) error {
		switch typed := item.(type) {
		case nil:
			output.WriteString("null")
		case bool:
			output.WriteString(strconv.FormatBool(typed))
		case string:
			encoded, _ := json.Marshal(typed)
			output.Write(encoded)
		case json.Number:
			if _, err := typed.Float64(); err != nil {
				return err
			}
			output.WriteString(typed.String())
		case float64:
			output.WriteString(strconv.FormatFloat(typed, 'g', -1, 64))
		case int:
			output.WriteString(strconv.Itoa(typed))
		case []string:
			output.WriteByte('[')
			for index, element := range typed {
				if index > 0 {
					output.WriteString(", ")
				}
				if err := encode(element); err != nil {
					return err
				}
			}
			output.WriteByte(']')
		case []any:
			output.WriteByte('[')
			for index, element := range typed {
				if index > 0 {
					output.WriteString(", ")
				}
				if err := encode(element); err != nil {
					return err
				}
			}
			output.WriteByte(']')
		case map[string]any:
			keys := make([]string, 0, len(typed))
			for key := range typed {
				keys = append(keys, key)
			}
			slices.Sort(keys)
			output.WriteByte('{')
			for index, key := range keys {
				if index > 0 {
					output.WriteString(", ")
				}
				if err := encode(key); err != nil {
					return err
				}
				output.WriteString(": ")
				if err := encode(typed[key]); err != nil {
					return err
				}
			}
			output.WriteByte('}')
		default:
			return fmt.Errorf("unsupported external JSON value %T", item)
		}
		return nil
	}
	if err := encode(value); err != nil {
		return "", err
	}
	return output.String(), nil
}

func normalizeExternalOperationalInstance(provider, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty operational provider instance")
	}
	if provider != "github" && provider != "gitlab" {
		return strings.ToLower(raw), nil
	}
	hasScheme := strings.Contains(raw, "://")
	parsed, err := url.Parse(raw)
	if !hasScheme {
		parsed, err = url.Parse("//" + raw)
	}
	hostName := parsed.Hostname()
	if err != nil || hostName == "" || !hasScheme && parsed.Path != "" || !validExternalHost(hostName) {
		return "", fmt.Errorf("invalid operational provider instance")
	}
	host := strings.ToLower(hostName)
	if provider == "github" && (host == "api.github.com" || host == "github.com") {
		return "github.com", nil
	}
	port := parsed.Port()
	scheme := strings.ToLower(parsed.Scheme)
	if scheme == "" {
		scheme = "https"
	}
	if port == "" || scheme == "https" && port == "443" || scheme == "http" && port == "80" {
		return host, nil
	}
	return net.JoinHostPort(host, port), nil
}

func validExternalHost(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || !externalAlphaNumeric(label[0]) || !externalAlphaNumeric(label[len(label)-1]) {
			return false
		}
		for _, character := range label {
			if !(character >= 'a' && character <= 'z' ||
				character >= 'A' && character <= 'Z' ||
				character >= '0' && character <= '9' ||
				character == '-') {
				return false
			}
		}
	}
	return true
}

func externalAlphaNumeric(character byte) bool {
	return character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9'
}

func externalTime(payload map[string]any, key string) (time.Time, error) {
	value, ok := payload[key].(string)
	if !ok || value == "" {
		return time.Time{}, fmt.Errorf("%s is required", key)
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s is invalid", key)
	}
	return parsed.UTC(), nil
}

func externalOptionalTime(payload map[string]any, key string) (time.Time, bool, error) {
	if payload[key] == nil {
		return time.Time{}, false, nil
	}
	value, err := externalTime(payload, key)
	return value, err == nil, err
}

func externalNullableTime(payload map[string]any, key string) any {
	value, ok, err := externalOptionalTime(payload, key)
	if err != nil || !ok {
		return nil
	}
	return value
}

func externalNullableString(payload map[string]any, key string) any {
	if value := stringField(payload, key); value != "" {
		return value
	}
	return nil
}

func externalStringPointer(payload map[string]any, key string) *string {
	value := stringField(payload, key)
	if value == "" {
		return nil
	}
	return &value
}

func externalNumberPointer(payload map[string]any, key string) *float64 {
	value, ok := numberField(payload, key)
	if !ok {
		return nil
	}
	return &value
}

func externalNullableNumber(payload map[string]any, key string) any {
	if value, ok := numberField(payload, key); ok {
		return value
	}
	return nil
}

func externalNullableUint(payload map[string]any, key string) any {
	if value, ok := integerField(payload, key); ok {
		return uint32(value)
	}
	return nil
}

func externalIntegerDefault(payload map[string]any, key string, fallback int64) int64 {
	if value, ok := integerField(payload, key); ok {
		return value
	}
	return fallback
}

func externalStringDefault(payload map[string]any, key, fallback string) string {
	if value := stringField(payload, key); value != "" {
		return value
	}
	return fallback
}

func externalBoolUint(payload map[string]any, key string, fallback bool) uint8 {
	value := externalBool(payload, key, fallback)
	if value {
		return 1
	}
	return 0
}

func externalBool(payload map[string]any, key string, fallback bool) bool {
	value, ok := payload[key].(bool)
	if !ok {
		return fallback
	}
	return value
}

func externalNullableInt32(payload map[string]any, key string) any {
	if value, ok := integerField(payload, key); ok {
		return int32(value)
	}
	return nil
}

func objectField(payload map[string]any, key string) map[string]any {
	value, _ := payload[key].(map[string]any)
	if value == nil {
		return map[string]any{}
	}
	return value
}

func stringArrayField(payload map[string]any, key string) []string {
	values, _ := payload[key].([]any)
	result := make([]string, 0, len(values))
	for _, value := range values {
		if text, ok := value.(string); ok {
			result = append(result, text)
		}
	}
	return result
}

func externalClampUpdatedAt(value, now time.Time) time.Time {
	if value.After(now.Add(externalUpdatedAtClampSkew)) {
		return now
	}
	return value
}

func trackExternalTime(scope *ExternalRecomputeScope, value time.Time) {
	if value.IsZero() {
		return
	}
	if scope.WindowStart == nil || value.Before(*scope.WindowStart) {
		copy := value
		scope.WindowStart = &copy
	}
	if scope.WindowEnd == nil || value.After(*scope.WindowEnd) {
		copy := value
		scope.WindowEnd = &copy
	}
}

func sortedExternalStrings(values []string) []string {
	slices.Sort(values)
	return slices.Compact(values)
}
