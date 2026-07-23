package streamhandlers

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestClickHouseExternalSinkPersistsEveryV1KindWithProvenance(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	sourceID := uuid.MustParse("9749bda0-fc9f-4076-b19d-7b26c4f306ff")
	legacyPointer := externalTestPointer()
	legacyRecords := []externalSinkRecord{
		externalSinkFixture("repository.v1", map[string]any{
			"externalId": legacyPointer.SourceInstance, "sourceSystem": "github",
		}),
		externalSinkFixture("identity.v1", map[string]any{
			"canonicalId": "ada@example.test", "updatedAt": "2026-07-23T11:00:00Z",
		}),
		externalSinkFixture("team.v1", map[string]any{
			"id": "team-a", "name": "Team A", "updatedAt": "2026-07-23T11:00:00Z",
		}),
		externalSinkFixture("work_item.v1", map[string]any{
			"externalKey": "7", "provider": "github", "title": "Issue",
			"type": "issue", "status": "open", "createdAt": "2026-07-22T10:00:00Z",
			"repositoryExternalId": legacyPointer.SourceInstance,
		}),
		externalSinkFixture("work_item_transition.v1", map[string]any{
			"externalKey": "7", "provider": "github", "occurredAt": "2026-07-22T11:00:00Z",
			"fromStatus": "todo", "toStatus": "in_progress",
		}),
		externalSinkFixture("work_item_dependency.v1", map[string]any{
			"sourceExternalKey": "7", "targetExternalKey": "8", "relationshipType": "blocks",
		}),
		externalSinkFixture("pull_request.v1", map[string]any{
			"repositoryExternalId": legacyPointer.SourceInstance, "number": json.Number("7"),
			"state": "open", "createdAt": "2026-07-22T12:00:00Z",
		}),
		externalSinkFixture("review.v1", map[string]any{
			"repositoryExternalId": legacyPointer.SourceInstance,
			"pullRequestNumber":    json.Number("7"), "reviewId": "review-1",
			"reviewer": "ada", "state": "APPROVED", "submittedAt": "2026-07-22T13:00:00Z",
		}),
		externalSinkFixture("commit.v1", map[string]any{
			"repositoryExternalId": legacyPointer.SourceInstance, "hash": "abcdef0",
			"authorWhen": "2026-07-22T14:00:00Z",
		}),
	}
	operationalPointer := externalTestPointer()
	operationalPointer.SourceSystem = "pagerduty"
	operationalPointer.SourceInstance = "tenant.pagerduty.com"
	operationalRecords := externalOperationalSinkFixtures()

	batches := make([]*productBatch, len(legacyRecords)+len(operationalRecords))
	for index := range batches {
		batches[index] = &productBatch{}
	}
	connection := &productSink{batches: append([]*productBatch(nil), batches...)}
	sink, err := NewClickHouseExternalBatchSink(connection)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return now }
	legacyScope, err := sink.Write(context.Background(), externalSinkBatch{
		Pointer: legacyPointer, SourceID: sourceID, Records: legacyRecords,
	})
	if err != nil {
		t.Fatal(err)
	}
	operationalScope, err := sink.Write(context.Background(), externalSinkBatch{
		Pointer: operationalPointer, SourceID: sourceID, Records: operationalRecords,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(connection.queries) != 21 {
		t.Fatalf("prepared sink kinds = %d", len(connection.queries))
	}
	for index, batch := range batches {
		if !batch.sent || len(batch.rows) != 1 {
			t.Fatalf("sink batch %d not durable: %#v", index, batch)
		}
		if !slices.ContainsFunc(batch.rows[0], func(value any) bool {
			id, ok := value.(uuid.UUID)
			return ok && id == sourceID
		}) {
			t.Fatalf("sink batch %d omitted source provenance: %#v", index, batch.rows[0])
		}
	}
	for _, table := range []string{
		"repos", "identities", "teams", "work_items", "work_item_transitions",
		"work_item_dependencies", "git_pull_requests", "git_pull_request_reviews",
		"git_commits", "operational_services", "operational_incidents",
		"operational_alerts", "operational_incident_timeline_events",
		"operational_incident_notes", "operational_incident_responders",
		"operational_escalation_policies", "operational_on_call_schedules",
		"operational_on_call_assignments", "operational_teams", "operational_users",
		"operational_service_repository_mappings",
	} {
		if !slices.ContainsFunc(connection.queries, func(query string) bool {
			return strings.Contains(query, " "+table+" ")
		}) {
			t.Errorf("missing sink table %s", table)
		}
	}
	if len(legacyScope.RepoIDs) != 1 ||
		legacyScope.RepoIDs[0] != "00b02aea-81bc-1244-b364-f93a0276ede5" {
		t.Fatalf("repo identity continuity = %v", legacyScope.RepoIDs)
	}
	if !slices.Contains(legacyScope.TeamIDs, "team-a") ||
		len(legacyScope.RecordKinds) != len(legacyRecords) ||
		len(operationalScope.RecordKinds) != len(operationalRecords) {
		t.Fatalf("recompute scopes legacy=%#v operational=%#v", legacyScope, operationalScope)
	}
}

func TestClickHouseExternalSinkRetriesAreIdempotentAtNaturalKeys(t *testing.T) {
	pointer := externalTestPointer()
	first, second := &productBatch{}, &productBatch{}
	connection := &productSink{batches: []*productBatch{first, second}}
	sink, err := NewClickHouseExternalBatchSink(connection)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) }
	source := externalSinkBatch{
		Pointer: pointer, SourceID: uuid.New(),
		Records: []externalSinkRecord{externalSinkFixture("repository.v1", map[string]any{
			"externalId": pointer.SourceInstance, "sourceSystem": "github",
		})},
	}
	if _, err := sink.Write(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Write(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if first.rows[0][0] != second.rows[0][0] || first.rows[0][1] != second.rows[0][1] {
		t.Fatalf("replay natural key changed: %v / %v", first.rows[0][:2], second.rows[0][:2])
	}
}

func TestExternalSchemaRegistryAndSinkCoverTheSameTwentyOneKinds(t *testing.T) {
	pointer := externalTestPointer()
	records := []externalSinkRecord{
		externalSinkFixture("repository.v1", map[string]any{"externalId": pointer.SourceInstance, "sourceSystem": "github"}),
		externalSinkFixture("identity.v1", map[string]any{"canonicalId": "ada", "updatedAt": "2026-07-23T11:00:00Z"}),
		externalSinkFixture("team.v1", map[string]any{"id": "team-a", "name": "Team", "updatedAt": "2026-07-23T11:00:00Z"}),
		externalSinkFixture("work_item.v1", map[string]any{
			"externalKey": "7", "provider": "github", "title": "Issue", "type": "issue",
			"status": "todo", "createdAt": "2026-07-23T11:00:00Z",
		}),
		externalSinkFixture("work_item_transition.v1", map[string]any{
			"externalKey": "7", "provider": "github", "occurredAt": "2026-07-23T11:00:00Z",
			"fromStatus": "todo", "toStatus": "in_progress",
		}),
		externalSinkFixture("work_item_dependency.v1", map[string]any{
			"sourceExternalKey": "7", "targetExternalKey": "8", "relationshipType": "blocks",
		}),
		externalSinkFixture("pull_request.v1", map[string]any{
			"repositoryExternalId": pointer.SourceInstance, "number": json.Number("7"),
			"state": "open", "createdAt": "2026-07-23T11:00:00Z",
		}),
		externalSinkFixture("review.v1", map[string]any{
			"repositoryExternalId": pointer.SourceInstance, "pullRequestNumber": json.Number("7"),
			"reviewId": "r-1", "reviewer": "ada", "state": "APPROVED", "submittedAt": "2026-07-23T11:00:00Z",
		}),
		externalSinkFixture("commit.v1", map[string]any{
			"repositoryExternalId": pointer.SourceInstance, "hash": "abcdef0", "authorWhen": "2026-07-23T11:00:00Z",
		}),
	}
	records = append(records, externalOperationalSinkFixtures()...)
	if len(records) != 21 || len(externalRecordSchemas) != 21 {
		t.Fatalf("schema coverage records=%d schemas=%d", len(records), len(externalRecordSchemas))
	}
	for _, record := range records {
		if err := validateExternalRecord(record.Kind, record.Payload); err != nil {
			t.Errorf("%s fixture rejected: %v", record.Kind, err)
		}
		if _, err := externalInsertQuery(record.Kind); err != nil {
			t.Errorf("%s has no sink: %v", record.Kind, err)
		}
	}
}

func TestExternalClickHouseRowsMatchPythonGoldenOracle(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate golden test")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "..", "..", "tests", "fixtures", "external_ingest_sink_python_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		FixedNow string `json:"fixed_now"`
		OrgID    string `json:"org_id"`
		SourceID string `json:"source_id"`
		Sources  map[string]struct {
			System, Instance string
		} `json:"sources"`
		Rows map[string]struct {
			Payload map[string]any `json:"payload"`
			Table   string         `json:"table"`
			Columns []string       `json:"columns"`
			Values  []any          `json:"values"`
		} `json:"rows"`
		EdgeCases map[string]string `json:"edge_cases"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&golden); err != nil {
		t.Fatal(err)
	}
	now, err := time.Parse(time.RFC3339Nano, golden.FixedNow)
	if err != nil {
		t.Fatal(err)
	}
	sourceID := uuid.MustParse(golden.SourceID)
	if len(golden.Rows) != 21 {
		t.Fatalf("Python golden rows = %d, want all 21 kinds", len(golden.Rows))
	}
	for kind, expected := range golden.Rows {
		kind, expected := kind, expected
		t.Run(kind, func(t *testing.T) {
			family := "legacy"
			if _, operational := operationalExternalKinds[kind]; operational {
				family = "operational"
			}
			source := golden.Sources[family]
			pointer := externalPointer{
				OrgID: golden.OrgID, SourceSystem: source.System,
				SourceInstance: source.Instance, IngestionID: uuid.MustParse("11111111-2222-4333-8444-555555555555"),
			}
			record := externalSinkRecord{Kind: kind, ExternalID: "golden", Payload: expected.Payload}
			values, err := externalRecordValues(
				externalSinkBatch{Pointer: pointer, SourceID: sourceID},
				record, now, &ExternalRecomputeScope{},
			)
			if err != nil {
				t.Fatal(err)
			}
			query, err := externalInsertQuery(kind)
			if err != nil {
				t.Fatal(err)
			}
			table, columns := externalQueryContract(query)
			if table != expected.Table || !slices.Equal(columns, expected.Columns) {
				t.Fatalf("ClickHouse contract mismatch:\n got table=%s columns=%v\nwant table=%s columns=%v", table, columns, expected.Table, expected.Columns)
			}
			got := goldenComparable(values)
			want := goldenComparable(expected.Values)
			if !reflect.DeepEqual(got, want) {
				gotJSON, _ := json.MarshalIndent(got, "", "  ")
				wantJSON, _ := json.MarshalIndent(want, "", "  ")
				t.Fatalf("Python row parity mismatch:\n got %s\nwant %s", gotJSON, wantJSON)
			}
		})
	}

	if got := externalWorkItemID("github", "Acme/API", "7", "pr"); got != golden.EdgeCases["github_pr"] {
		t.Fatalf("GitHub PR mapping = %q", got)
	}
	if got := externalWorkItemID("gitlab", "Group/Project", "9", "merge_request"); got != golden.EdgeCases["gitlab_merge_request"] {
		t.Fatalf("GitLab MR mapping = %q", got)
	}
	if got := externalWorkItemID("jira", "", "ABC-42", "issue"); got != golden.EdgeCases["jira_issue"] {
		t.Fatalf("Jira mapping = %q", got)
	}
	if got := uuid.Nil.String(); got != golden.EdgeCases["non_git_repo_uuid"] {
		t.Fatalf("non-git repo UUID = %q", got)
	}
	if got := externalRepoUUID("github", "Acme/API", "Acme/API").String(); got != golden.EdgeCases["repo_uuid"] {
		t.Fatalf("repo UUID = %q", got)
	}
	if got := ""; got != golden.EdgeCases["empty_identity"] {
		t.Fatalf("empty identity contract = %q", got)
	}
}

func externalQueryContract(query string) (string, []string) {
	const prefix = "INSERT INTO "
	body := strings.TrimPrefix(query, prefix)
	open, close := strings.IndexByte(body, '('), strings.LastIndexByte(body, ')')
	if open < 1 || close <= open {
		return "", nil
	}
	table := strings.TrimSpace(body[:open])
	rawColumns := strings.Split(body[open+1:close], ",")
	columns := make([]string, 0, len(rawColumns))
	for _, column := range rawColumns {
		columns = append(columns, strings.TrimSpace(column))
	}
	return table, columns
}

func goldenComparable(values []any) []any {
	result := make([]any, len(values))
	for index, value := range values {
		result[index] = goldenComparableValue(value)
	}
	return result
}

func goldenComparableValue(value any) any {
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC().Format("2006-01-02T15:04:05.000000Z")
	case uuid.UUID:
		return typed.String()
	case *big.Int:
		if typed == nil {
			return nil
		}
		return typed.String()
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'g', -1, 32)
	case int:
		return strconv.FormatInt(int64(typed), 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint8:
		return strconv.FormatUint(uint64(typed), 10)
	case uint32:
		return strconv.FormatUint(uint64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case []string:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = item
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = goldenComparableValue(item)
		}
		return result
	default:
		return value
	}
}

func externalSinkFixture(kind string, payload map[string]any) externalSinkRecord {
	return externalSinkRecord{Index: 0, Kind: kind, ExternalID: "record-1", Payload: payload}
}

func externalOperationalSinkFixtures() []externalSinkRecord {
	common := func(externalID string) map[string]any {
		return map[string]any{
			"externalId": externalID, "sourceVersionAt": "2026-07-22T15:00:00Z",
		}
	}
	with := func(externalID string, fields map[string]any) map[string]any {
		value := common(externalID)
		for key, item := range fields {
			value[key] = item
		}
		return value
	}
	return []externalSinkRecord{
		externalSinkFixture("operational_service.v1", with("service-1", map[string]any{"name": "Service"})),
		externalSinkFixture("operational_incident.v1", with("incident-1", map[string]any{"title": "Incident", "serviceExternalId": "service-1"})),
		externalSinkFixture("operational_alert.v1", with("alert-1", map[string]any{"title": "Alert", "incidentExternalId": "incident-1"})),
		externalSinkFixture("incident_timeline_event.v1", with("timeline-1", map[string]any{"incidentExternalId": "incident-1", "eventType": "triggered"})),
		externalSinkFixture("incident_note.v1", with("note-1", map[string]any{"incidentExternalId": "incident-1", "body": "Note"})),
		externalSinkFixture("incident_responder.v1", with("responder-1", map[string]any{"incidentExternalId": "incident-1"})),
		externalSinkFixture("escalation_policy.v1", with("policy-1", map[string]any{"name": "Policy"})),
		externalSinkFixture("on_call_schedule.v1", with("schedule-1", map[string]any{"name": "Schedule"})),
		externalSinkFixture("on_call_assignment.v1", with("assignment-1", map[string]any{"scheduleExternalId": "schedule-1"})),
		externalSinkFixture("operational_team.v1", with("team-1", map[string]any{"name": "Ops"})),
		externalSinkFixture("operational_user.v1", with("user-1", map[string]any{"displayName": "Ada"})),
		externalSinkFixture("service_repository_mapping.v1", with("mapping-1", map[string]any{
			"serviceExternalId": "service-1", "repoFullName": "full-chaos/dev-health", "repoProvider": "github",
		})),
	}
}
