package streamhandlers

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/storedversion"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/google/uuid"
)

// A failed read of the current version must not fall through to an insert:
// the insert would write blanks over established values. The error is
// transient so the stream redelivers the message.
func TestInternalIngestWritesNothingWhenCurrentVersionReadFails(t *testing.T) {
	payloads := map[string]string{
		"pull-requests": `{"org_id":"org-1","repo_url":"https://example.test/acme/repo","items":[{"number":7,"title":"Ship it","state":"open","author_name":"Ada","created_at":"2026-07-23T12:00:00Z"}]}`,
		"deployments":   `{"org_id":"org-1","repo_url":"https://example.test/acme/repo","items":[{"deployment_id":"deploy-1","status":"success","environment":"prod"}]}`,
		"work-items":    `{"org_id":"org-1","items":[{"work_item_id":"jira:ABC-123","provider":"jira","title":"Ship it","type":"bug","status":"done","created_at":"2026-07-23T12:00:00Z"}]}`,
	}
	for _, entity := range []string{"pull-requests", "deployments", "work-items"} {
		t.Run(entity, func(t *testing.T) {
			sink := &productSink{batch: &productBatch{}, queryErr: errors.New("clickhouse unavailable")}
			handler, err := NewInternalIngestHandler(sink)
			if err != nil {
				t.Fatal(err)
			}
			err = handler.Handle(context.Background(), streamrunner.Message{
				Stream: "ingest:org-1:" + entity, Fields: map[string]string{"payload": payloads[entity]},
			})
			var permanent *streamrunner.PermanentError
			if err == nil || errors.As(err, &permanent) {
				t.Fatalf("Handle error = %v, want a transient error", err)
			}
			if sink.queryCalls != 1 || len(sink.queries) != 0 || sink.batch.sent {
				t.Fatalf("reads = %d, prepared = %v, sent = %v; want one read and no insert", sink.queryCalls, sink.queries, sink.batch.sent)
			}
		})
	}
}

func TestExternalSinkWritesNothingWhenCurrentVersionReadFails(t *testing.T) {
	payloads := map[string]map[string]any{
		"pull_request.v1": {"repositoryExternalId": "acme/api", "number": json.Number("7"), "state": "open", "createdAt": "2026-07-23T12:00:00Z"},
		"commit.v1":       {"repositoryExternalId": "acme/api", "hash": "abc1234", "authorWhen": "2026-07-23T12:00:00Z"},
		"work_item.v1":    {"externalKey": "ABC-123", "provider": "github", "title": "Ship it", "status": "todo", "createdAt": "2026-07-23T12:00:00Z"},
	}
	for _, kind := range []string{"pull_request.v1", "commit.v1", "work_item.v1"} {
		t.Run(kind, func(t *testing.T) {
			sink := &productSink{batch: &productBatch{}, queryErr: errors.New("clickhouse unavailable")}
			writer, err := NewClickHouseExternalBatchSink(sink)
			if err != nil {
				t.Fatal(err)
			}
			_, err = writer.Write(context.Background(), externalSinkBatch{
				Pointer:  externalPointer{IngestionID: uuid.New(), OrgID: "org-1", SourceSystem: "github", SourceInstance: "acme/api", SchemaVersion: externalSchemaVersion},
				SourceID: uuid.New(),
				Records:  []externalSinkRecord{{Kind: kind, ExternalID: "ABC-123", Payload: payloads[kind]}},
			})
			if err == nil {
				t.Fatal("Write succeeded, want the read failure")
			}
			if sink.queryCalls != 1 || len(sink.queries) != 0 || sink.batch.sent {
				t.Fatalf("reads = %d, prepared = %v, sent = %v; want one read and no insert", sink.queryCalls, sink.queries, sink.batch.sent)
			}
		})
	}
}

type contractUnderTest struct {
	name     string
	insert   string
	contract storedversion.Contract
	kind     string
}

func contractsUnderTest(t *testing.T) []contractUnderTest {
	t.Helper()
	out := []contractUnderTest{
		{"internal commits", internalCommitInsert, internalCommitContract, ""},
		{"internal reviews", internalReviewInsert, internalReviewContract, ""},
		{"internal pull requests", internalPullRequestInsert, internalPullRequestContract, ""},
		{"internal deployments", internalDeploymentInsert, internalDeploymentContract, ""},
		{"internal work items", internalWorkItemInsert, internalWorkItemContract, ""},
	}
	for _, kind := range []string{"pull_request.v1", "review.v1", "commit.v1", "work_item.v1", "repository.v1", "identity.v1"} {
		for _, system := range []string{"github", "gitlab", "jira", "linear", "custom", "pagerduty", "atlassian"} {
			if _, allowed := externalAllowedKinds[system][kind]; !allowed {
				continue
			}
			contract, ok := externalContract(kind, system)
			if !ok {
				t.Fatalf("%s from %s has no contract table", kind, system)
			}
			query, err := externalInsertQuery(kind)
			if err != nil {
				t.Fatal(err)
			}
			extended, _, err := withContractColumns(query, contract)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, contractUnderTest{"external " + kind + " from " + system, extended, contract, kind})
		}
	}
	return out
}

// Every column a writer inserts has exactly one row in that writer's
// contract table, and every row names a column the writer inserts.
func TestEveryWrittenColumnHasOneContractRow(t *testing.T) {
	for _, c := range contractsUnderTest(t) {
		t.Run(c.name, func(t *testing.T) {
			positions, err := storedversion.Positions(c.insert)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(c.insert, "INSERT INTO "+c.contract.Table+" (") {
				t.Fatalf("contract table %s does not match %q", c.contract.Table, c.insert)
			}
			rows := map[string]int{}
			identities := 0
			for _, column := range c.contract.Columns {
				rows[column.Name] = rows[column.Name] + 1
				if _, written := positions[column.Name]; !written {
					t.Errorf("contract row %s names a column the insert does not write", column.Name)
				}
				if column.Rule == storedversion.Identity {
					identities = identities + 1
				}
				if column.Rule == storedversion.Unstated && len(column.Fields) == 0 {
					t.Errorf("R2 row %s names no payload field", column.Name)
				}
				for _, field := range column.Fields {
					if c.kind == "" {
						continue
					}
					if _, declared := externalRecordSchemas[c.kind][field]; !declared {
						t.Errorf("R2 row %s names %s, which %s does not declare", column.Name, field, c.kind)
					}
				}
			}
			for _, column := range strings.Split(c.insert[strings.IndexByte(c.insert, '(')+1:strings.LastIndexByte(c.insert, ')')], ",") {
				if got := rows[strings.TrimSpace(column)]; got != 1 {
					t.Errorf("written column %s has %d contract rows, want 1", strings.TrimSpace(column), got)
				}
			}
			if identities == 0 {
				t.Error("contract names no identity column")
			}
		})
	}
}

func TestExternalKindsOfContractTablesAllHaveContracts(t *testing.T) {
	scoped := map[string]bool{"git_pull_requests": true, "git_pull_request_reviews": true, "git_commits": true, "deployments": true, "work_items": true, "repos": true, "identities": true}
	if len(externalRecordSchemas) == 0 {
		t.Fatal("no external schemas")
	}
	for kind := range externalRecordSchemas {
		query, err := externalInsertQuery(kind)
		if err != nil {
			continue
		}
		table := strings.TrimSpace(strings.TrimPrefix(query[:strings.IndexByte(query, '(')], "INSERT INTO "))
		if _, ok := externalContract(kind, "github"); scoped[table] && !ok {
			t.Errorf("%s writes %s without a contract table", kind, table)
		}
	}
}

// A contract with no kept column reads nothing before writing.
func TestWritersWithoutKeptColumnsDoNotReadTheCurrentVersion(t *testing.T) {
	sink := &productSink{batch: &productBatch{}, queryErr: errors.New("must not read")}
	writer, err := NewClickHouseExternalBatchSink(sink)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(context.Background(), externalSinkBatch{
		Pointer:  externalPointer{IngestionID: uuid.New(), OrgID: "org-1", SourceSystem: "github", SourceInstance: "acme/api", SchemaVersion: externalSchemaVersion},
		SourceID: uuid.New(),
		Records: []externalSinkRecord{{Kind: "review.v1", ExternalID: "r1", Payload: map[string]any{
			"repositoryExternalId": "acme/api", "pullRequestNumber": json.Number("7"), "reviewId": "r1",
			"reviewer": "grace", "state": "APPROVED", "submittedAt": "2026-07-23T12:00:00Z",
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	if sink.queryCalls != 0 || !sink.batch.sent {
		t.Fatalf("reads = %d, sent = %v; want no read and one insert", sink.queryCalls, sink.batch.sent)
	}
}

// Every payload key an in-scope external kind declares reaches a column
// through its contract table, or is named as unstored with its reason.
func TestEveryDeclaredExternalFieldReachesAColumnOrIsNamedUnstored(t *testing.T) {
	for _, c := range contractsUnderTest(t) {
		if c.kind == "" {
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			system := c.name[strings.LastIndex(c.name, " ")+1:]
			translated := map[string]bool{}
			for _, column := range c.contract.Columns {
				for _, field := range column.Fields {
					translated[field] = true
				}
			}
			declared := externalRecordSchemas[c.kind]
			if len(declared) == 0 {
				t.Fatalf("%s declares no fields", c.kind)
			}
			for field := range declared {
				reason := externalUnstoredFields[c.kind][field]
				if reason == "" {
					reason = externalUnstoredFields[c.kind+"/"+system][field]
				}
				if translated[field] == (reason != "") {
					t.Errorf("%s: translated=%v unstored=%q; want exactly one", field, translated[field], reason)
				}
			}
		})
	}
}

// frozenIngestFields reads testdata/python_ingest_declared_fields.json: the
// fields src/dev_health_ops/api/ingest/schemas.py declared per ingest class when
// the Python api source was scheduled for deletion.
func frozenIngestFields(t *testing.T) map[string][]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "python_ingest_declared_fields.json"))
	if err != nil {
		t.Fatal(err)
	}
	var frozen struct {
		Classes map[string][]string `json:"classes"`
	}
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	return frozen.Classes
}

var pythonIngestFieldLine = regexp.MustCompile(`^    ([a-z_]+): `)

// pythonIngestClassFields reads one class's declared fields out of schemas.py.
func pythonIngestClassFields(t *testing.T, source, class string) []string {
	t.Helper()
	start := strings.Index(source, "\nclass "+class+"(BaseModel):\n")
	if start < 0 {
		t.Fatalf("class %s not found", class)
	}
	body := source[start+1:]
	if end := strings.Index(body[1:], "\nclass "); end >= 0 {
		body = body[:end+1]
	}
	var fields []string
	for _, line := range strings.Split(body, "\n") {
		if match := pythonIngestFieldLine.FindStringSubmatch(line); match != nil {
			fields = append(fields, match[1])
		}
	}
	return fields
}

// Every field the ingest API declares for an entity reaches a column
// through the entity's contract table, or is named as unstored with its reason.
func TestEveryDeclaredIngestFieldReachesAColumnOrIsNamedUnstored(t *testing.T) {
	frozen := frozenIngestFields(t)
	contracts := []struct {
		class    string
		contract storedversion.Contract
	}{
		{"IngestCommit", internalCommitContract}, {"IngestPullRequestReview", internalReviewContract},
		{"IngestPullRequest", internalPullRequestContract}, {"IngestDeployment", internalDeploymentContract},
		{"IngestWorkItem", internalWorkItemContract},
	}
	for _, c := range contracts {
		t.Run(c.class, func(t *testing.T) {
			translated := map[string]bool{}
			for _, column := range c.contract.Columns {
				for _, name := range column.Fields {
					translated[name] = true
				}
			}
			declared := frozen[c.class]
			for _, name := range declared {
				reason := internalUnstoredFields[c.class][name]
				if translated[name] == (reason != "") {
					t.Errorf("%s: translated=%v unstored=%q; want exactly one", name, translated[name], reason)
				}
			}
			if len(declared) == 0 {
				t.Fatalf("%s declares no fields", c.class)
			}
		})
	}
}

func TestAppendedColumnsTranslateOrStartNil(t *testing.T) {
	contract := storedversion.Contract{Columns: []storedversion.Column{
		appended("description", "description", externalNullableString),
		{Name: "service_class", Rule: storedversion.NoField},
	}}
	got := appendedValues(contract, []string{"description", "service_class"}, map[string]any{"description": "details"})
	if len(got) != 2 || got[0] != "details" || got[1] != nil {
		t.Fatalf("appended values = %#v, want [details <nil>]", got)
	}
}
