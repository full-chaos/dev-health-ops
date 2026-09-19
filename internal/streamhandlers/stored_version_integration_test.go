//go:build integration

package streamhandlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/google/uuid"
)

const storedVersionOrg = "org-stored-version"

var (
	establishedAt = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	firstWriteAt  = time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
	secondWriteAt = time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC)
)

func establishRow(ctx context.Context, t *testing.T, conn driver.Conn, insert string, values ...any) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, insert)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(values...); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

// finalColumns reads the winning version the Go API and metrics jobs read.
func finalColumns(ctx context.Context, t *testing.T, conn driver.Conn, table, keyFilter string, columns []string, args ...any) map[string]string {
	t.Helper()
	selected := ""
	for i, column := range columns {
		if i > 0 {
			selected += ", "
		}
		selected += fmt.Sprintf("ifNull(toString(%s), 'NULL')", column)
	}
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT %s FROM %s FINAL WHERE org_id = ? AND %s", selected, table, keyFilter),
		append([]any{storedVersionOrg}, args...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	out := map[string]string{}
	for rows.Next() {
		count++
		values := make([]string, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatal(err)
		}
		for i, column := range columns {
			out[column] = values[i]
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("%s %v: FINAL rows = %d, want 1", table, args, count)
	}
	return out
}

func expectColumns(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	for column, value := range want {
		if got[column] != value {
			t.Errorf("%s = %q, want %q", column, got[column], value)
		}
	}
}

func internalIngest(ctx context.Context, t *testing.T, conn driver.Conn, at time.Time, entity, repoURL, items string) {
	t.Helper()
	handler, err := NewInternalIngestHandler(conn)
	if err != nil {
		t.Fatal(err)
	}
	handler.now = func() time.Time { return at }
	payload := fmt.Sprintf(`{"org_id":%q,"repo_url":%q,"items":%s}`, storedVersionOrg, repoURL, items)
	if err := handler.Handle(ctx, streamrunner.Message{
		Stream: "ingest:" + storedVersionOrg + ":" + entity, Fields: map[string]string{"payload": payload},
	}); err != nil {
		t.Fatal(err)
	}
}

func externalWrite(ctx context.Context, t *testing.T, conn driver.Conn, at time.Time, system, instance, kind string, payloads ...string) {
	t.Helper()
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return at }
	records := make([]externalSinkRecord, 0, len(payloads))
	for i, raw := range payloads {
		decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
		decoder.UseNumber()
		var payload map[string]any
		if err := decoder.Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if err := validateExternalRecord(kind, payload); err != nil {
			t.Fatalf("payload %d is not a valid %s: %v", i, kind, err)
		}
		records = append(records, externalSinkRecord{Index: i, Kind: kind, ExternalID: fmt.Sprint(i), Payload: payload})
	}
	if _, err := sink.Write(ctx, externalSinkBatch{
		Pointer: externalPointer{
			IngestionID: uuid.New(), OrgID: storedVersionOrg, SourceSystem: system,
			SourceInstance: instance, SchemaVersion: externalSchemaVersion,
		},
		SourceID: uuid.MustParse("5e1f2b1a-7c2d-4e8f-9a0b-1c2d3e4f5a6b"),
		Records:  records,
	}); err != nil {
		t.Fatal(err)
	}
}

const establishPullRequest = "INSERT INTO git_pull_requests (org_id,repo_id,number,title,body,state,author_name,created_at,merged_at,closed_at,additions,first_review_at,first_comment_at,changes_requested_count,reviews_count,comments_count,last_synced)"

var prColumns = []string{"title", "body", "state", "merged_at", "closed_at", "additions", "first_review_at", "first_comment_at", "changes_requested_count", "reviews_count", "comments_count"}

func TestStoredVersionsKeepEstablishedValuesAgainstRealClickHouse(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	created := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	merged := time.Date(2026, 6, 3, 9, 0, 0, 0, time.UTC)
	reviewed := time.Date(2026, 6, 2, 9, 0, 0, 0, time.UTC)
	commented := time.Date(2026, 6, 2, 8, 0, 0, 0, time.UTC)

	const internalRepo = "https://example.test/acme/api"
	internalRepoID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(internalRepo))
	externalRepoID := externalRepoUUID("github", "acme/api", "acme/api")

	t.Run("internal pull request keeps unstated review columns and refuses null merged_at", func(t *testing.T) {
		establishRow(ctx, t, conn, establishPullRequest, storedVersionOrg, internalRepoID, uint32(7), "Old", "old body", "merged", "Ada",
			created, merged, merged, uint32(4), reviewed, commented, uint32(1), uint32(3), uint32(5), establishedAt)
		internalIngest(ctx, t, conn, firstWriteAt, "pull-requests", internalRepo,
			`[{"number":7,"title":"New","body":null,"state":"merged","author_name":"Ada","created_at":"2026-06-01T09:00:00Z","merged_at":null,"closed_at":null,"additions":null}]`)
		expectColumns(t, finalColumns(ctx, t, conn, "git_pull_requests", "repo_id = ? AND number = ?", prColumns, internalRepoID.String(), uint32(7)), map[string]string{
			"title": "New", "body": "NULL", "closed_at": "NULL", "additions": "NULL",
			"merged_at": "2026-06-03 09:00:00.000", "first_review_at": "2026-06-02 09:00:00.000", "first_comment_at": "2026-06-02 08:00:00.000",
			"changes_requested_count": "1", "reviews_count": "3", "comments_count": "5",
		})
	})

	t.Run("internal pull request without an earlier version writes its own values", func(t *testing.T) {
		internalIngest(ctx, t, conn, firstWriteAt, "pull-requests", internalRepo,
			`[{"number":8,"title":"Fresh","state":"open","author_name":"Ada","created_at":"2026-06-01T09:00:00Z","merged_at":null,"additions":null,"deletions":12}]`)
		expectColumns(t, finalColumns(ctx, t, conn, "git_pull_requests", "repo_id = ? AND number = ?", append(prColumns, "deletions"), internalRepoID.String(), uint32(8)), map[string]string{
			"title": "Fresh", "merged_at": "NULL", "first_review_at": "NULL", "reviews_count": "0", "additions": "NULL", "deletions": "12",
		})
	})

	t.Run("internal pull request stated twice in one batch keeps the first merged_at", func(t *testing.T) {
		internalIngest(ctx, t, conn, firstWriteAt, "pull-requests", internalRepo,
			`[{"number":9,"title":"A","state":"merged","author_name":"Ada","created_at":"2026-06-01T09:00:00Z","merged_at":"2026-06-03T09:00:00Z"},`+
				`{"number":9,"title":"B","state":"merged","author_name":"Ada","created_at":"2026-06-01T09:00:00Z","merged_at":null}]`)
		expectColumns(t, finalColumns(ctx, t, conn, "git_pull_requests", "repo_id = ? AND number = ?", prColumns, internalRepoID.String(), uint32(9)), map[string]string{
			"title": "B", "merged_at": "2026-06-03 09:00:00.000",
		})
	})

	t.Run("internal deployment keeps merged_at it has no field for", func(t *testing.T) {
		establishRow(ctx, t, conn, "INSERT INTO deployments (org_id,repo_id,deployment_id,status,environment,merged_at,pull_request_number,last_synced)",
			storedVersionOrg, internalRepoID, "deploy-1", "success", "prod", merged, uint32(7), establishedAt)
		internalIngest(ctx, t, conn, firstWriteAt, "deployments", internalRepo,
			`[{"deployment_id":"deploy-1","status":"failure","environment":"prod","pull_request_number":null}]`)
		expectColumns(t, finalColumns(ctx, t, conn, "deployments", "repo_id = ? AND deployment_id = ?", []string{"status", "merged_at", "pull_request_number"}, internalRepoID.String(), "deploy-1"), map[string]string{
			"status": "failure", "merged_at": "2026-06-03 09:00:00.000", "pull_request_number": "NULL",
		})
	})

	t.Run("internal work item keeps columns the ingest payload has no field for", func(t *testing.T) {
		establishRow(ctx, t, conn, "INSERT INTO work_items (org_id,repo_id,work_item_id,provider,title,type,status,status_raw,project_key,project_id,native_team_key,project_name,assignees,reporter,created_at,updated_at,closed_at,labels,sprint_id,sprint_name,parent_id,epic_id,url,service_class,due_at,last_synced)",
			storedVersionOrg, uuid.Nil, "jira:ABC-123", "jira", "Old", "bug", "done", "Done", "ABC", "10001", "TEAM", "Platform", []string{}, "", created, merged, merged, []string{},
			"sprint-9", "Sprint 9", "jira:ABC-1", "jira:ABC-2", "", "expedite", merged, establishedAt)
		internalIngest(ctx, t, conn, firstWriteAt, "work-items", "",
			`[{"work_item_id":"jira:ABC-123","provider":"jira","title":"New","type":"bug","status":"done","created_at":"2026-06-01T09:00:00Z","updated_at":null}]`)
		columns := []string{"title", "updated_at", "project_id", "native_team_key", "project_name", "closed_at", "sprint_id", "sprint_name", "parent_id", "epic_id", "service_class", "due_at"}
		expectColumns(t, finalColumns(ctx, t, conn, "work_items", "repo_id = ? AND work_item_id = ?", columns, uuid.Nil.String(), "jira:ABC-123"), map[string]string{
			"title": "New", "updated_at": "2026-06-03 09:00:00.000", "project_id": "10001", "native_team_key": "TEAM", "project_name": "Platform",
			"closed_at": "2026-06-03 09:00:00.000", "sprint_id": "sprint-9", "sprint_name": "Sprint 9", "parent_id": "jira:ABC-1", "epic_id": "jira:ABC-2",
			"service_class": "expedite", "due_at": "2026-06-03 09:00:00.000",
		})
		internalIngest(ctx, t, conn, secondWriteAt, "work-items", "",
			`[{"work_item_id":"jira:ABC-123","provider":"jira","title":"New","type":"bug","status":"done","created_at":"2026-06-01T09:00:00Z","updated_at":"2026-06-05T09:00:00Z"}]`)
		expectColumns(t, finalColumns(ctx, t, conn, "work_items", "repo_id = ? AND work_item_id = ?", columns, uuid.Nil.String(), "jira:ABC-123"), map[string]string{
			"updated_at": "2026-06-05 09:00:00.000", "sprint_id": "sprint-9",
		})
	})

	t.Run("external pull request keeps unstated columns and writes stated nulls", func(t *testing.T) {
		establishRow(ctx, t, conn, establishPullRequest, storedVersionOrg, externalRepoID, uint32(7), "Old", "old body", "merged", "Ada",
			created, merged, merged, uint32(4), reviewed, commented, uint32(1), uint32(3), uint32(5), establishedAt)
		externalWrite(ctx, t, conn, firstWriteAt, "github", "acme/api", "pull_request.v1",
			`{"repositoryExternalId":"acme/api","number":7,"state":"merged","createdAt":"2026-06-01T09:00:00Z","title":"New","firstCommentAt":null,"mergedAt":null,"firstReviewAt":null}`)
		expectColumns(t, finalColumns(ctx, t, conn, "git_pull_requests", "repo_id = ? AND number = ?", prColumns, externalRepoID.String(), uint32(7)), map[string]string{
			"title": "New", "body": "old body", "additions": "4", "closed_at": "NULL", "first_comment_at": "NULL",
			"merged_at": "2026-06-03 09:00:00.000", "first_review_at": "2026-06-02 09:00:00.000",
			"changes_requested_count": "1", "reviews_count": "3", "comments_count": "5",
		})
	})

	t.Run("external pull request without an earlier version and twice in one batch", func(t *testing.T) {
		externalWrite(ctx, t, conn, firstWriteAt, "github", "acme/api", "pull_request.v1",
			`{"repositoryExternalId":"acme/api","number":8,"state":"open","createdAt":"2026-06-01T09:00:00Z","reviewsCount":2,"firstReviewAt":"2026-06-02T09:00:00Z"}`,
			`{"repositoryExternalId":"acme/api","number":8,"state":"open","createdAt":"2026-06-01T09:00:00Z","title":"Second"}`)
		expectColumns(t, finalColumns(ctx, t, conn, "git_pull_requests", "repo_id = ? AND number = ?", prColumns, externalRepoID.String(), uint32(8)), map[string]string{
			"title": "Second", "reviews_count": "2", "first_review_at": "2026-06-02 09:00:00.000", "merged_at": "NULL", "comments_count": "0",
		})
	})

	t.Run("external commit keeps unstated message and writes a stated null", func(t *testing.T) {
		establishRow(ctx, t, conn, "INSERT INTO git_commits (org_id,repo_id,hash,message,author_name,author_email,author_when,committer_name,committer_email,committer_when,parents,last_synced)",
			storedVersionOrg, externalRepoID, "abc1234", "fix: keep rows", "Ada", "ada@example.test", created, "Grace", "grace@example.test", reviewed, uint32(2), establishedAt)
		externalWrite(ctx, t, conn, firstWriteAt, "github", "acme/api", "commit.v1",
			`{"repositoryExternalId":"acme/api","hash":"abc1234","authorWhen":"2026-06-01T09:00:00Z","authorName":null}`)
		expectColumns(t, finalColumns(ctx, t, conn, "git_commits", "repo_id = ? AND hash = ?",
			[]string{"message", "author_name", "author_email", "committer_name", "committer_when", "parents"}, externalRepoID.String(), "abc1234"), map[string]string{
			"message": "fix: keep rows", "author_name": "NULL", "author_email": "ada@example.test",
			"committer_name": "Grace", "committer_when": "2026-06-02 09:00:00.000", "parents": "2",
		})
	})

	t.Run("external work item keeps unstated and unwritten columns", func(t *testing.T) {
		establishRow(ctx, t, conn, "INSERT INTO work_items (org_id,repo_id,work_item_id,provider,title,description,type,status,status_raw,project_key,project_id,native_team_key,project_name,assignees,reporter,created_at,updated_at,labels,story_points,sprint_id,sprint_name,parent_id,epic_id,url,priority_raw,last_synced)",
			storedVersionOrg, uuid.Nil, "jira:ABC-124", "jira", "Old", "details", "bug", "in_progress", "In Progress", "ABC", "10001", "", "Platform",
			[]string{"ada@example.test"}, "grace@example.test", created, merged, []string{"backend"}, 3.0, "sprint-9", "Sprint 9", "jira:ABC-1", "", "https://jira.example.test/ABC-124", "High", establishedAt)
		externalWrite(ctx, t, conn, firstWriteAt, "jira", "acme", "work_item.v1",
			`{"externalKey":"ABC-124","provider":"jira","title":"New","status":"in_progress","createdAt":"2026-06-01T09:00:00Z","sprintName":null}`)
		columns := []string{"title", "description", "type", "status_raw", "project_key", "project_id", "project_name", "assignees", "reporter", "updated_at", "labels", "story_points", "sprint_id", "sprint_name", "parent_id", "url", "priority_raw"}
		expectColumns(t, finalColumns(ctx, t, conn, "work_items", "repo_id = ? AND work_item_id = ?", columns, uuid.Nil.String(), "jira:ABC-124"), map[string]string{
			"title": "New", "description": "details", "type": "bug", "status_raw": "In Progress", "project_key": "ABC", "project_id": "10001",
			"project_name": "Platform", "assignees": "['ada@example.test']", "reporter": "grace@example.test", "updated_at": "2026-06-03 09:00:00.000",
			"labels": "['backend']", "story_points": "3", "sprint_id": "sprint-9", "sprint_name": "", "parent_id": "jira:ABC-1",
			"url": "https://jira.example.test/ABC-124", "priority_raw": "High",
		})
		externalWrite(ctx, t, conn, secondWriteAt, "jira", "acme", "work_item.v1",
			`{"externalKey":"ABC-124","provider":"jira","title":"New","status":"in_progress","createdAt":"2026-06-01T09:00:00Z","projectKey":"XYZ",`+
				`"description":null,"priorityRaw":"Low","serviceClass":"standard","dueAt":"2026-07-01T00:00:00Z"}`)
		expectColumns(t, finalColumns(ctx, t, conn, "work_items", "repo_id = ? AND work_item_id = ?", append(columns, "service_class", "due_at"), uuid.Nil.String(), "jira:ABC-124"), map[string]string{
			"project_key": "XYZ", "project_id": "10001", "project_name": "Platform", "sprint_id": "sprint-9",
			"description": "NULL", "priority_raw": "Low", "service_class": "standard", "due_at": "2026-07-01 00:00:00.000",
		})
	})

	t.Run("external linear and github work items follow the system's project derivation", func(t *testing.T) {
		const establishWorkItem = "INSERT INTO work_items (org_id,repo_id,work_item_id,provider,title,type,status,status_raw,project_key,project_id,native_team_key,project_name,assignees,reporter,created_at,updated_at,labels,story_points,sprint_id,sprint_name,parent_id,epic_id,url,last_synced)"
		projectColumns := []string{"project_key", "project_id", "native_team_key", "project_name", "story_points"}
		establishRow(ctx, t, conn, establishWorkItem, storedVersionOrg, uuid.Nil, "linear:ABC-125", "linear", "Old", "issue", "todo", "Todo",
			"LIN", "project-uuid", "ENG", "Roadmap", []string{}, "", created, merged, []string{}, 5.0, "", "", "", "", "", establishedAt)
		externalWrite(ctx, t, conn, firstWriteAt, "linear", "acme", "work_item.v1",
			`{"externalKey":"ABC-125","provider":"linear","title":"New","status":"todo","createdAt":"2026-06-01T09:00:00Z"}`)
		expectColumns(t, finalColumns(ctx, t, conn, "work_items", "repo_id = ? AND work_item_id = ?", projectColumns, uuid.Nil.String(), "linear:ABC-125"), map[string]string{
			"project_key": "LIN", "project_id": "project-uuid", "native_team_key": "ENG", "project_name": "Roadmap", "story_points": "5",
		})
		externalWrite(ctx, t, conn, secondWriteAt, "linear", "acme", "work_item.v1",
			`{"externalKey":"ABC-125","provider":"linear","title":"New","status":"todo","createdAt":"2026-06-01T09:00:00Z","nativeTeamKey":"OPS","storyPoints":null}`)
		expectColumns(t, finalColumns(ctx, t, conn, "work_items", "repo_id = ? AND work_item_id = ?", projectColumns, uuid.Nil.String(), "linear:ABC-125"), map[string]string{
			"project_key": "LIN", "project_id": "OPS", "native_team_key": "OPS", "project_name": "Roadmap", "story_points": "NULL",
		})
		externalWrite(ctx, t, conn, secondWriteAt.Add(time.Hour), "linear", "acme", "work_item.v1",
			`{"externalKey":"ABC-125","provider":"linear","title":"New","status":"todo","createdAt":"2026-06-01T09:00:00Z","projectId":"project-2"}`)
		expectColumns(t, finalColumns(ctx, t, conn, "work_items", "repo_id = ? AND work_item_id = ?", projectColumns, uuid.Nil.String(), "linear:ABC-125"), map[string]string{
			"project_id": "project-2", "native_team_key": "OPS", "project_name": "Roadmap",
		})

		githubRepoID := externalRepoUUID("github", "acme/api", "acme/api")
		establishRow(ctx, t, conn, establishWorkItem, storedVersionOrg, githubRepoID, "gh:acme/api#12", "github", "Old", "issue", "todo", "open",
			"KEY", "acme/old", "TEAM", "Board", []string{}, "", created, merged, []string{}, 2.0, "", "", "", "", "", establishedAt)
		externalWrite(ctx, t, conn, firstWriteAt, "github", "acme/api", "work_item.v1",
			`{"externalKey":"12","provider":"github","type":"issue","title":"New","status":"todo","createdAt":"2026-06-01T09:00:00Z","repositoryExternalId":"acme/api"}`)
		expectColumns(t, finalColumns(ctx, t, conn, "work_items", "repo_id = ? AND work_item_id = ?", projectColumns, githubRepoID.String(), "gh:acme/api#12"), map[string]string{
			"project_key": "KEY", "project_id": "acme/api", "native_team_key": "TEAM", "project_name": "Board", "story_points": "2",
		})
	})

	t.Run("internal work item writes a stated null story_points as null", func(t *testing.T) {
		internalIngest(ctx, t, conn, firstWriteAt, "work-items", "",
			`[{"work_item_id":"jira:ABC-126","provider":"jira","title":"New","type":"bug","status":"done","created_at":"2026-06-01T09:00:00Z","story_points":null}]`)
		expectColumns(t, finalColumns(ctx, t, conn, "work_items", "repo_id = ? AND work_item_id = ?", []string{"story_points", "sprint_id"}, uuid.Nil.String(), "jira:ABC-126"), map[string]string{
			"story_points": "NULL", "sprint_id": "",
		})
	})

	t.Run("stated merged_at replaces a held one and only a refused null logs a refusal", func(t *testing.T) {
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		t.Cleanup(func() { slog.SetDefault(previous) })

		establishRow(ctx, t, conn, establishPullRequest, storedVersionOrg, externalRepoID, uint32(10), "Old", nil, "open", "Ada",
			created, merged, nil, nil, nil, nil, uint32(0), uint32(0), uint32(0), establishedAt)
		externalWrite(ctx, t, conn, firstWriteAt, "github", "acme/api", "pull_request.v1",
			`{"repositoryExternalId":"acme/api","number":10,"state":"merged","createdAt":"2026-06-01T09:00:00Z","mergedAt":"2026-06-04T09:00:00Z","firstReviewAt":null}`)
		expectColumns(t, finalColumns(ctx, t, conn, "git_pull_requests", "repo_id = ? AND number = ?", prColumns, externalRepoID.String(), uint32(10)), map[string]string{
			"merged_at": "2026-06-04 09:00:00.000", "first_review_at": "NULL",
		})
		if strings.Contains(logs.String(), storedVersionRefusedEvent) {
			t.Fatalf("a null over a held null logged a refusal:\n%s", logs.String())
		}
		externalWrite(ctx, t, conn, secondWriteAt, "github", "acme/api", "pull_request.v1",
			`{"repositoryExternalId":"acme/api","number":10,"state":"merged","createdAt":"2026-06-01T09:00:00Z","mergedAt":null}`)
		if got := strings.Count(logs.String(), storedVersionRefusedEvent); got != 1 || !strings.Contains(logs.String(), "columns=merged_at") {
			t.Fatalf("refusal events = %d, want one naming merged_at:\n%s", got, logs.String())
		}
	})

	t.Run("external repository and identity keep unstated columns and created_at", func(t *testing.T) {
		repoID := externalRepoUUID("github", "acme/api", "acme/web")
		establishRow(ctx, t, conn, "INSERT INTO repos (id,repo,ref,created_at,settings,tags,provider,last_synced,org_id)",
			repoID, "acme/web", "main", created, `{"a":1}`, `["x"]`, "github", establishedAt, storedVersionOrg)
		externalWrite(ctx, t, conn, firstWriteAt, "github", "acme/api", "repository.v1",
			`{"externalId":"acme/web","sourceSystem":"github"}`)
		repoColumns := []string{"repo", "ref", "created_at", "settings", "tags", "provider"}
		expectColumns(t, finalColumns(ctx, t, conn, "repos", "id = ?", repoColumns, repoID.String()), map[string]string{
			"repo": "acme/web", "ref": "main", "created_at": "2026-06-01 09:00:00.000", "settings": `{"a":1}`, "tags": `["x"]`, "provider": "github",
		})
		externalWrite(ctx, t, conn, secondWriteAt, "github", "acme/api", "repository.v1",
			`{"externalId":"acme/web","sourceSystem":"github","defaultRef":null,"tags":["y"]}`)
		expectColumns(t, finalColumns(ctx, t, conn, "repos", "id = ?", repoColumns, repoID.String()), map[string]string{
			"ref": "NULL", "created_at": "2026-06-01 09:00:00.000", "settings": `{"a":1}`, "tags": `["y"]`,
		})
		for _, field := range []string{"settings", "tags"} {
			if err := validateExternalRecord("repository.v1", map[string]any{"externalId": "acme/web", "sourceSystem": "github", field: nil}); err == nil {
				t.Errorf("repository.v1 with %s: null was accepted; the reference refuses it", field)
			}
		}

		establishRow(ctx, t, conn, "INSERT INTO identities (org_id,canonical_id,identity_uuid,display_name,email,provider_identities,team_ids,is_active,updated_at)",
			storedVersionOrg, "ada", uuid.New(), "Ada", "ada@example.test", `{"github":["ada"]}`, []string{"team-a"}, uint8(1), establishedAt)
		externalWrite(ctx, t, conn, firstWriteAt, "github", "acme/api", "identity.v1",
			`{"canonicalId":"ada","updatedAt":"2026-07-01T12:00:00Z"}`)
		identityColumns := []string{"display_name", "email", "provider_identities", "team_ids", "is_active", "updated_at"}
		expectColumns(t, finalColumns(ctx, t, conn, "identities", "canonical_id = ?", identityColumns, "ada"), map[string]string{
			"display_name": "Ada", "email": "ada@example.test", "provider_identities": `{"github":["ada"]}`, "team_ids": "['team-a']", "is_active": "1",
			"updated_at": "2026-07-01 12:00:00.000000",
		})
		externalWrite(ctx, t, conn, firstWriteAt, "github", "acme/api", "identity.v1",
			`{"canonicalId":"ada","updatedAt":"2026-07-01T18:00:00Z","email":null}`)
		expectColumns(t, finalColumns(ctx, t, conn, "identities", "canonical_id = ?", identityColumns, "ada"), map[string]string{
			"display_name": "Ada", "email": "NULL", "team_ids": "['team-a']", "updated_at": "2026-07-01 18:00:00.000000",
		})
	})
}
