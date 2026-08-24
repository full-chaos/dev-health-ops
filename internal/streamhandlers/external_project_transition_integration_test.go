//go:build integration

package streamhandlers

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
)

const projectTransitionTestOrg = "2b237281-6b27-4b46-8b23-14f14f2cf429"

// newProjectTransitionConn applies the REAL migration chain rather than a
// hand-authored CREATE TABLE. A local DDL copy would prove only the copy: the
// FINAL-dedupe claim below is a property of the deployed engine clause and
// sorting key, and a test that declares its own table can never contradict the
// migration it is supposed to be checking.
func newProjectTransitionConn(t *testing.T) (context.Context, driver.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ctx, conn
}

func projectTransitionPointer() externalPointer {
	return externalPointer{
		IngestionID:  uuid.MustParse("2dc94e6c-b35d-4b0f-839d-20720d48d7fa"),
		OrgID:        projectTransitionTestOrg,
		SourceSystem: "github", SourceInstance: "full-chaos/dev-health",
		SchemaVersion: externalSchemaVersion,
	}
}

// TestProjectTransitionRoundTripsAndDedupesOnResync is the acceptance claim for
// CHAOS-4194 deliverable 1: an emitted work_item_project_transition.v1 lands as
// one queryable row, and a re-sync of the SAME provider event collapses back to
// one row under FINAL instead of accumulating a duplicate per sync.
//
// Both syncs go through the production sink with a DIFFERENT last_synced, which
// is the case that actually matters: identical rows would dedupe under any
// sorting key, whereas rows differing only in the ReplacingMergeTree version
// column only collapse if event_id is genuinely in the key.
func TestProjectTransitionRoundTripsAndDedupesOnResync(t *testing.T) {
	ctx, conn := newProjectTransitionConn(t)
	pointer := projectTransitionPointer()
	sourceID := uuid.MustParse("9749bda0-fc9f-4076-b19d-7b26c4f306ff")
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	record := externalSinkRecord{Index: 0, Kind: "work_item_project_transition.v1", ExternalID: "evt-1", Payload: map[string]any{
		"externalKey": "7", "provider": "github", "eventId": "evt-1", "workItemType": "issue",
		"occurredAt": "2026-07-22T11:00:00Z", "toProjectId": "ghprojv2:full-chaos#4",
		"toProjectKey": "PLATFORM", "actor": "ada",
	}}
	for index, syncedAt := range []time.Time{
		time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC),
	} {
		sink.now = func() time.Time { return syncedAt }
		if _, err := sink.Write(ctx, externalSinkBatch{
			Pointer: pointer, SourceID: sourceID, Records: []externalSinkRecord{record},
		}); err != nil {
			t.Fatalf("sync %d: %v", index+1, err)
		}
	}
	var raw uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM work_item_project_transitions WHERE org_id = ?`, projectTransitionTestOrg,
	).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	// Not an assertion about the fix, but the premise of one: if the two syncs
	// had already collapsed at write time the FINAL check below would pass
	// without proving anything about the sorting key.
	if raw != 2 {
		t.Fatalf("re-sync wrote %d raw rows, want 2 -- the dedupe claim below is untested otherwise", raw)
	}
	var (
		rows       uint64
		workItemID string
		fromID     string
		toID       string
		toKey      string
		actor      *string
		provider   string
		occurredAt time.Time
		lastSynced time.Time
		repoID     uuid.UUID
	)
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_item_project_transitions FINAL WHERE org_id = ?`,
		projectTransitionTestOrg).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("FINAL row count after re-sync = %d, want 1", rows)
	}
	if err := conn.QueryRow(ctx, `
SELECT work_item_id, from_project_id, to_project_id, to_project_key, actor, provider, occurred_at, last_synced, repo_id
FROM work_item_project_transitions FINAL WHERE org_id = ?`, projectTransitionTestOrg).Scan(
		&workItemID, &fromID, &toID, &toKey, &actor, &provider, &occurredAt, &lastSynced, &repoID); err != nil {
		t.Fatal(err)
	}
	if workItemID != "gh:full-chaos/dev-health#7" || toID != "ghprojv2:full-chaos#4" || toKey != "PLATFORM" {
		t.Errorf("row identity = %s / %s / %s", workItemID, toID, toKey)
	}
	// The FIRST assignment case: from_* is the empty string, not NULL, per the
	// CHAOS-4194 provisional default. A NULL here would mean the column was
	// declared Nullable against the lock.
	if fromID != "" {
		t.Errorf("from_project_id = %q, want the empty-string first-assignment default", fromID)
	}
	if actor == nil || *actor != "github:ada" {
		t.Errorf("actor = %v, want github:ada", actor)
	}
	if provider != "github" || !occurredAt.Equal(time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC)) {
		t.Errorf("provider/occurred_at = %s / %s", provider, occurredAt)
	}
	// The surviving row must be the LATER observation. ReplacingMergeTree keeps
	// the max of its version column, so this is what proves last_synced is
	// wired as the version and not merely stored.
	if !lastSynced.Equal(time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("last_synced = %s, want the re-sync observation", lastSynced)
	}
	if repoID == uuid.Nil {
		t.Errorf("repo_id = %s, want the derived github repo uuid", repoID)
	}
}

// TestWorkItemProjectPresencePrefersTransitionsAndFallsBackToTheColumn is
// deliverable 2's acceptance claim, over the real migrated view.
//
// The two arms are asserted together on purpose. Either one alone is easy to
// satisfy and wrong: a projection that reads only transitions silently loses
// every pre-CDC work item, and one that reads only the column is the
// history-less shape CHAOS-4194 exists to replace. What has to hold is that a
// work item with history is answered by its LATEST transition while a work item
// without history still resolves, and that a consumer can tell which it got.
func TestWorkItemProjectPresencePrefersTransitionsAndFallsBackToTheColumn(t *testing.T) {
	ctx, conn := newProjectTransitionConn(t)
	pointer := projectTransitionPointer()
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC) }
	// Two reassignments for work item 7, out of chronological order in the
	// batch so the projection cannot pass by accident of insertion order.
	records := []externalSinkRecord{
		{Index: 0, Kind: "work_item_project_transition.v1", ExternalID: "evt-2", Payload: map[string]any{
			"externalKey": "7", "provider": "github", "eventId": "evt-2", "workItemType": "issue",
			"occurredAt": "2026-07-22T15:00:00Z", "fromProjectId": "ghprojv2:full-chaos#4",
			"toProjectId": "ghprojv2:full-chaos#9", "toProjectKey": "RUNTIME",
		}},
		{Index: 1, Kind: "work_item_project_transition.v1", ExternalID: "evt-1", Payload: map[string]any{
			"externalKey": "7", "provider": "github", "eventId": "evt-1", "workItemType": "issue",
			"occurredAt": "2026-07-22T11:00:00Z", "toProjectId": "ghprojv2:full-chaos#4",
			"toProjectKey": "PLATFORM",
		}},
		// A work item that also carries a work_items row, to prove the
		// fallback arm does not double-count one with history.
		{Index: 2, Kind: "work_item.v1", ExternalID: "7", Payload: map[string]any{
			"externalKey": "7", "provider": "github", "title": "Has history",
			"type": "issue", "status": "in_progress", "createdAt": "2026-07-20T10:00:00Z",
			"repositoryExternalId": pointer.SourceInstance,
		}},
		// A work item with NO transition history at all: the pre-CDC case.
		{Index: 3, Kind: "work_item.v1", ExternalID: "8", Payload: map[string]any{
			"externalKey": "8", "provider": "github", "title": "No history",
			"type": "issue", "status": "todo", "createdAt": "2026-07-20T10:00:00Z",
			"repositoryExternalId": pointer.SourceInstance,
		}},
	}
	if _, err := sink.Write(ctx, externalSinkBatch{
		Pointer: pointer, SourceID: uuid.New(), Records: records,
	}); err != nil {
		t.Fatal(err)
	}
	type presence struct {
		projectID, projectKey, source string
	}
	found := map[string]presence{}
	result, err := conn.Query(ctx,
		`SELECT work_item_id, project_id, project_key, source FROM work_item_project_presence WHERE org_id = ?`,
		projectTransitionTestOrg)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	for result.Next() {
		var workItemID, projectID, projectKey, source string
		if err := result.Scan(&workItemID, &projectID, &projectKey, &source); err != nil {
			t.Fatal(err)
		}
		if _, duplicate := found[workItemID]; duplicate {
			t.Fatalf("%s appears twice -- the arms overlap", workItemID)
		}
		found[workItemID] = presence{projectID, projectKey, source}
	}
	if err := result.Err(); err != nil {
		t.Fatal(err)
	}
	withHistory, ok := found["gh:full-chaos/dev-health#7"]
	if !ok {
		t.Fatalf("no presence edge for the work item with history: %#v", found)
	}
	if withHistory.projectID != "ghprojv2:full-chaos#9" || withHistory.projectKey != "RUNTIME" {
		t.Errorf("presence resolved to %#v, want the LATEST transition", withHistory)
	}
	if withHistory.source != "transition" {
		t.Errorf("source = %q, want transition", withHistory.source)
	}
	withoutHistory, ok := found["gh:full-chaos/dev-health#8"]
	if !ok {
		t.Fatalf("pre-CDC work item lost its project edge: %#v", found)
	}
	if withoutHistory.source != "work_item_column" {
		t.Errorf("source = %q, want work_item_column", withoutHistory.source)
	}
	if withoutHistory.projectID == "" {
		t.Errorf("fallback arm produced an empty project id: %#v", withoutHistory)
	}
}

// TestSameSecondReassignmentsSurviveAsDistinctRows is the other half of the
// dedupe claim, and the one a re-sync test cannot make. Bulk project moves land
// in the same millisecond routinely, so if occurred_at alone decided identity
// they would silently collapse into one row and the work item's history would
// show a single move it never made. event_id in the sorting key is what keeps
// them apart -- and the presence projection must still resolve to exactly one
// of them rather than mixing their columns.
func TestSameSecondReassignmentsSurviveAsDistinctRows(t *testing.T) {
	ctx, conn := newProjectTransitionConn(t)
	pointer := projectTransitionPointer()
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC) }
	shared := "2026-07-22T11:00:00.000Z"
	records := []externalSinkRecord{}
	for index, event := range []struct{ id, project, key string }{
		{"evt-a", "ghprojv2:full-chaos#4", "PLATFORM"},
		{"evt-b", "ghprojv2:full-chaos#9", "RUNTIME"},
	} {
		records = append(records, externalSinkRecord{
			Index: index, Kind: "work_item_project_transition.v1", ExternalID: event.id,
			Payload: map[string]any{
				"externalKey": "7", "provider": "github", "eventId": event.id,
				"workItemType": "issue", "occurredAt": shared,
				"toProjectId": event.project, "toProjectKey": event.key,
			},
		})
	}
	if _, err := sink.Write(ctx, externalSinkBatch{
		Pointer: pointer, SourceID: uuid.New(), Records: records,
	}); err != nil {
		t.Fatal(err)
	}
	var rows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM work_item_project_transitions FINAL WHERE org_id = ?`,
		projectTransitionTestOrg).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("same-timestamp reassignments collapsed to %d rows, want 2", rows)
	}
	// The projection picks ONE of them whole. Which one is decided by the
	// event_id tiebreak, so it must not be a blend of both rows' columns.
	var projectID, projectKey string
	if err := conn.QueryRow(ctx,
		`SELECT project_id, project_key FROM work_item_project_presence WHERE org_id = ?`,
		projectTransitionTestOrg).Scan(&projectID, &projectKey); err != nil {
		t.Fatal(err)
	}
	pairs := map[string]string{
		"ghprojv2:full-chaos#4": "PLATFORM",
		"ghprojv2:full-chaos#9": "RUNTIME",
	}
	if want, known := pairs[projectID]; !known || want != projectKey {
		t.Fatalf("presence mixed columns from different rows: project_id=%q project_key=%q", projectID, projectKey)
	}
}
