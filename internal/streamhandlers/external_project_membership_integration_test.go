//go:build integration

package streamhandlers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
)

const projectMembershipTestOrg = "2b237281-6b27-4b46-8b23-14f14f2cf429"

// newProjectMembershipConn applies the REAL migration chain rather than a
// hand-authored CREATE TABLE. A local DDL copy would prove only the copy: the
// FINAL-dedupe claim below is a property of the deployed engine clause and
// sorting key, and a test that declares its own table can never contradict the
// migration it is supposed to be checking.
func newProjectMembershipConn(t *testing.T) (context.Context, driver.Conn) {
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

func projectMembershipPointer() externalPointer {
	return externalPointer{
		IngestionID:  uuid.MustParse("2dc94e6c-b35d-4b0f-839d-20720d48d7fa"),
		OrgID:        projectMembershipTestOrg,
		SourceSystem: "github", SourceInstance: "full-chaos/dev-health",
		SchemaVersion: externalSchemaVersion,
	}
}

func linearMembershipPointer() externalPointer {
	pointer := projectMembershipPointer()
	pointer.IngestionID = uuid.MustParse("6f1e2c1b-6f0f-46da-9d29-2b8b6a2f4a11")
	pointer.SourceSystem, pointer.SourceInstance = "linear", "full-chaos"
	return pointer
}

// TestProjectMembershipRoundTripsAndDedupesOnResync is the acceptance claim for
// CHAOS-4194 deliverable 1: an emitted project_membership_transition.v1 lands as
// one queryable row, and a re-sync of the SAME provider event collapses back to
// one row under FINAL instead of accumulating a duplicate per sync.
//
// Both syncs go through the production sink with a DIFFERENT last_synced, which
// is the case that actually matters: identical rows would dedupe under any
// sorting key, whereas rows differing only in the ReplacingMergeTree version
// column only collapse if event_id is genuinely in the key.
func TestProjectMembershipRoundTripsAndDedupesOnResync(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	pointer := projectMembershipPointer()
	sourceID := uuid.MustParse("9749bda0-fc9f-4076-b19d-7b26c4f306ff")
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	record := externalSinkRecord{Index: 0, Kind: "project_membership_transition.v1", ExternalID: "evt-1", Payload: map[string]any{
		"externalKey": "7", "provider": "github", "eventId": "evt-1",
		"subjectKind": "work_item", "workItemType": "issue",
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
		`SELECT count() FROM project_membership_transitions WHERE org_id = ?`, projectMembershipTestOrg,
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
		rows        uint64
		subjectKind string
		subjectID   string
		fromID      string
		toID        string
		toKey       string
		actor       string
		provider    string
		occurredAt  time.Time
		lastSynced  time.Time
		repoID      uuid.UUID
	)
	if err := conn.QueryRow(ctx, `SELECT count() FROM project_membership_transitions FINAL WHERE org_id = ?`,
		projectMembershipTestOrg).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("FINAL row count after re-sync = %d, want 1", rows)
	}
	if err := conn.QueryRow(ctx, `
SELECT subject_kind, subject_id, from_project_id, to_project_id, to_project_key, actor, provider, occurred_at, last_synced, repo_id
FROM project_membership_transitions FINAL WHERE org_id = ?`, projectMembershipTestOrg).Scan(
		&subjectKind, &subjectID, &fromID, &toID, &toKey, &actor, &provider, &occurredAt, &lastSynced, &repoID); err != nil {
		t.Fatal(err)
	}
	if subjectKind != "work_item" || subjectID != "gh:full-chaos/dev-health#7" ||
		toID != "ghprojv2:full-chaos#4" || toKey != "PLATFORM" {
		t.Errorf("row identity = %s / %s / %s / %s", subjectKind, subjectID, toID, toKey)
	}
	// The FIRST assignment case: from_* is the empty string, not NULL. The
	// column is a bare String -- source_id is the only Nullable one -- so a
	// scan into a plain string is itself part of the assertion; a Nullable
	// column would fail to scan here.
	if fromID != "" {
		t.Errorf("from_project_id = %q, want the empty-string first-assignment default", fromID)
	}
	if actor != "github:ada" {
		t.Errorf("actor = %q, want github:ada as a bare String", actor)
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

// TestPullRequestMembershipIsQueryableInTheGraph is CHAOS-4194's headline
// acceptance criterion: "a PR that is a project item in the provider has a
// queryable PR->project mapping in the graph after sync." Before this ticket
// there was nowhere for that fact to live at all -- git_pull_requests carries
// no project columns and nothing else wrote a PR->project association.
//
// The subject identity is asserted against the repo uuid pull_request.v1 itself
// derives, over the real migration, because the whole value of the row is that
// a consumer can join it back to the PR. A repo_id that only agrees with
// itself would look identical here and join to nothing in production.
func TestPullRequestMembershipIsQueryableInTheGraph(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	pointer := projectMembershipPointer()
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC) }
	if _, err := sink.Write(ctx, externalSinkBatch{
		Pointer: pointer, SourceID: uuid.New(), Records: []externalSinkRecord{
			{Index: 0, Kind: "pull_request.v1", ExternalID: "42", Payload: map[string]any{
				// json.Number, not a Go int: the real envelope decoder runs
				// with UseNumber(), so an int literal here would exercise a
				// shape production never produces -- and externalInteger reads
				// it as 0, which is how this test first "passed" while the PR
				// row it meant to join to was numbered zero.
				"repositoryExternalId": pointer.SourceInstance, "number": json.Number("42"),
				"state": "open", "createdAt": "2026-07-20T10:00:00Z", "title": "A PR",
			}},
			{Index: 1, Kind: "project_membership_transition.v1", ExternalID: "evt-pr", Payload: map[string]any{
				"externalKey": "42", "provider": "github", "eventId": "evt-pr",
				"subjectKind": "pull_request", "repositoryExternalId": pointer.SourceInstance,
				"occurredAt": "2026-07-22T11:00:00Z", "toProjectId": "ghprojv2:full-chaos#4",
				"toProjectKey": "PLATFORM",
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	var (
		subjectKind, subjectID, projectID, source string
		presenceRepo                              uuid.UUID
	)
	if err := conn.QueryRow(ctx, `
SELECT subject_kind, subject_id, repo_id, project_id, source
FROM project_membership_presence WHERE org_id = ? AND subject_kind = 'pull_request'`,
		projectMembershipTestOrg).Scan(&subjectKind, &subjectID, &presenceRepo, &projectID, &source); err != nil {
		t.Fatal(err)
	}
	if subjectID != "42" || projectID != "ghprojv2:full-chaos#4" || source != "transition" {
		t.Errorf("PR presence = %s / %s / %s / %s", subjectKind, subjectID, projectID, source)
	}
	var prRepo uuid.UUID
	if err := conn.QueryRow(ctx,
		`SELECT repo_id FROM git_pull_requests FINAL WHERE org_id = ? AND number = 42`,
		projectMembershipTestOrg).Scan(&prRepo); err != nil {
		t.Fatal(err)
	}
	if presenceRepo != prRepo {
		t.Fatalf("membership repo_id %s does not join to the PR's %s", presenceRepo, prRepo)
	}
}

// TestProjectMembershipPresencePrefersTransitionsAndFallsBackToTheColumn is
// deliverable 2's acceptance claim, over the real migrated view.
//
// The two arms are asserted together on purpose. Either one alone is easy to
// satisfy and wrong: a projection that reads only transitions silently loses
// every pre-CDC work item, and one that reads only the column is the
// history-less shape CHAOS-4194 exists to replace. What has to hold is that a
// subject with history is answered by its LATEST transition while a subject
// without history still resolves, and that a consumer can tell which it got.
//
// The fallback arm is exercised through LINEAR rather than github, and that is
// the vocabulary constraint showing through rather than a convenience: for
// github the external-ingest path writes the REPOSITORY full name into
// work_items.project_id (externalProjectScope), which is not a project entity
// and is filtered out by the arm's provider predicate. Linear's column holds
// the provider's own project id, so it is the case where a fallback is
// legitimate at all. See TestPresenceColumnArmRefusesRepoAsProjectValues for
// the github half.
func TestProjectMembershipPresencePrefersTransitionsAndFallsBackToTheColumn(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	pointer := projectMembershipPointer()
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC) }
	// Two reassignments for work item 7, out of chronological order in the
	// batch so the projection cannot pass by accident of insertion order.
	if _, err := sink.Write(ctx, externalSinkBatch{
		Pointer: pointer, SourceID: uuid.New(), Records: []externalSinkRecord{
			{Index: 0, Kind: "project_membership_transition.v1", ExternalID: "evt-2", Payload: map[string]any{
				"externalKey": "7", "provider": "github", "eventId": "evt-2",
				"subjectKind": "work_item", "workItemType": "issue",
				"occurredAt": "2026-07-22T15:00:00Z", "fromProjectId": "ghprojv2:full-chaos#4",
				"toProjectId": "ghprojv2:full-chaos#9", "toProjectKey": "RUNTIME",
			}},
			{Index: 1, Kind: "project_membership_transition.v1", ExternalID: "evt-1", Payload: map[string]any{
				"externalKey": "7", "provider": "github", "eventId": "evt-1",
				"subjectKind": "work_item", "workItemType": "issue",
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
		},
	}); err != nil {
		t.Fatal(err)
	}
	// A linear work item with NO transition history at all: the pre-CDC case,
	// and the only provider shape where the column is a project entity.
	linear := linearMembershipPointer()
	if _, err := sink.Write(ctx, externalSinkBatch{
		Pointer: linear, SourceID: uuid.New(), Records: []externalSinkRecord{
			{Index: 0, Kind: "work_item.v1", ExternalID: "ENG-8", Payload: map[string]any{
				"externalKey": "ENG-8", "provider": "linear", "title": "No history",
				"type": "issue", "status": "todo", "createdAt": "2026-07-20T10:00:00Z",
				"projectId": "linear-project-1", "projectName": "Runtime",
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	type presence struct {
		projectID, projectKey, source, subjectKind string
	}
	found := map[string]presence{}
	result, err := conn.Query(ctx,
		`SELECT subject_id, subject_kind, project_id, project_key, source FROM project_membership_presence WHERE org_id = ?`,
		projectMembershipTestOrg)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	for result.Next() {
		var subjectID, subjectKind, projectID, projectKey, source string
		if err := result.Scan(&subjectID, &subjectKind, &projectID, &projectKey, &source); err != nil {
			t.Fatal(err)
		}
		if _, duplicate := found[subjectID]; duplicate {
			t.Fatalf("%s appears twice -- the arms overlap", subjectID)
		}
		found[subjectID] = presence{projectID, projectKey, source, subjectKind}
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
	if withHistory.source != "transition" || withHistory.subjectKind != "work_item" {
		t.Errorf("provenance = %q / %q, want transition / work_item", withHistory.source, withHistory.subjectKind)
	}
	withoutHistory, ok := found["linear:ENG-8"]
	if !ok {
		t.Fatalf("pre-CDC work item lost its project edge: %#v", found)
	}
	if withoutHistory.source != "work_item_column" {
		t.Errorf("source = %q, want work_item_column", withoutHistory.source)
	}
	if withoutHistory.projectID != "linear-project-1" {
		t.Errorf("fallback arm resolved to %#v, want the linear project id", withoutHistory)
	}
}

// TestPresenceColumnArmRefusesRepoAsProjectValues is the vocabulary constraint
// on the READ side, and the half a sink-only check cannot cover.
//
// There is a THIRD meaning of "project" in this schema: the external-ingest
// path writes the REPOSITORY full name into work_items.project_id for github
// and gitlab (external_clickhouse.go's externalProjectScope), while the real
// entity is `ghprojv2:<org>#<n>`. The sink refuses such a value on the way in,
// but the column arm reads rows that were written long before this ticket and
// never passed through that check. Without the arm's own prefix filter, every
// github work item in the table would project a presence edge naming a
// `projects` row that does not and will never exist -- an edge that looks
// entirely real and resolves to nothing.
//
// gitlab is asserted alongside it because its exclusion is total rather than
// prefixed: GitLab's "project" IS a repository in this schema, so there is no
// value of work_items.project_id that could be a project entity there.
func TestPresenceColumnArmRefusesRepoAsProjectValues(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC) }
	for _, system := range []string{"github", "gitlab"} {
		pointer := projectMembershipPointer()
		pointer.SourceSystem = system
		if _, err := sink.Write(ctx, externalSinkBatch{
			Pointer: pointer, SourceID: uuid.New(), Records: []externalSinkRecord{
				{Index: 0, Kind: "work_item.v1", ExternalID: "7", Payload: map[string]any{
					"externalKey": "7", "provider": system, "title": "Repo as project",
					"type": "issue", "status": "todo", "createdAt": "2026-07-20T10:00:00Z",
					"repositoryExternalId": pointer.SourceInstance,
				}},
			},
		}); err != nil {
			t.Fatalf("%s: %v", system, err)
		}
	}
	// Premise of the assertion: the rows really do carry the repo full name in
	// the project column, so the emptiness below is the filter working and not
	// an empty table.
	var stored uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM work_items FINAL WHERE org_id = ? AND project_id = 'full-chaos/dev-health'`,
		projectMembershipTestOrg).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 2 {
		t.Fatalf("work_items carries %d repo-as-project rows, want 2 -- the filter claim is untested otherwise", stored)
	}
	var projected uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM project_membership_presence WHERE org_id = ?`,
		projectMembershipTestOrg).Scan(&projected); err != nil {
		t.Fatal(err)
	}
	if projected != 0 {
		t.Fatalf("%d repo-as-project presence edges were projected; each names a projects row that cannot exist", projected)
	}
}

// membershipPresence reads every active (subject, project) pair for one
// subject. Returning the SET rather than one row is the whole point of the
// per-project keying: a query that scanned a single row could not tell "one
// active membership" from "two, one of which we lost".
func membershipPresence(t *testing.T, ctx context.Context, conn driver.Conn, subjectID string) map[string]string {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT project_id, source FROM project_membership_presence
WHERE org_id = ? AND subject_id = ?`, projectMembershipTestOrg, subjectID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	found := map[string]string{}
	for rows.Next() {
		var projectID, source string
		if err := rows.Scan(&projectID, &source); err != nil {
			t.Fatal(err)
		}
		if previous, duplicate := found[projectID]; duplicate {
			t.Fatalf("%s/%s appears twice (%s and %s)", subjectID, projectID, previous, source)
		}
		found[projectID] = source
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return found
}

func membershipRecord(index int, eventID, subjectKind, externalKey, repository, occurredAt, from, to string) externalSinkRecord {
	payload := map[string]any{
		"externalKey": externalKey, "provider": "github", "eventId": eventID,
		"subjectKind": subjectKind, "repositoryExternalId": repository,
		"occurredAt": occurredAt, "fromProjectId": from, "toProjectId": to,
	}
	if subjectKind == "work_item" {
		payload["workItemType"] = "issue"
	}
	return externalSinkRecord{
		Index: index, Kind: "project_membership_transition.v1",
		ExternalID: eventID, Payload: payload,
	}
}

// TestASubjectHoldsSeveralBoardsAtOnceAndLosesThemIndependently is the
// acceptance claim for Context Fabric's per-(subject, project) presence ruling,
// and it is the test the previous view could not have passed.
//
// A GitHub pull request goes on as many boards as someone adds it to. The
// earlier view argMax'd ONE project per subject, so the second board silently
// replaced the first -- and removing the subject from that second board made
// the transition arm yield nothing at all, so a PR still on board A vanished
// from presence entirely because it had left board B. One correct answer became
// none, which is worse than the incomplete answer it started from.
//
// The three phases are asserted in sequence against the same subject, because
// each one is only meaningful given the last: two active memberships, then a
// removal that takes exactly one of them, then a move that retires and creates
// in a single observed event.
func TestASubjectHoldsSeveralBoardsAtOnceAndLosesThemIndependently(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	pointer := projectMembershipPointer()
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC) }
	write := func(records ...externalSinkRecord) {
		t.Helper()
		if _, err := sink.Write(ctx, externalSinkBatch{
			Pointer: pointer, SourceID: uuid.New(), Records: records,
		}); err != nil {
			t.Fatal(err)
		}
	}
	const (
		boardA = "ghprojv2:full-chaos#4"
		boardB = "ghprojv2:full-chaos#9"
		boardC = "ghprojv2:full-chaos#11"
	)

	// Added to A, then to B. Both are additions -- ("", P) -- and neither
	// mentions the other, so nothing in the data pairs them up except the
	// subject they share.
	write(
		membershipRecord(0, "evt-a", "pull_request", "42", pointer.SourceInstance,
			"2026-07-22T11:00:00Z", "", boardA),
		membershipRecord(1, "evt-b", "pull_request", "42", pointer.SourceInstance,
			"2026-07-22T15:00:00Z", "", boardB),
	)
	active := membershipPresence(t, ctx, conn, "42")
	if len(active) != 2 || active[boardA] != "transition" || active[boardB] != "transition" {
		t.Fatalf("a PR on two boards resolved to %#v, want both active", active)
	}

	// Removed from B only. B must go; A must be untouched. Under the previous
	// keying this step returned NOTHING for the subject.
	write(membershipRecord(0, "evt-c", "pull_request", "42", pointer.SourceInstance,
		"2026-07-23T11:00:00Z", boardB, ""))
	active = membershipPresence(t, ctx, conn, "42")
	if len(active) != 1 || active[boardA] != "transition" {
		t.Fatalf("after leaving one board the PR resolved to %#v, want only board A", active)
	}

	// Moved A -> C in a single row. One observed event retires one membership
	// and creates another, which is why both sides live in the same row.
	write(membershipRecord(0, "evt-d", "pull_request", "42", pointer.SourceInstance,
		"2026-07-24T11:00:00Z", boardA, boardC))
	active = membershipPresence(t, ctx, conn, "42")
	if len(active) != 1 || active[boardC] != "transition" {
		t.Fatalf("after a move the PR resolved to %#v, want only board C", active)
	}

	// The history keeps every observed event. Only presence changes.
	var events uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM project_membership_transitions FINAL WHERE org_id = ? AND subject_id = '42'`,
		projectMembershipTestOrg).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 4 {
		t.Fatalf("transition history = %d rows, want all four observed events", events)
	}
}

// TestAReassertedMembershipWithARenamedKeyIsDeterministic covers the (P, P)
// row: a provider re-asserting a membership the subject already holds, with a
// DIFFERENT key on each side because the project was renamed.
//
// Both sides name the same project, so an unpivot that emitted a touch per side
// would produce two touches carrying the same (occurred_at, event_id). The
// argMax over that pair would then be a tie, and ClickHouse could return either
// key -- the same row read twice giving two different answers, with nothing in
// the data to say which was right. The view emits ONE touch for such a row, the
// joined side, because to_project_key is by definition the key the subject has
// now (codex adversarial review, round 3).
//
// HONEST LIMIT OF THIS TEST: it does NOT go red against the pre-fix view. The
// two-touch version was measured, and ClickHouse resolved the argMax tie toward
// the first-listed tuple -- the joined side -- which is the same answer the fix
// specifies. So this test pins behaviour the current engine already happens to
// produce, and it cannot demonstrate the defect it was written for.
//
// It is kept anyway, and the distinction is worth being precise about: an
// argMax tie is UNSPECIFIED, not defined-as-first. Nothing in ClickHouse
// promises that resolution across versions, part layouts, or thread counts, so
// the pre-fix view was correct by coincidence rather than by construction. The
// fix removes the tie instead of relying on how it currently breaks, and this
// test is the statement of which key is correct if the coincidence ever ends.
// Recorded rather than presented as a red-first kill, because a test that
// passes before and after is weak evidence and saying otherwise would overstate
// what was verified.
//
// The loop is for the same reason: a single read cannot distinguish
// "deterministic" from "got lucky once".
func TestAReassertedMembershipWithARenamedKeyIsDeterministic(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	pointer := projectMembershipPointer()
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC) }
	if _, err := sink.Write(ctx, externalSinkBatch{
		Pointer: pointer, SourceID: uuid.New(), Records: []externalSinkRecord{
			{Index: 0, Kind: "project_membership_transition.v1", ExternalID: "evt-rename", Payload: map[string]any{
				"externalKey": "42", "provider": "github", "eventId": "evt-rename",
				"subjectKind": "pull_request", "repositoryExternalId": pointer.SourceInstance,
				"occurredAt":    "2026-07-22T11:00:00Z",
				"fromProjectId": "ghprojv2:full-chaos#4", "fromProjectKey": "PLATFORM",
				"toProjectId": "ghprojv2:full-chaos#4", "toProjectKey": "PLATFORM-RENAMED",
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	for attempt := range 5 {
		var projectID, projectKey string
		var rows uint64
		if err := conn.QueryRow(ctx, `
SELECT count(), any(project_id), any(project_key) FROM project_membership_presence
WHERE org_id = ? AND subject_id = '42'`, projectMembershipTestOrg,
		).Scan(&rows, &projectID, &projectKey); err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("attempt %d: a re-asserted membership produced %d presence rows, want 1", attempt, rows)
		}
		if projectID != "ghprojv2:full-chaos#4" {
			t.Fatalf("attempt %d: project_id = %q", attempt, projectID)
		}
		// The JOINED side's key, every time. The other key in the row is the
		// name the project used to have.
		if projectKey != "PLATFORM-RENAMED" {
			t.Fatalf("attempt %d: project_key = %q, want the joined side's key -- "+
				"a tie in the argMax would return either", attempt, projectKey)
		}
	}
}

// TestASingleProjectSubjectIsUnaffectedByPerProjectKeying pins the degeneracy
// Context Fabric's ruling promised. jira and linear work items belong to one
// project at a time, so the per-(subject, project) keying must give them
// exactly the answer the per-subject keying did -- one active membership after
// an add, and none after the removal that names the project left.
//
// Without this the ruling would have been adopted on the strength of the
// multi-board case alone, and a regression for the single-project majority
// would have shipped behind a green multi-board test.
func TestASingleProjectSubjectIsUnaffectedByPerProjectKeying(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	linear := linearMembershipPointer()
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC) }
	if _, err := sink.Write(ctx, externalSinkBatch{
		Pointer: linear, SourceID: uuid.New(), Records: []externalSinkRecord{
			// A work_items row for the same subject, so a fall-through would be
			// visible rather than silent.
			{Index: 0, Kind: "work_item.v1", ExternalID: "ENG-8", Payload: map[string]any{
				"externalKey": "ENG-8", "provider": "linear", "title": "One project at a time",
				"type": "issue", "status": "todo", "createdAt": "2026-07-20T10:00:00Z",
				"projectId": "linear-project-1", "projectName": "Runtime",
			}},
			{Index: 1, Kind: "project_membership_transition.v1", ExternalID: "evt-1", Payload: map[string]any{
				"externalKey": "ENG-8", "provider": "linear", "eventId": "evt-1",
				"subjectKind": "work_item", "workItemType": "issue",
				"occurredAt":  "2026-07-22T11:00:00Z",
				"toProjectId": "linear-project-1", "toProjectKey": "RUNTIME",
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	active := membershipPresence(t, ctx, conn, "linear:ENG-8")
	if len(active) != 1 || active["linear-project-1"] != "transition" {
		t.Fatalf("single-project subject resolved to %#v", active)
	}

	// The removal NAMES the project left, as the ruling requires. The subject
	// keeps its work_items row, so if the column arm resurrected it this would
	// report a membership nobody currently holds.
	if _, err := sink.Write(ctx, externalSinkBatch{
		Pointer: linear, SourceID: uuid.New(), Records: []externalSinkRecord{
			{Index: 0, Kind: "project_membership_transition.v1", ExternalID: "evt-2", Payload: map[string]any{
				"externalKey": "ENG-8", "provider": "linear", "eventId": "evt-2",
				"subjectKind": "work_item", "workItemType": "issue",
				"occurredAt":    "2026-07-23T11:00:00Z",
				"fromProjectId": "linear-project-1", "fromProjectKey": "RUNTIME",
				"toProjectId": "", "toProjectKey": "",
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if active := membershipPresence(t, ctx, conn, "linear:ENG-8"); len(active) != 0 {
		t.Fatalf("a removed subject still resolves to %#v -- the column arm resurrected it", active)
	}
}

// TestSameMillisecondReassignmentsSurviveAsDistinctRows is the other half of the
// dedupe claim, and the one a re-sync test cannot make. Bulk project moves land
// in the same millisecond routinely, so if occurred_at alone decided identity
// they would silently collapse into one row and the subject's history would
// show a single move it never made. event_id in the sorting key is what keeps
// them apart -- and the presence projection must still resolve to exactly one
// of them rather than mixing their columns.
func TestSameMillisecondReassignmentsSurviveAsDistinctRows(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	pointer := projectMembershipPointer()
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
			Index: index, Kind: "project_membership_transition.v1", ExternalID: event.id,
			Payload: map[string]any{
				"externalKey": "7", "provider": "github", "eventId": event.id,
				"subjectKind": "work_item", "workItemType": "issue", "occurredAt": shared,
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
		`SELECT count() FROM project_membership_transitions FINAL WHERE org_id = ?`,
		projectMembershipTestOrg).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("same-timestamp reassignments collapsed to %d rows, want 2", rows)
	}
	// The projection picks ONE of them whole. Which one is decided by the
	// event_id tiebreak, so it must not be a blend of both rows' columns.
	var projectID, projectKey string
	if err := conn.QueryRow(ctx,
		`SELECT project_id, project_key FROM project_membership_presence WHERE org_id = ?`,
		projectMembershipTestOrg).Scan(&projectID, &projectKey); err != nil {
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
