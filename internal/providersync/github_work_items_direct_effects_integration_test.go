//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// This file authors NO DDL. Every table comes from the real migration chain
// (src/dev_health_ops/migrations/clickhouse), applied by chschema through the
// project's own canonical entrypoint -- the same call `migrate clickhouse
// upgrade` makes, so the .py migrations 027/042/055/061 that a .sql-only
// replay would miss are covered too.
//
// The previous hand-typed CREATE TABLE constants were a second, unversioned
// copy of the schema: a readback test over them could only ever confirm what
// the constants themselves declared. The sibling derived-surface tables had
// already drifted that way (a PRE-053 enum kept a genuinely reachable row from
// ever being rejected), which is what chschema was written to remove.

// A negative-offset zone where the local date disagrees with the UTC date, so
// any accidental local-time formatting shows up as a wrong day rather than
// passing by coincidence.
var workItemTestZone = time.FixedZone("test-west", -7*60*60)

func newWorkItemEffectsConn(t *testing.T) (context.Context, driver.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	// Migrate BEFORE opening the Go connection, so a failure to apply the real
	// chain surfaces as a migration failure rather than as a confusing "table
	// does not exist" from the first query.
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ctx, conn
}

func workItemEffect(t *testing.T, destination string, rows ...any) (GitHubWorkItemEffectIdentity, EffectBatch) {
	t.Helper()
	claim := nativeTestClaim("github", "work-items")
	encoded := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, raw)
	}
	effect, err := BuildEffectBatch(destination, EffectReadbackRequired, encoded)
	if err != nil {
		t.Fatal(err)
	}
	return GitHubWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: "github", Dataset: "work-items",
		Generation: claim.GenerationKey(), Destination: destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}, effect
}

func workItemTestOrgID(t *testing.T) string {
	t.Helper()
	return nativeTestClaim("github", "work-items").OrgID
}

// ai_attribution.org_id is a UUID column, unlike the String org_id every other
// direct destination carries, so its rows can only exist for a tenant whose id
// parses as a UUID -- which is why normalizeGitHubPullRequestBundle already
// refuses the row otherwise. The shared test claim's "org-acme" cannot be used
// here, and substituting it would fail at the driver rather than prove anything.
const aiAttributionTestOrgID = "6f1c2d3e-4a5b-4c6d-8e9f-0a1b2c3d4e5f"

func aiAttributionEffect(t *testing.T, row githubAIAttributionRow) (GitHubWorkItemEffectIdentity, EffectBatch) {
	t.Helper()
	identity, effect := workItemEffect(t, "ai_attribution", row)
	identity.OrgID = row.OrgID.String()
	return identity, effect
}

// -----------------------------------------------------------------------------
// The contract every direct adapter owes the recovery lane
// -----------------------------------------------------------------------------

// Per the #1529 binding contract item 9 the snapshot supplies expected effects,
// not write receipts, so a recovered worker re-runs the adapter over the very
// same payload. Inspect must answer Exact for that replay: an Absent would
// rewrite forever and a Conflict would hard-stop the unit.
//
// This is the test the whole "carry the write stamp in the effect row" design
// exists to make passable -- a wall-clock stamp would write a strictly newer
// version on every replay and could never answer Exact.
func TestDirectAdaptersAnswerExactWhenARecoverySnapshotIsReplayed(t *testing.T) {
	orgID := workItemTestOrgID(t)
	observed := time.Date(2026, 8, 31, 23, 30, 0, 123456789, time.UTC)

	for _, testCase := range directAdapterCases(orgID, observed) {
		t.Run(testCase.destination, func(t *testing.T) {
			ctx, conn := newWorkItemEffectsConn(t)
			adapter := testCase.adapter(conn)
			identity, effect := workItemEffect(t, testCase.destination, testCase.row)

			if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectAbsent {
				t.Fatalf("before write: inspection=%s err=%v, want absent", inspection, err)
			}
			if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
				t.Fatalf("write: %v", err)
			}
			if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectExact {
				t.Fatalf("after write: inspection=%s err=%v, want exact", inspection, err)
			}

			// The replay: identical payload, exactly as the snapshot stores it.
			if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
				t.Fatalf("replay write: %v", err)
			}
			if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectExact {
				t.Fatalf("after replay: inspection=%s err=%v, want exact", inspection, err)
			}
		})
	}
}

// A row written under one tenant must never satisfy another tenant's readback,
// even at an identical natural key. org_id leads every sorting key here, so a
// dropped predicate is easy to write and invisible without this.
func TestDirectAdaptersReadbackExcludesOtherTenantsAtTheSameKey(t *testing.T) {
	orgID := workItemTestOrgID(t)
	observed := time.Date(2026, 8, 31, 23, 30, 0, 123456789, time.UTC)

	for _, testCase := range directAdapterCases(orgID, observed) {
		if testCase.foreignRow == nil {
			continue
		}
		t.Run(testCase.destination, func(t *testing.T) {
			ctx, conn := newWorkItemEffectsConn(t)
			adapter := testCase.adapter(conn)

			foreignIdentity, foreignEffect := workItemEffect(t, testCase.destination, testCase.foreignRow)
			foreignIdentity.OrgID = testCase.foreignOrgID
			if err := adapter.WriteGitHubWorkItemEffect(ctx, foreignIdentity, foreignEffect); err != nil {
				t.Fatalf("write foreign tenant row: %v", err)
			}

			identity, effect := workItemEffect(t, testCase.destination, testCase.row)
			if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectAbsent {
				t.Fatalf("another tenant's row leaked: inspection=%s err=%v, want absent", inspection, err)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// ai_attribution: the partitioned destination (PR #1535)
// -----------------------------------------------------------------------------

// ai_attribution partitions on toYYYYMM(observed_at) while its sorting key
// (org_id, provider, subject_type, repo_id, subject_id, source) does not carry
// observed_at, so one sorting key observed either side of a month boundary is
// the PR #1535 shape.
//
// What the fix turned out to be is not what it looked like. Measured here:
// with ClickHouse's default do_not_merge_across_partitions_select_final=0,
// FINAL merges ACROSS partitions, so the sorting key has exactly ONE current
// row -- the greatest computed_at -- and the earlier observed_at row is
// superseded, not co-resident. Fencing the readback on the partition would
// filter that single winner out and answer Absent for a row that was written
// and correctly superseded, which replays forever.
//
// So the readback fences the sorting key only and lets found > 1 answer
// Conflict. This test pins both halves: the winner is retrievable and compares
// Exact, and the superseded row answers Conflict rather than Absent.
//
// The two instants are chosen so a UTC-07:00 reading puts both in the SAME
// local month while UTC puts them in different months, so a partition
// expression evaluated in local time would not reproduce the boundary at all.
// Run under BOTH values of do_not_merge_across_partitions_select_final. That
// knob is a server setting this code does not control, and it is exactly what
// made the FINAL-based readback's verdict unstable: with the knob on, a
// correctly superseded row came back as two rows and became a permanent
// Conflict. Asserting only the default would leave that failure mode untested.
func TestAIAttributionReadbackResolvesTheSupersededRowAcrossAMonthBoundary(t *testing.T) {
	for _, crossPartitionMerge := range []int{0, 1} {
		t.Run(fmt.Sprintf("do_not_merge_across_partitions_select_final=%d", crossPartitionMerge), func(t *testing.T) {
			aiAttributionSupersededRowCase(t, crossPartitionMerge)
		})
	}
}

func aiAttributionSupersededRowCase(t *testing.T, crossPartitionMerge int) {
	t.Helper()
	ctx, conn := newWorkItemEffectsConn(t)
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(map[string]any{
		"do_not_merge_across_partitions_select_final": crossPartitionMerge,
	}))
	adapter := GitHubAIAttributionClickHouseAdapter{Conn: conn}
	orgID := aiAttributionTestOrgID

	august := time.Date(2026, 8, 31, 23, 30, 0, 123456789, time.UTC)
	september := time.Date(2026, 9, 1, 1, 30, 0, 0, time.UTC)
	if august.In(workItemTestZone).Month() != september.In(workItemTestZone).Month() {
		t.Fatalf("fixture no longer exercises the boundary: %s vs %s local",
			august.In(workItemTestZone), september.In(workItemTestZone))
	}
	if august.Month() == september.Month() {
		t.Fatal("fixture no longer spans two UTC months")
	}

	augustRow := aiAttributionTestRow(t, orgID, august)
	septemberRow := aiAttributionTestRow(t, orgID, september)

	augustIdentity, augustEffect := aiAttributionEffect(t, augustRow)
	if err := adapter.WriteGitHubWorkItemEffect(ctx, augustIdentity, augustEffect); err != nil {
		t.Fatalf("write august row: %v", err)
	}
	if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, augustIdentity, augustEffect); err != nil || inspection != EffectExact {
		t.Fatalf("august before supersede: inspection=%s err=%v, want exact", inspection, err)
	}

	septemberIdentity, septemberEffect := aiAttributionEffect(t, septemberRow)
	if err := adapter.WriteGitHubWorkItemEffect(ctx, septemberIdentity, septemberEffect); err != nil {
		t.Fatalf("write september row: %v", err)
	}

	// Both rows are on disk; only one is current.
	var stored uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM ai_attribution WHERE org_id = ?`, uuid.MustParse(orgID),
	).Scan(&stored); err != nil {
		t.Fatalf("count stored rows: %v", err)
	}
	if stored != 2 {
		t.Fatalf("expected both months on disk, got %d", stored)
	}

	// The later computed_at wins globally, across the partition boundary.
	if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, septemberIdentity, septemberEffect); err != nil || inspection != EffectExact {
		t.Fatalf("september: inspection=%s err=%v, want exact", inspection, err)
	}
	// And the superseded row must say Conflict -- NOT Absent, which would make
	// the committer rewrite it on every pass forever.
	if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, augustIdentity, augustEffect); err != nil || inspection != EffectConflict {
		t.Fatalf("superseded august row: inspection=%s err=%v, want conflict", inspection, err)
	}
}

// The stored evidence column must hold Python's exact bytes, not Go's default
// map marshalling. Asserting the decoded map would pass while the stored string
// differed in key order, spacing, and escaping.
func TestAIAttributionStoresPythonEvidenceBytes(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	adapter := GitHubAIAttributionClickHouseAdapter{Conn: conn}
	orgID := aiAttributionTestOrgID
	observed := time.Date(2026, 8, 31, 23, 30, 0, 123456789, time.UTC)

	row := aiAttributionTestRow(t, orgID, observed)
	row.Evidence = map[string]any{
		"login": "copilot[bot]", "user_type": "Bot",
		"app_slug": "github-copilot", "known_ai_bot": true,
	}
	identity, effect := aiAttributionEffect(t, row)
	if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatalf("write: %v", err)
	}
	var stored string
	if err := conn.QueryRow(ctx,
		`SELECT evidence FROM ai_attribution FINAL WHERE org_id = ?`, uuid.MustParse(orgID),
	).Scan(&stored); err != nil {
		t.Fatalf("read evidence: %v", err)
	}
	const want = `{"login": "copilot[bot]", "user_type": "Bot", "app_slug": "github-copilot", "known_ai_bot": true}`
	if stored != want {
		t.Fatalf("stored evidence bytes\n got=%s\nwant=%s", stored, want)
	}
}

// -----------------------------------------------------------------------------
// version semantics
// -----------------------------------------------------------------------------

// A stored row older than ours has not been overwritten by us yet and must
// replay; a stored row newer than ours belongs to somebody else and must not be
// silently clobbered. Both directions are asserted against a real
// ReplacingMergeTree rather than against the comparator in isolation.
func TestWorkItemsReadbackDistinguishesStaleFromOverwritten(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	adapter := GitHubWorkItemsClickHouseAdapter{Conn: conn}
	orgID := workItemTestOrgID(t)
	now := time.Date(2026, 8, 31, 23, 30, 0, 123456789, time.UTC)

	current := workItemTestRow(orgID, now)
	previous := workItemTestRow(orgID, now.Add(-time.Hour))
	previous.Title = "older title"

	previousIdentity, previousEffect := workItemEffect(t, "work_items", previous)
	if err := adapter.WriteGitHubWorkItemEffect(ctx, previousIdentity, previousEffect); err != nil {
		t.Fatalf("write previous: %v", err)
	}
	currentIdentity, currentEffect := workItemEffect(t, "work_items", current)
	if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, currentIdentity, currentEffect); err != nil || inspection != EffectAbsent {
		t.Fatalf("stale stored row: inspection=%s err=%v, want absent", inspection, err)
	}
	if err := adapter.WriteGitHubWorkItemEffect(ctx, currentIdentity, currentEffect); err != nil {
		t.Fatalf("write current: %v", err)
	}
	if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, currentIdentity, currentEffect); err != nil || inspection != EffectExact {
		t.Fatalf("winning row: inspection=%s err=%v, want exact", inspection, err)
	}
	// Now the stored row is newer than what the older effect expects.
	if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, previousIdentity, previousEffect); err != nil || inspection != EffectConflict {
		t.Fatalf("overwritten row: inspection=%s err=%v, want conflict", inspection, err)
	}
}

// A batch where some rows landed and others did not cannot be replayed without
// rewriting the ones that did, so the committer must be told Conflict.
func TestWorkItemsPartlyLandedBatchIsAConflict(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	adapter := GitHubWorkItemsClickHouseAdapter{Conn: conn}
	orgID := workItemTestOrgID(t)
	now := time.Date(2026, 8, 31, 23, 30, 0, 123456789, time.UTC)

	landed := workItemTestRow(orgID, now)
	missing := workItemTestRow(orgID, now)
	missing.WorkItemID = "gh:acme/api#999"

	landedIdentity, landedEffect := workItemEffect(t, "work_items", landed)
	if err := adapter.WriteGitHubWorkItemEffect(ctx, landedIdentity, landedEffect); err != nil {
		t.Fatalf("write landed row: %v", err)
	}
	bothIdentity, bothEffect := workItemEffect(t, "work_items", landed, missing)
	if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, bothIdentity, bothEffect); err != nil || inspection != EffectConflict {
		t.Fatalf("mixed batch: inspection=%s err=%v, want conflict", inspection, err)
	}
}

// -----------------------------------------------------------------------------
// D16 omissions, observed in storage rather than in the SQL string
// -----------------------------------------------------------------------------

// write_work_items never writes description/priority_raw/service_class/due_at
// and write_work_item_transitions never writes provider. A ReplacingMergeTree
// replaces whole rows, so a Go adapter that "helpfully" filled these in would
// store values the running Python system never stores. Asserting the stored
// columns proves the omission reached the table, which reading the INSERT
// string cannot.
func TestDirectAdaptersLeaveThePythonOmittedColumnsAtTheirDefaults(t *testing.T) {
	orgID := workItemTestOrgID(t)
	now := time.Date(2026, 8, 31, 23, 30, 0, 123456789, time.UTC)

	t.Run("work_items", func(t *testing.T) {
		ctx, conn := newWorkItemEffectsConn(t)
		adapter := GitHubWorkItemsClickHouseAdapter{Conn: conn}
		row := workItemTestRow(orgID, now)
		// The semantic row carries all four; the sink still must not write them.
		row.Description = stringPointer("body text")
		row.PriorityRaw = stringPointer("p1")
		row.ServiceClass = stringPointer("expedite")
		due := now.Add(48 * time.Hour)
		row.DueAt = &due

		identity, effect := workItemEffect(t, "work_items", row)
		if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
			t.Fatalf("write: %v", err)
		}
		var description, priorityRaw, serviceClass *string
		var dueAt *time.Time
		if err := conn.QueryRow(ctx,
			`SELECT description, priority_raw, service_class, due_at FROM work_items FINAL WHERE org_id = ?`,
			orgID,
		).Scan(&description, &priorityRaw, &serviceClass, &dueAt); err != nil {
			t.Fatalf("read omitted columns: %v", err)
		}
		if description != nil || priorityRaw != nil || serviceClass != nil || dueAt != nil {
			t.Fatalf("this unit wrote a column write_work_items omits: description=%v priority_raw=%v service_class=%v due_at=%v",
				description, priorityRaw, serviceClass, dueAt)
		}
	})

	t.Run("work_item_transitions", func(t *testing.T) {
		ctx, conn := newWorkItemEffectsConn(t)
		adapter := GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn}
		row := workItemTransitionTestRow(orgID, now)
		row.Provider = "github"

		identity, effect := workItemEffect(t, "work_item_transitions", row)
		if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
			t.Fatalf("write: %v", err)
		}
		var provider string
		if err := conn.QueryRow(ctx,
			`SELECT provider FROM work_item_transitions FINAL WHERE org_id = ?`, orgID,
		).Scan(&provider); err != nil {
			t.Fatalf("read provider: %v", err)
		}
		if provider != "" {
			t.Fatalf("this unit wrote provider=%q, which write_work_item_transitions omits", provider)
		}
	})
}

// -----------------------------------------------------------------------------
// fixtures
// -----------------------------------------------------------------------------

type directAdapterCase struct {
	destination  string
	adapter      func(driver.Conn) GitHubWorkItemEffectAdapter
	row          any
	foreignRow   any
	foreignOrgID string
}

const foreignTenantOrgID = "11111111-2222-3333-4444-555555555555"

func directAdapterCases(orgID string, now time.Time) []directAdapterCase {
	foreignWorkItem := workItemTestRow(foreignTenantOrgID, now)
	foreignTransition := workItemTransitionTestRow(foreignTenantOrgID, now)
	foreignDependency := workItemDependencyTestRow(foreignTenantOrgID, now)
	foreignReopen := workItemReopenTestRow(foreignTenantOrgID, now)
	foreignInteraction := workItemInteractionTestRow(foreignTenantOrgID, now)
	foreignSprint := sprintTestRow(foreignTenantOrgID, now)
	return []directAdapterCase{
		{
			destination: "work_items",
			adapter: func(conn driver.Conn) GitHubWorkItemEffectAdapter {
				return GitHubWorkItemsClickHouseAdapter{Conn: conn}
			},
			row: workItemTestRow(orgID, now), foreignRow: foreignWorkItem,
			foreignOrgID: foreignTenantOrgID,
		},
		{
			destination: "work_item_transitions",
			adapter: func(conn driver.Conn) GitHubWorkItemEffectAdapter {
				return GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn}
			},
			row: workItemTransitionTestRow(orgID, now), foreignRow: foreignTransition,
			foreignOrgID: foreignTenantOrgID,
		},
		{
			destination: "work_item_dependencies",
			adapter: func(conn driver.Conn) GitHubWorkItemEffectAdapter {
				return GitHubWorkItemDependenciesClickHouseAdapter{Conn: conn}
			},
			row: workItemDependencyTestRow(orgID, now), foreignRow: foreignDependency,
			foreignOrgID: foreignTenantOrgID,
		},
		{
			destination: "work_item_reopen_events",
			adapter: func(conn driver.Conn) GitHubWorkItemEffectAdapter {
				return GitHubWorkItemReopenEventsClickHouseAdapter{Conn: conn}
			},
			row: workItemReopenTestRow(orgID, now), foreignRow: foreignReopen,
			foreignOrgID: foreignTenantOrgID,
		},
		{
			destination: "work_item_interactions",
			adapter: func(conn driver.Conn) GitHubWorkItemEffectAdapter {
				return GitHubWorkItemInteractionsClickHouseAdapter{Conn: conn}
			},
			row: workItemInteractionTestRow(orgID, now), foreignRow: foreignInteraction,
			foreignOrgID: foreignTenantOrgID,
		},
		{
			destination: "sprints",
			adapter: func(conn driver.Conn) GitHubWorkItemEffectAdapter {
				return GitHubSprintsClickHouseAdapter{Conn: conn}
			},
			row: sprintTestRow(orgID, now), foreignRow: foreignSprint,
			foreignOrgID: foreignTenantOrgID,
		},
	}
}

func workItemTestRow(orgID string, now time.Time) githubWorkItemRow {
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	return githubWorkItemRow{
		WorkItemID: "gh:acme/api#42", Provider: "github", Title: "Repair path",
		Type: "issue", Status: "doing", StatusRaw: stringPointer("open"),
		RepoID: &repoID, ProjectID: stringPointer("acme/api"),
		Assignees: []string{"dev"}, Reporter: stringPointer("reporter"),
		CreatedAt: now.Add(-72 * time.Hour), UpdatedAt: now.Add(-time.Hour),
		Labels: []string{"bug"}, URL: stringPointer("https://github.com/acme/api/issues/42"),
		OrgID: orgID, LastSynced: now,
	}
}

func workItemTransitionTestRow(orgID string, now time.Time) githubWorkItemTransitionRow {
	return githubWorkItemTransitionRow{
		WorkItemID: "gh:acme/api#42", Provider: "github",
		OccurredAt: now.Add(-2 * time.Hour), FromStatus: "todo", ToStatus: "doing",
		ToStatusRaw: stringPointer("doing"), OrgID: orgID, LastSynced: now,
	}
}

func workItemDependencyTestRow(orgID string, now time.Time) githubWorkItemDependencyRow {
	return githubWorkItemDependencyRow{
		SourceWorkItemID: "gh:acme/api#42", TargetWorkItemID: "gh:acme/api#7",
		RelationshipType: "blocks", RelationshipTypeRaw: "blocks",
		RelationshipSemanticsVersion: "canonical-blocks.v2",
		LastSynced:                   now, OrgID: orgID,
	}
}

func workItemReopenTestRow(orgID string, now time.Time) githubWorkItemReopenRow {
	return githubWorkItemReopenRow{
		WorkItemID: "gh:acme/api#42", OccurredAt: now.Add(-3 * time.Hour),
		FromStatus: "done", ToStatus: "doing", LastSynced: now, OrgID: orgID,
	}
}

func workItemInteractionTestRow(orgID string, now time.Time) githubWorkItemInteractionRow {
	return githubWorkItemInteractionRow{
		WorkItemID: "gh:acme/api#42", Provider: "github",
		InteractionType: "comment", OccurredAt: now.Add(-4 * time.Hour),
		Actor: stringPointer("dev"), BodyLength: 128, LastSynced: now, OrgID: orgID,
	}
}

func sprintTestRow(orgID string, now time.Time) githubSprintRow {
	started := now.Add(-240 * time.Hour)
	return githubSprintRow{
		Provider: "github", SprintID: "gh:acme/api/milestone/3",
		Name: stringPointer("Sprint 3"), State: stringPointer("open"),
		StartedAt: &started, LastSynced: now, OrgID: orgID,
	}
}

func aiAttributionTestRow(t *testing.T, orgID string, observed time.Time) githubAIAttributionRow {
	t.Helper()
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	return githubAIAttributionRow{
		RecordID: uuid.MustParse("9f8b7c6d-5e4f-4a3b-8c1d-2e3f4a5b6c7d"),
		OrgID:    uuid.MustParse(orgID), Provider: "github",
		SubjectType: "pull_request", SubjectID: "17", RepoID: &repoID,
		Kind: "ai_assisted", Source: "commit_trailer", Confidence: 0.85,
		Actor:      stringPointer("claude"),
		Evidence:   map[string]any{"label": "ai-generated"},
		ObservedAt: observed, IngestedAt: observed.Add(time.Minute),
	}
}

// The replay contract, for the one adapter directAdapterCases cannot carry
// because its tenant column is a UUID rather than a String.
func TestAIAttributionAnswersExactWhenARecoverySnapshotIsReplayed(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	adapter := GitHubAIAttributionClickHouseAdapter{Conn: conn}
	observed := time.Date(2026, 8, 31, 23, 30, 0, 123456789, time.UTC)
	identity, effect := aiAttributionEffect(t, aiAttributionTestRow(t, aiAttributionTestOrgID, observed))

	if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("before write: inspection=%s err=%v, want absent", inspection, err)
	}
	if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatalf("write: %v", err)
	}
	if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectExact {
		t.Fatalf("after write: inspection=%s err=%v, want exact", inspection, err)
	}
	if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatalf("replay write: %v", err)
	}
	if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectExact {
		t.Fatalf("after replay: inspection=%s err=%v, want exact", inspection, err)
	}
}

// A row belonging to another tenant must not satisfy this tenant's readback
// even at an identical subject key and partition.
func TestAIAttributionReadbackExcludesOtherTenants(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	adapter := GitHubAIAttributionClickHouseAdapter{Conn: conn}
	observed := time.Date(2026, 8, 31, 23, 30, 0, 123456789, time.UTC)

	foreign := aiAttributionTestRow(t, "8a7b6c5d-4e3f-4a2b-9c8d-7e6f5a4b3c2d", observed)
	foreignIdentity, foreignEffect := aiAttributionEffect(t, foreign)
	if err := adapter.WriteGitHubWorkItemEffect(ctx, foreignIdentity, foreignEffect); err != nil {
		t.Fatalf("write foreign tenant row: %v", err)
	}
	identity, effect := aiAttributionEffect(t, aiAttributionTestRow(t, aiAttributionTestOrgID, observed))
	if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("another tenant's row leaked: inspection=%s err=%v, want absent", inspection, err)
	}
}

// A batch carrying two rows at one sorting key is constructible in production:
// two issue events in the same second yield two transitions at the same
// (org_id, repo_id, work_item_id, occurred_at); the parse-failure fallback to
// the item's createdAt makes it reachable deterministically; and
// github_work_items_projects_v2.go concatenates the repository and projects-v2
// transition slices without dedup.
//
// ClickHouse collapses such a pair on write. Without dedup the readback finds
// ONE row, the comparator sees a field mismatch against whichever expectation
// lost, and the verdict is a permanent Conflict -- which reaches the committer
// as ErrEffectRecoveryAmbiguous and wedges the unit with no way to self-heal.
// Python does not fail there; it silently loses one row. This asserts the port
// mirrors that loss rather than converting it into a hard stop.
func TestDirectAdaptersSurviveTwoRowsSharingASortingKeyInOneBatch(t *testing.T) {
	orgID := workItemTestOrgID(t)
	now := time.Date(2026, 8, 31, 23, 30, 0, 123456789, time.UTC)

	t.Run("work_item_transitions", func(t *testing.T) {
		ctx, conn := newWorkItemEffectsConn(t)
		adapter := GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn}
		first := workItemTransitionTestRow(orgID, now)
		second := workItemTransitionTestRow(orgID, now)
		second.ToStatus = "done" // same key, different payload
		assertCollapsesToTheLastRow(t, ctx, conn, adapter,
			"work_item_transitions", first, second,
			`SELECT to_status FROM work_item_transitions FINAL WHERE org_id = ?`, orgID, "done")
	})

	t.Run("work_item_reopen_events", func(t *testing.T) {
		ctx, conn := newWorkItemEffectsConn(t)
		adapter := GitHubWorkItemReopenEventsClickHouseAdapter{Conn: conn}
		first := workItemReopenTestRow(orgID, now)
		second := workItemReopenTestRow(orgID, now)
		second.ToStatus = "reopened"
		assertCollapsesToTheLastRow(t, ctx, conn, adapter,
			"work_item_reopen_events", first, second,
			`SELECT to_status FROM work_item_reopen_events FINAL WHERE org_id = ?`, orgID, "reopened")
	})

	t.Run("work_item_interactions", func(t *testing.T) {
		ctx, conn := newWorkItemEffectsConn(t)
		adapter := GitHubWorkItemInteractionsClickHouseAdapter{Conn: conn}
		first := workItemInteractionTestRow(orgID, now)
		second := workItemInteractionTestRow(orgID, now)
		second.BodyLength = 999
		assertCollapsesToTheLastRow(t, ctx, conn, adapter,
			"work_item_interactions", first, second,
			`SELECT toString(body_length) FROM work_item_interactions FINAL WHERE org_id = ?`, orgID, "999")
	})

	t.Run("work_item_dependencies", func(t *testing.T) {
		ctx, conn := newWorkItemEffectsConn(t)
		adapter := GitHubWorkItemDependenciesClickHouseAdapter{Conn: conn}
		first := workItemDependencyTestRow(orgID, now)
		second := workItemDependencyTestRow(orgID, now)
		second.RelationshipTypeRaw = "is blocked by"
		assertCollapsesToTheLastRow(t, ctx, conn, adapter,
			"work_item_dependencies", first, second,
			`SELECT relationship_type_raw FROM work_item_dependencies FINAL WHERE org_id = ?`,
			orgID, "is blocked by")
	})
}

func assertCollapsesToTheLastRow(
	t *testing.T,
	ctx context.Context,
	conn driver.Conn,
	adapter GitHubWorkItemEffectAdapter,
	destination string,
	first, second any,
	probe string,
	orgID string,
	wantStored string,
) {
	t.Helper()
	identity, effect := workItemEffect(t, destination, first, second)
	if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatalf("write colliding batch: %v", err)
	}
	// Exactly one row survives, and it is the LAST of the pair -- matching what
	// the engine retains for equal versions.
	var stored string
	if err := conn.QueryRow(ctx, probe, orgID).Scan(&stored); err != nil {
		t.Fatalf("probe stored row: %v", err)
	}
	if stored != wantStored {
		t.Fatalf("stored row is %q, want the last of the colliding pair (%q)", stored, wantStored)
	}
	// And the verdict must be Exact, not the permanent Conflict that an
	// un-deduped expected set would produce.
	inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("colliding batch: inspection=%s err=%v, want exact", inspection, err)
	}
	// Replay must remain Exact too.
	if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatalf("replay colliding batch: %v", err)
	}
	if inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectExact {
		t.Fatalf("colliding batch replay: inspection=%s err=%v, want exact", inspection, err)
	}
}

// Every timestamp column these adapters write must actually store the precision
// the truncation assumes. Read from the real schema rather than restated here,
// so a migration that changes one column's precision fails this test instead of
// silently producing a verdict that can never be Exact.
//
// Both directions are fatal, which is why this asserts equality rather than
// "at least milliseconds": truncating coarser than the column stores makes the
// comparator reject a row it just wrote, and truncating finer leaves a value the
// column cannot hold. A sibling lane hit the second shape one table over, where
// a version column was plain DateTime (seconds) and a shared millisecond helper
// left the verdict permanently Absent.
func TestWorkItemTimestampColumnsAllHaveTheAssumedPrecision(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)

	// The version column of each destination is called out separately: it is the
	// one the comparator orders on, so a precision mismatch there decides
	// rewrite-vs-done rather than merely comparing a field.
	versionColumns := map[string]string{
		"work_items": "last_synced", "work_item_transitions": "last_synced",
		"work_item_dependencies": "last_synced", "work_item_reopen_events": "last_synced",
		"work_item_interactions": "last_synced", "sprints": "last_synced",
		"ai_attribution": "computed_at",
	}

	rows, err := conn.Query(ctx,
		`SELECT table, name, type FROM system.columns
		 WHERE database = currentDatabase() AND table IN (?, ?, ?, ?, ?, ?, ?)
		   AND (type LIKE 'DateTime%' OR type LIKE 'Nullable(DateTime%')`,
		"work_items", "work_item_transitions", "work_item_dependencies",
		"work_item_reopen_events", "work_item_interactions", "sprints",
		"ai_attribution")
	if err != nil {
		t.Fatalf("read column types: %v", err)
	}
	defer rows.Close()

	seenVersionColumns := map[string]bool{}
	inspected := 0
	for rows.Next() {
		var table, name, columnType string
		if err := rows.Scan(&table, &name, &columnType); err != nil {
			t.Fatal(err)
		}
		inspected++
		if versionColumns[table] == name {
			seenVersionColumns[table] = true
		}
		precision, err := clickHouseColumnPrecision(columnType)
		if err != nil {
			t.Fatalf("%s.%s: %v", table, name, err)
		}
		if precision != workItemColumnPrecision {
			t.Fatalf("%s.%s stores %s (type %q) but the adapters truncate to %s -- "+
				"make the truncation per-destination rather than widening the constant",
				table, name, precision, columnType, workItemColumnPrecision)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	// A query that matched nothing would pass every assertion above.
	if inspected == 0 {
		t.Fatal("no timestamp columns were inspected, so this test asserted nothing")
	}
	for table, column := range versionColumns {
		if !seenVersionColumns[table] {
			t.Fatalf("the version column %s.%s was never inspected -- it is the column "+
				"the comparator orders on, so leaving it uncovered is the whole risk",
				table, column)
		}
	}
}

// clickHouseColumnPrecision maps a ClickHouse date/time column type to the
// smallest interval it can represent. Plain DateTime is seconds; DateTime64(N)
// is 10^-N seconds. An unrecognised type is an error, never a default, because
// a silently-assumed precision is the defect this exists to catch.
func clickHouseColumnPrecision(columnType string) (time.Duration, error) {
	inner := strings.TrimSuffix(strings.TrimPrefix(columnType, "Nullable("), ")")
	if strings.HasPrefix(inner, "DateTime64(") {
		digits := strings.TrimPrefix(inner, "DateTime64(")
		digits = strings.SplitN(digits, ",", 2)[0]
		digits = strings.TrimSpace(strings.TrimSuffix(digits, ")"))
		scale, err := strconv.Atoi(digits)
		if err != nil {
			return 0, fmt.Errorf("unparsable DateTime64 scale in %q", columnType)
		}
		precision := time.Second
		for i := 0; i < scale; i++ {
			precision /= 10
		}
		if precision <= 0 {
			return 0, fmt.Errorf("scale %d in %q is finer than Go can express", scale, columnType)
		}
		return precision, nil
	}
	if strings.HasPrefix(inner, "DateTime") {
		return time.Second, nil
	}
	return 0, fmt.Errorf("not a date/time column type: %q", columnType)
}
