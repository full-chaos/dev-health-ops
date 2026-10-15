package providersync

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// sprintReadbackConn serves stored sprints rows to the sprints readback
// select; every other driver.Conn method panics.
type sprintReadbackConn struct {
	driver.Conn
	stored []githubSprintRow
}

func (conn *sprintReadbackConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	if !strings.Contains(query, "FROM sprints FINAL") {
		return nil, fmt.Errorf("sprintReadbackConn: unexpected query: %s", query)
	}
	return &sprintReadbackRows{rows: conn.stored, index: -1}, nil
}

type sprintReadbackRows struct {
	driver.Rows
	rows  []githubSprintRow
	index int
}

func (rows *sprintReadbackRows) Next() bool {
	rows.index++
	return rows.index < len(rows.rows)
}

func (rows *sprintReadbackRows) Scan(dest ...any) error {
	row := rows.rows[rows.index]
	*dest[0].(*string) = row.Provider
	*dest[1].(*string) = row.SprintID
	switch target := dest[2].(type) {
	case *string:
		*target = derefString(row.NativeTeamKey)
	case **string:
		*target = row.NativeTeamKey
	default:
		return fmt.Errorf("sprintReadbackRows: native_team_key scanned into %T", dest[2])
	}
	*dest[3].(**string) = row.Name
	*dest[4].(**string) = row.State
	*dest[5].(**time.Time) = utcPointer(row.StartedAt)
	*dest[6].(**time.Time) = utcPointer(row.EndedAt)
	*dest[7].(**time.Time) = utcPointer(row.CompletedAt)
	*dest[8].(*time.Time) = clickHouseMillis(row.LastSynced)
	*dest[9].(*string) = row.OrgID
	return nil
}

func (rows *sprintReadbackRows) Err() error   { return nil }
func (rows *sprintReadbackRows) Close() error { return nil }

var (
	unitAWrite = time.Date(2026, 7, 23, 10, 0, 0, 0, time.UTC)
	unitBWrite = unitAWrite.Add(time.Minute)
)

func sprintFixture(provider, sprintID, name string, lastSynced time.Time) githubSprintRow {
	state := "active"
	started := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	return githubSprintRow{
		Provider: provider, SprintID: sprintID,
		Name: &name, State: &state, StartedAt: &started,
		LastSynced: lastSynced, OrgID: "org-acme",
	}
}

func groupMilestoneSprint(name string, lastSynced time.Time) gitlabSprintRow {
	return sprintFixture("gitlab", "gitlab:acme:milestone:31", name, lastSynced)
}

func sprintTestLease() providerfoundation.LeaseGuard {
	return providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
}

// Unit A of a GitLab group writes the group milestone row, then crashes before
// its ledger commit; unit B of the same group writes the row later. A's
// recovery reads B's row back.
func TestGitLabGroupSprintReadbackAfterSiblingRewrite(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	expected := groupMilestoneSprint("Sprint 7", unitAWrite)
	effect, err := buildGitLabWorkItemEffect("sprints", GitLabWorkItemEffectRows{Sprints: []gitlabSprintRow{expected}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		stored gitlabSprintRow
		want   EffectInspection
	}{
		{"own write landed", groupMilestoneSprint("Sprint 7", unitAWrite), EffectExact},
		{"sibling rewrote same content later", groupMilestoneSprint("Sprint 7", unitBWrite), EffectExact},
		{"sibling wrote newer observation later", groupMilestoneSprint("Sprint 7 (renamed)", unitBWrite), EffectExact},
		{"different content at own version", groupMilestoneSprint("Sprint 7 (renamed)", unitAWrite), EffectConflict},
		{"older row only", groupMilestoneSprint("Sprint 7", unitAWrite.Add(-time.Hour)), EffectAbsent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := NewGitLabWorkItemClickHouseEffects(&sprintReadbackConn{stored: []githubSprintRow{tc.stored}}, sprintTestLease())
			got, err := sink.InspectEffect(context.Background(), claim, effect)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("inspection = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSprintReadbackNewerRowBySharedDeclaration(t *testing.T) {
	githubSink := func(conn driver.Conn) GitHubWorkItemEffectAdapter {
		sink, err := NewGitHubWorkItemClickHouseEffects(conn, sprintTestLease(), nil)
		if err != nil {
			t.Fatal(err)
		}
		return sink.Sprints
	}
	jiraSink := func(conn driver.Conn) GitHubWorkItemEffectAdapter {
		return NewJiraAtlassianClickHouseEffects(conn, sprintTestLease()).Sprints
	}
	for _, tc := range []struct {
		name     string
		adapter  func(driver.Conn) GitHubWorkItemEffectAdapter
		provider string
		sprintID string
		storedAs string
		want     EffectInspection
	}{
		{"jira shared sprint, newer observation", jiraSink, "jira", "jira:sprint:9", "Sprint 9 (closed)", EffectExact},
		{"jira shared sprint, newer same content", jiraSink, "jira", "jira:sprint:9", "Sprint 9", EffectExact},
		{"github single-owner milestone, newer different content", githubSink, "github", "ghms:acme/api:4", "Sprint 9 (closed)", EffectConflict},
		{"github single-owner milestone, newer same content", githubSink, "github", "ghms:acme/api:4", "Sprint 9", EffectExact},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := githubWorkItemsRawMessages([]githubSprintRow{sprintFixture(tc.provider, tc.sprintID, "Sprint 9", unitAWrite)})
			if err != nil {
				t.Fatal(err)
			}
			effect, err := BuildEffectBatch("sprints", EffectReadbackRequired, raw)
			if err != nil {
				t.Fatal(err)
			}
			identity := GitHubWorkItemEffectIdentity{
				OrgID: "org-acme", Provider: tc.provider, Dataset: "work-items", Generation: "g1",
				Destination: "sprints", ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
			}
			stored := sprintFixture(tc.provider, tc.sprintID, tc.storedAs, unitBWrite)
			adapter := tc.adapter(&sprintReadbackConn{stored: []githubSprintRow{stored}})
			got, err := adapter.InspectGitHubWorkItemEffect(context.Background(), identity, effect)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("inspection = %q, want %q", got, tc.want)
			}
		})
	}
}

// The committer recovers unit A's crashed sprints effect after unit B's later
// write: no ErrEffectRecoveryAmbiguous, no replay, and a log line that names
// why the newer row counted as committed.
func TestGitLabGroupSprintCrashRecoveryCommitsWithoutOperator(t *testing.T) {
	now := unitBWrite.Add(time.Minute)
	claim := nativeTestClaim("gitlab", "work-items")
	effect, err := buildGitLabWorkItemEffect("sprints", GitLabWorkItemEffectRows{
		Sprints: []gitlabSprintRow{groupMilestoneSprint("Sprint 7", unitAWrite)},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		stored     gitlabSprintRow
		wantReason string
	}{
		{"same content", groupMilestoneSprint("Sprint 7", unitBWrite), "rewritten_by_newer_sibling"},
		{"newer observation", groupMilestoneSprint("Sprint 7 (closed)", unitBWrite), "superseded_by_newer_sibling"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, err := NewEffectLedgerState(claim, []EffectBatch{effect}, unitAWrite)
			if err != nil {
				t.Fatal(err)
			}
			started := unitAWrite
			state.Effects[0].Status = GenerationBlockWriting
			state.Effects[0].StartedAt = &started
			ledger := &memoryEffectLedger{state: state}
			writes := &memoryEffectSink{}
			readback := NewGitLabWorkItemClickHouseEffects(&sprintReadbackConn{stored: []githubSprintRow{tc.stored}}, sprintTestLease())
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			defer slog.SetDefault(previous)

			result, err := (EffectCommitter{
				Ledger: ledger, Sink: writes, Readback: readback,
				Now: func() time.Time { return now },
			}).Commit(context.Background(), claim, []EffectBatch{effect}, unitAWrite)
			if err != nil {
				t.Fatalf("recovery error = %v", err)
			}
			if result.MarkedCommitted != 1 || len(writes.destinations) != 0 ||
				ledger.state.Effects[0].Status != GenerationBlockCommitted {
				t.Fatalf("result=%+v writes=%v state=%+v", result, writes.destinations, ledger.state.Effects[0])
			}
			if !strings.Contains(logs.String(), `"msg":"sprint_effect_readback_newer_row"`) ||
				!strings.Contains(logs.String(), `"reason":"`+tc.wantReason+`"`) {
				t.Fatalf("log = %s", logs.String())
			}
		})
	}
}

// The Linear reference catalog and the Linear work-items unit write the same
// linear:cycle:<id> key, so a newer catalog row supersedes a crashed
// work-items write.
func TestLinearCycleReadbackAfterNewerCatalogWrite(t *testing.T) {
	family, err := NewLinearWorkItemFamilyClickHouseEffects(
		&sprintReadbackConn{stored: []githubSprintRow{sprintFixture("linear", "linear:cycle:c1", "Cycle 3 (completed)", unitBWrite)}},
		sprintTestLease(), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := githubWorkItemsRawMessages([]githubSprintRow{sprintFixture("linear", "linear:cycle:c1", "Cycle 3", unitAWrite)})
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch("sprints", EffectReadbackRequired, raw)
	if err != nil {
		t.Fatal(err)
	}
	identity := LinearWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "linear", Dataset: "work-items", Generation: "g1",
		Destination: "sprints", ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	got, err := family.Raw.Sprints.InspectLinearWorkItemEffect(context.Background(), identity, effect)
	if err != nil {
		t.Fatal(err)
	}
	if got != EffectExact {
		t.Fatalf("inspection = %q, want %q", got, EffectExact)
	}
}

// The reverse order: the catalog recovers its crashed write after a newer
// work-items row under the same cycle key.
func TestLinearCatalogSprintReadbackAfterNewerWorkItemsWrite(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	effect, err := effectBatchFromValues(linearReferenceCatalogSprintsDestination, EffectReadbackRequired,
		[]linearSprintRow{sprintFixture("linear", "linear:cycle:c1", "Cycle 3", unitAWrite)})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		stored linearSprintRow
		want   EffectInspection
	}{
		{"own write landed", sprintFixture("linear", "linear:cycle:c1", "Cycle 3", unitAWrite), EffectExact},
		{"work-items rewrote same content later", sprintFixture("linear", "linear:cycle:c1", "Cycle 3", unitBWrite), EffectExact},
		{"work-items wrote newer observation later", sprintFixture("linear", "linear:cycle:c1", "Cycle 3 (completed)", unitBWrite), EffectExact},
		{"different content at own version", sprintFixture("linear", "linear:cycle:c1", "Cycle 3 (completed)", unitAWrite), EffectConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := LinearReferenceCatalogClickHouseEffects{
				Conn: &sprintReadbackConn{stored: []githubSprintRow{tc.stored}}, Lease: sprintTestLease(),
			}
			got, err := sink.InspectEffect(context.Background(), claim, effect)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("inspection = %q, want %q", got, tc.want)
			}
		})
	}
}
