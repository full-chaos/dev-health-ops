//go:build integration

package atlassianteams

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// linkAnswerIs answers every link question with one fixed answer; the team and
// member questions are never asked in these tests (the team is in the run).
type linkAnswerIs struct {
	everyAbsentFactIsGone
	answer providersync.SnapshotAbsence
}

func (double linkAnswerIs) LinkAbsence(context.Context, string, providersync.ProjectID) providersync.SnapshotAbsence {
	return double.answer
}

// CHAOS-9124: a link written under a key-built id ("org:jira:KEY") closes only on
// an answer. When the answer is not proven (an error) or the budget of the kind
// is spent, the key-built row stays open, the native-id link is written in the
// same Write, and the NEXT run closes the row.
func TestAKeyBuiltProjectLinkClosesOnTheNextRunWhenTheAnswerIsMissingOrTheBudgetIsSpent(t *testing.T) {
	const org = "org-1"
	const insert = `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES `
	openCount := func(t *testing.T, kind string, conn driver.Conn) string {
		t.Helper()
		got := lines(t, conn, `SELECT toString(count()) FROM team_project_ownership FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND valid_to IS NULL AND project_id LIKE 'org-1:jira:%'`)
		if len(got) != 1 {
			t.Fatalf("%s: open key-built count rows = %v", kind, got)
		}
		return got[0]
	}
	nativeOpen := func(t *testing.T, conn driver.Conn) string {
		t.Helper()
		got := lines(t, conn, `SELECT toString(count()) FROM team_project_ownership FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND valid_to IS NULL AND project_id = '10001'`)
		if len(got) != 1 {
			t.Fatalf("native open count rows = %v", got)
		}
		return got[0]
	}
	run := func(t *testing.T, conn driver.Conn, at time.Time, prover AbsenceProver) Result {
		t.Helper()
		g := newGateway(t, everyLinkWritable)
		p := params(everything)
		p.Now = at
		rows, err := Collect(context.Background(), g.client(), p)
		if err != nil {
			t.Fatal(err)
		}
		result, err := Write(context.Background(), conn, org, rows, everything, soleScope(), prover)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	seed := func(t *testing.T, conn driver.Conn, keys int) {
		t.Helper()
		exec(t, conn, insert+`('org-1', 'jira', '`+idA+`', 'org-1:jira:PLAT', 'PLAT', 'native', 1, 110, 10, '2026-09-01 00:00:00', NULL, '2026-09-01 00:00:00')`)
		for i := 0; i < keys; i++ {
			key := fmt.Sprintf("Z%03d", i)
			exec(t, conn, insert+`('org-1', 'jira', '`+idA+`', 'org-1:jira:`+key+`', '`+key+`', 'native', 1, 110, 10, '2026-09-01 00:00:00', NULL, '2026-09-01 00:00:00')`)
		}
	}
	first := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)

	t.Run("the answer is an error: the row stays open, the native link is written, the next run closes it", func(t *testing.T) {
		conn := openClickHouse(t)
		seed(t, conn, 0)
		result := run(t, conn, first, linkAnswerIs{answer: providersync.SnapshotAbsenceNotProven})
		if result.OwnershipWritten != 1 || result.ExpiredOwnership != 0 {
			t.Fatalf("run 1: result=%+v, want the native link written and nothing closed", result)
		}
		if got := openCount(t, "run 1", conn); got != "1" {
			t.Fatalf("run 1: open key-built rows = %s, want 1 (still open)", got)
		}
		if got := nativeOpen(t, conn); got != "1" {
			t.Fatalf("run 1: open native rows = %s, want 1", got)
		}
		result = run(t, conn, first.Add(time.Hour), linkAnswerIs{answer: providersync.SnapshotAbsenceProven})
		if result.ExpiredOwnership != 1 {
			t.Fatalf("run 2: result=%+v, want the key-built row closed", result)
		}
		if got := openCount(t, "run 2", conn); got != "0" {
			t.Fatalf("run 2: open key-built rows = %s, want 0", got)
		}
		if got := nativeOpen(t, conn); got != "1" {
			t.Fatalf("run 2: open native rows = %s, want 1 (the native link stays)", got)
		}
	})

	t.Run("the budget of the kind is spent: the rows beyond it stay open and the next run closes them", func(t *testing.T) {
		conn := openClickHouse(t)
		over := providersync.AbsenceLookupBudget // one key-built row (PLAT) + budget keys = budget+1 candidates
		seed(t, conn, over)
		result := run(t, conn, first, linkAnswerIs{answer: providersync.SnapshotAbsenceProven})
		if result.ExpiredOwnership != over {
			t.Fatalf("run 1: closed %d links, want exactly the budget (%d)", result.ExpiredOwnership, over)
		}
		if got := openCount(t, "run 1", conn); got != "1" {
			t.Fatalf("run 1: open key-built rows = %s, want 1 beyond the budget", got)
		}
		if got := nativeOpen(t, conn); got != "1" {
			t.Fatalf("run 1: open native rows = %s, want 1", got)
		}
		result = run(t, conn, first.Add(time.Hour), linkAnswerIs{answer: providersync.SnapshotAbsenceProven})
		if result.ExpiredOwnership != 1 {
			t.Fatalf("run 2: closed %d links, want the 1 left over", result.ExpiredOwnership)
		}
		if got := openCount(t, "run 2", conn); got != "0" {
			t.Fatalf("run 2: open key-built rows = %s, want 0", got)
		}
	})
}
