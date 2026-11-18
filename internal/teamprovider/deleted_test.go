package teamprovider_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider/teamprovidertest"
)

func copiedRow(id string, roster teamprovider.Roster, deleted teamprovider.Deleted) teamprovider.Write {
	row := rosterRow(id, roster, nil, nil)
	row.Deleted = deleted
	if roster == teamprovider.RosterSource {
		row.RosterSource = "old"
	}
	return row
}

// A copy rule copies an admin delete that is stored; a row that states a
// deleted_at of its own with a copy rule is refused, and so is a copy rule
// with no read to copy from or no column to write.
func TestACopiedDeletedAtIsNeverTheWritersOwn(t *testing.T) {
	own := time.Date(2026, 11, 18, 9, 30, 0, 0, time.UTC)
	withOwn := func(row teamprovider.Write) teamprovider.Write {
		row.Values[9] = &own
		return row
	}
	for name, tc := range map[string]struct {
		columns []string
		row     teamprovider.Write
	}{
		"kept with its own deleted_at":   {insertColumns, withOwn(copiedRow("gh:a", teamprovider.RosterKept, teamprovider.DeletedKept))},
		"source with its own deleted_at": {insertColumns, withOwn(copiedRow("gh:a", teamprovider.RosterSource, teamprovider.DeletedSource))},
		"kept with a stated roster":      {insertColumns, copiedRow("gh:a", teamprovider.RosterStated, teamprovider.DeletedKept)},
		"source with no source team":     {insertColumns, copiedRow("gh:a", teamprovider.RosterKept, teamprovider.DeletedSource)},
		"an unknown rule":                {insertColumns, copiedRow("gh:a", teamprovider.RosterKept, teamprovider.DeletedSource+1)},
		"no deleted_at column": {insertColumns[:9], func() teamprovider.Write {
			r := copiedRow("gh:a", teamprovider.RosterKept, teamprovider.DeletedKept)
			r.Values = r.Values[:9]
			return r
		}()},
	} {
		t.Run(name, func(t *testing.T) {
			conn := &recordingExec{}
			err := teamprovider.Insert(context.Background(), conn, "org-1", tc.columns, []teamprovider.Write{tc.row})
			if !errors.Is(err, teamprovider.ErrInvalidWrite) || conn.query != "" {
				t.Fatalf("err = %v, query sent %q; want ErrInvalidWrite and no statement", err, conn.query)
			}
		})
	}
}

// With no copy rule the statement is the one Insert sent before the rules:
// deleted_at and is_active are the writer's values, nothing is read for them.
func TestNoCopyRuleLeavesDeletedAtToTheWriter(t *testing.T) {
	for _, roster := range []teamprovider.Roster{teamprovider.RosterStated, teamprovider.RosterKept, teamprovider.RosterSource} {
		conn := &recordingExec{}
		row := copiedRow("gh:a", roster, teamprovider.DeletedStated)
		if err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, []teamprovider.Write{row}); err != nil {
			t.Fatal(err)
		}
		for _, part := range []string{"deleted_rule", "kept_deleted", "moved_from_deleted"} {
			if strings.Contains(conn.query, part) {
				t.Fatalf("roster rule %d: a write with no copy rule reads %q:\n%s", roster, part, conn.query)
			}
		}
	}
}

// The copy rules are read inside the insert: deleted_at of teams FINAL for a
// kept row, and the source's newest admin delete (then inactive) for a moved
// row; each row's rule travels with it.
func TestTheCopyRulesAreReadInsideTheInsert(t *testing.T) {
	conn := &recordingExec{}
	rows := []teamprovider.Write{
		copiedRow("old", teamprovider.RosterKept, teamprovider.DeletedKept),
		copiedRow("gh:new", teamprovider.RosterSource, teamprovider.DeletedSource),
	}
	if err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, rows); err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"deleted_at AS kept_deleted FROM teams FINAL", "AS moved_from_deleted", "kept.kept_deleted", "toUInt8(0)"} {
		if !strings.Contains(conn.query, part) {
			t.Fatalf("statement misses %q:\n%s", part, conn.query)
		}
	}
	_, decoded, err := teamprovidertest.Decode(conn.query, conn.args)
	if err != nil {
		t.Fatal(err)
	}
	if decoded[0].Deleted != teamprovider.DeletedKept || decoded[1].Deleted != teamprovider.DeletedSource {
		t.Fatalf("decoded rules %d, %d", decoded[0].Deleted, decoded[1].Deleted)
	}
}

// A write of kept rows only (no moved row) copies the kept delete and reads
// no source team: a team id carry that only retires rows.
func TestAKeptCopyWithNoSourceReadsNoSourceTeam(t *testing.T) {
	conn := &recordingExec{}
	if err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, []teamprovider.Write{copiedRow("old", teamprovider.RosterKept, teamprovider.DeletedKept)}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(conn.query, "moved_from") || !strings.Contains(conn.query, "kept.kept_deleted") {
		t.Fatalf("a kept-only copy reads a source team or misses the kept delete:\n%s", conn.query)
	}
}

// acp (CHAOS-14135 x 14094): one statement never holds admin rules (Kept,
// VersionAfterStored, SkipDeleted) and deleted_at copy rules: Insert refuses
// it before a statement is built, in one row or across rows.
func TestAStatementWithAdminAndCopyRulesIsRefused(t *testing.T) {
	admin := func(row teamprovider.Write) teamprovider.Write {
		row.Kept = teamprovider.KeptName
		return row
	}
	for name, rows := range map[string][]teamprovider.Write{
		"one row, kept copy and admin kept": {admin(copiedRow("old", teamprovider.RosterKept, teamprovider.DeletedKept))},
		"one row, source copy and skip deleted": {func() teamprovider.Write {
			r := copiedRow("gh:new", teamprovider.RosterSource, teamprovider.DeletedSource)
			r.SkipDeleted = true
			return r
		}()},
		"one row, kept copy and version after": {func() teamprovider.Write {
			r := copiedRow("old", teamprovider.RosterKept, teamprovider.DeletedKept)
			r.VersionAfterStored = true
			return r
		}()},
		"two rows, an admin row and a copy row": {admin(rosterRow("a", teamprovider.RosterKept, nil, nil)), copiedRow("old", teamprovider.RosterKept, teamprovider.DeletedKept)},
	} {
		t.Run(name, func(t *testing.T) {
			conn := &recordingExec{}
			err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, rows)
			if !errors.Is(err, teamprovider.ErrInvalidWrite) || conn.query != "" {
				t.Fatalf("err = %v, query sent %q; want ErrInvalidWrite and no statement", err, conn.query)
			}
		})
	}
}

// carryTeamsColumns is the column list the team id carry gives Insert
// (internal/providersync/team_id_carry.go writeTeams: its teams columns
// without the provider identity, then deleted_at).
var carryTeamsColumns = []string{"id", "team_uuid", "name", "description", "manual_members", "updated_at", "last_synced", "org_id", "provider", "native_team_key", "parent_team_id", "project_keys", "repo_patterns", "is_active", "source_id", "created_at", "deleted_at"}

// The carry's statement with its copy rules through the merged builder (the
// admin rules of CHAOS-14094 beside them): it reads the copy columns and none
// of the admin ones, and its text and parameter names are pinned like the
// sync writers' (TestASyncWritersTeamsStatementIsPinned has no copy rule).
func TestTheCarryStatementWithCopyRulesIsPinned(t *testing.T) {
	retired := pinRow(carryTeamsColumns, "web", teamprovider.RosterKept)
	retired.Deleted = teamprovider.DeletedKept
	retired.Values[len(carryTeamsColumns)-1] = (*time.Time)(nil)
	moved := pinRow(carryTeamsColumns, "gh:web", teamprovider.RosterSource)
	moved.RosterSource, moved.Deleted, moved.Gone = "web", teamprovider.DeletedSource, teamprovider.GoneStated
	moved.Values[len(carryTeamsColumns)-1] = (*time.Time)(nil)
	conn := &recordingExec{}
	if err := teamprovider.Insert(context.Background(), conn, "org-1", carryTeamsColumns, []teamprovider.Write{moved, retired}); err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"kept.kept_deleted", "moved_from.moved_from_deleted", "toUInt8(0)", "deleted_rule"} {
		if !strings.Contains(conn.query, part) {
			t.Fatalf("the carry statement misses %q:\n%s", part, conn.query)
		}
	}
	for _, part := range []string{"kept_mask", "newest_found", "skip_deleted", "kept_found"} {
		if strings.Contains(conn.query, part) {
			t.Fatalf("the carry statement reads the admin rule %q:\n%s", part, conn.query)
		}
	}
	names := make([]string, 0, len(conn.args))
	for _, arg := range conn.args {
		names = append(names, arg.(driver.NamedValue).Name)
	}
	sum := sha256.Sum256([]byte(conn.query + "\n" + strings.Join(names, ",")))
	const want = "7dff901a15dcbf1f8b6453b6194d0042ec42472c55fe5fa87a383ac56b121e1d"
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Errorf("PIN carry_copy_rules %s: the carry statement changed (want %s)", got, want)
	}
}
