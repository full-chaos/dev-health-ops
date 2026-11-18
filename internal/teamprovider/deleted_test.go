package teamprovider_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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
