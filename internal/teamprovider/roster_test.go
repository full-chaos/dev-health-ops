package teamprovider_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider/teamprovidertest"
)

func rosterRow(id string, roster teamprovider.Roster, add, remove []string) teamprovider.Write {
	updated := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	return teamprovider.Write{
		Values: []any{id, uuid.New(), "A", (*string)(nil), []string{"stated"}, true, updated, "org-1", updated, (*time.Time)(nil)},
		Roster: roster, RosterAdd: add, RosterRemove: remove,
	}
}

// A statement with only stated rosters is the one Insert sent before roster
// rules: no read of the stored roster, no roster parameter.
func TestAStatedRosterIsNotReadFromTheStore(t *testing.T) {
	conn := &recordingExec{}
	if err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, []teamprovider.Write{rosterRow("gh:a", teamprovider.RosterStated, nil, nil)}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(conn.query, "kept_manual") || strings.Contains(conn.query, "roster_rule") {
		t.Fatalf("a stated roster reads the stored one: %s", conn.query)
	}
}

// A kept or delta roster is read from teams FINAL inside the insert statement,
// and the rule of every row travels with it.
func TestAKeptOrDeltaRosterIsReadInsideTheInsert(t *testing.T) {
	conn := &recordingExec{}
	rows := []teamprovider.Write{
		rosterRow("gh:a", teamprovider.RosterStated, nil, nil),
		rosterRow("gh:b", teamprovider.RosterKept, nil, nil),
		rosterRow("gh:c", teamprovider.RosterDelta, []string{"add"}, []string{"drop"}),
	}
	if err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, rows); err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{
		"LEFT JOIN (SELECT id AS kept_id, if(deleted_at IS NULL, manual_members, []) AS kept_manual FROM teams FINAL WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)}) AS kept ON kept.kept_id = incoming.v0",
		"multiIf(incoming.roster_rule = 1, kept.kept_manual, incoming.roster_rule = 2, ",
		"NOT has(incoming.roster_remove, m), arrayConcat(kept.kept_manual, incoming.roster_add)",
	} {
		if !strings.Contains(conn.query, part) {
			t.Fatalf("statement misses %q:\n%s", part, conn.query)
		}
	}
	_, decoded, err := teamprovidertest.Decode(conn.query, conn.args)
	if err != nil {
		t.Fatal(err)
	}
	var got []teamprovider.Roster
	for _, write := range decoded {
		got = append(got, write.Roster)
	}
	if want := []teamprovider.Roster{teamprovider.RosterStated, teamprovider.RosterKept, teamprovider.RosterDelta}; !reflect.DeepEqual(got, want) {
		t.Fatalf("roster rules = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(decoded[2].RosterAdd, []string{"add"}) || !reflect.DeepEqual(decoded[2].RosterRemove, []string{"drop"}) {
		t.Fatalf("delta = +%v -%v, want +[add] -[drop]", decoded[2].RosterAdd, decoded[2].RosterRemove)
	}
}

func TestARosterRuleNeedsTheRosterColumnAndAKnownRule(t *testing.T) {
	noRoster := []string{"id", "team_uuid", "name", "description", "is_active", "updated_at", "org_id", "created_at", "deleted_at"}
	row := rosterRow("gh:a", teamprovider.RosterKept, nil, nil)
	row.Values = append(row.Values[:4], row.Values[5:]...)
	conn := &recordingExec{}
	if err := teamprovider.Insert(context.Background(), conn, "org-1", noRoster, []teamprovider.Write{row}); !errors.Is(err, teamprovider.ErrInvalidWrite) {
		t.Fatalf("kept roster without the column: err = %v, want ErrInvalidWrite", err)
	}
	if err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, []teamprovider.Write{rosterRow("gh:a", teamprovider.RosterSource+1, nil, nil)}); !errors.Is(err, teamprovider.ErrInvalidWrite) {
		t.Fatalf("unknown roster rule: err = %v, want ErrInvalidWrite", err)
	}
	if err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, []teamprovider.Write{rosterRow("gh:a", teamprovider.RosterSource, nil, nil)}); !errors.Is(err, teamprovider.ErrInvalidWrite) {
		t.Fatalf("source roster with no source team: err = %v, want ErrInvalidWrite", err)
	}
	if conn.calls != 0 {
		t.Fatalf("a refused write sent %d statements", conn.calls)
	}
}

// A source roster row (a team moved to a new id) reads its source team's
// newest version inside the insert and takes that roster only when the
// version is newer than the one the writer read; a statement with no such row
// has no read of a source.
func TestASourceRosterIsReadFromTheSourceTeamInsideTheInsert(t *testing.T) {
	conn := &recordingExec{}
	if err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, []teamprovider.Write{rosterRow("gh:a", teamprovider.RosterKept, nil, nil)}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(conn.query, "moved_from") || strings.Contains(conn.query, "roster_source") {
		t.Fatalf("a statement with no source roster reads a source: %s", conn.query)
	}
	read := teamprovider.Version{UpdatedAt: time.Date(2026, 9, 1, 0, 0, 0, 123456000, time.UTC), LastSynced: time.Date(2026, 9, 1, 0, 0, 1, 0, time.UTC)}
	moved := rosterRow("gh:web", teamprovider.RosterSource, nil, nil)
	moved.RosterSource, moved.RosterSourceRead = "web", read
	if err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, []teamprovider.Write{moved, rosterRow("web", teamprovider.RosterKept, nil, nil)}); err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{
		"LEFT JOIN (SELECT id AS moved_from_id, argMax(if(deleted_at IS NULL, manual_members, []), (updated_at, last_synced)) AS moved_from_manual, " +
			"max((toUnixTimestamp64Micro(updated_at), toUnixTimestamp64Micro(last_synced))) AS moved_from_version " +
			"FROM teams WHERE org_id = {org_id:String} AND id IN {roster_source:Array(String)} GROUP BY id) AS moved_from ON moved_from.moved_from_id = incoming.roster_source",
		"incoming.roster_rule = 3 AND moved_from.moved_from_version > (toUnixTimestamp64Micro(incoming.roster_read_at), toUnixTimestamp64Micro(incoming.roster_read_synced)), moved_from.moved_from_manual, incoming.v4)",
	} {
		if !strings.Contains(conn.query, part) {
			t.Fatalf("statement misses %q:\n%s", part, conn.query)
		}
	}
	_, decoded, err := teamprovidertest.Decode(conn.query, conn.args)
	if err != nil {
		t.Fatal(err)
	}
	if decoded[0].Roster != teamprovider.RosterSource || decoded[0].RosterSource != "web" || decoded[0].RosterSourceRead != read {
		t.Fatalf("source row = rule %d from %q read %+v, want rule %d from web read %+v", decoded[0].Roster, decoded[0].RosterSource, decoded[0].RosterSourceRead, teamprovider.RosterSource, read)
	}
	if decoded[1].Roster != teamprovider.RosterKept || decoded[1].RosterSource != "" {
		t.Fatalf("kept row = rule %d from %q, want a kept roster from no source", decoded[1].Roster, decoded[1].RosterSource)
	}
}
