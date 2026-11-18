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
	if err := teamprovider.Insert(context.Background(), conn, "org-1", insertColumns, []teamprovider.Write{rosterRow("gh:a", teamprovider.RosterDelta+1, nil, nil)}); !errors.Is(err, teamprovider.ErrInvalidWrite) {
		t.Fatalf("unknown roster rule: err = %v, want ErrInvalidWrite", err)
	}
	if conn.calls != 0 {
		t.Fatalf("a refused write sent %d statements", conn.calls)
	}
}
