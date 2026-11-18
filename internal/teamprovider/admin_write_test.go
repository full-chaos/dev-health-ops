package teamprovider_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider/teamprovidertest"
)

var adminColumns = []string{"id", "team_uuid", "name", "description", "manual_members", "project_keys", "repo_patterns", "is_active", "updated_at", "org_id", "deleted_at"}

func adminRow(id string) teamprovider.Write {
	updated := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	return teamprovider.Write{
		Values: []any{id, uuid.New(), "A", (*string)(nil), []string{}, []string{"K"}, []string{}, true, updated, "org-1", (*time.Time)(nil)},
		Roster: teamprovider.RosterKept,
	}
}

// CHAOS-14094: an admin row keeps the columns it does not edit from teams
// FINAL, moves its version past the newest stored one and is not written
// over a deleted team, all inside the insert statement; each row's rules
// travel with it.
func TestAnAdminRowKeepsItsColumnsAndVersionInsideTheInsert(t *testing.T) {
	conn := &recordingExec{}
	kept := adminRow("gh:a")
	kept.Kept, kept.VersionAfterStored, kept.SkipDeleted = teamprovider.KeptName|teamprovider.KeptRepoPatterns, true, true
	plain := adminRow("gh:b")
	plain.VersionAfterStored = true
	if err := teamprovider.Insert(context.Background(), conn, "org-1", adminColumns, []teamprovider.Write{kept, plain}); err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{
		"if(bitAnd(incoming.kept_mask, 1) != 0 AND kept.kept_found = 1, kept.kept_name, incoming.v2)",
		"if(bitAnd(incoming.kept_mask, 2) != 0 AND kept.kept_found = 1, kept.kept_description, incoming.v3)",
		"if(bitAnd(incoming.kept_mask, 4) != 0 AND kept.kept_found = 1, kept.kept_project_keys, incoming.v5)",
		"if(bitAnd(incoming.kept_mask, 8) != 0 AND kept.kept_found = 1, kept.kept_repo_patterns, incoming.v6)",
		"if(incoming.version_after = 1 AND newest.newest_found = 1 AND newest.newest_at >= incoming.v8, addMicroseconds(newest.newest_at, 1), incoming.v8)",
		"deleted_at AS kept_deleted_at FROM teams FINAL WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)}",
		"toTimeZone(max(updated_at), 'UTC') AS newest_at FROM teams WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)} GROUP BY id",
		"WHERE NOT (incoming.skip_deleted = 1 AND (kept.kept_found != 1 OR kept.kept_deleted_at IS NOT NULL)) ORDER BY incoming.ordinal",
	} {
		if !strings.Contains(conn.query, part) {
			t.Errorf("statement lacks %q:\n%s", part, conn.query)
		}
	}
	_, writes, err := teamprovidertest.Decode(conn.query, conn.args)
	if err != nil {
		t.Fatal(err)
	}
	if got := writes[0]; got.Kept != kept.Kept || !got.VersionAfterStored || !got.SkipDeleted {
		t.Errorf("row 0 rules = %d %v %v", got.Kept, got.VersionAfterStored, got.SkipDeleted)
	}
	if got := writes[1]; got.Kept != 0 || !got.VersionAfterStored || got.SkipDeleted {
		t.Errorf("row 1 rules = %d %v %v", got.Kept, got.VersionAfterStored, got.SkipDeleted)
	}
}

// A row with no admin rule sends the statement it sent before them.
func TestARowWithNoAdminRuleReadsNoAdminColumn(t *testing.T) {
	conn := &recordingExec{}
	if err := teamprovider.Insert(context.Background(), conn, "org-1", adminColumns, []teamprovider.Write{adminRow("gh:a")}); err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"kept_mask", "kept_found", "newest_at", "skip_deleted", "WHERE NOT"} {
		if strings.Contains(conn.query, part) {
			t.Errorf("a row with no admin rule reads %q: %s", part, conn.query)
		}
	}
}

func TestAnAdminRuleNeedsItsColumnsAndKnownBits(t *testing.T) {
	conn := &recordingExec{}
	unknown := adminRow("gh:a")
	unknown.Kept = teamprovider.KeptRepoPatterns << 1
	if err := teamprovider.Insert(context.Background(), conn, "org-1", adminColumns, []teamprovider.Write{unknown}); !errors.Is(err, teamprovider.ErrInvalidWrite) {
		t.Fatalf("unknown kept column: err = %v, want ErrInvalidWrite", err)
	}
	noName := append([]string{}, adminColumns[:2]...)
	noName = append(noName, adminColumns[3:]...)
	row := adminRow("gh:a")
	row.Values = append(append([]any{}, row.Values[:2]...), row.Values[3:]...)
	row.SkipDeleted = true
	if err := teamprovider.Insert(context.Background(), conn, "org-1", noName, []teamprovider.Write{row}); !errors.Is(err, teamprovider.ErrInvalidWrite) {
		t.Fatalf("admin rule with no name column: err = %v, want ErrInvalidWrite", err)
	}
	if conn.calls != 0 {
		t.Fatalf("a refused write sent %d statements", conn.calls)
	}
}
