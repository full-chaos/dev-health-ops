package teamsidentity

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// The admin team answer renders the stored creation time, not the update time.
func TestTeamAnswerRendersStoredCreatedAt(t *testing.T) {
	created := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	updated := time.Date(2026, 9, 21, 11, 37, 5, 895620000, time.UTC)
	team := Team{TeamID: "custom:team-1", CreatedAt: created, UpdatedAt: updated}
	for name, object := range map[string]struct{ created, updated string }{
		"read":    {created: stringOf(t, teamJSON(team), "created_at"), updated: stringOf(t, teamJSON(team), "updated_at")},
		"written": {created: stringOf(t, teamWrittenJSON(team), "created_at"), updated: stringOf(t, teamWrittenJSON(team), "updated_at")},
	} {
		if object.created == object.updated {
			t.Errorf("%s: created_at == updated_at (%s)", name, object.created)
		}
	}
	if got, want := stringOf(t, teamJSON(team), "created_at"), "2026-01-02T03:04:05.123456"; got != want {
		t.Errorf("read created_at = %s, want %s", got, want)
	}
	if got, want := stringOf(t, teamWrittenJSON(team), "created_at"), "2026-01-02T03:04:05.123456Z"; got != want {
		t.Errorf("written created_at = %s, want %s", got, want)
	}
	if got, want := stringOf(t, teamJSON(team), "updated_at"), "2026-09-21T11:37:05.895620"; got != want {
		t.Errorf("read updated_at = %s, want %s", got, want)
	}
}

func stringOf(t *testing.T, object *pyjson.Object, key string) string {
	t.Helper()
	value, ok := object.Get(key)
	if !ok {
		t.Fatalf("no %s in the answer", key)
	}
	text, ok := value.(string)
	if !ok {
		t.Fatalf("%s = %#v, want a string", key, value)
	}
	return text
}
