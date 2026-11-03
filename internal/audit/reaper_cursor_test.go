package audit

import (
	"strings"
	"testing"
)

// A push row above the reader's low-water mark is never reaped, whatever its
// age: the reader has not decided it yet. A row of any other aggregate type is
// reaped as before, and a push row with no cursor row at all is kept.
func TestReapSelectionKeepsAPushRowAboveTheCursor(t *testing.T) {
	sql := reapSelection(`"auth".auth_outbox_events`, `"auth".projection_cursor`)
	for _, want := range []string{
		"e.published_at IS NOT NULL",
		"e.published_at < $1",
		"(e.aggregate_type <> 'external_push_credential' OR e.id <= COALESCE((SELECT c.low_water FROM \"auth\".projection_cursor AS c WHERE c.stream = 'external_push'), 0))",
		"FOR UPDATE SKIP LOCKED",
		"LIMIT $2",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("the reap selection lacks %q:\n%s", want, sql)
		}
	}
	if !strings.Contains(reapStatement(`"auth".auth_outbox_events`, `"auth".projection_cursor`), `"auth".projection_cursor`) {
		t.Error("the delete statement does not carry the cursor table")
	}
}
