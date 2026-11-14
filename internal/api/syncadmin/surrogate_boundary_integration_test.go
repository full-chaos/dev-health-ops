//go:build integration

package syncadmin

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/google/uuid"
)

func TestBackfillSelectorRefusesALoneSurrogateAtTheBoundary(t *testing.T) {
	v := startCascadeVenue(t, false)
	one := v.seed("github", `["git"]`, []string{"repo-metadata", "commits"})
	selected := uuid.New()
	v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repo', 'acme/selected', 'acme/selected', 'acme/selected', $4::json, true, now(), now())`,
		selected, v.org, one.integration, `{"planner_managed_sync_config_id": "`+one.config.String()+`"}`)
	path := "/api/v1/admin/sync-configs/" + one.config.String() + "/backfill"
	show := func(s string) string {
		if len(s) > 220 {
			s = s[:220]
		}
		return "valid_utf8=" + map[bool]string{true: "true", false: "false"}[utf8.ValidString(s)] + " body=" + s
	}
	triggers := func() int {
		return v.count(`SELECT count(*) FROM sync_manual_triggers t JOIN scheduled_sync_occurrences o ON o.occurrence_id = t.occurrence_id WHERE o.sync_config_id = $1`, one.config)
	}
	base := `"since":"2026-09-01T00:00:00Z","before":"2026-09-01T12:00:00Z"`
	for _, c := range []struct {
		name, body string
		refused    bool
	}{
		{"control source_ids ok", `{"selector":{` + base + `,"source_ids":["` + selected.String() + `"]}}`, false},
		{"list item source_ids", `{"selector":{` + base + `,"source_ids":["\ud800"]}}`, true},
		{"list item dataset_keys", `{"selector":{` + base + `,"dataset_keys":["\ud800"]}}`, true},
		{"list item dataset_keys + clean source", `{"selector":{` + base + `,"source_ids":["` + selected.String() + `"],"dataset_keys":["a\udc00b"]}}`, true},
	} {
		before := triggers()
		status, body := v.call("POST", path, c.body)
		lower := strings.ToLower(body)
		if c.refused {
			if status != 422 || !strings.Contains(body, `"string_unicode"`) {
				t.Errorf("%s: %d %s, want 422 string_unicode", c.name, status, show(body))
			}
			if strings.Contains(lower, "invalid byte sequence") || strings.Contains(lower, "0xed") || strings.Contains(lower, "ud800") || strings.Contains(lower, "udc00") || strings.Contains(lower, `"input"`) {
				t.Errorf("%s: the body echoes the input or a database error: %s", c.name, show(body))
			}
			if got := triggers(); got != before {
				t.Errorf("%s: a refused body wrote %d trigger row(s)", c.name, got-before)
			}
		} else if status != 202 {
			t.Errorf("%s: %d %s, want 202", c.name, status, show(body))
		}
	}
}
