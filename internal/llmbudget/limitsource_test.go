package llmbudget

import (
	"regexp"
	"testing"
)

func TestTheLimitReadFollowsItsSource(t *testing.T) {
	if got, want := readSettingSQL(LimitFromSettings), `SELECT value, is_encrypted FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`; got != want {
		t.Errorf("the settings-table limit read changed: %s", got)
	}
	definer := readSettingSQL(LimitFromCategorizeBYODefiner)
	if regexp.MustCompile(`(?i)\bFROM\s+settings\b`).MatchString(definer) {
		t.Errorf("the definer limit read names the settings table: %s", definer)
	}
	if want := `SELECT value, is_encrypted FROM public.categorize_byo_settings($1) AS settings WHERE org_id = $1 AND category = $2 AND key = $3`; definer != want {
		t.Errorf("the definer limit read = %s", definer)
	}
	var zero PgStore
	if zero.Limit != LimitFromSettings {
		t.Error("a PgStore without a Limit must read the settings table (the api wiring)")
	}
}
