//go:build integration

package pgmigrate_test

import (
	"strings"
	"testing"
)

// CHAOS-7469: the Python producers of the alias, history and revisions goldens run in a CLOSED environment.
// The history producer used to delete MIGRATION_DATABASE_URI and DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER from
// os.Environ() by hand because they change the answer; they, and every other variable of the test process,
// must be absent by construction.
func TestPgmigratePythonEnvIsClosed(t *testing.T) {
	for _, name := range []string{"MIGRATION_DATABASE_URI", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER", "ORG_ID", "HTTPS_PROXY", "PYTHONHASHSEED", "TZ"} {
		t.Setenv(name, "from-the-test-process")
	}
	allowed := map[string]bool{"PATH": true, "HOME": true, "PYTHONPATH": true, "PYTHONDONTWRITEBYTECODE": true, "OTEL_ENABLED": true, "PYTHONHASHSEED": true}
	for label, settings := range map[string]map[string]string{"alias and revisions": pgmigratePythonSettings, "history": historyPythonSettings} {
		for _, entry := range pgmigratePythonEnv("/checkout", settings) {
			name, value, _ := strings.Cut(entry, "=")
			if !allowed[name] {
				t.Errorf("%s: the producer environment carries %s", label, name)
			}
			if value == "from-the-test-process" {
				t.Errorf("%s: %s was inherited from the test process", label, name)
			}
		}
	}
}
