//go:build integration

package pgmigrate_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// CHAOS-7469: the Python producers of the alias, history and revisions goldens run in a CLOSED environment.
// The history producer used to delete MIGRATION_DATABASE_URI and DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER from
// os.Environ() by hand because they change the answer; they, and every other variable of the test process,
// must be absent by construction. Since CHAOS-8295 the environment is the launcher's (Producer.Command,
// pyoracle.ClosedEnv); this test keeps the property visible for this package's settings.
func TestPgmigratePythonEnvIsClosed(t *testing.T) {
	for _, name := range []string{"MIGRATION_DATABASE_URI", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER", "ORG_ID", "HTTPS_PROXY", "PYTHONHASHSEED", "TZ", "HOME"} {
		t.Setenv(name, "from-the-test-process")
	}
	allowed := map[string]bool{"PATH": true, "LANG": true, "LC_ALL": true, "TZ": true, "PYTHONHASHSEED": true, "PYTHONDONTWRITEBYTECODE": true, "PYTHONPATH": true, "OTEL_ENABLED": true}
	producer := &venueoracle.Producer{Root: "/checkout"}
	for label, settings := range map[string]map[string]string{"alias and revisions": pgmigratePythonSettings, "history": historyPythonSettings, "downgrade": downgradePythonSettings, "upgrade": upgradePythonSettings} {
		declared := declaredWith(settings, nil)
		for _, entry := range producer.Env(declared) {
			name, value, _ := strings.Cut(entry, "=")
			if !allowed[name] && declared[name] == "" {
				t.Errorf("%s: the producer environment carries %s", label, name)
			}
			if value == "from-the-test-process" {
				t.Errorf("%s: %s was inherited from the test process", label, name)
			}
		}
	}
}
