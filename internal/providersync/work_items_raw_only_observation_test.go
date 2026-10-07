package providersync

import (
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// assertWorkItemDerivedTablesLeftToDailyJob fails unless the provider's
// "derived tables left to the daily job" counter holds exactly want.
func assertWorkItemDerivedTablesLeftToDailyJob(
	t *testing.T, metrics *providerfoundation.Metrics, provider string, want int,
) {
	t.Helper()
	var rendered strings.Builder
	if err := metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	line := `dev_health_work_item_derived_tables_left_to_daily_job_total{provider="` + provider + `"} `
	for _, got := range strings.Split(rendered.String(), "\n") {
		if strings.HasPrefix(got, line) {
			if got != line+strconv.Itoa(want) {
				t.Fatalf("counter line=%q want value %d", got, want)
			}
			return
		}
	}
	if want != 0 {
		t.Fatalf("no counter line for provider %q:\n%s", provider, rendered.String())
	}
}

// The counter label is a closed set: the four providers that have a
// work-items unit, and "other" for anything else. A nil registry is a no-op.
func TestWorkItemDerivedTablesLeftToDailyJobCounterIsBoundedByProvider(t *testing.T) {
	metrics := providerfoundation.NewMetrics()
	for _, provider := range []string{"github", "gitlab", "jira", "linear", "linear", "not-a-provider", "also-unknown"} {
		claim := nativeTestClaim("github", "work-items")
		claim.Provider = provider
		observeWorkItemDerivedTablesLeftToDailyJob(metrics, claim, 3)
	}
	assertWorkItemDerivedTablesLeftToDailyJob(t, metrics, "github", 1)
	assertWorkItemDerivedTablesLeftToDailyJob(t, metrics, "gitlab", 1)
	assertWorkItemDerivedTablesLeftToDailyJob(t, metrics, "jira", 1)
	assertWorkItemDerivedTablesLeftToDailyJob(t, metrics, "linear", 2)
	assertWorkItemDerivedTablesLeftToDailyJob(t, metrics, "other", 2)
	assertWorkItemDerivedTablesLeftToDailyJob(t, metrics, "not-a-provider", 0)
	observeWorkItemDerivedTablesLeftToDailyJob(nil, nativeTestClaim("github", "work-items"), 0)
}
