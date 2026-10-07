package providersync

import "testing"

// TestRowsOwnSyncSelectionOnlyForAWholeIntegrationConfigThatIsNotPagerDuty:
// one clause per row.
func TestRowsOwnSyncSelectionOnlyForAWholeIntegrationConfigThatIsNotPagerDuty(t *testing.T) {
	for _, testCase := range []struct {
		name                      string
		provider                  string
		hasIntegration, hasSource bool
		want                      bool
	}{
		{"whole-integration github", "github", true, false, true},
		{"whole-integration Jira, mixed case", "Jira", true, false, true},
		{"child pinned to a source", "github", true, true, false},
		{"no integration", "linear", false, false, false},
		{"no integration, a source", "gitlab", false, true, false},
		{"pagerduty", "pagerduty", true, false, false},
		{"PagerDuty, mixed case", "PagerDuty", true, false, false},
	} {
		if got := RowsOwnSyncSelection(testCase.provider, testCase.hasIntegration, testCase.hasSource); got != testCase.want {
			t.Errorf("%s: %v, want %v", testCase.name, got, testCase.want)
		}
	}
}
