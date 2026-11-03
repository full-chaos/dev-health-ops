package synccoverage

import (
	"reflect"
	"testing"
)

// D6467: the coverage scope never lists a (provider, dataset) pair the provider does not support, so the projection
// shows no gap and the admin page offers no backfill for it (GitHub has no incidents dataset). The planner's capability
// table is the one source of truth.
func TestSupportedDatasetKeysDropsWhatTheProviderDoesNotSupport(t *testing.T) {
	for _, tc := range []struct {
		name, provider string
		keys, want     []string
	}{
		{"github has no incidents", "github", []string{"commits", "incidents", "work-items"}, []string{"commits", "work-items"}},
		{"gitlab keeps incidents", "gitlab", []string{"commits", "incidents"}, []string{"commits", "incidents"}},
		{"mixed-case provider", "GitHub", []string{"incidents", "prs"}, []string{"prs"}},
		{"nothing supported", "github", []string{"incidents"}, []string{}},
		{"empty stays empty", "github", nil, []string{}},
	} {
		got := supportedDatasetKeys(tc.provider, tc.keys)
		if len(got) == 0 && len(tc.want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: supportedDatasetKeys(%q, %v) = %v, want %v", tc.name, tc.provider, tc.keys, got, tc.want)
		}
	}
}
