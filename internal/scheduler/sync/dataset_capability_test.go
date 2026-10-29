package sync

import "testing"

// D6467: a run that plans zero units because the provider has no such dataset (GitHub has no incidents dataset) is a
// no-op success, not a failure. The planner's capability table (supportedProviderDatasets) is the one place that knows.
func TestProviderSupportsDatasetFollowsThePlannersCapabilityTable(t *testing.T) {
	for _, tc := range []struct {
		provider, dataset string
		want              bool
	}{
		{"github", "incidents", false}, {"GitHub", "incidents", false}, {"github", "commits", true},
		{"gitlab", "incidents", true}, {"jira", "incidents", true}, {"pagerduty", "incidents", true},
		{"linear", "incidents", false}, {"launchdarkly", "feature-flags", true}, {"bitbucket", "commits", false},
	} {
		if got := ProviderSupportsDataset(tc.provider, tc.dataset); got != tc.want {
			t.Errorf("ProviderSupportsDataset(%q, %q) = %v, want %v", tc.provider, tc.dataset, got, tc.want)
		}
	}
}

func TestZeroUnitCapabilityReasonNamesOnlyAProviderThatKnowsNoRequestedDataset(t *testing.T) {
	ds := func(keys ...string) []PlanDataset {
		out := make([]PlanDataset, 0, len(keys))
		for _, key := range keys {
			out = append(out, PlanDataset{Key: key})
		}
		return out
	}
	for _, tc := range []struct {
		name, provider string
		datasets       []PlanDataset
		want           string
	}{
		{"github incidents only", "github", ds("incidents"), NoDatasetForProviderReason},
		{"github incidents on a mixed-case provider", "GitHub", ds("incidents"), NoDatasetForProviderReason},
		{"linear incidents and security", "linear", ds("incidents", "security"), NoDatasetForProviderReason},
		{"a supported dataset is requested too", "github", ds("incidents", "commits"), ""},
		{"a supported dataset only", "github", ds("commits"), ""},
		{"gitlab supports incidents", "gitlab", ds("incidents"), ""},
		{"nothing requested is not a capability reason", "github", nil, ""},
		{"an unknown provider is a different problem", "bitbucket", ds("incidents"), ""},
	} {
		if got := zeroUnitCapabilityReason(tc.provider, tc.datasets); got != tc.want {
			t.Errorf("%s: zeroUnitCapabilityReason = %q, want %q", tc.name, got, tc.want)
		}
	}
}
