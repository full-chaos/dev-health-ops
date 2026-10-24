package sync

import (
	"errors"
	"reflect"
	"slices"
	"sort"
	"testing"
)

// canonicalIncidentDatasetsByProvider is the stated answer for every provider
// of the dataset catalogue: the datasets that need the canonical-incident
// feature. github, linear and launchdarkly have none; every pagerduty dataset
// is one.
var canonicalIncidentDatasetsByProvider = map[string][]string{
	"github":       {},
	"gitlab":       {"incidents"},
	"jira":         {"incidents"},
	"linear":       {},
	"launchdarkly": {},
	"pagerduty": {
		"business-services", "escalation-policies", "incident-alerts", "incident-log-entries",
		"incident-notes", "incidents", "on-calls", "schedules", "services", "teams", "users",
	},
}

func TestSkippedDatasetsResultNamesEachDatasetWithItsReason(t *testing.T) {
	if got := SkippedDatasetsResult(nil); got != nil {
		t.Fatalf("no skip = %v, want nil so the run keeps its result", got)
	}
	got := SkippedDatasetsResult([]string{"incidents", "services"})
	want := []map[string]string{
		{"dataset_key": "incidents", "reason": "feature_disabled", "message": "skipped: feature off"},
		{"dataset_key": "services", "reason": "feature_disabled", "message": "skipped: feature off"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SkippedDatasetsResult = %v, want %v", got, want)
	}
}

// Every provider of the catalogue is classified, dataset by dataset. A new
// provider or a new dataset must be given an answer here before it ships.
func TestPlanDatasetRequiresCanonicalIncidentForEveryCatalogueDataset(t *testing.T) {
	for provider := range supportedProviderDatasets {
		if _, ok := canonicalIncidentDatasetsByProvider[provider]; !ok {
			t.Errorf("provider %q of the dataset catalogue has no stated set of canonical-incident datasets", provider)
		}
	}
	for provider, want := range canonicalIncidentDatasetsByProvider {
		catalogue, ok := supportedProviderDatasets[provider]
		if !ok {
			t.Errorf("provider %q is not in the dataset catalogue", provider)
			continue
		}
		got := []string{}
		for dataset := range catalogue {
			if PlanDatasetRequiresCanonicalIncident(provider, dataset) {
				got = append(got, dataset)
			}
		}
		sort.Strings(got)
		if !slices.Equal(got, want) {
			t.Errorf("%s canonical-incident datasets = %v, want %v", provider, got, want)
		}
	}
	for _, tc := range []struct{ provider, dataset string }{
		{"github", "incidents"}, {"linear", "incidents"}, {"jira", "not-a-dataset"}, {"not-a-provider", "incidents"},
	} {
		if PlanDatasetRequiresCanonicalIncident(tc.provider, tc.dataset) {
			t.Errorf("%s/%s is not a catalogue dataset and must not be gated", tc.provider, tc.dataset)
		}
	}
}

func datasetKeys(datasets []PlanDataset) []string {
	keys := []string{}
	for _, dataset := range datasets {
		keys = append(keys, dataset.Key)
	}
	return keys
}

// The split takes out exactly the datasets that need the feature, keeps the
// order of the rest, and keeps each dataset's own options.
func TestSplitCanonicalIncidentDatasetsKeepsTheOtherDatasetsInOrder(t *testing.T) {
	depth := 30
	for _, tc := range []struct {
		provider              string
		datasets              []string
		wantKept, wantSkipped []string
	}{
		{"jira", []string{"incidents", "work-item-labels", "work-items"}, []string{"work-item-labels", "work-items"}, []string{"incidents"}},
		{"jira", []string{"work-items", "incidents"}, []string{"work-items"}, []string{"incidents"}},
		{"jira", []string{"incidents"}, []string{}, []string{"incidents"}},
		{"gitlab", []string{"commits", "incidents", "prs", "security"}, []string{"commits", "prs", "security"}, []string{"incidents"}},
		{"gitlab", []string{"commits", "prs"}, []string{"commits", "prs"}, []string{}},
		{"github", []string{"commits", "incidents", "work-items"}, []string{"commits", "incidents", "work-items"}, []string{}},
		{"linear", []string{"incidents", "work-items"}, []string{"incidents", "work-items"}, []string{}},
		{"pagerduty", []string{"incidents", "services", "users"}, []string{}, []string{"incidents", "services", "users"}},
		{"jira", nil, []string{}, []string{}},
	} {
		var input []PlanDataset
		for _, key := range tc.datasets {
			input = append(input, PlanDataset{Key: key, InitialDepthDays: &depth})
		}
		kept, skipped := splitCanonicalIncidentDatasets(tc.provider, input)
		if got := datasetKeys(kept); !slices.Equal(got, tc.wantKept) {
			t.Errorf("%s %v: kept = %v, want %v", tc.provider, tc.datasets, got, tc.wantKept)
		}
		if got := datasetKeys(skipped); !slices.Equal(got, tc.wantSkipped) {
			t.Errorf("%s %v: skipped = %v, want %v", tc.provider, tc.datasets, got, tc.wantSkipped)
		}
		for _, dataset := range append(kept, skipped...) {
			if dataset.InitialDepthDays != &depth {
				t.Errorf("%s %v: dataset %s lost its options", tc.provider, tc.datasets, dataset.Key)
			}
		}
	}
}

// A plan that left datasets out and holds no unit is not a run; every other
// combination is.
func TestErrIfOnlySkippedDatasetsHadWork(t *testing.T) {
	for _, tc := range []struct {
		skipped, units int
		ineligible     bool
	}{
		{skipped: 1, units: 0, ineligible: true},
		{skipped: 3, units: 0, ineligible: true},
		{skipped: 1, units: 1},
		{skipped: 0, units: 0},
		{skipped: 0, units: 4},
	} {
		err := errIfOnlySkippedDatasetsHadWork(tc.skipped, tc.units)
		if tc.ineligible != errors.Is(err, ErrOccurrenceIneligible) || (!tc.ineligible && err != nil) {
			t.Errorf("skipped=%d units=%d: err = %v, want ineligible=%v", tc.skipped, tc.units, err, tc.ineligible)
		}
	}
}
