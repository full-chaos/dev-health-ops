package goapiproof

import (
	"reflect"
	"sort"
	"testing"
)

// Every measure the web pages send to one of the four documents has its own
// single-request variant, and each variant's declared echo names exactly the
// measure and dimension its variables ask for.
func TestAnalyticsBatchDocuments_CoverEveryWebMeasureAndEchoWhatTheyAsk(t *testing.T) {
	want := map[string][]string{
		"testOpsPipeline":       {"BREAKDOWN_TEAM_PIPELINE_FAILURE_RATE", "TEAM_PIPELINE_DURATION_P95", "TEAM_PIPELINE_FAILURE_RATE", "TEAM_PIPELINE_QUEUE_TIME", "TEAM_PIPELINE_RERUN_RATE", "TEAM_PIPELINE_SUCCESS_RATE"},
		"testOpsTest":           {"BREAKDOWN_TEAM_TEST_FLAKE_RATE", "TEAM_TEST_FAILURE_RATE", "TEAM_TEST_FLAKE_RATE", "TEAM_TEST_PASS_RATE", "TEAM_TEST_SUITE_DURATION_P95"},
		"testOpsCoverage":       {"BREAKDOWN_REPO_COVERAGE_LINE_PCT", "TEAM_COVERAGE_BRANCH_PCT", "TEAM_COVERAGE_DELTA_PCT", "TEAM_COVERAGE_LINE_PCT"},
		"featureFlagTimeseries": {"REPO_FLAG_ACTIVATION_RATE", "REPO_FLAG_COVERAGE_RATIO", "REPO_FLAG_ERROR_RATE_DELTA", "REPO_FLAG_FRICTION_DELTA"},
	}
	for op, names := range want {
		spec, err := SpecFor(op)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, v := range spec.Variants {
			got = append(got, v.Name)
			vars := v.Variables("org", DefaultWindow())
			batch := vars["batch"].(map[string]any)
			list, listName := batch["timeseries"].([]any), "timeseries"
			if len(list) == 0 {
				list, listName = batch["breakdowns"].([]any), "breakdowns"
			}
			if len(list) != 1 {
				t.Fatalf("%s/%s asks for %d entries", op, v.Name, len(list))
			}
			entry := list[0].(map[string]any)
			if listName == "timeseries" && entry["interval"] != "DAY" {
				t.Errorf("%s/%s interval %v: the web pages ask for daily buckets", op, v.Name, entry["interval"])
			}
			if listName == "breakdowns" && entry["topN"] != 10 {
				t.Errorf("%s/%s topN %v", op, v.Name, entry["topN"])
			}
			var echoed []string
			for _, e := range v.Parity.ScopeEcho {
				if e.List != "data.analytics."+listName {
					t.Errorf("%s/%s echo list %s", op, v.Name, e.List)
				}
				echoed = append(echoed, e.Fields[0]+"="+e.Value)
			}
			sort.Strings(echoed)
			wantEcho := []string{"dimension=" + entry["dimension"].(string), "measure=" + entry["measure"].(string)}
			if !reflect.DeepEqual(echoed, wantEcho) {
				t.Errorf("%s/%s echo %v want %v", op, v.Name, echoed, wantEcho)
			}
			if !reflect.DeepEqual(v.Parity.RequireNonEmpty, []string{"data.analytics." + listName}) {
				t.Errorf("%s/%s must require a non-empty %s: %v", op, v.Name, listName, v.Parity.RequireNonEmpty)
			}
		}
		sort.Strings(got)
		if !reflect.DeepEqual(got, names) {
			t.Errorf("%s variants %v want %v", op, got, names)
		}
		base := spec.Variables("org", DefaultWindow())["batch"].(map[string]any)
		if len(base["timeseries"].([]any)) != map[string]int{"testOpsPipeline": 5, "testOpsTest": 4, "testOpsCoverage": 3, "featureFlagTimeseries": 4}[op] {
			t.Errorf("%s base request must carry the whole page batch", op)
		}
		if len(spec.Parity.RequireNonEmpty) == 0 {
			t.Errorf("%s base must require a non-empty answer", op)
		}
		for _, e := range spec.Parity.ScopeEcho {
			if e.List != "data.analytics.breakdowns" {
				t.Errorf("%s base carries several series, so only its single breakdown can echo: %s", op, e.List)
			}
		}
	}
}
