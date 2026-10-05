package goapiproof

import (
	"testing"
)

// CHAOS-8733: the proof of capacityCompletionDistribution sends the shape its document reads, and compares what is comparable.

func completionDistributionBody(daysCounts ...int) map[string]any {
	bins := make([]any, 0, len(daysCounts))
	for i, count := range daysCounts {
		bins = append(bins, map[string]any{"value": i + 3, "count": count, "__typename": "CapacityDistributionBin"})
	}
	return map[string]any{"capacityForecast": map[string]any{
		"__typename": "CapacityForecast",
		"completionDistribution": map[string]any{
			"__typename": "CapacityDistribution",
			"days":       bins,
			"items":      nil,
		},
	}}
}

func TestCapacityCompletionDistributionProofSendsTheDocumentsInputShape(t *testing.T) {
	spec, err := SpecFor("capacityCompletionDistribution")
	if err != nil {
		t.Fatal(err)
	}
	variables := spec.Variables("org-proof", Window{})
	if _, topLevel := variables["teamId"]; topLevel {
		t.Fatalf("variables %v carry a top-level teamId: the document declares $orgId and $input only, so it would be dropped", variables)
	}
	if _, ok := variables["input"].(map[string]any); !ok || variables["orgId"] != "org-proof" || len(variables) != 2 {
		t.Fatalf("variables %v, want exactly orgId and an input object", variables)
	}
}

// Two draws of the same request differ in their bins on both planes; that is not a mismatch. A difference outside
// the bins still is one.
func TestCapacityCompletionDistributionProofComparesWhatIsComparableAndNamesTheRest(t *testing.T) {
	spec, err := SpecFor("capacityCompletionDistribution")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := func(data any) Snapshot { return Snapshot{Data: data, DataPresent: true} }
	t.Run("different draws are not a mismatch", func(t *testing.T) {
		result := Compare(snapshot(completionDistributionBody(5000, 5000)), snapshot(completionDistributionBody(5001, 4999)), spec.Parity)
		if result.DifferencesOutsideBaselineDefect != 0 {
			t.Fatalf("two draws differing only in bin counts are a mismatch: state %s, outside %d, %v", result.TerminalState, result.DifferencesOutsideBaselineDefect, result.OutsideByShape)
		}
	})
	t.Run("a difference outside the bins is still a mismatch", func(t *testing.T) {
		other := completionDistributionBody(5000, 5000)
		other["capacityForecast"].(map[string]any)["completionDistribution"].(map[string]any)["items"] = []any{}
		result := Compare(snapshot(completionDistributionBody(5000, 5000)), snapshot(other), spec.Parity)
		if result.DifferencesOutsideBaselineDefect == 0 {
			t.Fatal("a null items on one plane and a list on the other was not reported")
		}
	})
}
