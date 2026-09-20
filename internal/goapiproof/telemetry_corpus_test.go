package goapiproof

import (
	"reflect"
	"testing"
)

func TestExperimentsRepoValidVariantBindsTheSuppliedRepository(t *testing.T) {
	spec, err := SpecFor("experiments")
	if err != nil {
		t.Fatal(err)
	}
	var found *Variant
	for i := range spec.Variants {
		if spec.Variants[i].Name == "REPO_VALID" {
			found = &spec.Variants[i]
		}
	}
	if found == nil || found.Instance == nil {
		t.Fatal("experiments.REPO_VALID must be an instance variant")
	}
	vars := found.Variables("org-1", DefaultWindow())
	found.Instance.Bind(vars, "repo-x")
	scope := vars["filters"].(map[string]any)["scope"].(map[string]any)
	if scope["level"] != "REPO" || !reflect.DeepEqual(scope["ids"], []string{"repo-x"}) {
		t.Fatalf("scope = %v", scope)
	}
	if !reflect.DeepEqual(found.Parity.RequireNonEmpty, []string{"data.experiments.items"}) {
		t.Fatalf("RequireNonEmpty = %v", found.Parity.RequireNonEmpty)
	}
}

func TestProductTelemetryBaseRequiresANonEmptyAnswerAndTheEmptyRangeDoesNot(t *testing.T) {
	spec, err := SpecFor("productTelemetryDashboard")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spec.Parity.RequireNonEmpty, []string{"data.productTelemetryDashboard.dailyActiveUsers"}) {
		t.Fatalf("base RequireNonEmpty = %v", spec.Parity.RequireNonEmpty)
	}
	for _, v := range spec.Variants {
		if v.Name == "EMPTY_RANGE" && len(v.Parity.RequireNonEmpty) != 0 {
			t.Fatalf("the empty range is a deliberate empty-vs-empty case, got %v", v.Parity.RequireNonEmpty)
		}
	}
}
