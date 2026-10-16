package venuechanged

import "testing"

func TestAnEmptyOrIncompleteDerivationIsRefused(t *testing.T) {
	good := map[string][]string{"internal/a": {"internal/x", "~internal/x/data"}}
	if err := validateClosures([]string{"internal/a"}, good); err != nil {
		t.Fatalf("a complete derivation was refused: %v", err)
	}
	for name, closures := range map[string]map[string][]string{
		"no test binary for the package": {},
		"no in-module package":           {"internal/a": {"~internal/x/data"}},
		"no embedded directory anywhere": {"internal/a": {"internal/x"}},
		"an empty list":                  {"internal/a": nil},
	} {
		if err := validateClosures([]string{"internal/a"}, closures); err == nil {
			t.Errorf("%s: the derivation was accepted", name)
		}
	}
}
