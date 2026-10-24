package admin

import (
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

func boolPtr(v bool) *bool { return &v }

// Ruling A, one test for each branch: the categorize purpose keeps its 400
// unless the stored opt-in is "true" or the same PUT sets it true.
func TestCategorizePurposeIsAcceptedOnlyForAnOptedInOrg(t *testing.T) {
	stored := func(value bool) func() (bool, error) { return func() (bool, error) { return value, nil } }
	for name, tc := range map[string]struct {
		body   *bool
		stored func() (bool, error)
		want   bool
	}{
		"no body field, stored off":           {nil, stored(false), false},
		"no body field, stored on":            {nil, stored(true), true},
		"body true, stored off":               {boolPtr(true), stored(false), true},
		"body true, stored on":                {boolPtr(true), stored(true), true},
		"body false, stored off":              {boolPtr(false), stored(false), false},
		"body false, stored on (opt-out PUT)": {boolPtr(false), stored(true), true},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := categorizeOptedIn(tc.body, tc.stored)
			if err != nil || got != tc.want {
				t.Fatalf("categorizeOptedIn = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
	if _, err := categorizeOptedIn(nil, func() (bool, error) { return false, errors.New("down") }); err == nil {
		t.Fatal("an unreadable opt-in must be an error, never read as off or on")
	}
	read := false
	if on, _ := categorizeOptedIn(boolPtr(true), func() (bool, error) { read = true; return false, nil }); !on || read {
		t.Fatal("a request that sets the opt-in true must not need the stored value")
	}
}

func TestCategorizeIsNotAListedPurposeOfTheAdminAPI(t *testing.T) {
	if _, valid := llmorgsettings.ParsePurpose("categorize"); valid {
		t.Fatal("categorize must not parse as an ordinary purpose")
	}
}
