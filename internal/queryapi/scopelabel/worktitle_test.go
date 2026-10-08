package scopelabel

import "testing"

func TestCleanTitle(t *testing.T) {
	const id = "0b8f6a52-3c1d-4e7a-9d2e-5f4a1c7b8e90"
	for _, tc := range []struct {
		raw  string
		want *string
	}{
		{"Fix login redirect", ptr("Fix login redirect")},
		{"  Fix login redirect \n", ptr("Fix login redirect")},
		{"", nil},
		{"   \t", nil},
		{id, nil},
		{"  " + id + " ", nil},
		{"Fix " + id, ptr("Fix " + id)},
	} {
		got := CleanTitle(tc.raw)
		if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
			t.Errorf("CleanTitle(%q) = %v, want %v", tc.raw, deref(got), deref(tc.want))
		}
	}
}

func ptr(s string) *string { return &s }

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
