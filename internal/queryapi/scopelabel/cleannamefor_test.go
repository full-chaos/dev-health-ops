package scopelabel

import "testing"

func TestCleanNameForDropsANameEqualToItsOwnID(t *testing.T) {
	cases := []struct {
		raw, id, want string
		ok            bool
	}{
		{"jira:OPS-3", "jira:OPS-3", "", false},
		{"  JIRA:ops-3 ", "jira:OPS-3", "", false},
		{"jira:OPS-3", " jira:OPS-3 ", "", false},
		{"Fix jira:OPS-3 crash", "jira:OPS-3", "Fix jira:OPS-3 crash", true},
		{"Fix login redirect", "jira:OPS-3", "Fix login redirect", true},
		{"jira:OPS-3", "jira:OPS-4", "jira:OPS-3", true},
		{"", "jira:OPS-3", "", false},
		{"0b8f6a52-3c1d-4e7a-9d2e-5f4a1c7b8e90", "x", "", false},
	}
	for _, c := range cases {
		got, ok := CleanNameFor(c.raw, c.id)
		if got != c.want || ok != c.ok {
			t.Fatalf("CleanNameFor(%q,%q) = %q,%v want %q,%v", c.raw, c.id, got, ok, c.want, c.ok)
		}
	}
}
