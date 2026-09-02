package investment

import "testing"

// TestScopeGuardStripsEverythingPythonStrips closes codex round 2's P2 on
// CHAOS-4441 PR2.
//
// Go's unicode.IsSpace is a strict SUBSET of Python's str.isspace(). Comparing
// every code point in 0..0x10FFFF between the two predicates, the delta is
// exactly four characters, in one direction only:
//
//	0x1c FILE SEPARATOR   0x1d GROUP SEPARATOR
//	0x1e RECORD SEPARATOR 0x1f UNIT SEPARATOR
//
// strings.TrimSpace therefore left "\x1c" non-empty, and the guard ACCEPTED a
// scope Python rejects. Python raises before any fetch; Go proceeded into a
// silent zero-row run. That is a rejection-parity break even though it does not
// widen the query, because the two planes disagree about which runs are legal.
func TestScopeGuardStripsEverythingPythonStrips(t *testing.T) {
	for _, testCase := range []struct {
		name string
		org  string
	}{
		// The four the delta is made of -- these are what regressed.
		{name: "file separator", org: "\x1c"},
		{name: "group separator", org: "\x1d"},
		{name: "record separator", org: "\x1e"},
		{name: "unit separator", org: "\x1f"},
		{name: "separators mixed with ordinary whitespace", org: " \x1c\t\x1f\n"},

		// Already covered by unicode.IsSpace before this fix. Kept so that a
		// NARROWING of the predicate is caught too -- a hand-rolled replacement
		// that only listed the four separators would pass the cases above and
		// silently start accepting these.
		{name: "next line", org: ""},
		{name: "non breaking space", org: " "},
		{name: "ogham space mark", org: " "},
		{name: "line separator", org: " "},
		{name: "narrow no break space", org: " "},
		{name: "ideographic space", org: "　"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := RequireOrganizationScope(testCase.org); err == nil {
				t.Errorf(
					"org %q was accepted; Python's .strip() reduces it to empty and "+
						"refuses the run, so accepting it here is a rejection-parity break",
					testCase.org,
				)
			}
		})
	}

	// Stripping must not eat a real scope that merely sits next to a separator.
	if err := RequireOrganizationScope("\x1c70d529e0\x1f"); err != nil {
		t.Errorf("a real scope adjacent to separators must be accepted: %v", err)
	}
}
