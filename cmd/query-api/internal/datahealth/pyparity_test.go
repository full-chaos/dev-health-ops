package datahealth

import "testing"

func TestPyReprMatchesPython(t *testing.T) {
	for _, tc := range pyReprCases {
		value, ok := decodeOrdered(tc.json)
		if !ok {
			t.Fatalf("decode %s", tc.json)
		}
		if got := pyRepr(value); got != tc.want {
			t.Errorf("repr(%s) = %q, want %q", tc.json, got, tc.want)
		}
	}
}

func TestPyStrLeavesStringsAlone(t *testing.T) {
	if got := pyStr("plain 'text'"); got != "plain 'text'" {
		t.Fatalf("pyStr(string) = %q", got)
	}
}

func TestPyLowerMatchesPython(t *testing.T) {
	for _, tc := range pyLowerCases {
		if got := pyLower(tc.in); got != tc.want {
			t.Errorf("lower(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestNormMatchesPython(t *testing.T) {
	for _, tc := range pyNormCases {
		if got := norm(tc.in); got != tc.want {
			t.Errorf("norm(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestPyIntStringMatchesPython(t *testing.T) {
	for _, tc := range pyIntCases {
		if got := pyIntString(tc.in); got != tc.want {
			t.Errorf("int(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestUnicodeIdentityMappedThroughReader(t *testing.T) {
	// "İ" lowercases to "i" plus a combining dot on both sides of the match.
	ch := &routingCH{rules: []chRule{
		{match: "FROM git_commits", rows: [][]any{{"git", "İvan@example.com", "İvan", uint64(1)}}},
		{match: "FROM identities FINAL", rows: [][]any{{"ivan-c", "ivan@example.com", "", `{}`, []string{}}}},
	}}
	got := (&Reader{ClickHouse: ch}).IdentityMapping(t.Context(), "org-1", "")
	if got.UnmappedCount != 1 {
		t.Fatalf("Python's lower() keeps the dotted form, so this identity stays unmapped: %+v", got)
	}
	ch = &routingCH{rules: []chRule{
		{match: "FROM git_commits", rows: [][]any{{"git", "İvan@example.com", "İvan", uint64(1)}}},
		{match: "FROM identities FINAL", rows: [][]any{{"ivan-c", "İVAN@EXAMPLE.COM", "", `{}`, []string{}}}},
	}}
	if got := (&Reader{ClickHouse: ch}).IdentityMapping(t.Context(), "org-1", ""); got.UnmappedCount != 0 {
		t.Fatalf("the same dotted capital on both sides maps: %+v", got)
	}
}

func TestStageAndScopeUsePythonStr(t *testing.T) {
	status := int32(3)
	r := &Reader{}
	got := r.connectorFailure(connectorRow{lastSyncError: sp("x"), hasRun: true, runStatus: &status, runResult: []byte(`{"stage":{"repo":"acme"}}`)})
	if got == nil || got.Stage == nil || *got.Stage != "{'repo': 'acme'}" {
		t.Fatalf("stage = %+v", got)
	}
	if scope := connectorScope(connectorRow{name: "n", syncTargets: []byte(`[{"a":1},["b"]]`)}); scope != "{'a': 1}, ['b']" {
		t.Fatalf("scope = %q", scope)
	}
	stats := jsonMapping([]byte(`{"rows_ingested":"１２"}`))
	if rowsIngested(stats) != 12 {
		t.Fatalf("rows = %d", rowsIngested(stats))
	}
}

func TestTrailingJSONContentIsRejectedLikePython(t *testing.T) {
	ch := &routingCH{rules: []chRule{
		{match: "FROM git_commits", rows: [][]any{{"git", "alice@example.com", "Alice", uint64(1)}}},
		{match: "FROM identities FINAL", rows: [][]any{{"alice-c", "", "", `{"git":["alice@example.com"]} trailing`, []string{}}}},
	}}
	if got := (&Reader{ClickHouse: ch}).IdentityMapping(t.Context(), "org-1", ""); got.UnmappedCount != 1 {
		t.Fatalf("malformed provider identities decode to none, so the identity stays unmapped: %+v", got)
	}
	if stats := jsonMapping([]byte(`{"rows":5} x`)); stats != nil {
		t.Fatalf("trailing content rejects the stats mapping: %v", stats)
	}
	if _, ok := decodeOrdered(`[1] [2]`); ok {
		t.Fatal("two values are not one JSON document")
	}
	if _, ok := decodeOrdered(` {"a": 1} `); !ok {
		t.Fatal("surrounding whitespace is fine")
	}
}
