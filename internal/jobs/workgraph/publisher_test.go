package workgraph

import "testing"

func TestSameJSONAcceptsPostgresJSONBFormatting(t *testing.T) {
	t.Parallel()
	left := []byte(`{"from_date": "2026-01-01", "to_date": "2026-01-14"}`)
	right := []byte(`{"from_date":"2026-01-01","to_date":"2026-01-14"}`)
	if !sameJSON(left, right) {
		t.Fatal("equivalent JSONB scope was rejected as a mutated duplicate")
	}
	if sameJSON(left, []byte(`{"from_date":"2026-01-02","to_date":"2026-01-14"}`)) {
		t.Fatal("mutated JSON scope was accepted")
	}
}

// TestSameJSONAcceptsPostgresJSONBKeyReordering pins the actual JSONB
// round-trip behavior this comparison exists for: Postgres does not
// preserve object-key insertion order (or the marshaler's whitespace) when
// a jsonb column is read back, so a caller re-marshaling the same scope on
// retry sees its own keys in a different order than what WriteTx's
// existing-row readback round-trips as. Reproduces the exact scope shape
// cmd/dev-health-worker/sync_dispatch.go's postSyncWorkGraphScope writes
// (`{"from_date":...,"to_date":...}`) and a reordered/respaced form
// equivalent to what a live work_graph_execution_requests row can read
// back as -- json.Compact alone (the prior implementation) rejected this
// as a mutated duplicate on every retry.
func TestSameJSONAcceptsPostgresJSONBKeyReordering(t *testing.T) {
	t.Parallel()
	marshaled := []byte(`{"from_date":"2026-07-10T00:00:00Z","to_date":"2026-07-23T00:00:00Z"}`)
	roundTripped := []byte(`{"to_date": "2026-07-23T00:00:00Z", "from_date": "2026-07-10T00:00:00Z"}`)
	if !sameJSON(marshaled, roundTripped) {
		t.Fatal("key-reordered, respaced JSONB round-trip was rejected as a mutated duplicate")
	}
	if sameJSON(marshaled, []byte(`{"to_date": "2026-07-24T00:00:00Z", "from_date": "2026-07-10T00:00:00Z"}`)) {
		t.Fatal("a genuinely mutated value hiding behind reordered keys was accepted")
	}
}

// TestSameJSONRejectsNestedValueMutation guards the DeepEqual replacement:
// key reordering must be accepted, but a nested array/object value change
// must still be caught.
func TestSameJSONRejectsNestedValueMutation(t *testing.T) {
	t.Parallel()
	left := []byte(`{"repo_ids":["repo-a","repo-b"],"team_ids":["team-a"]}`)
	sameOrder := []byte(`{"team_ids": ["team-a"], "repo_ids": ["repo-a", "repo-b"]}`)
	mutated := []byte(`{"team_ids": ["team-a"], "repo_ids": ["repo-a", "repo-c"]}`)
	if !sameJSON(left, sameOrder) {
		t.Fatal("reordered top-level keys with identical nested arrays were rejected")
	}
	if sameJSON(left, mutated) {
		t.Fatal("a mutated nested array element was accepted as identical")
	}
}

// TestSameJSONDistinguishesLargeIntegersAboveFloat64Precision guards the
// codex-review finding on this fix: decoding JSON numbers into a plain
// `any` converts them to float64, which loses precision above 2^53 and
// would make two different large integers compare equal. sameJSON must use
// UseNumber (or equivalent) so a genuinely mutated large-integer scope is
// still rejected.
func TestSameJSONDistinguishesLargeIntegersAboveFloat64Precision(t *testing.T) {
	t.Parallel()
	left := []byte(`{"limit":9007199254740992}`)
	right := []byte(`{"limit":9007199254740993}`)
	if sameJSON(left, right) {
		t.Fatal("distinct large integers above float64 precision were accepted as identical")
	}
	if !sameJSON(left, []byte(`{"limit": 9007199254740992}`)) {
		t.Fatal("identical large integer, differently spaced, was rejected as a mutated duplicate")
	}
}
