package computeparity

import (
	"math"
	"strings"
	"testing"
	"time"
)

// doraRow is a production-shaped row type. It stands in for the real
// dora_metrics_daily row: what matters for these tests is that it is a
// CONCRETE STRUCT, so TypedEncode reflects every field exhaustively and the
// SELECT list is derived from it rather than written beside it.
type doraRow struct {
	OrgID      string    `json:"org_id"`
	RepoID     string    `json:"repo_id"`
	Day        oracleDay `json:"day"`
	MetricName string    `json:"metric_name"`
	Value      float64   `json:"value"`
	ComputedAt time.Time `json:"computed_at"`
}

type oracleDay string

func (d oracleDay) OracleDate() string { return string(d) }

func doraTable() Table {
	return Table{
		Name:        "dora_metrics_daily",
		OrderBy:     "org_id, repo_id, day, metric_name",
		SemanticKey: []string{"org_id", "repo_id", "day", "metric_name"},
		Exclusions: map[string]string{
			"computed_at": "stamped from the wall clock once per job run; carries no " +
				"product meaning and differs on every execution",
		},
		Repeat: AppendDuplicates,
	}
}

func baseRows() []doraRow {
	stamp := time.Date(2026, 8, 22, 1, 2, 3, 0, time.UTC)
	return []doraRow{
		{"org", "repo", "2026-08-20", "deployment_frequency", 3, stamp},
		{"org", "repo", "2026-08-20", "lead_time_for_changes", 42.5, stamp},
		{"org", "repo", "2026-08-21", "change_failure_rate", 0.25, stamp},
	}
}

func snapshots(t *testing.T, left, right []doraRow) (Snapshot, Snapshot) {
	t.Helper()
	table := doraTable()
	return Encode(t, table, "python", left), Encode(t, table, "go", right)
}

func TestQueryIsDerivedFromTheRowType(t *testing.T) {
	// The point of deriving it: adding a field to the row type adds it to the
	// query AND to the diff in one edit, so a column cannot be dropped from
	// the comparison by being forgotten in a hand-written list.
	got := Query[doraRow](doraTable())
	want := "SELECT org_id, repo_id, day, metric_name, value, computed_at " +
		"FROM dora_metrics_daily ORDER BY org_id, repo_id, day, metric_name"
	if got != want {
		t.Fatalf("derived query:\n got %s\nwant %s", got, want)
	}
}

func TestIdenticalTablesCompareEqual(t *testing.T) {
	left, right := snapshots(t, baseRows(), baseRows())
	if messages := Compare(t, doraTable(), left, right); len(messages) != 0 {
		t.Fatalf("identical tables reported divergences: %v", messages)
	}
}

func TestAVolatileColumnDoesNotCreateADivergence(t *testing.T) {
	drifted := baseRows()
	for index := range drifted {
		drifted[index].ComputedAt = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	left, right := snapshots(t, baseRows(), drifted)
	if messages := Compare(t, doraTable(), left, right); len(messages) != 0 {
		t.Fatalf("a declared exclusion still diverged: %v", messages)
	}
}

// --------------------------------------------------------------------------
// The three negative controls this slice is accepted on.
// --------------------------------------------------------------------------

func TestNegativeControlMutatedRow(t *testing.T) {
	mutated := baseRows()
	mutated[1].Value = 41.5
	left, right := snapshots(t, baseRows(), mutated)

	messages := Compare(t, doraTable(), left, right)
	if len(messages) != 1 {
		t.Fatalf("want exactly one divergence, got %d: %v", len(messages), messages)
	}
	for _, fragment := range []string{
		"lead_time_for_changes", `field "value"`, "42.5", "41.5",
	} {
		if !strings.Contains(messages[0], fragment) {
			t.Errorf("divergence must name %q; got: %s", fragment, messages[0])
		}
	}
	// A mutated row is NOT a count or key-set difference, and reporting it as
	// one would send a lane looking in the wrong place.
	if strings.Contains(messages[0], "row count") {
		t.Errorf("a mutated row must not report as a count difference: %s", messages[0])
	}
}

func TestNegativeControlDroppedRow(t *testing.T) {
	dropped := baseRows()[:2]
	left, right := snapshots(t, baseRows(), dropped)

	messages := Compare(t, doraTable(), left, right)
	var sawCount, sawMissing bool
	for _, message := range messages {
		if strings.Contains(message, "row count") &&
			strings.Contains(message, "python=3") && strings.Contains(message, "go=2") {
			sawCount = true
		}
		if strings.Contains(message, "present in python but absent from go") &&
			strings.Contains(message, "change_failure_rate") {
			sawMissing = true
		}
	}
	if !sawCount {
		t.Errorf("a dropped row must report the count difference: %v", messages)
	}
	if !sawMissing {
		t.Errorf("a dropped row must be named by its semantic key: %v", messages)
	}
}

func TestNegativeControlFloatNudgedByOneULP(t *testing.T) {
	// The smallest difference float64 can represent. Every level except the
	// value comparison reports these tables as identical.
	nudged := baseRows()
	nudged[2].Value = math.Nextafter(0.25, math.Inf(1))
	if nudged[2].Value == 0.25 {
		t.Fatal("the control did not actually change the value")
	}
	left, right := snapshots(t, baseRows(), nudged)

	messages := Compare(t, doraTable(), left, right)
	if len(messages) != 1 {
		t.Fatalf("want exactly one divergence, got %d: %v", len(messages), messages)
	}
	if !strings.Contains(messages[0], "change_failure_rate") ||
		!strings.Contains(messages[0], `field "value"`) {
		t.Errorf("a one-ULP nudge must be attributed to the field: %s", messages[0])
	}
}

func TestNegativeControlExtraRow(t *testing.T) {
	extra := append(baseRows(), doraRow{
		"org", "repo", "2026-08-21", "time_to_restore_service", 3600,
		time.Date(2026, 8, 22, 1, 2, 3, 0, time.UTC),
	})
	left, right := snapshots(t, baseRows(), extra)

	var sawExtra bool
	for _, message := range Compare(t, doraTable(), left, right) {
		if strings.Contains(message, "present in go but absent from python") &&
			strings.Contains(message, "time_to_restore_service") {
			sawExtra = true
		}
	}
	if !sawExtra {
		t.Error("an extra row must be reported against the side that has it")
	}
}

func TestNegativeControlDuplicateKey(t *testing.T) {
	duplicated := append(baseRows(), baseRows()[0])
	left, right := snapshots(t, baseRows(), duplicated)

	var sawMultiplicity bool
	for _, message := range Compare(t, doraTable(), left, right) {
		if strings.Contains(message, "rows for this key") {
			sawMultiplicity = true
		}
	}
	if !sawMultiplicity {
		t.Error("a key written twice on one side must be reported as a multiplicity difference")
	}
}

// --------------------------------------------------------------------------
// Absence of evidence
// --------------------------------------------------------------------------

func TestTwoEmptyTablesAreNotAMatch(t *testing.T) {
	left, right := snapshots(t, nil, nil)
	messages := Compare(t, doraTable(), left, right)
	if len(messages) != 1 || !strings.Contains(messages[0], "empty on BOTH sides") {
		t.Fatalf("two empty tables must not read as parity: %v", messages)
	}
}

func TestTwoEmptyTablesMayBeAllowedExplicitly(t *testing.T) {
	table := doraTable()
	table.AllowEmpty = true
	left, right := snapshots(t, nil, nil)
	if messages := Compare(t, table, left, right); len(messages) != 0 {
		t.Fatalf("AllowEmpty must permit an empty pair: %v", messages)
	}
}

// --------------------------------------------------------------------------
// Exclusions must stay honest
// --------------------------------------------------------------------------

func TestAStaleExclusionIsReported(t *testing.T) {
	table := doraTable()
	table.Exclusions["no_such_column"] = "excused a column that does not exist"
	left, right := snapshots(t, baseRows(), baseRows())

	var sawStale bool
	for _, message := range Compare(t, table, left, right) {
		if strings.Contains(message, "no_such_column") &&
			strings.Contains(message, "stale exclusion") {
			sawStale = true
		}
	}
	if !sawStale {
		t.Error("an exclusion that never matches anything must be reported as stale")
	}
}

// --------------------------------------------------------------------------
// Semantic key integrity
// --------------------------------------------------------------------------

func TestADelimiterInsideAKeyValueCannotForgeAKey(t *testing.T) {
	table := doraTable()
	stamp := time.Date(2026, 8, 22, 1, 2, 3, 0, time.UTC)
	forged := Encode(t, table, "go", []doraRow{
		{"org", "repo", "2026-08-20", "a\x1fb", 1, stamp},
	})
	plain := Encode(t, table, "python", []doraRow{
		{"org", "repo", "2026-08-20", "a", 1, stamp},
	})
	if KeyOf(t, table, forged.Rows[0]) == KeyOf(t, table, plain.Rows[0]) {
		t.Fatal("a delimiter inside a key value must not collide with another key")
	}
}

// --------------------------------------------------------------------------
// Replay
// --------------------------------------------------------------------------

func TestRepeatPolicyObservations(t *testing.T) {
	stamp := time.Date(2026, 8, 22, 1, 2, 3, 0, time.UTC)
	tests := []struct {
		name     string
		declared RepeatPolicy
		after    []doraRow
		wantOK   bool
	}{
		{
			name:     "append_duplicates accepts growth with a stable key set",
			declared: AppendDuplicates,
			after:    append(baseRows(), baseRows()...),
			wantOK:   true,
		},
		{
			name:     "idempotent rejects growth",
			declared: Idempotent,
			after:    append(baseRows(), baseRows()...),
			wantOK:   false,
		},
		{
			name:     "idempotent accepts an unchanged table",
			declared: Idempotent,
			after:    baseRows(),
			wantOK:   true,
		},
		{
			name:     "append_duplicates rejects an unchanged table",
			declared: AppendDuplicates,
			after:    baseRows(),
			wantOK:   false,
		},
		{
			name:     "replace_window accepts same count, moved values",
			declared: ReplaceWindow,
			after: []doraRow{
				{"org", "repo", "2026-08-20", "deployment_frequency", 9, stamp},
				{"org", "repo", "2026-08-20", "lead_time_for_changes", 42.5, stamp},
				{"org", "repo", "2026-08-21", "change_failure_rate", 0.25, stamp},
			},
			wantOK: true,
		},
		{
			name:     "a changed key set is never any declared policy",
			declared: AppendDuplicates,
			after: []doraRow{
				{"org", "repo", "2026-08-20", "something_else", 3, stamp},
				{"org", "repo", "2026-08-20", "lead_time_for_changes", 42.5, stamp},
				{"org", "repo", "2026-08-21", "change_failure_rate", 0.25, stamp},
			},
			wantOK: false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			table := doraTable()
			table.Repeat = test.declared
			before := Encode(t, table, "python", baseRows())
			after := Encode(t, table, "python", test.after)
			messages := EvaluateRepeat(table, before, after)
			if test.wantOK && len(messages) != 0 {
				t.Fatalf("policy %q should hold: %v", test.declared, messages)
			}
			if !test.wantOK && len(messages) == 0 {
				t.Fatalf("policy %q should have been violated", test.declared)
			}
		})
	}
}

func TestRepeatIsEvaluatedPerSideAndNamesTheSide(t *testing.T) {
	table := doraTable()
	table.Repeat = Idempotent
	before := Encode(t, table, "go", baseRows())
	after := Encode(t, table, "go", append(baseRows(), baseRows()...))
	messages := EvaluateRepeat(table, before, after)
	if len(messages) != 1 || !strings.Contains(messages[0], "side go") {
		t.Fatalf("a replay violation must name the side it belongs to: %v", messages)
	}
}

func TestPortProofRequiresTwoDifferentImplementations(t *testing.T) {
	// The failure this guards is the worst kind for a release gate: a port
	// test that runs the reference producer on both sides stays GREEN while
	// the port is broken, missing, or wired to the wrong entry point.
	tests := []struct {
		name          string
		left, right   Producer
		wantViolation bool
	}{
		{
			name:          "two of the same implementation is a self-test, not a port proof",
			left:          Producer{Side: "python", Implementation: "python"},
			right:         Producer{Side: "python_replica", Implementation: "python"},
			wantViolation: true,
		},
		{
			name:          "an unnamed implementation proves nothing",
			left:          Producer{Side: "python", Implementation: "python"},
			right:         Producer{Side: "go", Implementation: ""},
			wantViolation: true,
		},
		{
			name:          "python against go is a real port proof",
			left:          Producer{Side: "python", Implementation: "python"},
			right:         Producer{Side: "go", Implementation: "go"},
			wantViolation: false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			violation := PortProofViolation(test.left, test.right)
			if test.wantViolation && violation == "" {
				t.Fatal("this pair must not qualify as a port proof")
			}
			if !test.wantViolation && violation != "" {
				t.Fatalf("a real port proof must be accepted, got: %s", violation)
			}
		})
	}
}
