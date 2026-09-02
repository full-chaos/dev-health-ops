package units

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type timeBoundsCase struct {
	Label     string                       `json:"label"`
	Nodes     [][]string                   `json:"nodes"`
	WorkItems map[string]map[string]string `json:"work_items"`
	PRs       map[string]map[string]string `json:"prs"`
	Commits   map[string]map[string]string `json:"commits"`
	Start     *string                      `json:"start"`
	End       *string                      `json:"end"`
	IsNone    bool                         `json:"is_none"`
}

func loadTimeBoundsCases(t *testing.T) []timeBoundsCase {
	t.Helper()
	path := filepath.Join(
		repositoryRootPath(t), "tests", "fixtures", "time_bounds_python_golden.json",
	)
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read time-bounds golden: %v (regenerate with: uv run python "+
			"tests/fixtures/generate_time_bounds_golden.py)", err)
	}
	var golden struct {
		Cases []timeBoundsCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse time-bounds golden: %v", err)
	}
	if len(golden.Cases) == 0 {
		t.Fatal("golden contains no cases")
	}
	return golden.Cases
}

// parseOffsetTime keeps the ORIGINAL zone offset rather than normalising on
// read. The mixed-zone cases only discriminate if the value reaches the port
// still carrying its offset -- parsing straight to UTC here would do the port's
// job for it and the test would pass against a wall-clock implementation.
func parseOffsetTime(t *testing.T, value *string) (time.Time, bool) {
	t.Helper()
	if value == nil {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, *value)
	if err != nil {
		t.Fatalf("parse %q: %v", *value, err)
	}
	return parsed, true
}

func fieldTime(t *testing.T, fields map[string]string, name string) (time.Time, bool) {
	t.Helper()
	raw, present := fields[name]
	if !present || raw == "" {
		return time.Time{}, false
	}
	value := raw
	return parseOffsetTime(t, &value)
}

// TestComputeTimeBoundsMatchesLivePython drives the port against outputs
// captured from compute_time_bounds itself.
func TestComputeTimeBoundsMatchesLivePython(t *testing.T) {
	var sawMixedZone, sawNone bool

	for _, testCase := range loadTimeBoundsCases(t) {
		t.Run(testCase.Label, func(t *testing.T) {
			nodes := make([]NodeKey, 0, len(testCase.Nodes))
			for _, pair := range testCase.Nodes {
				if len(pair) != 2 {
					t.Fatalf("malformed node %v", pair)
				}
				nodes = append(nodes, NodeKey{Type: pair[0], ID: pair[1]})
			}

			times := make(map[NodeKey]NodeTimes)
			for id, fields := range testCase.WorkItems {
				entry := NodeTimes{Present: true}
				entry.CreatedAt, _ = fieldTime(t, fields, "created_at")
				entry.UpdatedAt, _ = fieldTime(t, fields, "updated_at")
				if value, ok := fieldTime(t, fields, "completed_at"); ok {
					entry.CompletedAt = &value
				}
				times[NodeKey{Type: "issue", ID: id}] = entry
			}
			for id, fields := range testCase.PRs {
				entry := NodeTimes{Present: true}
				entry.CreatedAt, _ = fieldTime(t, fields, "created_at")
				if value, ok := fieldTime(t, fields, "merged_at"); ok {
					entry.MergedAt = &value
				}
				if value, ok := fieldTime(t, fields, "closed_at"); ok {
					entry.ClosedAt = &value
				}
				times[NodeKey{Type: "pr", ID: id}] = entry
			}
			for id, fields := range testCase.Commits {
				entry := NodeTimes{Present: true}
				entry.AuthorWhen, _ = fieldTime(t, fields, "author_when")
				times[NodeKey{Type: "commit", ID: id}] = entry
			}

			bounds, ok := ComputeTimeBounds(nodes, times)

			if testCase.IsNone {
				sawNone = true
				if ok {
					t.Errorf("got bounds %v..%v, python returned None",
						bounds.Start, bounds.End)
				}
				return
			}
			if !ok {
				t.Fatalf("got None, python returned %v..%v",
					derefOr(testCase.Start), derefOr(testCase.End))
			}

			wantStart, _ := parseOffsetTime(t, testCase.Start)
			wantEnd, _ := parseOffsetTime(t, testCase.End)

			// Compared as INSTANTS, which is the whole point: Equal() is
			// zone-independent, so this asserts the same moment rather than the
			// same rendering.
			if !bounds.Start.Equal(wantStart) {
				t.Errorf("start = %v (epoch_ms %d), python = %v (epoch_ms %d)",
					bounds.Start, bounds.Start.UnixMilli(),
					wantStart, wantStart.UnixMilli())
			}
			if !bounds.End.Equal(wantEnd) {
				t.Errorf("end = %v (epoch_ms %d), python = %v (epoch_ms %d)",
					bounds.End, bounds.End.UnixMilli(), wantEnd, wantEnd.UnixMilli())
			}
			// And the returned bounds must be on UTC, since _node_time_bounds
			// pushes every value through _ensure_utc.
			if bounds.Start.Location() != time.UTC || bounds.End.Location() != time.UTC {
				t.Errorf("bounds must be on UTC, got start=%v end=%v",
					bounds.Start.Location(), bounds.End.Location())
			}
		})

		if len(testCase.Label) > 11 && testCase.Label[:11] == "mixed_zones" {
			sawMixedZone = true
		}
	}

	// Guard the corpus. Without a mixed-zone case the whole suite passes
	// against a port that compares WALL CLOCKS -- which is exactly the defect
	// PR2 shipped, and exactly what an all-UTC corpus cannot see.
	if !sawMixedZone {
		t.Error("no mixed-zone case present; min/max would be untested for " +
			"instant-versus-wall-clock comparison")
	}
	if !sawNone {
		t.Error("no case returns None; the empty-bounds path is untested")
	}
}

func derefOr(value *string) string {
	if value == nil {
		return "<nil>"
	}
	return *value
}

// TestUnknownNodeTypeYieldsNothingEvenWhenTimesArePresent covers a case the
// Python golden CANNOT produce, which is why it is asserted directly.
//
// compute_time_bounds selects the map by node type, so an unrecognised type
// always reaches _node_time_bounds with an empty dict and its "no data" and
// "unknown type" paths coincide. Go's shape is different: `times` is one map
// keyed by NodeKey, so a caller can supply a populated NodeTimes for a type the
// switch does not handle.
//
// Found by mutation: making the default branch return the zero time as a real
// bound passed the whole golden, because every unknown-type node in the corpus
// was also absent from the maps and the Present check short-circuited first.
// The two conditions were never separated.
//
// A zero time.Time is year 1, so treating it as a real bound would set a work
// unit's start to 0001-01-01 rather than omitting the bounds.
func TestUnknownNodeTypeYieldsNothingEvenWhenTimesArePresent(t *testing.T) {
	moment := time.Date(2026, 9, 2, 10, 30, 0, 0, time.UTC)
	populated := NodeTimes{
		Present:    true,
		CreatedAt:  moment,
		UpdatedAt:  moment,
		AuthorWhen: moment,
	}

	for _, nodeType := range []string{"release", "deployment", "", "Issue", "ISSUE", "issues"} {
		t.Run(nodeType, func(t *testing.T) {
			node := NodeKey{Type: nodeType, ID: "x"}
			_, ok := ComputeTimeBounds(
				[]NodeKey{node},
				map[NodeKey]NodeTimes{node: populated},
			)
			if ok {
				t.Errorf("node type %q produced bounds; only issue/pr/commit are "+
					"recognised, and Python returns None for anything else",
					nodeType)
			}
		})
	}

	// The type match is case-SENSITIVE, as Python's string equality is: "Issue"
	// is not "issue". Covered by the loop above; called out because a
	// case-insensitive "improvement" would silently start producing bounds.
	known := NodeKey{Type: "issue", ID: "x"}
	if _, ok := ComputeTimeBounds([]NodeKey{known}, map[NodeKey]NodeTimes{known: populated}); !ok {
		t.Error("the lowercase type must still be recognised")
	}
}
