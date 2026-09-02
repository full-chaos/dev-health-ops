package edges

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestEndpointFieldParity closes the axis the golden cannot reach: WHICH FIELD
// carries the value.
//
// builder.py:871-874 is one expression over two fields —
//
//	if self._parse_pr_dependency_source(source_id) or self._parse_pr_dependency_source(target_id):
//
// — and `or` SHORT-CIRCUITS, so the endpoints are not interchangeable. A valid
// PR in the source means the target is never parsed, which MASKS a malformed
// target that would otherwise abort the build. Whether the row survives depends
// on which field holds the bad value, not just on what the bad value is.
//
// The frozen golden cannot test this. It holds 2828 PR-shaped sources and ZERO
// PR-shaped targets (counted, not assumed), so a source-only implementation
// reproduces it perfectly and every existing assertion stays green.
//
// The expectations are the reference's own evaluation of that expression over
// pairs, frozen by tests/fixtures/generate_pr_dependency_id_parity.py.
func TestEndpointFieldParity(t *testing.T) {
	corpus := loadEndpointParity(t)
	buildClock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	sawMasking := false

	for _, observation := range corpus {
		name := observation.Source + " | " + observation.Target
		t.Run(name, func(t *testing.T) {
			row := DependencyRow{
				SourceWorkItemID: observation.Source,
				TargetWorkItemID: observation.Target,
				RelationshipType: "blocks",
				RelationshipRaw:  "blocks",
				SemanticsVersion: blockerProjectionRuleVersion,
				LastSynced:       time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
			}
			result, err := DeriveIssueIssueEdges([]DependencyRow{row}, buildClock)
			if err != nil {
				t.Fatalf("derive: %v", err)
			}
			got := result.Outcomes[0]

			// The builder checks for an empty endpoint FIRST (builder.py:869)
			// and only then for a PR shape (:871). A row with an empty id
			// therefore never reaches the PR parse, whatever the other endpoint
			// holds — so these rows pin the ORDER of the two gates, which the
			// isolated expression in the corpus cannot express.
			if observation.Source == "" || observation.Target == "" {
				if got != OutcomeSkippedEmptyID {
					t.Fatalf("got %q; an empty endpoint must be caught by the empty-id gate "+
						"BEFORE the PR gate, or a malformed id in the other field would "+
						"decide the row's fate on a row Python never parses", got)
				}
				return
			}

			switch observation.Outcome {
			case "kept":
				if got != OutcomeEmitted {
					t.Fatalf("reference kept this row; this port returned %q", got)
				}
			case "skipped":
				if got != OutcomeSkippedPRShaped {
					t.Fatalf("reference skipped this row as PR-shaped; this port returned %q", got)
				}
				if observation.Target == malformedPRID && observation.Source != malformedPRID {
					// Python's `or` never evaluated the target. If this port
					// parsed both endpoints eagerly it would report
					// malformed_pr_id here and refuse a row Python writes off
					// as someone else's — a divergence in the direction that
					// costs a row from BOTH pipelines.
					sawMasking = true
				}
			case "raises":
				if got != OutcomeMalformedPRID {
					t.Fatalf("reference raises on this pair; this port must reject the row "+
						"as malformed_pr_id, got %q", got)
				}
			default:
				t.Fatalf("unknown recorded outcome %q", observation.Outcome)
			}
		})
	}

	if !sawMasking {
		t.Error("the corpus no longer contains the short-circuit masking case " +
			"(valid PR source hiding a malformed target); without it this test " +
			"cannot tell an eager two-endpoint parse from Python's `or`")
	}
}

const malformedPRID = "ghpr:acme/app#5²"

type endpointObservation struct {
	Source  string `json:"source"`
	Target  string `json:"target"`
	Outcome string `json:"outcome"`
}

func loadEndpointParity(t *testing.T) []endpointObservation {
	t.Helper()
	path := filepath.Join(repositoryRootPath(t), "tests", "fixtures", "pr_dependency_id_parity.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read endpoint parity corpus: %v", err)
	}
	var document struct {
		Schema   string                `json:"schema"`
		Endpoint []endpointObservation `json:"endpoint_observations"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode endpoint parity corpus: %v", err)
	}
	if document.Schema != "pr_dependency_id_parity.v2" {
		t.Fatalf("unexpected corpus schema %q", document.Schema)
	}
	// `null` and an absent key both decode to nil with no error.
	if len(document.Endpoint) == 0 {
		t.Fatal("endpoint parity corpus decoded to zero observations")
	}
	return document.Endpoint
}
