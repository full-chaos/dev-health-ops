package remaining

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// membershipGoldenPayload mirrors tests/fixtures/generate_membership_backfill_golden.py's
// JSON shape exactly. Field types match the encoder's own output, not a
// convenient reinterpretation of it.
type membershipGoldenPayload struct {
	OrgID                        string                 `json:"org_id"`
	Edges                        []membershipGoldenEdge `json:"edges"`
	ThemeDistributionPairs       [][2]any               `json:"theme_distribution_pairs"`
	SubcategoryDistributionPairs [][2]any               `json:"subcategory_distribution_pairs"`
	CategorizationStatus         string                 `json:"categorization_status"`
	Stats                        struct {
		Components          int `json:"components"`
		Matched             int `json:"matched"`
		Skipped             int `json:"skipped"`
		Memberships         int `json:"memberships"`
		OversizedComponents int `json:"oversized_components"`
		DroppedEdges        int `json:"dropped_edges"`
		DroppedNodes        int `json:"dropped_nodes"`
	} `json:"stats"`
	MembershipRows []membershipGoldenRow `json:"membership_rows"`
	RunRecord      struct {
		OrgID string `json:"org_id"`
	} `json:"run_record"`
}

type membershipGoldenEdge struct {
	EdgeID     string  `json:"edge_id"`
	SourceType string  `json:"source_type"`
	SourceID   string  `json:"source_id"`
	TargetType string  `json:"target_type"`
	TargetID   string  `json:"target_id"`
	EdgeType   string  `json:"edge_type"`
	RepoID     string  `json:"repo_id"`
	Provider   string  `json:"provider"`
	Provenance string  `json:"provenance"`
	Confidence float64 `json:"confidence"`
	Evidence   string  `json:"evidence"`
}

type membershipGoldenRow struct {
	OrgID                string  `json:"org_id"`
	NodeType             string  `json:"node_type"`
	NodeID               string  `json:"node_id"`
	WorkUnitID           string  `json:"work_unit_id"`
	CategoryKind         string  `json:"category_kind"`
	Category             string  `json:"category"`
	Weight               float64 `json:"weight"`
	IsDominant           int     `json:"is_dominant"`
	CategorizationStatus string  `json:"categorization_status"`
}

func loadMembershipGolden(t *testing.T) membershipGoldenPayload {
	t.Helper()
	root := membershipRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "tests", "fixtures", "membership_backfill_golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var payload membershipGoldenPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return payload
}

func membershipRepoRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(directory, "go.mod")); statErr == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("no go.mod above the test")
		}
		directory = parent
	}
}

// TestComputeOrgMatchesLivePythonBackfillMemberships is the byte-parity
// oracle for CHAOS-4282: the golden JSON was captured from the REAL Python
// backfill_memberships (tests/fixtures/generate_membership_backfill_golden.py),
// driven through an injected fake sink rather than a live database -- the
// same "live producer, faked I/O boundary" shape this repo already uses for
// the capacity forecast golden. Regenerate with:
//
//	PYTHONPATH=src python3 tests/fixtures/generate_membership_backfill_golden.py \
//	  > tests/fixtures/membership_backfill_golden.json
//
// and verify staleness first with the script's own --check mode before
// trusting a manual diff.
//
// This test drives ComputeOrg -- the FULL orchestration (component build,
// distribution match/skip, row projection, write-then-marker sequencing) --
// against the SAME edges and the SAME (ordered!) distributions the Python
// side used, through the package's own fakeMembershipEdges/
// fakeMembershipDistributions/fakeMembershipWriter (membership_native_test.go),
// and asserts the resulting rows are IDENTICAL to Python's, field for field,
// IN ORDER, except run_id/computed_at -- both wall-clock/uuid stamped fresh
// on every real run in both languages, and therefore never a parity target.
func TestComputeOrgMatchesLivePythonBackfillMemberships(t *testing.T) {
	golden := loadMembershipGolden(t)

	edgeRows := make([]chquery.EdgeRow, len(golden.Edges))
	for i, edge := range golden.Edges {
		edgeRows[i] = chquery.EdgeRow{
			Edge: units.Edge{
				EdgeID: edge.EdgeID, SourceType: edge.SourceType, SourceID: edge.SourceID,
				TargetType: edge.TargetType, TargetID: edge.TargetID, Confidence: edge.Confidence,
			},
			EdgeType: edge.EdgeType, RepoID: edge.RepoID, Provider: edge.Provider,
			Provenance: edge.Provenance, Evidence: edge.Evidence,
		}
	}

	if len(golden.MembershipRows) == 0 {
		t.Fatal("golden has no membership rows -- the fixture generator's own assertions should have caught this")
	}
	matchedID := golden.MembershipRows[0].WorkUnitID
	distribution := membershipDistribution{
		ThemeDistribution:       distributionFromGoldenPairs(t, golden.ThemeDistributionPairs),
		SubcategoryDistribution: distributionFromGoldenPairs(t, golden.SubcategoryDistributionPairs),
		CategorizationStatus:    golden.CategorizationStatus,
	}

	writer := &fakeMembershipWriter{}
	executor := newTestMembershipExecutor(
		fakeMembershipEdges{rows: edgeRows},
		fakeMembershipDistributions{byUnit: map[string]membershipDistribution{matchedID: distribution}},
		writer,
	)
	executor.conn = fakeDriverConnSentinel{}

	outcome, err := executor.ComputeOrg(context.Background(), golden.OrgID, nil, time.Now())
	if err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}

	if outcome.Components != golden.Stats.Components ||
		outcome.Matched != golden.Stats.Matched ||
		outcome.Skipped != golden.Stats.Skipped ||
		outcome.MembershipRows != golden.Stats.Memberships ||
		outcome.OversizedComponents != golden.Stats.OversizedComponents ||
		outcome.DroppedEdges != golden.Stats.DroppedEdges ||
		outcome.DroppedNodes != golden.Stats.DroppedNodes {
		t.Fatalf("stats mismatch: got %+v, want %+v", outcome, golden.Stats)
	}

	if len(writer.membershipRecords) != len(golden.MembershipRows) {
		t.Fatalf("row count mismatch: got %d, want %d", len(writer.membershipRecords), len(golden.MembershipRows))
	}
	for i, want := range golden.MembershipRows {
		got := writer.membershipRecords[i]
		gotComparable := membershipGoldenRow{
			OrgID: got.OrgID, NodeType: got.NodeType, NodeID: got.NodeID,
			WorkUnitID: got.WorkUnitID, CategoryKind: got.CategoryKind, Category: got.Category,
			Weight: got.Weight, IsDominant: got.IsDominant,
			CategorizationStatus: got.CategorizationStatus,
		}
		if !reflect.DeepEqual(gotComparable, want) {
			t.Errorf("row %d mismatch:\n got  %+v\n want %+v", i, gotComparable, want)
		}
	}

	if writer.runRecord == nil || writer.runRecord.OrgID != golden.RunRecord.OrgID {
		t.Errorf("run record org_id mismatch: got %+v, want org_id=%q", writer.runRecord, golden.RunRecord.OrgID)
	}
	if !writer.pruneCalled || writer.pruneKeep != membershipRetentionKeep {
		t.Errorf("expected a keep=%d prune call, got called=%v keep=%d",
			membershipRetentionKeep, writer.pruneCalled, writer.pruneKeep)
	}
}

func distributionFromGoldenPairs(t *testing.T, pairs [][2]any) *units.Distribution {
	t.Helper()
	weights := make([]units.CategoryWeight, 0, len(pairs))
	for _, pair := range pairs {
		category, ok := pair[0].(string)
		if !ok {
			t.Fatalf("golden distribution pair category is not a string: %#v", pair[0])
		}
		weight, ok := pair[1].(float64)
		if !ok {
			t.Fatalf("golden distribution pair weight is not a number: %#v", pair[1])
		}
		weights = append(weights, units.CategoryWeight{Category: category, Weight: weight})
	}
	return units.NewDistribution(weights...)
}
