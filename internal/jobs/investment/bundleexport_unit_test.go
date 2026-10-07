package investment

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// exportedRecord builds a record through the REAL producer
// (MaterializeComponent -> BuildTextBundle), never from hand-authored bundle
// JSON.
func exportedRecord(t *testing.T, title, description string, labels []string) BundleRecord {
	t.Helper()
	issueID := "linear:X-1"
	prID := testRepoID + "#pr7"
	commitID := testRepoID + "@abc123"
	created := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	component := units.Component{
		Nodes: []units.NodeKey{{Type: "issue", ID: issueID}, {Type: "pr", ID: prID}, {Type: "commit", ID: commitID}},
		Edges: []units.Edge{
			{EdgeID: "e1", SourceType: "issue", SourceID: issueID, TargetType: "pr", TargetID: prID, Confidence: 1},
			{EdgeID: "e2", SourceType: "issue", SourceID: issueID, TargetType: "commit", TargetID: commitID, Confidence: 1},
		},
	}
	workItems := map[string]chquery.WorkItem{
		issueID: {WorkItemID: issueID, Provider: "linear", Title: title, Description: description, Type: "bug", Labels: labels, CreatedAt: created, UpdatedAt: created},
	}
	result, err := MaterializeComponent(MaterializeComponentInput{
		Component: component, WorkItems: workItems,
		PRs:         map[string]chquery.PullRequest{prID: {RepoID: testRepoID, Number: 7, Title: "fix: login", Body: "closes X-1", CreatedAt: created}},
		Commits:     map[string]chquery.Commit{commitID: {RepoID: testRepoID, Hash: "abc123", Message: "fix: login\n\nbody", AuthorWhen: created}},
		EdgeRepoIDs: map[string]string{"e1": testRepoID, "e2": testRepoID},
		FromTS:      wideFrom, ToTS: wideTo,
	})
	if err != nil {
		t.Fatalf("MaterializeComponent: %v", err)
	}
	if result.Skipped != "" {
		t.Fatalf("component skipped: %s", result.Skipped)
	}
	return buildBundleRecord(component, result, workItems)
}

func TestBuildBundleRecordShape(t *testing.T) {
	record := exportedRecord(t, "Fix login", "It broke", nil)
	if !strings.HasPrefix(record.SourceBlock, "[issue] E1\nFix login It broke") {
		t.Fatalf("unexpected source block: %q", record.SourceBlock)
	}
	if record.Entities != (EntityCounts{Issues: 1, PRs: 1, Commits: 1}) || record.Selected != record.Entities {
		t.Fatalf("entities=%+v selected=%+v", record.Entities, record.Selected)
	}
	wantHandles := []string{"E1:issue:linear:X-1", "E2:pr:" + testRepoID + "#pr7", "E3:commit:" + testRepoID + "@abc123"}
	for i, h := range record.Handles {
		if got := h.Handle + ":" + h.SourceType + ":" + h.SourceID; got != wantHandles[i] {
			t.Fatalf("handle %d = %s, want %s", i, got, wantHandles[i])
		}
	}
	if record.BundleID != "bnd_"+record.InputHash[:16] || record.WorkUnitID == "" {
		t.Fatalf("ids: %+v", record)
	}
	if record.SourceBlockLen != len([]rune(record.SourceBlock)) {
		t.Fatalf("source_block_len %d", record.SourceBlockLen)
	}
	// Tiny text: must NOT pass the production gate, and the reason is named.
	if record.PassesGate || record.GateReason != "insufficient_evidence" {
		t.Fatalf("gate: pass=%v reason=%q chars=%d", record.PassesGate, record.GateReason, record.TextCharCount)
	}
	if record.LabelsPresent || record.LabelsInBlock {
		t.Fatalf("no labels expected: %+v", record)
	}
}

// The gate must flip exactly at minEvidenceChars, on TextCharCount.
func TestBuildBundleRecordGateBoundary(t *testing.T) {
	long := strings.Repeat("a", 280)
	record := exportedRecord(t, long, long, nil)
	if !record.PassesGate || record.GateReason != "" {
		t.Fatalf("expected pass, got reason=%q chars=%d", record.GateReason, record.TextCharCount)
	}
	if record.TextCharCount < minEvidenceChars {
		t.Fatalf("chars %d under gate yet passing", record.TextCharCount)
	}
}

func TestBuildBundleRecordLabels(t *testing.T) {
	record := exportedRecord(t, "Fix login", "It broke", []string{"customer", "p1"})
	if !record.LabelsPresent || !record.LabelsInBlock {
		t.Fatalf("labels should be present and visible: %+v", record)
	}
	if !strings.Contains(record.SourceBlock, "Labels: customer, p1") {
		t.Fatalf("labels missing from block: %q", record.SourceBlock)
	}
}

func TestPickIncumbentPrefersMatchingHashAndRealAnswer(t *testing.T) {
	t0 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	rows := []incumbentRow{
		{Status: "ok", InputHash: "OLD", ComputedAt: t0.Add(3 * time.Hour), Subcategories: map[string]float64{"quality.bugfix": 1}},
		{Status: "invalid_llm_output", InputHash: "H", ComputedAt: t0.Add(2 * time.Hour)},
		{Status: "repaired", InputHash: "H", ComputedAt: t0, Subcategories: map[string]float64{"risk.security": 0.6, "quality.bugfix": 0.4}},
	}
	got := pickIncumbent(rows, "H")
	if got == nil || got.Status != "repaired" || !got.InputHashMatch || got.TopSubcategory != "risk.security" || got.RowsForUnit != 3 {
		t.Fatalf("picked %+v", got)
	}
	// Only a stale-hash row: returned, but flagged.
	stale := pickIncumbent(rows[:1], "H")
	if stale == nil || stale.InputHashMatch {
		t.Fatalf("stale hash must be flagged: %+v", stale)
	}
	if pickIncumbent(nil, "H") != nil {
		t.Fatalf("no rows must give nil, not an empty incumbent")
	}
}

// Tie on every ranking input: lexical order decides, deterministically.
func TestPickIncumbentTopSubcategoryTieIsDeterministic(t *testing.T) {
	rows := []incumbentRow{{Status: "ok", InputHash: "H", Subcategories: map[string]float64{"maintenance.debt": 0.5, "feature_delivery.customer": 0.5}}}
	for i := 0; i < 20; i++ {
		if got := pickIncumbent(rows, "H").TopSubcategory; got != "feature_delivery.customer" {
			t.Fatalf("tie broken non-deterministically: %s", got)
		}
	}
}

func TestExportBundlesRequiresOrg(t *testing.T) {
	if _, err := ExportBundles(t.Context(), nil, ExportConfig{}, nil); err == nil {
		t.Fatal("empty org must fail before any query")
	}
}

// The gate flips exactly at minEvidenceChars, and an empty source set is its
// own reason (mirrors the two cases of materialize.go Run's gate switch).
func TestGateBoundaryExact(t *testing.T) {
	cases := []struct {
		chars, sources int
		pass           bool
		reason         string
	}{
		{minEvidenceChars - 1, 1, false, "insufficient_evidence"},
		{minEvidenceChars, 1, true, ""},
		{minEvidenceChars, 0, false, "no_text_sources"},
		{0, 0, false, "insufficient_evidence"},
	}
	for _, c := range cases {
		result := MaterializeComponentResult{Bundle: units.TextBundle{TextCharCount: c.chars, TextSourceCount: c.sources, InputHash: "h"}}
		record := buildBundleRecord(units.Component{}, result, nil)
		if record.PassesGate != c.pass || record.GateReason != c.reason {
			t.Fatalf("chars=%d sources=%d: pass=%v reason=%q", c.chars, c.sources, record.PassesGate, record.GateReason)
		}
	}
}
