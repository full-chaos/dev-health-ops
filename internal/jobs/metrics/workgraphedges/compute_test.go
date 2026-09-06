package workgraphedges

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	oracleOrg   = uuid.MustParse("70d529e0-3c06-4597-8480-794fd02328b6")
	oracleRepoA = uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	oracleRepoB = uuid.MustParse("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")
	oracleRepoC = uuid.MustParse("11111111-2222-3333-4444-555555555555")
)

func utc(year int, month time.Month, day, hour, minute, second, micro int) time.Time {
	return time.Date(year, month, day, hour, minute, second, micro*1000, time.UTC)
}

func timePtr(value time.Time) *time.Time { return &value }
func stringPtr(value string) *string     { return &value }
func uint32Ptr(value uint32) *uint32     { return &value }

// oracleParams builds the fixture that
// testdata/python_work_graph_edges_oracle.py builds, row for row and in the
// same order. Any edit here must be mirrored there; the oracle compares the
// two outputs elementwise, so a drift shows up as a diff rather than a silent
// weakening.
func oracleParams() Params {
	return Params{
		OrgID:    oracleOrg,
		Provider: "github",
		Reviews: []ReviewRow{
			{
				RepoID: oracleRepoA, Number: 101, ReviewID: "r-approved",
				State:       stringPtr("APPROVED"),
				SubmittedAt: timePtr(utc(2026, 9, 3, 9, 0, 0, 0)),
				LastSynced:  timePtr(utc(2026, 9, 3, 23, 0, 0, 0)),
			},
			{
				RepoID: oracleRepoA, Number: 102, ReviewID: "r-null-state",
				State:       nil,
				SubmittedAt: timePtr(utc(2026, 9, 3, 10, 0, 0, 0)),
				LastSynced:  timePtr(utc(2026, 9, 3, 23, 0, 0, 0)),
			},
			{
				RepoID: oracleRepoA, Number: 103, ReviewID: "r-empty-state",
				State:       stringPtr(""),
				SubmittedAt: timePtr(utc(2026, 9, 3, 11, 0, 0, 0)),
				LastSynced:  timePtr(utc(2026, 9, 3, 23, 0, 0, 0)),
			},
			{
				RepoID: oracleRepoB, Number: 201, ReviewID: "r-no-submitted",
				State:       stringPtr("COMMENTED"),
				SubmittedAt: nil,
				LastSynced:  timePtr(utc(2026, 9, 3, 22, 30, 0, 0)),
			},
			{
				RepoID: oracleRepoB, Number: 202, ReviewID: "r-subsecond",
				State:       stringPtr("CHANGES_REQUESTED"),
				SubmittedAt: timePtr(utc(2026, 9, 3, 12, 0, 0, 123000)),
				LastSynced:  timePtr(utc(2026, 9, 3, 23, 0, 0, 0)),
			},
			{
				RepoID: oracleRepoA, Number: 104, ReviewID: "",
				State:       stringPtr("APPROVED"),
				SubmittedAt: timePtr(utc(2026, 9, 3, 13, 0, 0, 0)),
				LastSynced:  timePtr(utc(2026, 9, 3, 23, 0, 0, 0)),
			},
		},
		Deployments: []DeploymentRow{
			{
				RepoID: oracleRepoA, DeploymentID: "dep-a1",
				PullRequestNumber: uint32Ptr(101),
				StartedAt:         timePtr(utc(2026, 9, 3, 14, 0, 0, 0)),
				FinishedAt:        timePtr(utc(2026, 9, 3, 14, 5, 0, 0)),
				DeployedAt:        timePtr(utc(2026, 9, 3, 14, 10, 0, 0)),
				LastSynced:        timePtr(utc(2026, 9, 3, 23, 0, 0, 0)),
			},
			{
				RepoID: oracleRepoA, DeploymentID: "dep-a2-no-pr",
				PullRequestNumber: nil,
				StartedAt:         timePtr(utc(2026, 9, 3, 15, 0, 0, 0)),
				FinishedAt:        nil,
				DeployedAt:        nil,
				LastSynced:        timePtr(utc(2026, 9, 3, 23, 0, 0, 0)),
			},
			{
				RepoID: oracleRepoB, DeploymentID: "dep-b1",
				PullRequestNumber: uint32Ptr(201),
				StartedAt:         timePtr(utc(2026, 9, 3, 16, 0, 0, 0)),
				FinishedAt:        nil,
				DeployedAt:        nil,
				LastSynced:        timePtr(utc(2026, 9, 3, 23, 0, 0, 0)),
			},
		},
		Incidents: []IncidentRow{
			{
				RepoID: oracleRepoA, IncidentID: "inc-heuristic",
				DeploymentID: "",
				StartedAt:    timePtr(utc(2026, 9, 3, 17, 0, 0, 0)),
				LastSynced:   timePtr(utc(2026, 9, 3, 23, 0, 0, 0)),
			},
			{
				RepoID: oracleRepoB, IncidentID: "inc-native",
				DeploymentID: "dep-not-in-todays-list",
				StartedAt:    timePtr(utc(2026, 9, 3, 18, 0, 0, 0)),
				LastSynced:   timePtr(utc(2026, 9, 3, 23, 0, 0, 0)),
			},
			{
				RepoID: oracleRepoC, IncidentID: "inc-no-deployments",
				DeploymentID: "",
				StartedAt:    timePtr(utc(2026, 9, 3, 19, 0, 0, 0)),
				LastSynced:   timePtr(utc(2026, 9, 3, 23, 0, 0, 0)),
			},
		},
		// Never reached by this fixture: every row carries a usable timestamp.
		// A non-zero value here would mask a _dt() fall-through bug by making
		// the wrong answer look plausible, so it is deliberately the zero time.
		Now: time.Time{},
	}
}

type pythonEdges struct {
	ReviewOutcomeEdges []struct {
		EdgeID          string  `json:"edge_id"`
		OrgID           string  `json:"org_id"`
		PRID            string  `json:"pr_id"`
		ReviewOutcomeID string  `json:"review_outcome_id"`
		Outcome         *string `json:"outcome"`
		Provider        string  `json:"provider"`
		RepoID          *string `json:"repo_id"`
		Confidence      float64 `json:"confidence"`
		Source          string  `json:"source"`
		Evidence        string  `json:"evidence"`
		ObservedAt      string  `json:"observed_at"`
	} `json:"review_outcome_edges"`
	PRDeploymentEdges []struct {
		EdgeID       string  `json:"edge_id"`
		OrgID        string  `json:"org_id"`
		PRID         string  `json:"pr_id"`
		DeploymentID string  `json:"deployment_id"`
		Provider     string  `json:"provider"`
		RepoID       *string `json:"repo_id"`
		Confidence   float64 `json:"confidence"`
		Source       string  `json:"source"`
		Evidence     string  `json:"evidence"`
		ObservedAt   string  `json:"observed_at"`
	} `json:"pr_deployment_edges"`
	DeploymentIncidentEdges []struct {
		EdgeID       string  `json:"edge_id"`
		OrgID        string  `json:"org_id"`
		DeploymentID string  `json:"deployment_id"`
		IncidentID   string  `json:"incident_id"`
		Provider     string  `json:"provider"`
		RepoID       *string `json:"repo_id"`
		Confidence   float64 `json:"confidence"`
		Source       string  `json:"source"`
		Evidence     string  `json:"evidence"`
		ObservedAt   string  `json:"observed_at"`
	} `json:"deployment_incident_edges"`
}

// CHAOS-5234/CHAOS-3092: runPythonOracle (the live Python invocation) is
// DELETED -- chris's standing rule (CHAOS-5233): once a family's Go
// executor is on main, its Python compute is deleted, never kept alive
// just to give a rot guard something to compare against.
// extract_review_deployment_incident_edges (src/dev_health_ops/work_graph/
// extractors/ai_workflow.py) and testdata/python_work_graph_edges_oracle.py
// are both deleted; loadFrozenWorkGraphEdgesGolden below reads a FROZEN
// snapshot of the oracle's last live run (tests/fixtures/
// work_graph_edges_python_golden.json, captured on bigboy before deletion)
// instead of shelling out to Python -- same shape as issueprlinks' CHAOS-5249
// retirement.
func loadFrozenWorkGraphEdgesGolden(t *testing.T) pythonEdges {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "tests", "fixtures", "work_graph_edges_python_golden.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read frozen golden: %v", err)
	}
	var doc struct {
		Schema string `json:"schema"`
		pythonEdges
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode frozen golden: %v", err)
	}
	if doc.Schema != "work_graph_edges_python_golden.v1" {
		t.Fatalf("unexpected golden schema %q -- regenerate or update the decoder", doc.Schema)
	}
	if len(doc.ReviewOutcomeEdges) == 0 && len(doc.PRDeploymentEdges) == 0 && len(doc.DeploymentIncidentEdges) == 0 {
		t.Fatal("frozen golden has zero rows across all three edge lists; regenerate it")
	}
	return doc.pythonEdges
}

// pythonISOFormat renders a UTC instant the way CPython's
// datetime.isoformat() does: no fractional part at exactly zero microseconds,
// and exactly six digits otherwise.
//
// Go's ".999999" verb TRIMS trailing zeros, so it would render
// 12:00:00.123000 as "12:00:00.123" and disagree with Python on every
// DateTime64(3) value. That exact mistake was codex round-1 P2 on #2229.
func pythonISOFormat(value time.Time) string {
	utcValue := value.UTC()
	if utcValue.Nanosecond() == 0 {
		return utcValue.Format("2006-01-02T15:04:05+00:00")
	}
	return utcValue.Format("2006-01-02T15:04:05.000000+00:00")
}

// optionalUUID renders a nullable repo_id the way the Python oracle emits it:
// the canonical string, or the literal "<nil>" when absent. Needed because the
// Go side carries *uuid.UUID and Python emits `str(...)` or JSON null.
func optionalUUID(value *uuid.UUID) string {
	if value == nil {
		return "<nil>"
	}
	return value.String()
}

func optionalString(value *string) string {
	if value == nil {
		return "<nil>"
	}
	return *value
}

// TestWorkGraphEdgesMatchFrozenPythonGolden is this family's L2 rot guard:
// it runs the REAL ExtractReviewDeploymentIncidentEdges and compares every
// persisted column of all three edge lists, including edge_id, against a
// FROZEN snapshot (CHAOS-5234/CHAOS-3092) instead of a live Python run --
// Python's extract_review_deployment_incident_edges is deleted, so "Python
// still agrees with itself" stops being the protection that matters; this
// frozen comparison is the regression contract going forward.
//
// # THAT SENTENCE WAS FALSE UNTIL #2240 ROUND 1
//
// It is kept verbatim so the correction stays visible. The comparator used to
// omit org_id, provider and repo_id from the deployment and incident loops,
// and repo_id from the review loop, while this comment claimed full coverage.
// The reviewer proved it: setting the deployment edges' Provider to a literal
// left every asserted field unchanged -- provider is not in the hash, so
// edge_id did not move either -- and the oracle passed on rows carrying a
// wrong persisted provider.
//
// Widening it surfaced NO mismatches: the kernel was correct all along. That
// is exactly why the gap survived. A comparator that omits a field it happens
// to get right never fails, so nothing ever points at the omission; only
// enumerating what is actually asserted finds it. Do not add a field to the
// structs above without adding it here.
//
// Comparing edge_id is possible here and was not on #2229: Python's edge ids
// are a sha256 over the identity tuple, not a uuid4, so there is a stable
// Python answer to assert against. That single difference is the whole of this
// oracle's advantage over #2229's -- it is not broadly "stronger", and an
// earlier version of this comment overstated it as such.
func TestWorkGraphEdgesMatchFrozenPythonGolden(t *testing.T) {
	expected := loadFrozenWorkGraphEdgesGolden(t)

	actual, err := ExtractReviewDeploymentIncidentEdges(oracleParams())
	if err != nil {
		t.Fatalf("extract: %v", err)
	}

	if len(actual.ReviewOutcomeEdges) != len(expected.ReviewOutcomeEdges) {
		t.Fatalf("review edge count: Go %d, Python %d",
			len(actual.ReviewOutcomeEdges), len(expected.ReviewOutcomeEdges))
	}
	for index, want := range expected.ReviewOutcomeEdges {
		got := actual.ReviewOutcomeEdges[index]
		if got.EdgeID != want.EdgeID {
			t.Errorf("review[%d] edge_id: Go %s, Python %s", index, got.EdgeID, want.EdgeID)
		}
		if got.OrgID.String() != want.OrgID {
			t.Errorf("review[%d] org_id: Go %s, Python %s", index, got.OrgID, want.OrgID)
		}
		if got.PRID != want.PRID {
			t.Errorf("review[%d] pr_id: Go %s, Python %s", index, got.PRID, want.PRID)
		}
		if got.ReviewOutcomeID != want.ReviewOutcomeID {
			t.Errorf("review[%d] review_outcome_id: Go %s, Python %s", index, got.ReviewOutcomeID, want.ReviewOutcomeID)
		}
		if optionalString(got.Outcome) != optionalString(want.Outcome) {
			t.Errorf("review[%d] outcome: Go %s, Python %s",
				index, optionalString(got.Outcome), optionalString(want.Outcome))
		}
		if got.Provider != want.Provider {
			t.Errorf("review[%d] provider: Go %s, Python %s", index, got.Provider, want.Provider)
		}
		if got.Confidence != want.Confidence {
			t.Errorf("review[%d] confidence: Go %v, Python %v", index, got.Confidence, want.Confidence)
		}
		if got.Source != want.Source {
			t.Errorf("review[%d] source: Go %s, Python %s", index, got.Source, want.Source)
		}
		if got.Evidence != want.Evidence {
			t.Errorf("review[%d] evidence: Go %s, Python %s", index, got.Evidence, want.Evidence)
		}
		if optionalUUID(got.RepoID) != optionalString(want.RepoID) {
			t.Errorf("review[%d] repo_id: Go %s, Python %s",
				index, optionalUUID(got.RepoID), optionalString(want.RepoID))
		}
		if pythonISOFormat(got.ObservedAt) != want.ObservedAt {
			t.Errorf("review[%d] observed_at: Go %s, Python %s",
				index, pythonISOFormat(got.ObservedAt), want.ObservedAt)
		}
	}

	if len(actual.PRDeploymentEdges) != len(expected.PRDeploymentEdges) {
		t.Fatalf("deployment edge count: Go %d, Python %d",
			len(actual.PRDeploymentEdges), len(expected.PRDeploymentEdges))
	}
	for index, want := range expected.PRDeploymentEdges {
		got := actual.PRDeploymentEdges[index]
		if got.EdgeID != want.EdgeID {
			t.Errorf("deployment[%d] edge_id: Go %s, Python %s", index, got.EdgeID, want.EdgeID)
		}
		if got.PRID != want.PRID {
			t.Errorf("deployment[%d] pr_id: Go %s, Python %s", index, got.PRID, want.PRID)
		}
		if got.DeploymentID != want.DeploymentID {
			t.Errorf("deployment[%d] deployment_id: Go %s, Python %s", index, got.DeploymentID, want.DeploymentID)
		}
		if got.OrgID.String() != want.OrgID {
			t.Errorf("deployment[%d] org_id: Go %s, Python %s", index, got.OrgID, want.OrgID)
		}
		if got.Provider != want.Provider {
			t.Errorf("deployment[%d] provider: Go %s, Python %s", index, got.Provider, want.Provider)
		}
		if optionalUUID(got.RepoID) != optionalString(want.RepoID) {
			t.Errorf("deployment[%d] repo_id: Go %s, Python %s",
				index, optionalUUID(got.RepoID), optionalString(want.RepoID))
		}
		if got.Confidence != want.Confidence {
			t.Errorf("deployment[%d] confidence: Go %v, Python %v", index, got.Confidence, want.Confidence)
		}
		if got.Source != want.Source {
			t.Errorf("deployment[%d] source: Go %s, Python %s", index, got.Source, want.Source)
		}
		if got.Evidence != want.Evidence {
			t.Errorf("deployment[%d] evidence: Go %s, Python %s", index, got.Evidence, want.Evidence)
		}
		if pythonISOFormat(got.ObservedAt) != want.ObservedAt {
			t.Errorf("deployment[%d] observed_at: Go %s, Python %s",
				index, pythonISOFormat(got.ObservedAt), want.ObservedAt)
		}
	}

	if len(actual.DeploymentIncidentEdges) != len(expected.DeploymentIncidentEdges) {
		t.Fatalf("incident edge count: Go %d, Python %d",
			len(actual.DeploymentIncidentEdges), len(expected.DeploymentIncidentEdges))
	}
	for index, want := range expected.DeploymentIncidentEdges {
		got := actual.DeploymentIncidentEdges[index]
		if got.EdgeID != want.EdgeID {
			t.Errorf("incident[%d] edge_id: Go %s, Python %s", index, got.EdgeID, want.EdgeID)
		}
		if got.DeploymentID != want.DeploymentID {
			t.Errorf("incident[%d] deployment_id: Go %s, Python %s", index, got.DeploymentID, want.DeploymentID)
		}
		if got.IncidentID != want.IncidentID {
			t.Errorf("incident[%d] incident_id: Go %s, Python %s", index, got.IncidentID, want.IncidentID)
		}
		if got.OrgID.String() != want.OrgID {
			t.Errorf("incident[%d] org_id: Go %s, Python %s", index, got.OrgID, want.OrgID)
		}
		if got.Provider != want.Provider {
			t.Errorf("incident[%d] provider: Go %s, Python %s", index, got.Provider, want.Provider)
		}
		if optionalUUID(got.RepoID) != optionalString(want.RepoID) {
			t.Errorf("incident[%d] repo_id: Go %s, Python %s",
				index, optionalUUID(got.RepoID), optionalString(want.RepoID))
		}
		if got.Confidence != want.Confidence {
			t.Errorf("incident[%d] confidence: Go %v, Python %v", index, got.Confidence, want.Confidence)
		}
		if got.Source != want.Source {
			t.Errorf("incident[%d] source: Go %s, Python %s", index, got.Source, want.Source)
		}
		if got.Evidence != want.Evidence {
			t.Errorf("incident[%d] evidence: Go %s, Python %s", index, got.Evidence, want.Evidence)
		}
		if pythonISOFormat(got.ObservedAt) != want.ObservedAt {
			t.Errorf("incident[%d] observed_at: Go %s, Python %s",
				index, pythonISOFormat(got.ObservedAt), want.ObservedAt)
		}
	}
}

// TestEmptyStateNilsTheOutcomeColumnButNotTheEvidenceJSON pins the one rule a
// reviewer is most likely to "clean up".
//
// Python reads the same column twice, differently:
//
//	outcome  = _str(row, "state") or None          -> None for NULL *and* ""
//	evidence = _json({..., "state": row.get("state")})  -> null only for NULL
//
// so an empty state disappears from the outcome column while surviving as ""
// inside the evidence JSON. Unifying them passes every non-empty and every
// NULL case and fails only this one.
//
// This runs without the oracle so the rule is pinned even when the live
// oracle is skipped.
func TestEmptyStateNilsTheOutcomeColumnButNotTheEvidenceJSON(t *testing.T) {
	result, err := ExtractReviewDeploymentIncidentEdges(Params{
		OrgID:    oracleOrg,
		Provider: "github",
		Reviews: []ReviewRow{
			{
				RepoID: oracleRepoA, Number: 1, ReviewID: "empty",
				State:       stringPtr(""),
				SubmittedAt: timePtr(utc(2026, 9, 3, 9, 0, 0, 0)),
			},
			{
				RepoID: oracleRepoA, Number: 2, ReviewID: "null",
				State:       nil,
				SubmittedAt: timePtr(utc(2026, 9, 3, 9, 0, 0, 0)),
			},
		},
	})
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if len(result.ReviewOutcomeEdges) != 2 {
		t.Fatalf("expected 2 review edges, got %d", len(result.ReviewOutcomeEdges))
	}

	empty := result.ReviewOutcomeEdges[0]
	if empty.Outcome != nil {
		t.Errorf("empty state must nil the outcome column, got %q", *empty.Outcome)
	}
	if want := `{"review_id":"empty","state":""}`; empty.Evidence != want {
		t.Errorf("empty state must survive as \"\" in evidence:\n got %s\nwant %s", empty.Evidence, want)
	}

	null := result.ReviewOutcomeEdges[1]
	if null.Outcome != nil {
		t.Errorf("null state must nil the outcome column, got %q", *null.Outcome)
	}
	if want := `{"review_id":"null","state":null}`; null.Evidence != want {
		t.Errorf("null state must be null in evidence:\n got %s\nwant %s", null.Evidence, want)
	}
}

// TestDeploymentWithoutAPRNumberStillLinksHeuristically pins the POSITION of
// the per-repo index append.
//
// Python appends to deployments_by_repo BEFORE `if pr_number_value is None:
// continue`, so a deployment with no PR number produces no PR->deployment edge
// yet remains a heuristic incident target. Moving the append below the
// continue is a natural-looking tidy-up that silently shrinks incident
// linkage for exactly the deployments most likely to lack a PR number.
func TestDeploymentWithoutAPRNumberStillLinksHeuristically(t *testing.T) {
	result, err := ExtractReviewDeploymentIncidentEdges(Params{
		OrgID:    oracleOrg,
		Provider: "github",
		Deployments: []DeploymentRow{
			{
				RepoID: oracleRepoA, DeploymentID: "with-pr",
				PullRequestNumber: uint32Ptr(7),
				DeployedAt:        timePtr(utc(2026, 9, 3, 14, 0, 0, 0)),
			},
			{
				RepoID: oracleRepoA, DeploymentID: "without-pr",
				PullRequestNumber: nil,
				DeployedAt:        timePtr(utc(2026, 9, 3, 15, 0, 0, 0)),
			},
		},
		Incidents: []IncidentRow{
			{
				RepoID: oracleRepoA, IncidentID: "inc",
				DeploymentID: "",
				StartedAt:    timePtr(utc(2026, 9, 3, 17, 0, 0, 0)),
			},
		},
	})
	if err != nil {
		t.Fatalf("extract: %v", err)
	}

	if len(result.PRDeploymentEdges) != 1 {
		t.Fatalf("only the PR-bearing deployment makes a PR edge, got %d", len(result.PRDeploymentEdges))
	}
	if result.PRDeploymentEdges[0].DeploymentID != "with-pr" {
		t.Errorf("wrong deployment made the PR edge: %s", result.PRDeploymentEdges[0].DeploymentID)
	}

	if len(result.DeploymentIncidentEdges) != 2 {
		t.Fatalf("the incident must fan out to BOTH deployments, got %d", len(result.DeploymentIncidentEdges))
	}
	// Input order, not map order.
	if got := result.DeploymentIncidentEdges[0].DeploymentID; got != "with-pr" {
		t.Errorf("incident edge[0] deployment: got %s, want with-pr", got)
	}
	if got := result.DeploymentIncidentEdges[1].DeploymentID; got != "without-pr" {
		t.Errorf("incident edge[1] deployment: got %s, want without-pr", got)
	}
	for index, edge := range result.DeploymentIncidentEdges {
		if edge.Source != "heuristic" || edge.Confidence != 0.3 {
			t.Errorf("incident edge[%d] must be heuristic/0.3, got %s/%v", index, edge.Source, edge.Confidence)
		}
	}
}

// TestHeuristicFanOutDoesNotCrossRepositories guards the per-repo scoping of
// the index a heuristic incident walks.
func TestHeuristicFanOutDoesNotCrossRepositories(t *testing.T) {
	result, err := ExtractReviewDeploymentIncidentEdges(Params{
		OrgID:    oracleOrg,
		Provider: "github",
		Deployments: []DeploymentRow{
			{RepoID: oracleRepoA, DeploymentID: "a1", DeployedAt: timePtr(utc(2026, 9, 3, 14, 0, 0, 0))},
			{RepoID: oracleRepoB, DeploymentID: "b1", DeployedAt: timePtr(utc(2026, 9, 3, 14, 0, 0, 0))},
		},
		Incidents: []IncidentRow{
			{RepoID: oracleRepoA, IncidentID: "inc-a", StartedAt: timePtr(utc(2026, 9, 3, 17, 0, 0, 0))},
		},
	})
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if len(result.DeploymentIncidentEdges) != 1 {
		t.Fatalf("expected exactly one edge, got %d", len(result.DeploymentIncidentEdges))
	}
	if got := result.DeploymentIncidentEdges[0].DeploymentID; got != "a1" {
		t.Errorf("incident linked across repositories: got %s, want a1", got)
	}
}

// TestTimestampFallbackFollowsPythonsCoalesceOrder pins _dt()'s candidate
// ORDER, not merely that some timestamp is chosen.
func TestTimestampFallbackFollowsPythonsCoalesceOrder(t *testing.T) {
	now := utc(2030, 1, 1, 0, 0, 0, 0)
	deployed := utc(2026, 9, 3, 14, 10, 0, 0)
	finished := utc(2026, 9, 3, 14, 5, 0, 0)
	started := utc(2026, 9, 3, 14, 0, 0, 0)
	lastSynced := utc(2026, 9, 3, 23, 0, 0, 0)

	for _, testCase := range []struct {
		name string
		row  DeploymentRow
		want time.Time
	}{
		{"deployed_at wins", DeploymentRow{DeployedAt: &deployed, FinishedAt: &finished, StartedAt: &started, LastSynced: &lastSynced}, deployed},
		{"finished_at next", DeploymentRow{FinishedAt: &finished, StartedAt: &started, LastSynced: &lastSynced}, finished},
		{"started_at next", DeploymentRow{StartedAt: &started, LastSynced: &lastSynced}, started},
		{"last_synced next", DeploymentRow{LastSynced: &lastSynced}, lastSynced},
		{"now is the last resort", DeploymentRow{}, now},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			row := testCase.row
			row.RepoID = oracleRepoA
			row.DeploymentID = "d"
			row.PullRequestNumber = uint32Ptr(1)
			result, err := ExtractReviewDeploymentIncidentEdges(Params{
				OrgID: oracleOrg, Provider: "github",
				Deployments: []DeploymentRow{row},
				Now:         now,
			})
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			if len(result.PRDeploymentEdges) != 1 {
				t.Fatalf("expected one edge, got %d", len(result.PRDeploymentEdges))
			}
			if got := result.PRDeploymentEdges[0].ObservedAt; !got.Equal(testCase.want) {
				t.Errorf("observed_at: got %s, want %s", got, testCase.want)
			}
		})
	}
}
