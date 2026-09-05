package aiworkflow

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// oracleOrg/oracleRepoA/oracleRepoB/oracleProvider MUST match
// testdata/python_ai_workflow_oracle.py's ORG/REPO_A/REPO_B/PROVIDER exactly.
var (
	oracleOrg      = uuid.MustParse("8f3f7b0a-1c2d-4e3f-9a5b-6c7d8e9f0a1b")
	oracleRepoA    = uuid.MustParse("3a9c1e00-1111-4222-8333-944444445555")
	oracleRepoB    = uuid.MustParse("3a9c1e00-2222-4333-8444-955555556666")
	oracleProvider = "github"
)

func oracleDT(year int, month time.Month, day, hour, minute, second, microsecond int) time.Time {
	return time.Date(year, month, day, hour, minute, second, microsecond*1000, time.UTC)
}

// oraclePullRequests MUST match testdata/python_ai_workflow_oracle.py's
// PULL_REQUESTS list row for row, field for field.
func oraclePullRequests() []PullRequestRow {
	merged1 := oracleDT(2026, 9, 1, 10, 0, 0, 0)
	merged3 := oracleDT(2026, 9, 1, 13, 0, 0, 0)
	merged4 := oracleDT(2026, 9, 1, 15, 0, 0, 0)
	closed6 := oracleDT(2026, 9, 1, 17, 30, 0, 123000)
	return []PullRequestRow{
		{
			RepoID: oracleRepoA, Number: 101, Title: "Add caching", Body: "",
			HeadBranch: "feature/cache", AuthorName: "dev-a",
			Labels:     []string{"copilot", "claude-code"},
			CreatedAt:  oracleDT(2026, 9, 1, 9, 0, 0, 0),
			MergedAt:   &merged1,
			LastSynced: oracleDT(2026, 9, 1, 23, 0, 0, 0),
		},
		{
			RepoID: oracleRepoA, Number: 102, Title: "Automated dependency bump", Body: "",
			HeadBranch: "", AuthorName: "", AuthorLogin: "copilot[bot]", AuthorUserType: "Bot",
			CreatedAt:  oracleDT(2026, 9, 1, 11, 0, 0, 0),
			LastSynced: oracleDT(2026, 9, 1, 23, 0, 0, 0),
		},
		{
			RepoID: oracleRepoA, Number: 103, Title: "Refactor flaky test", Body: "",
			HeadBranch: "devin/refactor-flaky-test", AuthorName: "dev-e",
			CreatedAt:  oracleDT(2026, 9, 1, 12, 0, 0, 0),
			MergedAt:   &merged3,
			LastSynced: oracleDT(2026, 9, 1, 23, 0, 0, 0),
		},
		{
			RepoID: oracleRepoB, Number: 201, Title: "Improve error messages",
			Body:       "This was ai assisted work.",
			HeadBranch: "", AuthorName: "dev-c",
			CreatedAt:  oracleDT(2026, 9, 1, 14, 0, 0, 0),
			MergedAt:   &merged4,
			LastSynced: oracleDT(2026, 9, 1, 23, 0, 0, 0),
		},
		{
			RepoID: oracleRepoB, Number: 202, Title: "Fix typo",
			Body:       "Just a typo fix, nothing AI about it.",
			HeadBranch: "fix/typo", AuthorName: "dev-d",
			CreatedAt:  oracleDT(2026, 9, 1, 16, 0, 0, 0),
			LastSynced: oracleDT(2026, 9, 1, 23, 0, 0, 0),
		},
		{
			RepoID: oracleRepoB, Number: 203, Title: "Agent-created cleanup",
			Body:       "agent-created housekeeping pass",
			HeadBranch: "", AuthorName: "dev-f",
			CreatedAt:  oracleDT(2026, 9, 1, 17, 0, 0, 0),
			ClosedAt:   &closed6,
			LastSynced: oracleDT(2026, 9, 1, 23, 0, 0, 0),
		},
	}
}

// oracleIssueIDsByPR MUST match testdata/python_ai_workflow_oracle.py's
// ISSUE_IDS_BY_PR.
func oracleIssueIDsByPR() map[string][]string {
	return map[string][]string{
		prIDFor(oracleRepoA, 101): {"jira:ABC-1", "jira:ABC-2"},
	}
}

type pythonRun struct {
	RunID           string  `json:"run_id"`
	OrgID           string  `json:"org_id"`
	Provider        string  `json:"provider"`
	RunKind         string  `json:"run_kind"`
	Status          *string `json:"status"`
	Tool            *string `json:"tool"`
	Actor           *string `json:"actor"`
	RepoID          *string `json:"repo_id"`
	PromptsRedacted bool    `json:"prompts_redacted"`
	StartedAt       *string `json:"started_at"`
	CompletedAt     *string `json:"completed_at"`
	ObservedAt      string  `json:"observed_at"`
	Metadata        string  `json:"metadata"`
}

type pythonArtifactEdge struct {
	EdgeID       string  `json:"edge_id"`
	OrgID        string  `json:"org_id"`
	RunID        string  `json:"run_id"`
	ArtifactType string  `json:"artifact_type"`
	ArtifactID   string  `json:"artifact_id"`
	Provider     string  `json:"provider"`
	RepoID       *string `json:"repo_id"`
	Confidence   float64 `json:"confidence"`
	Source       string  `json:"source"`
	Evidence     string  `json:"evidence"`
	ObservedAt   string  `json:"observed_at"`
}

type pythonIssueEdge struct {
	EdgeID     string  `json:"edge_id"`
	OrgID      string  `json:"org_id"`
	IssueID    string  `json:"issue_id"`
	RunID      string  `json:"run_id"`
	Provider   string  `json:"provider"`
	RepoID     *string `json:"repo_id"`
	Confidence float64 `json:"confidence"`
	Source     string  `json:"source"`
	Evidence   string  `json:"evidence"`
	ObservedAt string  `json:"observed_at"`
}

type pythonAIWorkflowResult struct {
	Runs          []pythonRun          `json:"runs"`
	ArtifactEdges []pythonArtifactEdge `json:"artifact_edges"`
	IssueEdges    []pythonIssueEdge    `json:"issue_edges"`
}

func runPythonOracle(t *testing.T, markerName string) pythonAIWorkflowResult {
	t.Helper()
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	python := os.Getenv("PYTHON")
	if python == "" {
		t.Fatal("PYTHON is required for the live ai_workflow Python oracle")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, filepath.Join("testdata", "python_ai_workflow_oracle.py"))
	command.Dir = filepath.Join(root, "internal", "jobs", "metrics", "aiworkflow")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("execute production Python oracle: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	output := bytes.TrimSpace(stdout.Bytes())
	if lastLine := bytes.LastIndexByte(output, '\n'); lastLine >= 0 {
		output = output[lastLine+1:]
	}
	var decoded pythonAIWorkflowResult
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode production Python oracle output %q: %v", output, err)
	}
	if writeErr := os.WriteFile(filepath.Join(proofDirectory, markerName), []byte("executed"), 0o644); writeErr != nil {
		t.Fatalf("write live-python-oracle proof: %v", writeErr)
	}
	return decoded
}

// pythonISOFormat renders a UTC instant the way CPython's
// datetime.isoformat() does -- see workgraphedges/compute_test.go's
// identical helper for why Go's ".999999" verb (which TRIMS trailing zeros)
// cannot be used here.
func pythonISOFormat(value time.Time) string {
	utcValue := value.UTC()
	if utcValue.Nanosecond() == 0 {
		return utcValue.Format("2006-01-02T15:04:05+00:00")
	}
	return utcValue.Format("2006-01-02T15:04:05.000000+00:00")
}

func optionalPythonISOFormat(value *time.Time) *string {
	if value == nil {
		return nil
	}
	formatted := pythonISOFormat(*value)
	return &formatted
}

func optionalUUIDString(value *uuid.UUID) *string {
	if value == nil {
		return nil
	}
	s := value.String()
	return &s
}

func optionalStringsEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// TestAIWorkflowMatchesLivePythonProduction is this family's L2 rot guard:
// it runs the REAL extract_ai_workflow_from_pull_requests (via the Python
// oracle subprocess) and Go's Compute over the byte-identical fixture, then
// compares every persisted column of all three result lists, including
// run_id/edge_id and the compact-JSON metadata/evidence strings.
//
// Do not add a field to Run/ArtifactEdge/IssueEdge without adding it here --
// see workgraphedges/compute_test.go's identical warning and the incident
// that motivated it (#2240 round 1: an omitted-from-comparison field silently
// carried a wrong value for a long time because nothing ever failed on it).
func TestAIWorkflowMatchesLivePythonProduction(t *testing.T) {
	expected := runPythonOracle(t, "ai-workflow-golden")

	now := time.Now().UTC()
	result := Compute(oraclePullRequests(), oracleOrg, oracleProvider, oracleIssueIDsByPR(), now)

	if len(result.Runs) != len(expected.Runs) {
		t.Fatalf("run count: Go %d, Python %d", len(result.Runs), len(expected.Runs))
	}
	for i, want := range expected.Runs {
		got := result.Runs[i]
		if got.RunID != want.RunID {
			t.Errorf("run[%d] run_id: Go %s, Python %s", i, got.RunID, want.RunID)
		}
		if got.OrgID.String() != want.OrgID {
			t.Errorf("run[%d] org_id: Go %s, Python %s", i, got.OrgID, want.OrgID)
		}
		if got.Provider != want.Provider {
			t.Errorf("run[%d] provider: Go %s, Python %s", i, got.Provider, want.Provider)
		}
		if got.RunKind != want.RunKind {
			t.Errorf("run[%d] run_kind: Go %s, Python %s", i, got.RunKind, want.RunKind)
		}
		gotStatus := got.Status
		if !optionalStringsEqual(gotStatus, want.Status) {
			t.Errorf("run[%d] status: Go %v, Python %v", i, gotStatus, want.Status)
		}
		if !optionalStringsEqual(got.Tool, want.Tool) {
			t.Errorf("run[%d] tool: Go %v, Python %v", i, got.Tool, want.Tool)
		}
		if !optionalStringsEqual(got.Actor, want.Actor) {
			t.Errorf("run[%d] actor: Go %v, Python %v", i, got.Actor, want.Actor)
		}
		if !optionalStringsEqual(optionalUUIDString(got.RepoID), want.RepoID) {
			t.Errorf("run[%d] repo_id: Go %v, Python %v", i, optionalUUIDString(got.RepoID), want.RepoID)
		}
		if got.PromptsRedacted != want.PromptsRedacted {
			t.Errorf("run[%d] prompts_redacted: Go %v, Python %v", i, got.PromptsRedacted, want.PromptsRedacted)
		}
		if !optionalStringsEqual(optionalPythonISOFormat(got.StartedAt), want.StartedAt) {
			t.Errorf("run[%d] started_at: Go %v, Python %v", i, optionalPythonISOFormat(got.StartedAt), want.StartedAt)
		}
		if !optionalStringsEqual(optionalPythonISOFormat(got.CompletedAt), want.CompletedAt) {
			t.Errorf("run[%d] completed_at: Go %v, Python %v", i, optionalPythonISOFormat(got.CompletedAt), want.CompletedAt)
		}
		if pythonISOFormat(got.ObservedAt) != want.ObservedAt {
			t.Errorf("run[%d] observed_at: Go %s, Python %s", i, pythonISOFormat(got.ObservedAt), want.ObservedAt)
		}
		gotMetadata, err := pythonparity.MarshalPythonJSONCompact(got.Metadata)
		if err != nil {
			t.Fatalf("run[%d] marshal metadata: %v", i, err)
		}
		if string(gotMetadata) != want.Metadata {
			t.Errorf("run[%d] metadata: Go %s, Python %s", i, gotMetadata, want.Metadata)
		}
	}

	if len(result.ArtifactEdges) != len(expected.ArtifactEdges) {
		t.Fatalf("artifact edge count: Go %d, Python %d", len(result.ArtifactEdges), len(expected.ArtifactEdges))
	}
	for i, want := range expected.ArtifactEdges {
		got := result.ArtifactEdges[i]
		if got.EdgeID != want.EdgeID {
			t.Errorf("artifact[%d] edge_id: Go %s, Python %s", i, got.EdgeID, want.EdgeID)
		}
		if got.OrgID.String() != want.OrgID {
			t.Errorf("artifact[%d] org_id: Go %s, Python %s", i, got.OrgID, want.OrgID)
		}
		if got.RunID != want.RunID {
			t.Errorf("artifact[%d] run_id: Go %s, Python %s", i, got.RunID, want.RunID)
		}
		if got.ArtifactType != want.ArtifactType {
			t.Errorf("artifact[%d] artifact_type: Go %s, Python %s", i, got.ArtifactType, want.ArtifactType)
		}
		if got.ArtifactID != want.ArtifactID {
			t.Errorf("artifact[%d] artifact_id: Go %s, Python %s", i, got.ArtifactID, want.ArtifactID)
		}
		if got.Provider != want.Provider {
			t.Errorf("artifact[%d] provider: Go %s, Python %s", i, got.Provider, want.Provider)
		}
		if !optionalStringsEqual(optionalUUIDString(got.RepoID), want.RepoID) {
			t.Errorf("artifact[%d] repo_id: Go %v, Python %v", i, optionalUUIDString(got.RepoID), want.RepoID)
		}
		if got.Confidence != want.Confidence {
			t.Errorf("artifact[%d] confidence: Go %v, Python %v", i, got.Confidence, want.Confidence)
		}
		if got.Source != want.Source {
			t.Errorf("artifact[%d] source: Go %s, Python %s", i, got.Source, want.Source)
		}
		if got.Evidence != want.Evidence {
			t.Errorf("artifact[%d] evidence: Go %s, Python %s", i, got.Evidence, want.Evidence)
		}
		if pythonISOFormat(got.ObservedAt) != want.ObservedAt {
			t.Errorf("artifact[%d] observed_at: Go %s, Python %s", i, pythonISOFormat(got.ObservedAt), want.ObservedAt)
		}
	}

	if len(result.IssueEdges) != len(expected.IssueEdges) {
		t.Fatalf("issue edge count: Go %d, Python %d", len(result.IssueEdges), len(expected.IssueEdges))
	}
	for i, want := range expected.IssueEdges {
		got := result.IssueEdges[i]
		if got.EdgeID != want.EdgeID {
			t.Errorf("issue[%d] edge_id: Go %s, Python %s", i, got.EdgeID, want.EdgeID)
		}
		if got.OrgID.String() != want.OrgID {
			t.Errorf("issue[%d] org_id: Go %s, Python %s", i, got.OrgID, want.OrgID)
		}
		if got.IssueID != want.IssueID {
			t.Errorf("issue[%d] issue_id: Go %s, Python %s", i, got.IssueID, want.IssueID)
		}
		if got.RunID != want.RunID {
			t.Errorf("issue[%d] run_id: Go %s, Python %s", i, got.RunID, want.RunID)
		}
		if got.Provider != want.Provider {
			t.Errorf("issue[%d] provider: Go %s, Python %s", i, got.Provider, want.Provider)
		}
		if !optionalStringsEqual(optionalUUIDString(got.RepoID), want.RepoID) {
			t.Errorf("issue[%d] repo_id: Go %v, Python %v", i, optionalUUIDString(got.RepoID), want.RepoID)
		}
		if got.Confidence != want.Confidence {
			t.Errorf("issue[%d] confidence: Go %v, Python %v", i, got.Confidence, want.Confidence)
		}
		if got.Source != want.Source {
			t.Errorf("issue[%d] source: Go %s, Python %s", i, got.Source, want.Source)
		}
		if got.Evidence != want.Evidence {
			t.Errorf("issue[%d] evidence: Go %s, Python %s", i, got.Evidence, want.Evidence)
		}
		if pythonISOFormat(got.ObservedAt) != want.ObservedAt {
			t.Errorf("issue[%d] observed_at: Go %s, Python %s", i, pythonISOFormat(got.ObservedAt), want.ObservedAt)
		}
	}
}
