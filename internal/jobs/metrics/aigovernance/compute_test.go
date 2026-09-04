package aigovernance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

// runPythonOracle executes testdata/python_governance_oracle.py and, on
// success, writes a proof marker into DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR
// -- required, not merely read: ci/check_go.sh's live-python-oracles verb
// checks the marker exists and reads "executed" after the run, so this oracle
// cannot be silently skipped, renamed, or filtered out of a -run pattern
// without the standing gate noticing. Same shape as
// internal/jobs/metrics/testops/compute_test.go's helper of the same name.
func runPythonOracle(t *testing.T, markerName string) pythonOracleOutput {
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
		t.Fatal("PYTHON is required for the live ai_governance Python oracle")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, filepath.Join("testdata", "python_governance_oracle.py"))
	command.Dir = filepath.Join(root, "internal", "jobs", "metrics", "aigovernance")
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
	var decoded pythonOracleOutput
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode production Python oracle output %q: %v", output, err)
	}
	if writeErr := os.WriteFile(filepath.Join(proofDirectory, markerName), []byte("executed"), 0o644); writeErr != nil {
		t.Fatalf("write live-python-oracle proof: %v", writeErr)
	}
	return decoded
}

type pythonOracleOutput struct {
	Violations []pythonViolation `json:"violations"`
	Coverage   []pythonCoverage  `json:"coverage"`
}

type pythonViolation struct {
	OrgID       string  `json:"org_id"`
	TeamID      *string `json:"team_id"`
	RepoID      *string `json:"repo_id"`
	RuleID      string  `json:"rule_id"`
	Severity    string  `json:"severity"`
	SubjectType string  `json:"subject_type"`
	SubjectID   string  `json:"subject_id"`
	ObservedAt  string  `json:"observed_at"`
	Evidence    string  `json:"evidence"`
}

type pythonCoverage struct {
	OrgID              string  `json:"org_id"`
	TeamID             *string `json:"team_id"`
	RepoID             *string `json:"repo_id"`
	Day                string  `json:"day"`
	AIArtifacts        uint64  `json:"ai_artifacts"`
	DeclaredArtifacts  uint64  `json:"declared_artifacts"`
	HumanReviewedPRs   uint64  `json:"human_reviewed_prs"`
	SecurityScannedPRs uint64  `json:"security_scanned_prs"`
	InPolicyArtifacts  uint64  `json:"in_policy_artifacts"`
}

// ---------------------------------------------------------------------------
// Fixture -- MUST stay byte-identical to testdata/python_governance_oracle.py.
// Any edit to one side without the other stops the oracle proving anything
// (the same rule testops' oracle fixtures carry).
// ---------------------------------------------------------------------------

const (
	fixtureOrgID = "70d529e0-3c06-4597-8480-794fd02328b6"
	// float32(0.95) widened to float64 -- what the ClickHouse driver hands
	// Python before json.dumps renders it. Written as the decimal expansion
	// rather than as float64(float32(0.95)) so the test asserts the VALUE the
	// oracle emits, not the Go expression's own arithmetic.
	fixtureConfidence = 0.949999988079071
)

var (
	fixtureRepoA = uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	// Sorts BEFORE the literal "None" that str(None) yields for a null repo,
	// while repoA's sorts after -- pinning rollup.py:32-35's sort key.
	fixtureRepoB = uuid.MustParse("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")
	fixtureDay   = time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
)

func fixtureArtifact(mutate func(*Artifact)) Artifact {
	confidence := fixtureConfidence
	toolName, modelName := "copilot", "gpt-4o"
	humanReviewed, securityScanned := true, true
	source, kind := "pr_label", "ai_assisted"
	repoID := fixtureRepoA
	artifact := Artifact{
		OrgID:                      fixtureOrgID,
		SubjectType:                "pull_request",
		SubjectID:                  "1",
		ObservedAt:                 time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC),
		TeamID:                     nil,
		RepoID:                     &repoID,
		AIDetected:                 true,
		DeclaredAI:                 true,
		HumanReviewed:              &humanReviewed,
		SensitiveRepo:              false,
		RepoAllowsAI:               true,
		SecurityScanned:            &securityScanned,
		LicenseOrDependencyFinding: false,
		ToolName:                   &toolName,
		ModelName:                  &modelName,
		ToolAllowlistStatus:        AllowlistAllowed,
		Evidence: ArtifactEvidence{
			Source: &source, Kind: &kind, Confidence: &confidence, ArtifactURL: nil,
		},
	}
	if mutate != nil {
		mutate(&artifact)
	}
	return artifact
}

func boolPtr(v bool) *bool       { return &v }
func stringPtr(v string) *string { return &v }

func fixtureArtifacts() []Artifact {
	repoB := fixtureRepoB
	return []Artifact{
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "1" }),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "2"; a.DeclaredAI = false }),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "3"; a.HumanReviewed = nil }),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "4"; a.SecurityScanned = nil }),
		fixtureArtifact(func(a *Artifact) {
			a.SubjectID = "abc123"
			a.SubjectType = "commit"
			a.HumanReviewed = nil
			a.SecurityScanned = nil
		}),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "6"; a.ToolAllowlistStatus = AllowlistDisallowed }),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "7"; a.SensitiveRepo = true; a.RepoAllowsAI = false }),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "8"; a.SensitiveRepo = true; a.RepoAllowsAI = true }),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "9"; a.LicenseOrDependencyFinding = true }),
		fixtureArtifact(func(a *Artifact) {
			a.SubjectID = "10"
			a.DeclaredAI = false
			a.HumanReviewed = boolPtr(false)
			a.SensitiveRepo = true
			a.RepoAllowsAI = false
			a.ToolAllowlistStatus = AllowlistDisallowed
			a.SecurityScanned = boolPtr(false)
			a.LicenseOrDependencyFinding = true
		}),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "11"; a.TeamID = nil; a.RepoID = &repoB }),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "12"; a.TeamID = stringPtr(""); a.RepoID = &repoB }),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "13"; a.RepoID = nil }),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "14"; a.TeamID = stringPtr("team-alpha") }),
		fixtureArtifact(func(a *Artifact) {
			a.SubjectID = "15"
			a.DeclaredAI = false
			a.ObservedAt = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
		}),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "16"; a.AIDetected = false; a.DeclaredAI = false }),
		fixtureArtifact(func(a *Artifact) { a.SubjectID = "17"; a.ToolName = nil; a.ModelName = nil }),
		// 19/20 carry MILLISECOND precision -- see the oracle script. Every
		// other artifact is an exact second, which is what let a
		// Truncate(time.Second) mutation pass both checks.
		fixtureArtifact(func(a *Artifact) {
			a.SubjectID = "19"
			a.DeclaredAI = false
			a.ObservedAt = time.Date(2026, 9, 3, 12, 0, 0, 123000000, time.UTC)
		}),
		fixtureArtifact(func(a *Artifact) {
			a.SubjectID = "20"
			a.ObservedAt = time.Date(2026, 9, 3, 23, 59, 59, 999000000, time.UTC)
		}),
		fixtureArtifact(func(a *Artifact) {
			a.SubjectID = "18"
			zero := 0.0
			a.Evidence.Source = stringPtr("pr_body")
			a.Evidence.Confidence = &zero
		}),
	}
}

// TestGovernanceRowsMatchLivePythonProduction is the family's live-Python rot
// guard. It compares every persisted column of both output tables EXCEPT
// event_id and computed_at, for the reasons stated at length in the oracle
// script's module docstring: Python's event_id is uuid4() so there is no
// Python answer to compare against, and computed_at is now() on both sides
// (the standing rot-guard rule: compare the payload, never provenance).
// event_id's own contract is pinned by TestDeriveEventIDIsStableAndKeyDependent.
func TestGovernanceRowsMatchLivePythonProduction(t *testing.T) {
	want := runPythonOracle(t, "ai-governance-golden")
	artifacts := fixtureArtifacts()

	gotViolations := EvaluateArtifacts(artifacts)
	if len(gotViolations) != len(want.Violations) {
		t.Fatalf("violation count: go=%d python=%d", len(gotViolations), len(want.Violations))
	}
	for index, wantViolation := range want.Violations {
		got := gotViolations[index]
		gotEvidence, err := got.EvidenceJSON()
		if err != nil {
			t.Fatalf("violation %d: encode evidence: %v", index, err)
		}
		gotRow := pythonViolation{
			OrgID:       got.OrgID,
			TeamID:      got.TeamID,
			RepoID:      uuidStringPtr(got.RepoID),
			RuleID:      string(got.RuleID),
			Severity:    string(got.Severity),
			SubjectType: got.SubjectType,
			SubjectID:   got.SubjectID,
			ObservedAt:  pythonISOFormat(got.ObservedAt),
			Evidence:    gotEvidence,
		}
		assertJSONEqual(t, fmt.Sprintf("violation %d", index), wantViolation, gotRow)
	}

	gotCoverage := RollupCoverageDaily(artifacts, fixtureDay)
	if len(gotCoverage) != len(want.Coverage) {
		t.Fatalf("coverage count: go=%d python=%d", len(gotCoverage), len(want.Coverage))
	}
	for index, wantRow := range want.Coverage {
		got := gotCoverage[index]
		gotRow := pythonCoverage{
			OrgID:              got.OrgID,
			TeamID:             got.TeamID,
			RepoID:             uuidStringPtr(got.RepoID),
			Day:                got.Day.Format("2006-01-02"),
			AIArtifacts:        got.AIArtifacts,
			DeclaredArtifacts:  got.DeclaredArtifacts,
			HumanReviewedPRs:   got.HumanReviewedPRs,
			SecurityScannedPRs: got.SecurityScannedPRs,
			InPolicyArtifacts:  got.InPolicyArtifacts,
		}
		assertJSONEqual(t, fmt.Sprintf("coverage %d", index), wantRow, gotRow)
	}
}

// TestCoverageRowOrderPutsNullRepoBetweenTheTwoUUIDs pins the one ordering
// property a count-based assertion structurally cannot see: Python sorts by
// str(repo_id), and str(None) is the literal "None", whose 'N' (0x4E) sorts
// AFTER ASCII digits and BEFORE lowercase hex letters. So a null-repo group
// lands in the MIDDLE of the UUID range, not at either end.
//
// This is asserted independently of the live oracle because the oracle only
// runs under ci/check_go.sh's live-python-oracles verb; a plain `go test`
// would otherwise never exercise the rule at all.
func TestCoverageRowOrderPutsNullRepoBetweenTheTwoUUIDs(t *testing.T) {
	rows := RollupCoverageDaily(fixtureArtifacts(), fixtureDay)
	var order []string
	for _, row := range rows {
		if row.RepoID == nil {
			order = append(order, "None")
			continue
		}
		order = append(order, row.RepoID.String())
	}
	want := []string{
		fixtureRepoB.String(), // 0a1b... team=nil
		fixtureRepoB.String(), // 0a1b... team=""  (sort tie, insertion order breaks it)
		"None",                // null repo sorts between the two UUIDs
		fixtureRepoA.String(), // d4f3... team=nil
		fixtureRepoA.String(), // d4f3... team="team-alpha"
	}
	if len(order) != len(want) {
		t.Fatalf("group count %d, want %d (%v)", len(order), len(want), order)
	}
	for index := range want {
		if order[index] != want[index] {
			t.Fatalf("group %d is %s, want %s\nfull order: %v", index, order[index], want[index], order)
		}
	}
}

// TestNilAndEmptyTeamAreDistinctGroupsWithATiedSortKey pins the other half of
// the ordering contract: None and "" are the SAME sort value (`team_id or ""`)
// but DIFFERENT dict keys, so they are two rows whose relative order comes
// only from Python's stable sort over insertion order. A Go port using an
// unstable sort would pass the count assertion and reorder these two.
func TestNilAndEmptyTeamAreDistinctGroupsWithATiedSortKey(t *testing.T) {
	rows := RollupCoverageDaily(fixtureArtifacts(), fixtureDay)
	if rows[0].TeamID != nil {
		t.Fatalf("row 0 team_id = %q, want nil (inserted first)", *rows[0].TeamID)
	}
	if rows[1].TeamID == nil || *rows[1].TeamID != "" {
		t.Fatalf("row 1 team_id = %v, want the empty string (inserted second)", rows[1].TeamID)
	}
	if rows[0].AIArtifacts != 1 || rows[1].AIArtifacts != 1 {
		t.Fatalf("nil-team and empty-team groups merged: %d and %d artifacts",
			rows[0].AIArtifacts, rows[1].AIArtifacts)
	}
}

// TestHasViolationsAgreesWithEvaluateArtifact is the property that keeps
// rollup.py:54's `not evaluate_artifact(a)` fast path honest. hasViolations
// duplicates EvaluateArtifact's predicate set for speed; a predicate added to
// one and not the other is exactly the "two implementations of one concept
// that disagree" trap. Generated over the full cross product of the inputs
// every predicate reads, so it cannot be satisfied by a lucky fixture.
func TestHasViolationsAgreesWithEvaluateArtifact(t *testing.T) {
	tri := []*bool{nil, boolPtr(false), boolPtr(true)}
	subjectTypes := []string{"pull_request", "commit"}
	statuses := []ToolAllowlistStatus{AllowlistAllowed, AllowlistDisallowed, AllowlistUnknown}
	checked := 0
	for _, aiDetected := range []bool{true, false} {
		for _, declared := range []bool{true, false} {
			for _, subjectType := range subjectTypes {
				for _, humanReviewed := range tri {
					for _, securityScanned := range tri {
						for _, sensitive := range []bool{true, false} {
							for _, allows := range []bool{true, false} {
								for _, status := range statuses {
									for _, finding := range []bool{true, false} {
										artifact := fixtureArtifact(func(a *Artifact) {
											a.AIDetected = aiDetected
											a.DeclaredAI = declared
											a.SubjectType = subjectType
											a.HumanReviewed = humanReviewed
											a.SecurityScanned = securityScanned
											a.SensitiveRepo = sensitive
											a.RepoAllowsAI = allows
											a.ToolAllowlistStatus = status
											a.LicenseOrDependencyFinding = finding
										})
										want := len(EvaluateArtifact(artifact)) > 0
										if got := hasViolations(artifact); got != want {
											t.Fatalf("hasViolations=%v but EvaluateArtifact produced %d violations for %+v",
												got, len(EvaluateArtifact(artifact)), artifact)
										}
										checked++
									}
								}
							}
						}
					}
				}
			}
		}
	}
	if checked != 2*2*2*3*3*2*2*3*2 {
		t.Fatalf("cross product covered %d combinations, expected the full grid", checked)
	}
}

// TestDeriveEventIDIsStableAndKeyDependent pins the deterministic event_id
// contract (design.md Q1). Two properties, both load-bearing:
//
//  1. STABLE -- the same key always derives the same id. This is what makes
//     ai_policy_events' ReplacingMergeTree actually dedup, and therefore what
//     makes the once-per-partition rewrite (AIGovernanceExecutor's doc
//     comment) idempotent instead of unbounded growth.
//  2. KEY-DEPENDENT -- changing ANY component of the ORDER BY key changes the
//     id. Without this, two genuinely different policy events could collide
//     onto one dedup key and one would silently disappear. Asserted per
//     component, not in aggregate, so a component dropped from the derivation
//     is caught individually.
func TestDeriveEventIDIsStableAndKeyDependent(t *testing.T) {
	base := fixtureArtifact(nil)
	baseID := deriveEventID(base, RuleMissingAIDeclaration)

	if again := deriveEventID(fixtureArtifact(nil), RuleMissingAIDeclaration); again != baseID {
		t.Fatalf("event id is not stable: %s then %s", baseID, again)
	}

	otherRepo := fixtureRepoB
	mutations := map[string]func(*Artifact){
		"org_id":       func(a *Artifact) { a.OrgID = "other-org" },
		"team_id":      func(a *Artifact) { a.TeamID = stringPtr("team-beta") },
		"repo_id":      func(a *Artifact) { a.RepoID = &otherRepo },
		"repo_id_nil":  func(a *Artifact) { a.RepoID = nil },
		"subject_type": func(a *Artifact) { a.SubjectType = "commit" },
		"subject_id":   func(a *Artifact) { a.SubjectID = "999" },
		"observed_at":  func(a *Artifact) { a.ObservedAt = a.ObservedAt.Add(time.Millisecond) },
	}
	for name, mutate := range mutations {
		if got := deriveEventID(fixtureArtifact(mutate), RuleMissingAIDeclaration); got == baseID {
			t.Fatalf("changing %s did not change the event id (%s) -- that component is not in the derivation", name, got)
		}
	}
	if got := deriveEventID(base, RuleMissingHumanReview); got == baseID {
		t.Fatalf("changing rule_id did not change the event id (%s)", got)
	}
}

// TestDeriveEventIDIsInjectiveAcrossDelimiterHostileKeys guards the specific
// way a naive concatenation would break: subject_id is opaque provider text
// and may contain whatever the delimiter is. The length-prefixed encoding
// makes the joined form injective; a plain separator would let these two
// distinct keys render identically and collide onto one dedup key.
func TestDeriveEventIDIsInjectiveAcrossDelimiterHostileKeys(t *testing.T) {
	seen := map[uuid.UUID]string{}
	hostile := []struct{ team, subject string }{
		{"a", "b:c"}, {"a:b", "c"},
		{"", "1:2"}, {"1", ":2"},
		{"x|y", "z"}, {"x", "y|z"},
		{"12", "3"}, {"1", "23"},
	}
	for _, entry := range hostile {
		id := deriveEventID(fixtureArtifact(func(a *Artifact) {
			a.TeamID = stringPtr(entry.team)
			a.SubjectID = entry.subject
		}), RuleMissingAIDeclaration)
		label := fmt.Sprintf("team=%q subject=%q", entry.team, entry.subject)
		if previous, clash := seen[id]; clash {
			t.Fatalf("event id collision between %s and %s -- the key encoding is not injective", previous, label)
		}
		seen[id] = label
	}
}

// TestRollupIsOrderInvariantOverItsInput is the order-invariance proof the
// design note promised. The loader's ORDER BY makes the input deterministic,
// but the ROLLUP must not depend on it beyond the documented insertion-order
// tie-break -- otherwise a future loader change would silently move rows.
// Shuffling only artifacts that land in DISTINCT groups keeps the one genuine
// insertion-order dependency (the nil/"" team tie) out of the property.
func TestRollupIsOrderInvariantOverItsInput(t *testing.T) {
	base := fixtureArtifacts()
	// Drop the empty-string-team artifact, whose group ties with the nil-team
	// group on the sort key and is therefore legitimately order-sensitive.
	var stable []Artifact
	for _, artifact := range base {
		if artifact.TeamID != nil && *artifact.TeamID == "" {
			continue
		}
		stable = append(stable, artifact)
	}
	want := RollupCoverageDaily(stable, fixtureDay)

	random := rand.New(rand.NewSource(20260904))
	for attempt := 0; attempt < 50; attempt++ {
		shuffled := append([]Artifact(nil), stable...)
		random.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		got := RollupCoverageDaily(shuffled, fixtureDay)
		assertJSONEqual(t, fmt.Sprintf("shuffle %d", attempt), want, got)
	}
}

// pythonISOFormat mirrors CPython's datetime.isoformat() for a tz-aware UTC
// value, INCLUDING its fractional-second rule: omitted entirely when the
// microsecond is zero, and exactly six digits otherwise (Python never trims
// trailing zeros, so 123000us renders ".123000", not ".123").
//
// The comparator previously used a seconds-only layout. That silently DISCARDED
// the fraction on both sides of the comparison, so a mutation truncating
// observed_at to whole seconds passed the oracle AND the frozen fixture --
// every fixture artifact happened to sit on an exact second. Found by codex
// round 1 on #2229 (P2), which is exactly the oracle-attack class the prompt
// asked for: a field the fixture never varies and the comparator throws away.
//
// Go's ".999999" layout verb would trim trailing zeros and diverge from Python
// on any value whose microseconds end in zero -- which, for a DateTime64(3)
// column, is EVERY non-zero value. Hence the explicit branch.
func pythonISOFormat(value time.Time) string {
	utc := value.UTC()
	if utc.Nanosecond() == 0 {
		return utc.Format("2006-01-02T15:04:05+00:00")
	}
	return utc.Format("2006-01-02T15:04:05.000000+00:00")
}

func uuidStringPtr(value *uuid.UUID) *string {
	if value == nil {
		return nil
	}
	rendered := value.String()
	return &rendered
}

func assertJSONEqual(t *testing.T, label string, want, got any) {
	t.Helper()
	wantJSON, _ := json.Marshal(want)
	gotJSON, _ := json.Marshal(got)
	if !bytes.Equal(wantJSON, gotJSON) {
		t.Fatalf("%s mismatch:\nwant=%s\ngot= %s", label, wantJSON, gotJSON)
	}
}
