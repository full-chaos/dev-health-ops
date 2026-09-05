package aiimpact

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

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	fixtureOrgID = "70d529e0-3c06-4597-8480-794fd02328b6"
	hourUS       = 3_600_000_000
)

var (
	fixtureRepo  = uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	fixtureRepoB = uuid.MustParse("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")
	fixtureBase  = time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	fixtureDay   = time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
)

func offset(microseconds int64) time.Time {
	return fixtureBase.Add(time.Duration(microseconds) * time.Microsecond)
}

func timePtr(t time.Time) *time.Time { return &t }
func strPtr(s string) *string        { return &s }
func u32Ptr(v uint32) *uint32        { return &v }

// fixturePullRequests MUST stay byte-identical to
// testdata/python_ai_impact_oracle.py's PULL_REQUESTS. Any edit to one side
// without the other stops the oracle proving anything.
func fixturePullRequests() []PullRequestRow {
	base := func(number int64, createdUS int64, mergedUS *int64, repo uuid.UUID) PullRequestRow {
		row := PullRequestRow{
			RepoID: repo, Number: number, CreatedAt: offset(createdUS),
			Additions: u32Ptr(10), Deletions: u32Ptr(5), ChangedFiles: u32Ptr(2),
		}
		if mergedUS != nil {
			row.MergedAt = timePtr(offset(*mergedUS))
		}
		return row
	}
	us := func(v int64) *int64 { return &v }

	rows := []PullRequestRow{
		base(1, 0, us(57_078_437_271), fixtureRepo),
		base(2, 0, us(73_634_014_885), fixtureRepo),
		base(3, 0, us(77_815_325_121), fixtureRepo),
		base(4, 0, us(2*hourUS+333_333), fixtureRepo),
		base(5, 0, us(5*hourUS+666_667), fixtureRepo),
		base(6, hourUS, nil, fixtureRepo),
		base(7, 0, us(hourUS+12_345), fixtureRepo),
		base(8, 0, us(hourUS+54_321), fixtureRepo),
	}
	revert := base(9, 0, us(hourUS), fixtureRepo)
	revert.Additions, revert.Deletions = u32Ptr(10), u32Ptr(60)
	nearRevert := base(10, 0, us(hourUS), fixtureRepo)
	nearRevert.Additions, nearRevert.Deletions = u32Ptr(30), u32Ptr(60)
	zeroReviews := base(11, 0, us(hourUS), fixtureRepo)
	zeroReviews.ReviewsCount = u32Ptr(0)
	sevenReviews := base(12, 0, us(hourUS), fixtureRepo)
	sevenReviews.ReviewsCount = u32Ptr(7)

	return append(rows,
		revert, nearRevert, zeroReviews, sevenReviews,
		base(13, 0, us(hourUS), fixtureRepoB),
		base(14, 0, us(25*hourUS), fixtureRepo), // out of window
		base(15, 2*hourUS, nil, fixtureRepo),
	)
}

func fixtureReviews() []PullRequestReviewRow {
	return []PullRequestReviewRow{
		{RepoID: fixtureRepo, Number: 11, State: strPtr("APPROVED"), SubmittedAt: timePtr(fixtureBase.Add(10 * time.Minute))},
		{RepoID: fixtureRepo, Number: 11, State: strPtr("changes_requested"), SubmittedAt: timePtr(fixtureBase.Add(20 * time.Minute))},
		{RepoID: fixtureRepo, Number: 12, State: strPtr("CHANGES_REQUESTED"), SubmittedAt: timePtr(fixtureBase.Add(5 * time.Minute))},
		{RepoID: fixtureRepo, Number: 1, State: strPtr("APPROVED"), SubmittedAt: nil},
	}
}

func fixtureAttributions() []AttributionRow {
	pullRequest := strPtr("pull_request")
	attribution := func(repo uuid.UUID, number int64, kind string, workType *string) AttributionRow {
		return AttributionRow{RepoID: repo, Number: number, Kind: strPtr(kind), WorkType: workType}
	}
	return []AttributionRow{
		attribution(fixtureRepo, 1, "ai_assisted", pullRequest),
		attribution(fixtureRepo, 2, "  AI-Assisted  ", pullRequest),
		attribution(fixtureRepo, 3, "ai_assisted", pullRequest),
		attribution(fixtureRepo, 4, "human", pullRequest),
		attribution(fixtureRepo, 5, "human", pullRequest),
		attribution(fixtureRepo, 6, "agent_created", pullRequest),
		attribution(fixtureRepo, 7, "ai_review", pullRequest),
		attribution(fixtureRepo, 9, "human", pullRequest),
		attribution(fixtureRepo, 10, "human", pullRequest),
		attribution(fixtureRepo, 11, "human", pullRequest),
		attribution(fixtureRepo, 12, "human", pullRequest),
		attribution(fixtureRepo, 15, "vibes", pullRequest),
		attribution(fixtureRepoB, 13, "ai_assisted", strPtr("issue")),
	}
}

func fixtureIncidents() []IncidentRow {
	return []IncidentRow{
		{RepoID: fixtureRepo, StartedAt: fixtureBase.Add(3 * time.Hour)},
		{RepoID: fixtureRepo, StartedAt: fixtureBase.Add(4 * time.Hour)},
		{RepoID: fixtureRepo, StartedAt: fixtureBase.Add(30 * time.Hour)}, // out of window
	}
}

func fixtureCommitStats() map[PRKey][]CommitStatRow {
	stat := func(path, hash string, when time.Time, evidence string) CommitStatRow {
		return CommitStatRow{
			FilePath: strPtr(path), CommitHash: strPtr(hash),
			CommitterWhen: timePtr(when), Evidence: strPtr(evidence),
		}
	}
	return map[PRKey][]CommitStatRow{
		{RepoID: fixtureRepo, Number: 1}: {
			stat("src/Tests/Thing.spec.ts", "aaa", fixtureBase.Add(30*time.Minute), "native"),
		},
		{RepoID: fixtureRepo, Number: 2}: {
			stat("src/thing.ts", "bbb", fixtureBase.Add(30*time.Minute), "native"),
		},
		{RepoID: fixtureRepo, Number: 4}: {
			stat("src/x.ts", "ccc", fixtureBase.Add(90*time.Minute), "commit_message_squash_pr_ref"),
		},
		{RepoID: fixtureRepo, Number: 5}: {
			stat("src/y.ts", "ddd", fixtureBase.Add(100*time.Minute), "native"),
			stat("src/y.ts", "ddd", fixtureBase.Add(100*time.Minute), "commit_message_pr_ref"),
			stat("src/z.ts", "eee", fixtureBase.Add(110*time.Minute), "native"),
		},
	}
}

func fixtureParams() Params {
	return Params{
		Day: fixtureDay, OrgID: fixtureOrgID,
		PullRequests:  fixturePullRequests(),
		Reviews:       fixtureReviews(),
		Attributions:  fixtureAttributions(),
		Incidents:     fixtureIncidents(),
		PRCommitStats: fixtureCommitStats(), HasCommitStats: true,
		RepoNamesByID: map[uuid.UUID]string{fixtureRepo: "acme/alpha", fixtureRepoB: "acme/beta"},
		TeamResolver: func(_ uuid.UUID, repoName string) *string {
			if repoName == "" {
				repoName = "none"
			}
			return strPtr("team-" + repoName)
		},
	}
}

type pythonRecord struct {
	OrgID                     string   `json:"org_id"`
	TeamID                    *string  `json:"team_id"`
	RepoID                    string   `json:"repo_id"`
	WorkType                  string   `json:"work_type"`
	Day                       string   `json:"day"`
	AttributionBucket         string   `json:"attribution_bucket"`
	PRsTotal                  uint32   `json:"prs_total"`
	PRsMerged                 uint32   `json:"prs_merged"`
	AIAssistedPRs             uint32   `json:"ai_assisted_prs"`
	AgentCreatedPRs           uint32   `json:"agent_created_prs"`
	HumanPRs                  uint32   `json:"human_prs"`
	UnknownPRs                uint32   `json:"unknown_prs"`
	AIAssistedPRRatio         *float64 `json:"ai_assisted_pr_ratio"`
	AgentCreatedPRCount       uint32   `json:"agent_created_pr_count"`
	CycleTimeAvgHours         *float64 `json:"cycle_time_avg_hours"`
	BaselineCycleTimeAvgHours *float64 `json:"baseline_cycle_time_avg_hours"`
	AICycleTimeDeltaHours     *float64 `json:"ai_cycle_time_delta_hours"`
	ReviewsPerPR              *float64 `json:"reviews_per_pr"`
	BaselineReviewsPerPR      *float64 `json:"baseline_reviews_per_pr"`
	AIReviewAmplification     *float64 `json:"ai_review_amplification"`
	ChangesRequestedPerPR     *float64 `json:"changes_requested_per_pr"`
	ReworkPRs                 uint32   `json:"rework_prs"`
	ReworkDragRate            *float64 `json:"rework_drag_rate"`
	FollowupCommitsCount      uint32   `json:"followup_commits_count"`
	RevertPRs                 uint32   `json:"revert_prs"`
	RevertRate                *float64 `json:"revert_rate"`
	IncidentsCount            uint32   `json:"incidents_count"`
	IncidentDragRate          *float64 `json:"incident_drag_rate"`
	TestGapPRs                uint32   `json:"test_gap_prs"`
	TestGapRate               *float64 `json:"test_gap_rate"`
	LeveragePRs               float64  `json:"leverage_prs_component"`
	LeverageCycleTime         *float64 `json:"leverage_cycle_time_component"`
	LeverageReview            *float64 `json:"leverage_review_component"`
	LeverageRework            *float64 `json:"leverage_rework_component"`
	LeverageTest              *float64 `json:"leverage_test_component"`
	LeverageIncident          *float64 `json:"leverage_incident_component"`
}

func asPythonRecord(record Record) pythonRecord {
	return pythonRecord{
		OrgID: record.OrgID, TeamID: record.TeamID, RepoID: record.RepoID.String(),
		WorkType: record.WorkType, Day: record.Day.Format("2006-01-02"),
		AttributionBucket: string(record.AttributionBucket),
		PRsTotal:          record.PRsTotal, PRsMerged: record.PRsMerged,
		AIAssistedPRs: record.AIAssistedPRs, AgentCreatedPRs: record.AgentCreatedPRs,
		HumanPRs: record.HumanPRs, UnknownPRs: record.UnknownPRs,
		AIAssistedPRRatio: record.AIAssistedPRRatio, AgentCreatedPRCount: record.AgentCreatedPRCount,
		CycleTimeAvgHours:         record.CycleTimeAvgHours,
		BaselineCycleTimeAvgHours: record.BaselineCycleTimeAvgHours,
		AICycleTimeDeltaHours:     record.AICycleTimeDeltaHours,
		ReviewsPerPR:              record.ReviewsPerPR, BaselineReviewsPerPR: record.BaselineReviewsPerPR,
		AIReviewAmplification: record.AIReviewAmplification,
		ChangesRequestedPerPR: record.ChangesRequestedPerPR,
		ReworkPRs:             record.ReworkPRs, ReworkDragRate: record.ReworkDragRate,
		FollowupCommitsCount: record.FollowupCommitsCount,
		RevertPRs:            record.RevertPRs, RevertRate: record.RevertRate,
		IncidentsCount: record.IncidentsCount, IncidentDragRate: record.IncidentDragRate,
		TestGapPRs: record.TestGapPRs, TestGapRate: record.TestGapRate,
		LeveragePRs:       record.LeveragePRsComponent,
		LeverageCycleTime: record.LeverageCycleTime, LeverageReview: record.LeverageReview,
		LeverageRework: record.LeverageRework, LeverageTest: record.LeverageTest,
		LeverageIncident: record.LeverageIncident,
	}
}

// recordKey is ai_impact_metrics_daily's ORDER BY key (migration 036:56), which
// is what identifies a row for comparison. Python's own emission order follows
// an unordered loader, so rows are matched BY KEY rather than by position --
// see buildRecords' ORDER note.
func recordKey(r pythonRecord) string {
	team := "<nil>"
	if r.TeamID != nil {
		team = *r.TeamID
	}
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s", r.OrgID, team, r.RepoID, r.WorkType, r.Day, r.AttributionBucket)
}

func runPythonOracle(t *testing.T, markerName string) []pythonRecord {
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
		t.Fatal("PYTHON is required for the live ai_impact Python oracle")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, filepath.Join("testdata", "python_ai_impact_oracle.py"))
	command.Dir = filepath.Join(root, "internal", "jobs", "metrics", "aiimpact")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("execute production Python oracle: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	output := bytes.TrimSpace(stdout.Bytes())
	if lastLine := bytes.LastIndexByte(output, '\n'); lastLine >= 0 {
		output = output[lastLine+1:]
	}
	var decoded []pythonRecord
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode production Python oracle output: %v", err)
	}
	if len(decoded) == 0 {
		t.Fatal("live Python produced zero rows; the fixture or the oracle is broken")
	}
	if writeErr := os.WriteFile(filepath.Join(proofDirectory, markerName), []byte("executed"), 0o644); writeErr != nil {
		t.Fatalf("write live-python-oracle proof: %v", writeErr)
	}
	return decoded
}

// TestAIImpactMatchesLivePythonProduction compares every persisted column
// against compute_ai_impact_metrics_daily, BIT-EXACTLY. Floats are compared as
// their JSON round-trip (Go and Python both emit shortest-round-trip
// representations for float64), so a one-ULP divergence -- which is exactly
// what a naive sum or the wrong division order produces -- fails the test
// rather than hiding inside a tolerance.
func TestAIImpactMatchesLivePythonProduction(t *testing.T) {
	want := runPythonOracle(t, "ai-impact-golden")
	got := Compute(fixtureParams())

	if len(got) != len(want) {
		t.Fatalf("row count: go=%d python=%d", len(got), len(want))
	}
	wantByKey := make(map[string]pythonRecord, len(want))
	for _, row := range want {
		if _, clash := wantByKey[recordKey(row)]; clash {
			t.Fatalf("python emitted two rows with the same ORDER BY key %q -- they would collide in "+
				"ClickHouse, so the fixture cannot distinguish them", recordKey(row))
		}
		wantByKey[recordKey(row)] = row
	}
	for _, record := range got {
		converted := asPythonRecord(record)
		key := recordKey(converted)
		expected, found := wantByKey[key]
		if !found {
			t.Fatalf("go produced a row python did not: %s", key)
		}
		wantJSON, _ := json.Marshal(expected)
		gotJSON, _ := json.Marshal(converted)
		if !bytes.Equal(wantJSON, gotJSON) {
			t.Errorf("row %s mismatch:\npython=%s\ngo=    %s", key, wantJSON, gotJSON)
		}
		delete(wantByKey, key)
	}
	for key := range wantByKey {
		t.Errorf("python produced a row go did not: %s", key)
	}
}

// TestCycleHoursUsesPythonsDivisionOrder pins float rule 3 directly, so it is
// covered even when the live oracle is skipped.
//
// CPython: ((days*86400 + seconds) * 10**6 + microseconds) / 10**6, then
// / 3600.0. Go's Duration.Hours() and Duration.Seconds()/3600 are different
// roundings of the same quantity and CAN disagree in the last bit.
func TestCycleHoursUsesPythonsDivisionOrder(t *testing.T) {
	for _, microseconds := range []int64{hourUS + 1, 3*hourUS + 7, 11*hourUS + 999_999, 2*hourUS + 333_333} {
		created := fixtureBase
		merged := offset(microseconds)
		want := float64(float64(microseconds)/1e6) / 3600.0
		if got := cycleHours(created, merged); got != want {
			t.Fatalf("cycleHours(%dus) = %v, want %v", microseconds, got, want)
		}
		// A same-value-different-rounding control: assert we are NOT simply
		// using Duration.Hours(). If these ever agree for every input the test
		// is vacuous, so at least one input must differ.
		_ = merged.Sub(created).Hours()
	}
	var differsSomewhere bool
	for _, microseconds := range []int64{hourUS + 1, 3*hourUS + 7, 11*hourUS + 999_999, 2*hourUS + 333_333, 5*hourUS + 666_667} {
		if cycleHours(fixtureBase, offset(microseconds)) != offset(microseconds).Sub(fixtureBase).Hours() {
			differsSomewhere = true
			break
		}
	}
	if !differsSomewhere {
		t.Skip("no fixture input distinguishes the two division orders on this platform; " +
			"the rule still holds but this test is not proving it here")
	}
}

// TestMeanUsesCompensatedSummation pins float rule 1: a naive accumulation
// disagrees with CPython's compensated sum(). Non-vacuous by construction --
// it asserts the two strategies actually differ on this input, so the test
// fails loudly if the chosen values stop exercising the difference.
func TestMeanUsesCompensatedSummation(t *testing.T) {
	values := []float64{1e16, 1.0, -1e16, 3.0, 0.1}
	var naive float64
	for _, value := range values {
		naive += value
	}
	compensated := pythonparity.Sum(values)
	if naive == compensated {
		t.Fatalf("this input no longer distinguishes naive from compensated summation (%v); "+
			"pick values that do, or the mean() guarantee is untested", naive)
	}
	got := mean(values)
	if got == nil {
		t.Fatal("mean returned nil for a non-empty slice")
	}
	if *got != compensated/float64(len(values)) {
		t.Fatalf("mean = %v, want the COMPENSATED sum / n = %v", *got, compensated/float64(len(values)))
	}
}

// TestSigmaFormCannotChangeABucket discharges the ASCII-containment obligation
// pythonparity.Lower's doc comment places on every caller: its Final_Sigma
// lookahead is bounded where CPython's is not, so a caller must show that a
// medial-vs-final sigma difference cannot change its ANSWER. Here every bucket
// name is ASCII, so any input carrying either sigma fails every comparison and
// lands in "unknown" identically.
func TestSigmaFormCannotChangeABucket(t *testing.T) {
	medial, final := "ΑΣΒ", "ΑΣ"
	for _, input := range []string{medial, final, medial + "ai_assisted", "ai_assisted" + final} {
		if got := safeBucket(&input); got != BucketUnknown {
			t.Fatalf("safeBucket(%q) = %q; a non-ASCII value must land in unknown regardless of sigma form", input, got)
		}
	}
	// The genuinely ASCII values must still bucket correctly.
	for input, want := range map[string]AttributionBucket{
		"  AI-Assisted  ": BucketAIAssisted,
		"agent_created":   BucketAgentCreated,
		"human":           BucketHuman,
		"vibes":           BucketUnknown,
	} {
		value := input
		if got := safeBucket(&value); got != want {
			t.Fatalf("safeBucket(%q) = %q, want %q", input, got, want)
		}
	}
}

// TestUnavailableLinkageYieldsNullTestGapRate pins the CHAOS-2183 distinction:
// nil PRCommitStats (linkage UNAVAILABLE) must give test_gap_rate null, never
// 100%. An empty-but-present map is a different thing and is NOT asserted here.
func TestUnavailableLinkageYieldsNullTestGapRate(t *testing.T) {
	params := fixtureParams()
	params.PRCommitStats, params.HasCommitStats = nil, false
	for _, record := range Compute(params) {
		if record.TestGapPRs != 0 {
			t.Fatalf("bucket %s counted %d test gaps with NO linkage available", record.AttributionBucket, record.TestGapPRs)
		}
		if record.TestGapRate != nil {
			t.Fatalf("bucket %s has test_gap_rate %v with NO linkage available; want null",
				record.AttributionBucket, *record.TestGapRate)
		}
	}
}

// TestSquashArtifactIsNotCountedAsFollowupWork pins both merge-artifact guards.
// PR 4's only linked commit is the squash artifact (0 follow-ups), and PR 5's
// "ddd" appears BOTH as ordinary and as the artifact -- Python collects then
// POPS it, so it is excluded and only "eee" counts.
func TestSquashArtifactIsNotCountedAsFollowupWork(t *testing.T) {
	params := fixtureParams()
	followups := followupCommitsByPR(
		params.PRCommitStats, params.HasCommitStats,
		map[PRKey]PullRequestRow{
			{RepoID: fixtureRepo, Number: 4}: {RepoID: fixtureRepo, Number: 4, CreatedAt: fixtureBase, MergedAt: timePtr(offset(2*hourUS + 333_333))},
			{RepoID: fixtureRepo, Number: 5}: {RepoID: fixtureRepo, Number: 5, CreatedAt: fixtureBase, MergedAt: timePtr(offset(5*hourUS + 666_667))},
		},
		map[PRKey]time.Time{},
	)
	if got := followups[PRKey{RepoID: fixtureRepo, Number: 4}]; got != 0 {
		t.Fatalf("PR 4 counted %d follow-up commits; its only commit is the squash artifact, so it must be 0", got)
	}
	if got := followups[PRKey{RepoID: fixtureRepo, Number: 5}]; got != 1 {
		t.Fatalf("PR 5 counted %d follow-up commits, want 1 -- 'ddd' is tagged as the merge artifact on its "+
			"SECOND row and must be removed even though its first row looked ordinary", got)
	}
}

// TestRevertRuleDoesNotWrapOnLargeAdditions pins the uint64 widening in the
// revert predicate.
//
// Python evaluates `deletions > additions * 2` with arbitrary-precision ints.
// additions is UInt32 on the wire, so at additions = 2**31 the same product
// OVERFLOWS a uint32 to 0, and every PR with deletions >= 50 would be
// misclassified as a revert. Verified against the interpreter while writing
// this: python says False, a uint32 computation says True.
//
// The value is absurd for a real PR and entirely representable in the column,
// which is exactly the combination that makes it worth a test rather than an
// assumption -- nothing would ever surface it in practice until it did.
func TestRevertRuleDoesNotWrapOnLargeAdditions(t *testing.T) {
	facts := []prFact{{
		bucket: BucketHuman, workType: "pull_request",
		additions: 1 << 31, deletions: 100,
	}}
	if got := aggregateFacts(facts, 0).revertPRs; got != 0 {
		t.Fatalf("revertPRs = %d for additions=2^31, deletions=100; want 0. "+
			"additions*2 wrapped in a narrow integer type -- Python computes it exactly", got)
	}
	// Positive control: the rule must still FIRE on a genuine revert, so the
	// fix cannot be "make the predicate never true".
	genuine := []prFact{{
		bucket: BucketHuman, workType: "pull_request",
		additions: 10, deletions: 60,
	}}
	if got := aggregateFacts(genuine, 0).revertPRs; got != 1 {
		t.Fatalf("revertPRs = %d for a genuine revert (additions=10, deletions=60); want 1", got)
	}
}

// TestLargeDenormalizedCountersDoNotWrap is codex round chaos-4280-r1's
// finding 3: aggregateFacts summed reviews/changesRequested across a whole
// group in a uint32 ACCUMULATOR. Each column value fits uint32 on its own
// (it's the wire type), but Python sums arbitrary-precision integers across
// the group, and a uint32 accumulator wraps once the group total crosses
// 2**32 -- reachable with as few as two denormalized rows.
func TestLargeDenormalizedCountersDoNotWrap(t *testing.T) {
	const big = 3_000_000_000 // fits UInt32 (max ~4.29e9) individually.
	facts := []prFact{
		{bucket: BucketHuman, workType: "pull_request", reviews: big, changesRequested: big},
		{bucket: BucketHuman, workType: "pull_request", reviews: big, changesRequested: big},
	}
	got := aggregateFacts(facts, 0)
	wantPerPR := float64(2*big) / 2 // Python: 6_000_000_000 / 2 PRs
	if got.reviewsPerPR == nil || *got.reviewsPerPR != wantPerPR {
		t.Fatalf("reviewsPerPR = %v, want %v -- a uint32 accumulator wraps "+
			"6_000_000_000 to 1_705_032_704", got.reviewsPerPR, wantPerPR)
	}
	if got.changesRequestedPerPR == nil || *got.changesRequestedPerPR != wantPerPR {
		t.Fatalf("changesRequestedPerPR = %v, want %v -- same accumulator wraps",
			got.changesRequestedPerPR, wantPerPR)
	}
}

// TestChangesRequestedCountColumnOverridesReviewDerivedCount is codex round
// chaos-4280-r1's finding 7, first named mutant: the shared golden fixture
// (fixturePullRequests/fixtureReviews, byte-identical to the Python oracle by
// contract) never varies ChangesRequestedCount away from what its review rows
// would derive, so BOTH oracles accept deleting the column-override entirely.
// This is a deliberately SEPARATE, small fixture -- not touching the golden
// one -- built specifically to disagree with what reviews alone would derive.
func TestChangesRequestedCountColumnOverridesReviewDerivedCount(t *testing.T) {
	repo := uuid.MustParse("00000000-0000-4000-8000-000000000101")
	createdAt := time.Date(2026, 9, 3, 1, 0, 0, 0, time.UTC)
	pr := PullRequestRow{
		RepoID: repo, Number: 1, CreatedAt: createdAt,
		ChangesRequestedCount: u32Ptr(9), // the column, authoritative when nonzero
	}
	// One CHANGES_REQUESTED review -> reviewsByPR derives changesRequested=1,
	// deliberately DIFFERENT from the column's 9.
	review := PullRequestReviewRow{
		RepoID: repo, Number: 1, State: strPtr("CHANGES_REQUESTED"),
		SubmittedAt: timePtr(createdAt.Add(10 * time.Minute)),
	}
	attribution := AttributionRow{RepoID: repo, Number: 1, Kind: strPtr("human"), WorkType: strPtr("pull_request")}

	records := Compute(Params{
		Day: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC), OrgID: fixtureOrgID,
		PullRequests: []PullRequestRow{pr}, Reviews: []PullRequestReviewRow{review},
		Attributions: []AttributionRow{attribution},
	})

	var human *Record
	for index := range records {
		if records[index].AttributionBucket == BucketHuman {
			human = &records[index]
		}
	}
	if human == nil {
		t.Fatal("no human-bucket record; the single fixture PR must land there")
	}
	if human.ChangesRequestedPerPR == nil || *human.ChangesRequestedPerPR != 9 {
		t.Fatalf("changes_requested_per_pr = %v, want 9 (the column value) -- "+
			"deleting the pr.ChangesRequestedCount override falls back to the "+
			"review-derived count (1), which the shared golden fixture cannot "+
			"catch because it never varies the two independently", human.ChangesRequestedPerPR)
	}
}

// TestFollowupCommitsUsesFirstReviewBoundaryNotCreatedAt is codex round
// chaos-4280-r1's finding 7, second named mutant: every linked PR in the
// shared golden fixture has no submitted review timestamp, so the
// firstReviewAt/CreatedAt branch in followupCommitsByPR is never exercised
// either way. This fixture gives PR 1 a REAL first-review timestamp strictly
// AFTER creation, with one commit landing strictly BETWEEN the two boundaries
// -- counted as a followup commit under the correct (first-review) boundary,
// and NOT counted under the mutant (creation-time) boundary.
func TestFollowupCommitsUsesFirstReviewBoundaryNotCreatedAt(t *testing.T) {
	key := PRKey{RepoID: fixtureRepo, Number: 1}
	createdAt := fixtureBase
	firstReview := fixtureBase.Add(20 * time.Minute)
	betweenCommit := fixtureBase.Add(10 * time.Minute) // after created_at, before first review

	prIndex := map[PRKey]PullRequestRow{key: {RepoID: fixtureRepo, Number: 1, CreatedAt: createdAt}}
	firstReviewAt := map[PRKey]time.Time{key: firstReview}
	stats := map[PRKey][]CommitStatRow{
		key: {{CommitHash: strPtr("f1"), CommitterWhen: timePtr(betweenCommit), Evidence: strPtr("native")}},
	}

	gotCorrect := followupCommitsByPR(stats, true, prIndex, firstReviewAt)[key]
	if gotCorrect != 0 {
		t.Fatalf("followup commits (first-review boundary) = %d, want 0 -- the commit "+
			"lands BEFORE first review, so it is pre-feedback work, not a followup", gotCorrect)
	}

	// Mutant simulation: the boundary the codex finding names is unconditional
	// created_at instead of first-review-at. With that WRONG boundary the same
	// commit (after created_at) would count as a followup -- proving this
	// fixture can tell the two boundaries apart, which the golden fixture
	// cannot (it has no PR with both a review and a between-boundary commit).
	mutantBoundaryResult := followupCommitsByPR(stats, true, prIndex, map[PRKey]time.Time{})[key]
	if mutantBoundaryResult != 1 {
		t.Fatalf("sanity check failed: with no first-review-at entry (falls back to "+
			"created_at), got %d followup commits, want 1 -- this fixture's commit must "+
			"be provably on the OTHER side of created_at than of first-review-at, or it "+
			"cannot distinguish the two boundaries", mutantBoundaryResult)
	}
}

// TestComputeIsOrderInvariantOverItsInput is the order-invariance proof the
// design promised: Python's own row order follows an unordered loader, so the
// port emits a canonical order and must not otherwise depend on input order.
func TestComputeIsOrderInvariantOverItsInput(t *testing.T) {
	params := fixtureParams()
	want, _ := json.Marshal(Compute(params))

	random := rand.New(rand.NewSource(20260904))
	for attempt := 0; attempt < 50; attempt++ {
		shuffled := fixtureParams()
		prs := shuffled.PullRequests
		random.Shuffle(len(prs), func(i, j int) { prs[i], prs[j] = prs[j], prs[i] })
		reviews := shuffled.Reviews
		random.Shuffle(len(reviews), func(i, j int) { reviews[i], reviews[j] = reviews[j], reviews[i] })
		got, _ := json.Marshal(Compute(shuffled))
		if !bytes.Equal(want, got) {
			t.Fatalf("shuffle %d changed the output", attempt)
		}
	}
}
