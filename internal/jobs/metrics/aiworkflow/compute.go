package aiworkflow

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Run kind/status string constants, ported from
// dev_health_ops.models.ai_workflow's StrEnums.
const (
	RunKindChatAssisted   = "chat_assisted"
	RunKindAgentAutonomous = "agent_autonomous"
	RunKindUnknown         = "unknown"

	RunStatusCompleted = "completed"

	ArtifactTypePullRequest = "pull_request"
)

// PullRequestRow is the row shape LoadAIWorkflowPullRequests scans, mirroring
// job_daily.py:301-314's wf_pr_rows SELECT EXACTLY -- including its
// omissions. Per CHAOS-4280 astra finding 2: production's SELECT has no
// labels/author_login/author_user_type/author_app_slug, so label detection
// and the unknown-bot branch of author detection NEVER fire in production
// today. This struct still carries those fields (zero-valued by the native
// loader) so the pure Compute function below is honestly testable with
// synthetic data -- widening the LOADER to populate them would silently
// change production behavior and break the live-Python oracle; that is
// tracked as a separate, out-of-scope ticket, not fixed here.
type PullRequestRow struct {
	RepoID         uuid.UUID
	Number         int64
	Title          string
	Body           string
	HeadBranch     string
	AuthorName     string
	AuthorLogin    string
	AuthorUserType string
	AuthorAppSlug  string
	Labels         []string
	CreatedAt      time.Time
	MergedAt       *time.Time
	ClosedAt       *time.Time
	LastSynced     time.Time
}

// IssuePRLink is one work_graph_issue_pr row: a PR linked to a work item.
type IssuePRLink struct {
	RepoID     uuid.UUID
	PRNumber   int64
	WorkItemID string
}

// Run ports dev_health_ops.models.ai_workflow.AIWorkflowRun (the fields this
// family actually populates; prompt_hash/prompt_length/model stay nil/unset,
// matching production, which never sets them either).
type Run struct {
	RunID           string
	OrgID           uuid.UUID
	Provider        string
	RunKind         string
	Status          *string
	Tool            *string
	Actor           *string
	RepoID          *uuid.UUID
	PromptsRedacted bool
	StartedAt       *time.Time
	CompletedAt     *time.Time
	ObservedAt      time.Time
	Metadata        map[string]any
}

// ArtifactEdge ports AIWorkflowArtifactEdge.
type ArtifactEdge struct {
	EdgeID       string
	OrgID        uuid.UUID
	RunID        string
	ArtifactType string
	ArtifactID   string
	Provider     string
	RepoID       *uuid.UUID
	Confidence   float64
	Source       string
	Evidence     string
	ObservedAt   time.Time
}

// IssueEdge ports AIWorkflowIssueEdge.
type IssueEdge struct {
	EdgeID     string
	OrgID      uuid.UUID
	IssueID    string
	RunID      string
	Provider   string
	RepoID     *uuid.UUID
	Confidence float64
	Source     string
	Evidence   string
	ObservedAt time.Time
}

// Result is what Compute returns: everything extract_ai_workflow_from_pull_requests
// would return, for one provider's worth of PR rows.
type Result struct {
	Runs          []Run
	ArtifactEdges []ArtifactEdge
	IssueEdges    []IssueEdge
}

// hashParts ports _hash(*parts): sha256(hex) of "|"-joined string parts, no
// length prefixing (ported faithfully -- this hash shape already exists
// elsewhere in this codebase, e.g. work_graph_edges' edge_id, and is not
// re-litigated here).
func hashParts(parts ...string) string {
	joined := strings.Join(parts, "|")
	sum := sha256.Sum256([]byte(joined))
	return hex.EncodeToString(sum[:])
}

// jsonCompact ports _json(value): json.dumps(value, sort_keys=True,
// separators=(",", ":"), default=str). Panics only on a programming error
// (a Go value MarshalPythonJSONCompact cannot represent), never on ordinary
// evidence maps built by this package -- those are always
// map[string]any of strings/bools/nil, all supported.
func jsonCompact(value map[string]any) string {
	encoded, err := pythonparity.MarshalPythonJSONCompact(value)
	if err != nil {
		// Evidence maps in this package are built exclusively from this
		// package's own detector functions, which only ever emit
		// string/bool/nil values -- an error here means a NEW detector
		// started emitting an unsupported type, a programming error to
		// fail loudly on rather than silently drop evidence.
		panic("aiworkflow: evidence map not representable as compact JSON: " + err.Error())
	}
	return string(encoded)
}

// runKind ports _run_kind: agent_created -> AGENT_AUTONOMOUS, ai_assisted ->
// CHAT_ASSISTED, anything else (ai_review included) -> UNKNOWN.
func runKind(signal Signal) string {
	switch signal.Kind {
	case KindAgentCreated:
		return RunKindAgentAutonomous
	case KindAIAssisted:
		return RunKindChatAssisted
	default:
		return RunKindUnknown
	}
}

// signalsFromPR ports _signals_from_pr, calling the four detectors in the
// SAME order Python does: labels, author, branch, body.
func signalsFromPR(row PullRequestRow) []Signal {
	var signals []Signal
	signals = append(signals, DetectFromPRLabels(row.Labels)...)

	authorLogin := row.AuthorName
	if authorLogin == "" {
		authorLogin = row.AuthorLogin
	}
	if authorLogin != "" {
		if authorSignal := DetectFromAuthor(AuthorInfo{
			Login:    authorLogin,
			UserType: row.AuthorUserType,
			AppSlug:  row.AuthorAppSlug,
		}); authorSignal != nil {
			signals = append(signals, *authorSignal)
		}
	}

	if branchSignal := DetectFromBranchName(row.HeadBranch); branchSignal != nil {
		signals = append(signals, *branchSignal)
	}
	if bodySignal := DetectFromPRBody(row.Body); bodySignal != nil {
		signals = append(signals, *bodySignal)
	}
	return signals
}

// strongestSignal ports `max(signals, key=lambda signal: float(signal.confidence))`.
//
// CPython's max() returns the FIRST maximal element on a tie -- it keeps a
// running best and only replaces it on a STRICT `>`, never on `>=` or `==`.
// Go has no builtin max-by-key, so this replicates that exact rule: iterate
// in order, replace the current best only when a later signal's confidence
// is STRICTLY greater, never on equal confidence. Getting this backwards
// (replacing on >=) would silently prefer the LAST tied signal instead of
// the first, diverging from Python whenever two signals of the same
// detector class both fire (e.g. two different AI labels, both confidence
// 0.95) -- see compute_test.go's tie-break goldens, one fixture per ordered
// pair of detector kinds.
func strongestSignal(signals []Signal) Signal {
	best := signals[0]
	for _, signal := range signals[1:] {
		if signal.Confidence > best.Confidence {
			best = signal
		}
	}
	return best
}

// pyDT ports _dt(row, *keys): the first key whose value is a non-nil
// timestamp, defaulting to "now" (passed in explicitly here, since Go has no
// implicit wall-clock read inside a pure kernel function).
func pyDT(now time.Time, candidates ...*time.Time) time.Time {
	for _, candidate := range candidates {
		if candidate != nil {
			return *candidate
		}
	}
	return now
}

// Compute ports extract_ai_workflow_from_pull_requests for ONE provider's PR
// rows (the caller splits by provider first, mirroring
// job_daily.py:461-467's per-provider loop -- see the native executor).
//
// issueIDsByPR keys are "repoID:number" strings, matching Python's
// f"{repo_id}:{pr_number}" convention exactly (used as both the PR's own
// identity string, pr_id, and the map lookup key).
func Compute(
	prs []PullRequestRow,
	orgID uuid.UUID,
	provider string,
	issueIDsByPR map[string][]string,
	now time.Time,
) Result {
	var result Result
	for _, pr := range prs {
		prID := prIDFor(pr.RepoID, pr.Number)
		signals := signalsFromPR(pr)
		if len(signals) == 0 {
			continue
		}

		observedAt := pyDT(now, pr.MergedAt, pr.ClosedAt, &pr.CreatedAt, &pr.LastSynced)
		strongest := strongestSignal(signals)
		runID := hashParts(orgID.String(), provider, "pull_request", prID, strongest.Source)

		repoID := pr.RepoID
		startedAt := pyDT(now, &pr.CreatedAt, &pr.LastSynced)
		completedAt := pyDT(now, pr.MergedAt, pr.ClosedAt, &pr.LastSynced)

		evidenceSignals := make([]any, 0, len(signals))
		for _, signal := range signals {
			evidenceSignals = append(evidenceSignals, signal.Evidence)
		}
		metadata := map[string]any{
			"subject_type": "pull_request",
			"subject_id":   prID,
			"signals":      evidenceSignals,
		}

		status := strPtr(RunStatusCompleted)
		tool := strongest.Actor

		// Python: `actor = strongest.actor or _str(row,"author_name") or None`
		// -- an `or` chain, so falls through on a FALSY strongest.actor (None
		// or ""). None of this package's detectors ever sets Actor to a
		// non-nil pointer to an empty string (checked at every call site
		// above), so a plain nil-check is equivalent to Python's truthiness
		// check in practice, not just in the cases exercised here.
		actor := strongest.Actor
		if actor == nil && pr.AuthorName != "" {
			actor = &pr.AuthorName
		}

		result.Runs = append(result.Runs, Run{
			RunID:           runID,
			OrgID:           orgID,
			Provider:        provider,
			RunKind:         runKind(strongest),
			Status:          status,
			Tool:            tool,
			Actor:           actor,
			RepoID:          &repoID,
			PromptsRedacted: true,
			StartedAt:       &startedAt,
			CompletedAt:     &completedAt,
			ObservedAt:      observedAt,
			Metadata:        metadata,
		})

		result.ArtifactEdges = append(result.ArtifactEdges, ArtifactEdge{
			EdgeID:       hashParts("ai_run_pr", orgID.String(), runID, prID),
			OrgID:        orgID,
			RunID:        runID,
			ArtifactType: ArtifactTypePullRequest,
			ArtifactID:   prID,
			Provider:     provider,
			RepoID:       &repoID,
			Confidence:   strongest.Confidence,
			Source:       strongest.Source,
			Evidence:     jsonCompact(strongest.Evidence),
			ObservedAt:   observedAt,
		})

		for _, issueID := range issueIDsByPR[prID] {
			result.IssueEdges = append(result.IssueEdges, IssueEdge{
				EdgeID:     hashParts("issue_ai_run", orgID.String(), issueID, runID),
				OrgID:      orgID,
				IssueID:    issueID,
				RunID:      runID,
				Provider:   provider,
				RepoID:     &repoID,
				Confidence: strongest.Confidence,
				Source:     strongest.Source,
				Evidence: jsonCompact(map[string]any{
					"pr_id":  prID,
					"signal": strongest.Evidence,
				}),
				ObservedAt: observedAt,
			})
		}
	}
	return result
}

// prIDFor ports f"{repo_id}:{pr_number}" -- Python's str(int(row["number"])),
// a plain decimal string, matching strconv.FormatInt base 10.
func prIDFor(repoID uuid.UUID, number int64) string {
	return repoID.String() + ":" + strconv.FormatInt(number, 10)
}
