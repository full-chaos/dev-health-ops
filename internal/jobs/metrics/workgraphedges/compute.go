// Package workgraphedges is the pure kernel of the work_graph_edges
// metrics.daily family (CHAOS-4286), ported from
// work_graph/extractors/ai_workflow.py:210
// extract_review_deployment_incident_edges.
//
// # WHY THIS PACKAGE IS NOT internal/jobs/workgraph/edges
//
// There is already a Go package called `edges`, and it writes a table called
// `work_graph_edges`. This family is NAMED `work_graph_edges` in families.json
// and writes three DIFFERENT tables:
// work_graph_pr_review_outcome_edges, work_graph_pr_deployment_edges and
// work_graph_deployment_incident_edges. The existing package is the
// issue-to-issue dependency family.
//
// So the family named after that table is not the family that writes it.
// Anyone grepping `work_graph_edges` lands in the wrong place, and this port
// nearly went into the wrong package for exactly that reason. The layout here
// follows ai_governance/ai_impact instead (kernel beside the other metrics
// kernels, adapter in metrics/daily) and renames nothing.
//
// # NO CHAOS-5102 EXPOSURE
//
// All three output tables are ReplacingMergeTree(computed_at) whose sorting
// keys -- (org_id, pr_id, review_outcome_id, source),
// (org_id, pr_id, deployment_id, source) and
// (org_id, deployment_id, incident_id, source) -- contain NO random component,
// and notably do not contain edge_id at all. A Python fallback rewrite after
// an ack-loss therefore lands on the SAME key and the engine collapses it.
// This family does not need ai_governance's load-bearing write ordering, and
// copying that caveat into this PR would be false.
package workgraphedges

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// ReviewRow is one git_pull_request_reviews row as the loader scans it.
//
// State is a pointer because the column is nullable and the NULL is
// load-bearing twice, in two DIFFERENT ways -- see reviewEdge.
type ReviewRow struct {
	RepoID      uuid.UUID
	Number      uint32
	ReviewID    string
	State       *string
	SubmittedAt *time.Time
	LastSynced  *time.Time
}

// DeploymentRow is one deployments row as the loader scans it.
type DeploymentRow struct {
	RepoID uuid.UUID
	// DeploymentID is the opaque provider id, NOT a UUID.
	DeploymentID string
	// PullRequestNumber is Nullable(UInt32). Nil means this deployment
	// produces no PR->deployment edge (Python: `if pr_number_value is None:
	// continue`), but it still registers in the per-repo index above that
	// check -- see ExtractReviewDeploymentIncidentEdges.
	PullRequestNumber *uint32
	StartedAt         *time.Time
	FinishedAt        *time.Time
	DeployedAt        *time.Time
	LastSynced        *time.Time
}

// IncidentRow is one incident row as the loader scans it.
type IncidentRow struct {
	RepoID     uuid.UUID
	IncidentID string
	// DeploymentID is "" when the incident carries no native deployment
	// linkage. Empty, not nil: Python reads it through _str, which turns None
	// into "" before the truthiness test that selects native vs heuristic.
	DeploymentID string
	StartedAt    *time.Time
	LastSynced   *time.Time
}

// PRReviewOutcomeEdge mirrors models/ai_workflow.py WorkGraphPRReviewOutcomeEdge.
type PRReviewOutcomeEdge struct {
	EdgeID          string
	OrgID           uuid.UUID
	PRID            string
	ReviewOutcomeID string
	Outcome         *string
	Provider        string
	RepoID          *uuid.UUID
	Confidence      float64
	Source          string
	Evidence        string
	ObservedAt      time.Time
}

// PRDeploymentEdge mirrors models/ai_workflow.py WorkGraphPRDeploymentEdge.
type PRDeploymentEdge struct {
	EdgeID       string
	OrgID        uuid.UUID
	PRID         string
	DeploymentID string
	Provider     string
	RepoID       *uuid.UUID
	Confidence   float64
	Source       string
	Evidence     string
	ObservedAt   time.Time
}

// DeploymentIncidentEdge mirrors models/ai_workflow.py
// WorkGraphDeploymentIncidentEdge.
type DeploymentIncidentEdge struct {
	EdgeID       string
	OrgID        uuid.UUID
	DeploymentID string
	IncidentID   string
	Provider     string
	RepoID       *uuid.UUID
	Confidence   float64
	Source       string
	Evidence     string
	ObservedAt   time.Time
}

// Params is one call of extract_review_deployment_incident_edges.
type Params struct {
	OrgID    uuid.UUID
	Provider string

	Reviews     []ReviewRow
	Deployments []DeploymentRow
	Incidents   []IncidentRow

	// Now is the _dt() last-resort fallback, injected rather than read from
	// the clock so the kernel stays deterministic and the fallback is
	// testable. Python calls datetime.now(timezone.utc) inline (:59), which
	// means an incomplete row gets a wall-clock observed_at that differs on
	// every run -- documented on CHAOS-4286, reproduced here rather than
	// silently "fixed" into something Python never produces.
	Now time.Time
}

// Result carries the three edge lists, in Python's emission order.
type Result struct {
	ReviewOutcomeEdges      []PRReviewOutcomeEdge
	PRDeploymentEdges       []PRDeploymentEdge
	DeploymentIncidentEdges []DeploymentIncidentEdge
}

// ExtractReviewDeploymentIncidentEdges ports
// extract_review_deployment_incident_edges (:210) exactly.
//
// The three loops run in Python's order (reviews, deployments, incidents) and
// each appends in input order, because the outputs are compared elementwise by
// the live-Python oracle and a reordering would show up as a diff even though
// the ReplacingMergeTree would not care.
func ExtractReviewDeploymentIncidentEdges(params Params) (Result, error) {
	var result Result

	for _, row := range params.Reviews {
		edge, ok, err := reviewEdge(params, row)
		if err != nil {
			return Result{}, err
		}
		if ok {
			result.ReviewOutcomeEdges = append(result.ReviewOutcomeEdges, edge)
		}
	}

	// deploymentsByRepo is Python's `deployments_by_repo` dict: repo id string
	// -> deployment ids, in INPUT order. It is consumed by the incident loop
	// when an incident carries no deployment of its own.
	//
	// ORDER MATTERS and so does POSITION: Python appends to this index BEFORE
	// the `if pr_number_value is None: continue`, so a deployment with no PR
	// number contributes no PR->deployment edge yet is still a candidate for
	// heuristic incident linkage. Moving the append below the continue would
	// silently shrink heuristic linkage for exactly the deployments most
	// likely to lack a PR number.
	// The map is keyed by repo and its VALUES are slices appended in input
	// order, so nothing here ever depends on Go map iteration order -- the
	// incident loop below indexes by one key and walks a slice.
	deploymentsByRepo := make(map[string][]string)

	for _, row := range params.Deployments {
		if row.DeploymentID == "" {
			continue
		}
		repoKey := row.RepoID.String()
		deploymentsByRepo[repoKey] = append(deploymentsByRepo[repoKey], row.DeploymentID)

		if row.PullRequestNumber == nil {
			continue
		}
		edge, err := deploymentEdge(params, row)
		if err != nil {
			return Result{}, err
		}
		result.PRDeploymentEdges = append(result.PRDeploymentEdges, edge)
	}
	for _, row := range params.Incidents {
		if row.IncidentID == "" {
			continue
		}
		linked := []string{row.DeploymentID}
		source := "native"
		confidence := 1.0
		if row.DeploymentID == "" {
			// Heuristic fallback: every same-repo deployment in the day.
			// Slice order is input order, never map iteration order.
			linked = deploymentsByRepo[row.RepoID.String()]
			source = "heuristic"
			confidence = 0.3
		}
		for _, deploymentID := range linked {
			edge, err := incidentEdge(params, row, deploymentID, source, confidence)
			if err != nil {
				return Result{}, err
			}
			result.DeploymentIncidentEdges = append(result.DeploymentIncidentEdges, edge)
		}
	}

	return result, nil
}

func reviewEdge(params Params, row ReviewRow) (PRReviewOutcomeEdge, bool, error) {
	if row.ReviewID == "" {
		return PRReviewOutcomeEdge{}, false, nil
	}
	prID := pullRequestID(row.RepoID, strconv.FormatUint(uint64(row.Number), 10))

	// TWO DIFFERENT NULL RULES ON ONE COLUMN. Python:
	//
	//	outcome=_str(row, "state") or None          -> None for NULL *and* ""
	//	evidence=_json({..., "state": row.get("state")})  -> null only for NULL
	//
	// _str turns NULL into "", and `"" or None` is None -- so an EMPTY state
	// collapses to NULL in the outcome column while surviving as "" inside the
	// evidence JSON. Unifying these two would be the obvious cleanup and would
	// diverge from Python on every empty-string state.
	var outcome *string
	if row.State != nil && *row.State != "" {
		value := *row.State
		outcome = &value
	}
	var evidenceState any
	if row.State != nil {
		evidenceState = *row.State
	}
	evidence, err := pythonparity.MarshalPythonJSONCompact(map[string]any{
		"review_id": row.ReviewID,
		"state":     evidenceState,
	})
	if err != nil {
		return PRReviewOutcomeEdge{}, false, fmt.Errorf("work_graph_edges: review evidence: %w", err)
	}

	repoID := row.RepoID
	return PRReviewOutcomeEdge{
		EdgeID:          pythonHash("pr_review", params.OrgID.String(), prID, row.ReviewID),
		OrgID:           params.OrgID,
		PRID:            prID,
		ReviewOutcomeID: row.ReviewID,
		Outcome:         outcome,
		Provider:        params.Provider,
		RepoID:          &repoID,
		Confidence:      1.0,
		Source:          "native",
		Evidence:        string(evidence),
		ObservedAt:      firstTimestamp(params.Now, row.SubmittedAt, row.LastSynced),
	}, true, nil
}

func deploymentEdge(params Params, row DeploymentRow) (PRDeploymentEdge, error) {
	// Python builds this pr_id from the RAW pull_request_number
	// (`f"{repo_id}:{pr_number_value}"`), where the reviews path above routes
	// its number through _int_str. That asymmetry is a real Python defect
	// (documented on CHAOS-4286): a value arriving as 123.0 would render
	// "123.0" here and "123" there, keying two edges to different PRs.
	//
	// It is UNREACHABLE from this loader in Go, because pull_request_number is
	// Nullable(UInt32) and scans into a *uint32 -- there is no float form to
	// render. This is therefore a benign, documented divergence: Go cannot
	// reproduce a defect that requires a type the column cannot hold.
	prID := pullRequestID(row.RepoID, strconv.FormatUint(uint64(*row.PullRequestNumber), 10))

	evidence, err := pythonparity.MarshalPythonJSONCompact(map[string]any{
		"deployment_id": row.DeploymentID,
	})
	if err != nil {
		return PRDeploymentEdge{}, fmt.Errorf("work_graph_edges: deployment evidence: %w", err)
	}

	repoID := row.RepoID
	return PRDeploymentEdge{
		EdgeID:       pythonHash("pr_deployment", params.OrgID.String(), prID, row.DeploymentID),
		OrgID:        params.OrgID,
		PRID:         prID,
		DeploymentID: row.DeploymentID,
		Provider:     params.Provider,
		RepoID:       &repoID,
		Confidence:   1.0,
		Source:       "native",
		Evidence:     string(evidence),
		ObservedAt: firstTimestamp(
			params.Now, row.DeployedAt, row.FinishedAt, row.StartedAt, row.LastSynced,
		),
	}, nil
}

func incidentEdge(
	params Params, row IncidentRow, deploymentID, source string, confidence float64,
) (DeploymentIncidentEdge, error) {
	evidence, err := pythonparity.MarshalPythonJSONCompact(map[string]any{
		"incident_id": row.IncidentID,
	})
	if err != nil {
		return DeploymentIncidentEdge{}, fmt.Errorf("work_graph_edges: incident evidence: %w", err)
	}

	repoID := row.RepoID
	return DeploymentIncidentEdge{
		EdgeID: pythonHash(
			"deployment_incident", params.OrgID.String(), deploymentID, row.IncidentID,
		),
		OrgID:        params.OrgID,
		DeploymentID: deploymentID,
		IncidentID:   row.IncidentID,
		Provider:     params.Provider,
		RepoID:       &repoID,
		Confidence:   confidence,
		Source:       source,
		Evidence:     string(evidence),
		ObservedAt:   firstTimestamp(params.Now, row.StartedAt, row.LastSynced),
	}, nil
}

// pullRequestID ports the `f"{repo_id}:{number}"` form used for pr_id.
//
// Python interpolates a UUID object, whose str() is the canonical lowercase
// hyphenated form -- the same thing uuid.UUID.String() produces.
func pullRequestID(repoID uuid.UUID, number string) string {
	return repoID.String() + ":" + number
}

// pythonHash ports _hash (:49):
//
//	hashlib.sha256("|".join("" if p is None else str(p) for p in parts))
//
// # THE JOIN IS NOT LENGTH-PREFIXED
//
// Because parts are joined by a bare "|", two different part tuples collide
// whenever a part CONTAINS a "|": ("a|b", "c") and ("a", "b|c") hash
// identically. ai_governance deliberately replaced its id derivation with a
// length-prefixed one, but that was replacing a uuid4 -- there was no Python
// value to match. Here Python's hash IS the persisted edge_id, so the port
// must reproduce it collision and all. Do not "improve" this.
//
// Every part this family passes is already a string (literal tags, an
// org_id rendered by uuid.UUID.String(), a pr_id, and opaque provider ids),
// so Python's None -> "" and str() coercion rules cannot fire. A caller that
// needs those must handle them before calling, not by widening this.
func pythonHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])
}

// firstTimestamp ports _dt (:76): the first candidate that is actually a
// datetime, normalised to UTC; otherwise the injected now.
//
// Python's check is `isinstance(value, datetime)`, so a NULL column (None)
// falls through to the next candidate rather than being used. A nil pointer
// here is that same fall-through. Python then attaches UTC to a naive value;
// the driver always hands back aware values, so the equivalent step is a
// plain .UTC() normalisation.
func firstTimestamp(now time.Time, candidates ...*time.Time) time.Time {
	for _, candidate := range candidates {
		if candidate != nil {
			return candidate.UTC()
		}
	}
	return now.UTC()
}
