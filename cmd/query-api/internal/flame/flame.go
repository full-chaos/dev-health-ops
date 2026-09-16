// Package flame is the Go port of GET /api/v1/flame
// (src/dev_health_ops/api/services/flame.py's build_flame_response and its
// three per-entity-type builders, api/queries/flame.py's four ClickHouse
// readers, and queries/scopes.py's parse_uuid via
// internal/pythonparity.ParseUUID).
//
// Python source read at this branch's base:
//   - api/main.py -- the flame() view (entity_type/entity_id query params,
//     _reject_comparative_params, the outer try/except -> 503 fallback).
//   - api/services/flame.py -- build_flame_response and the pr/issue/
//     deployment frame builders, _frame, validate_flame_frames,
//     _rework_windows, _submitted_reviews, _now_like, _parse_repo_entity.
//   - api/queries/flame.py -- fetch_pull_request, fetch_pull_request_reviews,
//     fetch_issue, fetch_deployment.
//   - api/models/schemas.py -- FlameTimeline/FlameFrame/FlameResponse, the
//     wire shape.
//
// DATETIME WIRE FORM (every start/end field): RFC 3339 (Go's default
// time.Time JSON marshaling), not Python's naive isoformat -- the same
// already-established, class-level direction this service's "pr" field
// family (drilldown/prs.go's PRItem) and "issue" field family
// (drilldown/issues.go's IssueItem) already carry: clickhouse_connect
// returns a tzinfo-less datetime for every DateTime64/DateTime column this
// package reads, so Pydantic's `datetime`-typed FlameFrame/FlameTimeline
// fields serialize with NO "Z"/offset suffix -- while Go's driver attaches
// UTC location, so this port's encoding/json marshaling emits RFC 3339.
// Python's wire form is the declared baseline defect; Go's RFC 3339 form is
// canonical, the same class already applied there.
//
// DATA-LAYER NOTE (ClickHouse ReplacingMergeTree dedup, class ruling): both
// git_pull_requests and git_pull_request_reviews are ReplacingMergeTree(
// last_synced) (000_raw_tables.sql), sort-keyed (org_id, repo_id, number)
// and (org_id, repo_id, number, review_id) respectively since migration 027
// (027_add_org_id_to_sorting_keys.py). fetchPullRequest's WHERE fully
// specifies the first sort key and fetchPullRequestReviews' WHERE specifies
// a prefix of the second -- but api/queries/flame.py's own
// fetch_pull_request reads with a bare `LIMIT 1` (no ORDER BY, no FINAL),
// and fetch_pull_request_reviews reads with `ORDER BY submitted_at` and no
// FINAL: neither dedups an unmerged physical version of the same logical
// row, so either can non-deterministically surface a STALE physical
// version. This port reads both FINAL (see sqlshape_test.go, which pins
// org_id at the same nesting depth as each FINAL source). This is the
// same declared Python-plane defect class already tracked for
// drilldown/prs.go's own read of git_pull_requests -- a different query
// site (api/queries/flame.py rather than api/queries/drilldown.py) over
// the same two tables, same missing-dedup mechanism.
//
// work_item_cycle_times is ReplacingMergeTree(computed_at)
// (001_metrics_v2.sql), and Python's own fetch_issue ALREADY reads it
// `AS wct FINAL` -- this port matches that, no divergence.
//
// deployments is ReplacingMergeTree(last_synced) (000_raw_tables.sql),
// sort-keyed (org_id, repo_id, deployment_id) since migration 027.
// fetch_deployment's WHERE fully specifies that sort key, and its own
// `ORDER BY last_synced DESC LIMIT 1` (no FINAL) is, for that reason, ALREADY
// a deterministic pick of the winning (highest-version) physical row --
// equivalent to FINAL/argMax for a query that narrows to exactly one sort
// key. No divergence is declared for this table; this port uses FINAL
// anyway, for the same uniform "always FINAL, never a hand-rolled
// ORDER-BY-DESC-LIMIT-1 idiom" convention every other ReplacingMergeTree
// reader in this service (quadrant.go's dedupFrom, drilldown's readers)
// already follows -- a style choice, not a behaviour change.
//
// DELIBERATE SIMPLIFICATION: fetch_issue's own query (api/queries/flame.py)
// LEFT JOINs PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE to read a team_id
// that _build_issue_flame_response (services/flame.py) never reads --
// dead code inherited from a copy-pasted query shape. This port's
// fetchIssue omits that join: the joined column is discarded on every
// Python code path, so dropping it cannot change any observable output,
// and the join's own subquery is already GROUP BY work_item_id (at most
// one row per work_item_id), so it could not have affected row count
// either.
//
// ID-BINDING GAP: the flame entity_id is itself an id, and this branch has
// no id-binding producer yet (a sibling PR is adding one) -- see
// internal/goapiproof/restcorpus.go's own REST:GET:/api/v1/flame entry for
// the corpus consequence (deterministic-refusal requests only, no live
// happy-path fixture).
package flame

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same single-method shape every sibling operation package
// (quadrant.QueryClient, etc.) declares independently.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// now is time.Now, overridable by a test so _now_like's fallback
// (services/flame.py's own `datetime.now(timezone.utc)`) is
// deterministically observable rather than merely "close to real time".
var now = time.Now

// Timeline is the wire shape of FlameTimeline (schemas.py:487-489).
type Timeline struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// Frame is the wire shape of FlameFrame (schemas.py:492-499).
type Frame struct {
	ID       string    `json:"id"`
	ParentID *string   `json:"parent_id"`
	Label    string    `json:"label"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	State    string    `json:"state"`
	Category string    `json:"category"`
}

// Response is the wire shape of FlameResponse (schemas.py:502-505). Entity
// varies by entity_type (pr/issue/deployment each build their own dict in
// Python), so it is a plain map here rather than a fixed struct.
type Response struct {
	Entity   map[string]any `json:"entity"`
	Timeline Timeline       `json:"timeline"`
	Frames   []Frame        `json:"frames"`
}

// RequestError carries an HTTP status the way Python's HTTPException does,
// so the route layer can answer the same status code build_flame_response
// (and its three callees) would raise for the same bad input, without this
// package importing net/http.
type RequestError struct {
	Status  int
	Message string
}

func (e *RequestError) Error() string { return e.Message }

func badRequest(msg string) error    { return &RequestError{Status: 400, Message: msg} }
func notFound(msg string) error      { return &RequestError{Status: 404, Message: msg} }
func unprocessable(msg string) error { return &RequestError{Status: 422, Message: msg} }

// AsRequestError extracts a *RequestError's status/message, or reports
// ok=false for any other error -- the route layer's 503 fallback (main.py's
// own `except Exception: raise HTTPException(503, "Data unavailable")`).
func AsRequestError(err error) (*RequestError, bool) {
	if reqErr, ok := err.(*RequestError); ok {
		return reqErr, true
	}
	return nil, false
}

// Params is BuildResponse's input -- the two required query params
// GET /api/v1/flame takes (main.py:785-790).
type Params struct {
	EntityType string
	EntityID   string
}

// parseRepoEntity ports _parse_repo_entity (services/flame.py:51-61): the
// "pr"/"deployment" entity_id shape is "<repo_id>:<suffix>", checked in
// this exact order -- missing separator, then repo id shape, then a
// non-empty suffix.
func parseRepoEntity(entityID string) (repoID, itemID string, err error) {
	idx := strings.Index(entityID, ":")
	if idx < 0 {
		return "", "", badRequest("Entity id must include repo_id prefix")
	}
	repoID = entityID[:idx]
	itemID = entityID[idx+1:]
	if _, parseErr := pythonparity.ParseUUID(repoID); parseErr != nil {
		return "", "", badRequest("Invalid repo id")
	}
	if itemID == "" {
		return "", "", badRequest("Entity id missing suffix")
	}
	return repoID, itemID, nil
}

// newFrame ports _frame (services/flame.py:26-48): nil (Python's None) when
// either timestamp is absent or the interval is empty/inverted (end <=
// start), never a zero-duration or backwards frame on the wire.
func newFrame(id string, parentID *string, label string, start, end *time.Time, state, category string) *Frame {
	if start == nil || end == nil {
		return nil
	}
	if !end.After(*start) {
		return nil
	}
	return &Frame{ID: id, ParentID: parentID, Label: label, Start: *start, End: *end, State: state, Category: category}
}

func strPtr(s string) *string { return &s }

// nullableString ports a Nullable(String) column's dict.get(...) ->
// dict[str, Any] passthrough: nil stays nil (JSON null), a set value
// passes through as-is.
func nullableString(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

// nowLike ports _now_like (services/flame.py:19-23). Python branches on
// whether `reference` carries tzinfo to decide between an aware and a
// naive "now" -- a distinction that exists only because Python's own
// driver sometimes returns naive and sometimes aware datetimes. Go's
// ClickHouse driver always attaches UTC location (see this package's own
// doc comment on the DATETIME WIRE FORM), so there is exactly one branch
// here, matching the every-Go-time-is-UTC-located reality rather than
// Python's two-branch accommodation for a distinction Go values never
// carry.
func nowLike() time.Time { return now().UTC() }

// validateFlameFrames ports validate_flame_frames (services/flame.py:
// 64-82): the top-level frames (parent_id == nil) must cover
// [timeline.Start, timeline.End) as a contiguous (gap-free) run once
// sorted by start.
func validateFlameFrames(timeline Timeline, frames []Frame) bool {
	if len(frames) == 0 {
		return false
	}
	var topLevel []Frame
	for _, f := range frames {
		if f.ParentID == nil {
			topLevel = append(topLevel, f)
		}
	}
	if len(topLevel) == 0 {
		return false
	}
	sort.SliceStable(topLevel, func(i, j int) bool { return topLevel[i].Start.Before(topLevel[j].Start) })
	cursor := topLevel[0].Start
	if cursor.After(timeline.Start) {
		return false
	}
	currentEnd := topLevel[0].End
	for _, f := range topLevel[1:] {
		if f.Start.After(currentEnd) {
			return false
		}
		if f.End.After(currentEnd) {
			currentEnd = f.End
		}
	}
	return !currentEnd.Before(timeline.End)
}

// reviewState ports _review_state (services/flame.py:85-86).
func reviewState(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// reworkWindow is one (start, end) pair _rework_windows yields.
type reworkWindow struct {
	start time.Time
	end   time.Time
}

// submittedReviews ports _submitted_reviews (services/flame.py:89-98):
// re-sorts by submitted_at. fetchPullRequestReviews' own SQL already
// filters to non-null submitted_at and orders by it, so this is a
// defensive no-op in practice -- kept because it is what the reference
// does, and it is what makes this function correct independent of its
// caller's own ordering.
func submittedReviews(reviews []reviewRow) []reviewRow {
	ordered := append([]reviewRow(nil), reviews...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].SubmittedAt.Before(ordered[j].SubmittedAt) })
	return ordered
}

// reworkWindows ports _rework_windows (services/flame.py:101-129).
func reworkWindows(reviews []reviewRow, reviewStart, reviewEnd time.Time) []reworkWindow {
	if len(reviews) == 0 {
		return nil
	}
	ordered := submittedReviews(reviews)

	var windows []reworkWindow
	for idx, review := range ordered {
		state := reviewState(review.State)
		if state != "changes_requested" && state != "requested_changes" && state != "request_changes" {
			continue
		}
		start := review.SubmittedAt
		nextTime := reviewEnd
		for _, candidate := range ordered[idx+1:] {
			if candidate.SubmittedAt.After(start) {
				nextTime = candidate.SubmittedAt
				break
			}
		}
		if start.Before(reviewStart) {
			start = reviewStart
		}
		if nextTime.After(reviewEnd) {
			nextTime = reviewEnd
		}
		if nextTime.After(start) {
			windows = append(windows, reworkWindow{start: start, end: nextTime})
		}
	}
	return windows
}

// buildPRFlameResponse ports _build_pr_flame_response (services/flame.py:
// 132-210).
func buildPRFlameResponse(repoID string, number int, pr pullRequestRow, reviews []reviewRow) (*Response, error) {
	start := pr.CreatedAt
	var end time.Time
	switch {
	case pr.MergedAt != nil:
		end = *pr.MergedAt
	case pr.ClosedAt != nil:
		end = *pr.ClosedAt
	default:
		end = nowLike()
	}

	rootID := fmt.Sprintf("pr:%s:%d", repoID, number)
	frames := make([]Frame, 0, 4)
	if f := newFrame(rootID, nil, "PR lifecycle", &start, &end, "active", "planned"); f != nil {
		frames = append(frames, *f)
	}

	if pr.FirstReviewAt != nil && pr.FirstReviewAt.After(start) {
		if f := newFrame(rootID+":wait", strPtr(rootID), "Review waiting", &start, pr.FirstReviewAt, "waiting", "planned"); f != nil {
			frames = append(frames, *f)
		}
	}

	reviewStart := start
	if pr.FirstReviewAt != nil {
		reviewStart = *pr.FirstReviewAt
	}
	reviewID := rootID + ":review"
	reviewFrame := newFrame(reviewID, strPtr(rootID), "Review and merge", &reviewStart, &end, "active", "planned")
	if reviewFrame != nil {
		frames = append(frames, *reviewFrame)
		for idx, w := range reworkWindows(reviews, reviewStart, end) {
			wStart, wEnd := w.start, w.end
			if f := newFrame(fmt.Sprintf("%s:rework:%d", rootID, idx+1), strPtr(reviewFrame.ID), "Rework loop", &wStart, &wEnd, "active", "rework"); f != nil {
				frames = append(frames, *f)
			}
		}
	}

	timeline := Timeline{Start: start, End: end}
	if !validateFlameFrames(timeline, frames) {
		return nil, unprocessable("Flame frames have gaps")
	}

	entity := map[string]any{
		"repo_id": repoID,
		"number":  number,
		"title":   nullableString(pr.Title),
		"state":   nullableString(pr.State),
	}
	return &Response{Entity: entity, Timeline: timeline, Frames: frames}, nil
}

// buildIssueFlameResponse ports _build_issue_flame_response (services/
// flame.py:213-274).
func buildIssueFlameResponse(entityID string, issue issueRow) (*Response, error) {
	start := issue.CreatedAt
	var end time.Time
	if issue.CompletedAt != nil {
		end = *issue.CompletedAt
	} else {
		end = nowLike()
	}

	rootID := "issue:" + entityID
	frames := make([]Frame, 0, 3)
	if f := newFrame(rootID, nil, "Issue lifecycle", &start, &end, "active", "planned"); f != nil {
		frames = append(frames, *f)
	}

	if issue.StartedAt != nil && issue.StartedAt.After(start) {
		if f := newFrame(rootID+":wait", strPtr(rootID), "Backlog waiting", &start, issue.StartedAt, "waiting", "planned"); f != nil {
			frames = append(frames, *f)
		}
	}

	progressStart := start
	if issue.StartedAt != nil {
		progressStart = *issue.StartedAt
	}
	if f := newFrame(rootID+":work", strPtr(rootID), "Active work", &progressStart, &end, "active", "planned"); f != nil {
		frames = append(frames, *f)
	}

	timeline := Timeline{Start: start, End: end}
	if !validateFlameFrames(timeline, frames) {
		return nil, unprocessable("Flame frames have gaps")
	}

	entity := map[string]any{
		"work_item_id": issue.WorkItemID,
		"provider":     nullableString(issue.Provider),
		"type":         nullableString(issue.Type),
		"status":       nullableString(issue.Status),
	}
	return &Response{Entity: entity, Timeline: timeline, Frames: frames}, nil
}

// buildDeploymentFlameResponse ports _build_deployment_flame_response
// (services/flame.py:277-360).
func buildDeploymentFlameResponse(repoID, deploymentID string, deployment deploymentRow) (*Response, error) {
	var start time.Time
	switch {
	case deployment.StartedAt != nil:
		start = *deployment.StartedAt
	case deployment.MergedAt != nil:
		start = *deployment.MergedAt
	case deployment.DeployedAt != nil:
		start = *deployment.DeployedAt
	default:
		return nil, notFound("Deployment timeline unavailable")
	}
	var end time.Time
	switch {
	case deployment.FinishedAt != nil:
		end = *deployment.FinishedAt
	case deployment.DeployedAt != nil:
		end = *deployment.DeployedAt
	default:
		end = nowLike()
	}

	rootID := fmt.Sprintf("deploy:%s:%s", repoID, deploymentID)
	frames := make([]Frame, 0, 4)
	root := newFrame(rootID, nil, "Deployment lifecycle", &start, &end, "ci", "planned")
	if root != nil {
		frames = append(frames, *root)
	}

	if deployment.MergedAt != nil && deployment.MergedAt.Before(start) {
		if f := newFrame(rootID+":queue", strPtr(rootID), "Queue waiting", deployment.MergedAt, &start, "waiting", "planned"); f != nil {
			frames = append(frames, *f)
		}
	}

	pipelineID := rootID + ":pipeline"
	pipeline := newFrame(pipelineID, strPtr(rootID), "Deploy pipeline", &start, &end, "ci", "planned")
	if pipeline != nil {
		frames = append(frames, *pipeline)
	}

	if pipeline != nil && deployment.DeployedAt != nil && deployment.DeployedAt.After(pipeline.Start) && end.After(*deployment.DeployedAt) {
		if f := newFrame(rootID+":deploy", strPtr(pipeline.ID), "Deploy", deployment.DeployedAt, &end, "active", "planned"); f != nil {
			frames = append(frames, *f)
		}
	}

	timeline := Timeline{Start: start, End: end}
	if !validateFlameFrames(timeline, frames) {
		return nil, unprocessable("Flame frames have gaps")
	}

	entity := map[string]any{
		"repo_id":       repoID,
		"deployment_id": deploymentID,
		"status":        nullableString(deployment.Status),
		"environment":   nullableString(deployment.Environment),
	}
	return &Response{Entity: entity, Timeline: timeline, Frames: frames}, nil
}

// BuildResponse ports build_flame_response (services/flame.py:363-427) for
// every entity_type Python serves (pr, issue, deployment).
func BuildResponse(ctx context.Context, client QueryClient, orgID string, params Params) (*Response, error) {
	switch params.EntityType {
	case "pr":
		repoID, suffix, err := parseRepoEntity(params.EntityID)
		if err != nil {
			return nil, err
		}
		number, convErr := strconv.Atoi(suffix)
		if convErr != nil {
			return nil, badRequest("PR id must be numeric")
		}

		pr, err := fetchPullRequest(ctx, client, repoID, number, orgID)
		if err != nil {
			return nil, err
		}
		if pr == nil {
			return nil, notFound("PR not found")
		}
		reviews, err := fetchPullRequestReviews(ctx, client, repoID, number, orgID)
		if err != nil {
			return nil, err
		}
		return buildPRFlameResponse(repoID, number, *pr, reviews)

	case "issue":
		issue, err := fetchIssue(ctx, client, params.EntityID, orgID)
		if err != nil {
			return nil, err
		}
		if issue == nil {
			return nil, notFound("Issue not found")
		}
		return buildIssueFlameResponse(params.EntityID, *issue)

	case "deployment":
		repoID, deploymentID, err := parseRepoEntity(params.EntityID)
		if err != nil {
			return nil, err
		}
		deployment, err := fetchDeployment(ctx, client, repoID, deploymentID, orgID)
		if err != nil {
			return nil, err
		}
		if deployment == nil {
			return nil, notFound("Deployment not found")
		}
		return buildDeploymentFlameResponse(repoID, deploymentID, *deployment)
	}

	return nil, notFound("Unknown entity type")
}
