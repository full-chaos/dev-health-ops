package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/operationaledges"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// flagGuardsEdgesPreStep and operationalIncidentEdgesPreStep adapt the two
// remaining operational-edges sub-builders (CHAOS-4924, already ported and
// codex-reviewed as internal/jobs/workgraph/operationaledges -- this file is
// wiring only, no new compute) to the build's pre-step seam. Same composition
// -root-only-file reasoning as workgraph_issue_pr_links.go /
// workgraph_pr_commit.go.
//
// Both are pure producers over the SAME ClickHouse connection every other
// pre-step uses; neither reads a table another pre-step writes (they read
// operational_*/feature_flag/work_items, not work_graph_issue_pr or
// work_graph_pr_commit), so their position in buildPreStepOrder relative to
// the other three steps is free -- placed last, preserving Python's own
// relative order between the two of them (flag_guards_edges then
// operational_incident_edges, builder.py:468/470).
type flagGuardsEdgesPreStep struct {
	conn driver.Conn
	now  func() time.Time
}

func newFlagGuardsEdgesPreStep(conn driver.Conn) (*flagGuardsEdgesPreStep, error) {
	if conn == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &flagGuardsEdgesPreStep{conn: conn, now: time.Now}, nil
}

func (step *flagGuardsEdgesPreStep) Name() string { return "flag_guards_edges" }

// Run ports _build_flag_guards_edges end to end: BOTH of its writes
// (work_graph_edges AND feature_flag_link) must happen here, or the port
// silently drops the link half Python also produces (builder.py:635-636,
// documented on BuildFlagGuardsEdges' own doc comment -- easy to miss because
// the Python sub-builder's RETURN value is only the edge count).
func (step *flagGuardsEdgesPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	orgID := claim.Request.OrganizationID
	now := step.now().UTC()

	edgeRows, linkRows, err := operationaledges.BuildFlagGuardsEdges(ctx, step.conn, orgID, now)
	if err != nil {
		return nil, err
	}
	edgesWritten, err := edges.WriteEdges(ctx, step.conn, orgID, edgeRows)
	if err != nil {
		return nil, err
	}
	linksWritten, err := operationaledges.WriteFeatureFlagLinks(ctx, step.conn, linkRows)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"edges_written": edgesWritten,
		"links_written": linksWritten,
	}, nil
}

type operationalIncidentEdgesPreStep struct {
	conn driver.Conn
	now  func() time.Time
}

func newOperationalIncidentEdgesPreStep(conn driver.Conn) (*operationalIncidentEdgesPreStep, error) {
	if conn == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &operationalIncidentEdgesPreStep{conn: conn, now: time.Now}, nil
}

func (step *operationalIncidentEdgesPreStep) Name() string { return "operational_incident_edges" }

func (step *operationalIncidentEdgesPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	orgID := claim.Request.OrganizationID
	// Python's own guard (`_build_operational_incident_edges`: `if not
	// self.config.org_id: return 0`) -- the ONE sub-builder in this family
	// that already refused unscoped on the Python side. Matched here rather
	// than routed through BuildOperationalIncidentEdges' own
	// RequireOrganizationScope refusal, so an unscoped claim gets Python's
	// exact "quietly did nothing" outcome (0 edges, no error) instead of a
	// step failure that would abort the whole build over a family Python
	// itself never attempted for this claim shape.
	if orgID == "" {
		return map[string]any{"edges_written": 0}, nil
	}

	now := step.now().UTC()
	window, err := operationalEdgesWindowFor(claim.Request.Scope, step.now)
	if err != nil {
		return nil, err
	}

	rows, err := operationaledges.BuildOperationalIncidentEdges(
		ctx, step.conn, orgID, now,
		window.heuristicDaysWindow, window.heuristicConfidence,
		window.fromDate, window.toDate, window.repoID,
	)
	if err != nil {
		return nil, err
	}
	written, err := edges.WriteEdges(ctx, step.conn, orgID, rows)
	if err != nil {
		return nil, err
	}
	return map[string]any{"edges_written": written}, nil
}

// operationalEdgesWindow is BuildOperationalIncidentEdges' scope, parsed from
// the claim's request scope. Unlike prCommitWindowFor (issue_pr_links/
// pr_commit_*'s window), Python's from_date/to_date here are NOT defaulted to
// a rolling 30-day window when absent -- _build_operational_incident_edges
// passes self.config.from_date/to_date straight through, and BuildConfig
// itself leaves them nil/None unless a caller (like the CLI) supplies its own
// default. The bridge path (worker_workgraph.py) supplies no such default
// either, so nil bounds here are the FAITHFUL translation of "read the whole
// table" -- not a bug to fix by inventing a 30-day default this family never
// had.
type operationalEdgesWindow struct {
	fromDate            *time.Time
	toDate              *time.Time
	repoID              *uuid.UUID
	heuristicDaysWindow int
	heuristicConfidence float64
}

func operationalEdgesWindowFor(rawScope []byte, now func() time.Time) (operationalEdgesWindow, error) {
	window := operationalEdgesWindow{heuristicDaysWindow: 7, heuristicConfidence: 0.3}

	scope := map[string]json.RawMessage{}
	if len(rawScope) > 0 {
		if strings.TrimSpace(string(rawScope)) == "null" {
			return operationalEdgesWindow{}, fmt.Errorf("build scope must be an object, got null")
		}
		if err := json.Unmarshal(rawScope, &scope); err != nil {
			return operationalEdgesWindow{}, fmt.Errorf("build scope must be an object: %w", err)
		}
	}
	for field := range scope {
		if _, allowed := buildScopeAllowedFields[field]; !allowed {
			return operationalEdgesWindow{}, fmt.Errorf(
				"build scope contains unsupported field %q; the bridge rejects it", field,
			)
		}
	}

	if text, present, err := scopeString(scope["to_date"]); err != nil {
		return operationalEdgesWindow{}, fmt.Errorf("build scope to_date: %w", err)
	} else if present {
		parsed, parseErr := parseScopeInstant(text)
		if parseErr != nil {
			return operationalEdgesWindow{}, fmt.Errorf("build scope to_date: %w", parseErr)
		}
		window.toDate = &parsed
	}
	if text, present, err := scopeString(scope["from_date"]); err != nil {
		return operationalEdgesWindow{}, fmt.Errorf("build scope from_date: %w", err)
	} else if present {
		parsed, parseErr := parseScopeInstant(text)
		if parseErr != nil {
			return operationalEdgesWindow{}, fmt.Errorf("build scope from_date: %w", parseErr)
		}
		window.fromDate = &parsed
	}
	if text, present, err := scopeString(scope["repo_id"]); err != nil {
		return operationalEdgesWindow{}, fmt.Errorf("build scope repo_id: %w", err)
	} else if present {
		repoID, parseErr := pythonparity.ParseUUID(text)
		if parseErr != nil {
			return operationalEdgesWindow{}, fmt.Errorf("build scope repo_id: %w", parseErr)
		}
		window.repoID = &repoID
	}
	if raw, ok := scope["heuristic_window"]; ok {
		parsed, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			return operationalEdgesWindow{}, fmt.Errorf("build scope heuristic_window: %w", err)
		}
		window.heuristicDaysWindow = parsed
	}
	if raw, ok := scope["heuristic_confidence"]; ok {
		parsed, err := strconv.ParseFloat(strings.TrimSpace(string(raw)), 64)
		if err != nil {
			return operationalEdgesWindow{}, fmt.Errorf("build scope heuristic_confidence: %w", err)
		}
		window.heuristicConfidence = parsed
	}
	return window, nil
}
