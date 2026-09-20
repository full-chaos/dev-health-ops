package aianalytics

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// The AI workflow edge union: one branch per edge table, each read with FINAL
// (reruns insert new versions of the same deterministic edge ids) and each
// repeating the frontier-id predicate on its own key columns so the key
// condition prunes before the merge. Every branch filters its table on the
// org.
const workflowEdgeStatement = `SELECT * FROM
(
    SELECT
        edge_id,
        'issue' AS source_type,
        issue_id AS source_id,
        'ai_workflow_run' AS target_type,
        run_id AS target_id,
        'has_ai_workflow' AS edge_type,
        confidence,
        source,
        evidence,
        provider,
        toString(repo_id) AS repo_id
    FROM ai_workflow_issue_edges FINAL
    WHERE org_id = {org_id:String}
      AND (issue_id IN {issue_ids:Array(String)} OR run_id IN {run_ids:Array(String)})

    UNION ALL

    SELECT
        edge_id,
        'ai_workflow_run' AS source_type,
        run_id AS source_id,
        if(artifact_type = 'pull_request', 'pr', artifact_type) AS target_type,
        artifact_id AS target_id,
        'generates' AS edge_type,
        confidence,
        source,
        evidence,
        provider,
        toString(repo_id) AS repo_id
    FROM ai_workflow_artifact_edges FINAL
    WHERE org_id = {org_id:String}
      AND (run_id IN {run_ids:Array(String)} OR artifact_id IN {artifact_ids:Array(String)})

    UNION ALL

    SELECT
        edge_id,
        'pr' AS source_type,
        pr_id AS source_id,
        'review_outcome' AS target_type,
        review_outcome_id AS target_id,
        'has_review_outcome' AS edge_type,
        confidence,
        source,
        evidence,
        provider,
        toString(repo_id) AS repo_id
    FROM work_graph_pr_review_outcome_edges FINAL
    WHERE org_id = {org_id:String}
      AND (pr_id IN {pr_ids:Array(String)} OR review_outcome_id IN {review_outcome_ids:Array(String)})

    UNION ALL

    SELECT
        edge_id,
        'pr' AS source_type,
        pr_id AS source_id,
        'deployment' AS target_type,
        deployment_id AS target_id,
        'deploys' AS edge_type,
        confidence,
        source,
        evidence,
        provider,
        toString(repo_id) AS repo_id
    FROM work_graph_pr_deployment_edges FINAL
    WHERE org_id = {org_id:String}
      AND (pr_id IN {pr_ids:Array(String)} OR deployment_id IN {deployment_ids:Array(String)})

    UNION ALL

    SELECT
        edge_id,
        'deployment' AS source_type,
        deployment_id AS source_id,
        'incident' AS target_type,
        incident_id AS target_id,
        'linked_incident' AS edge_type,
        confidence,
        source,
        evidence,
        provider,
        toString(repo_id) AS repo_id
    FROM work_graph_deployment_incident_edges FINAL
    WHERE org_id = {org_id:String}
      AND (deployment_id IN {deployment_ids:Array(String)} OR incident_id IN {incident_ids:Array(String)})
)
WHERE
    (source_type IN {node_types:Array(String)} AND source_id IN {node_ids:Array(String)})
    OR (target_type IN {node_types:Array(String)} AND target_id IN {node_ids:Array(String)})
LIMIT {limit:UInt32}`

type nodeKey struct{ typ, id string }

func (k nodeKey) less(o nodeKey) bool {
	if k.typ != o.typ {
		return k.typ < o.typ
	}
	return k.id < o.id
}

// workflowRootValue is the stored spelling of a root type enum value.
func workflowRootValue(t model.AIWorkflowRootTypeInput) string { return strings.ToLower(string(t)) }

// WorkflowDrilldown answers aiWorkflowDrilldown: a bounded breadth-first walk
// over the AI workflow evidence edges from an issue, pull request or work
// unit root. Node and edge metadata reads that no response field exposes are
// not performed.
func WorkflowDrilldown(ctx context.Context, client QueryClient, orgID string, rootType model.AIWorkflowRootTypeInput, rootID string, depth, limit int) (*model.AIWorkflowDrilldownResult, error) {
	if client == nil {
		return nil, fmt.Errorf("aianalytics: clickhouse client is required")
	}
	if rootID == "" {
		return nil, fmt.Errorf("root_id is required for AI workflow drilldown")
	}
	if depth < 0 {
		depth = 0
	}
	if limit < 0 {
		limit = 0
	}
	root := workflowRootValue(rootType)
	normalized := root
	if root == "work_unit" {
		normalized = "issue"
	}

	var nodes []model.AIWorkflowGraphNodeOut
	nodeSeen := map[nodeKey]bool{}
	addNode := func(typ, id string) {
		if id == "" || nodeSeen[nodeKey{typ, id}] {
			return
		}
		nodeSeen[nodeKey{typ, id}] = true
		nodes = append(nodes, model.AIWorkflowGraphNodeOut{NodeType: typ, NodeID: id})
	}
	edges := []model.AIWorkflowGraphEdgeOut{}
	edgeSeen := map[string]bool{}
	frontier := map[nodeKey]bool{{normalized, rootID}: true}
	visited := map[nodeKey]bool{}
	partial := false
	addNode(normalized, rootID)

	for hop := 0; hop < depth; hop++ {
		var current []nodeKey
		for k := range frontier {
			if !visited[k] {
				current = append(current, k)
			}
		}
		if len(current) == 0 || len(edges) >= limit {
			break
		}
		sort.Slice(current, func(i, j int) bool { return current[i].less(current[j]) })
		for _, k := range current {
			visited[k] = true
		}
		remaining := limit - len(edges)

		var nodeTypes, nodeIDs, artifactIDs []string
		roles := map[string][]string{}
		for _, k := range current {
			nodeTypes = append(nodeTypes, k.typ)
			nodeIDs = append(nodeIDs, k.id)
			roles[k.typ] = append(roles[k.typ], k.id)
			if k.typ != "issue" && k.typ != "ai_workflow_run" {
				artifactIDs = append(artifactIDs, k.id)
			}
		}
		rows, err := loadWorkflowEdges(ctx, client, orgID, map[string]any{
			"node_types":         nodeTypes,
			"node_ids":           nodeIDs,
			"issue_ids":          orEmpty(roles["issue"]),
			"run_ids":            orEmpty(roles["ai_workflow_run"]),
			"pr_ids":             orEmpty(roles["pr"]),
			"review_outcome_ids": orEmpty(roles["review_outcome"]),
			"deployment_ids":     orEmpty(roles["deployment"]),
			"incident_ids":       orEmpty(roles["incident"]),
			"artifact_ids":       orEmpty(artifactIDs),
			"limit":              uint32(remaining),
		})
		if err != nil {
			return nil, err
		}
		if len(rows) >= remaining {
			partial = true
		}
		next := map[nodeKey]bool{}
		for _, e := range rows {
			if e.EdgeID == "" || edgeSeen[e.EdgeID] {
				continue
			}
			edgeSeen[e.EdgeID] = true
			edges = append(edges, e)
			addNode(e.SourceType, e.SourceID)
			addNode(e.TargetType, e.TargetID)
			for _, k := range []nodeKey{{e.SourceType, e.SourceID}, {e.TargetType, e.TargetID}} {
				if !visited[k] {
					next[k] = true
				}
			}
		}
		frontier = next
	}

	if nodes == nil {
		nodes = []model.AIWorkflowGraphNodeOut{}
	}
	return &model.AIWorkflowDrilldownResult{
		OrgID: orgID, RootType: root, RootID: rootID,
		Nodes: nodes, Edges: edges, Partial: partial, DataAvailable: len(edges) > 0,
	}, nil
}

func orEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func loadWorkflowEdges(ctx context.Context, client QueryClient, orgID string, params map[string]any) ([]model.AIWorkflowGraphEdgeOut, error) {
	names := make([]string, 0, len(params))
	for name := range params {
		names = append(names, name)
	}
	sort.Strings(names)
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}
	for _, name := range names {
		bindings = append(bindings, clickhouse.Binding{Name: name, Value: params[name]})
	}
	rs, err := client.Query(ctx, workflowEdgeStatement, bindings)
	if err != nil {
		return nil, fmt.Errorf("aianalytics: workflow edge query: %w", err)
	}
	defer rs.Close()
	var out []model.AIWorkflowGraphEdgeOut
	for rs.Next() {
		var (
			edgeID, srcType, srcID, tgtType, tgtID, edgeType, source, evidence, provider string
			conf                                                                         float32
			repo                                                                         *string
		)
		if err := rs.Scan(&edgeID, &srcType, &srcID, &tgtType, &tgtID, &edgeType, &conf, &source, &evidence, &provider, &repo); err != nil {
			return nil, fmt.Errorf("aianalytics: workflow edge scan: %w", err)
		}
		out = append(out, model.AIWorkflowGraphEdgeOut{
			EdgeID: edgeID, SourceType: srcType, SourceID: srcID, TargetType: tgtType, TargetID: tgtID,
			EdgeType: edgeType, Confidence: float64(conf), Source: source, Evidence: evidence,
			Provider: &provider, RepoID: repo,
		})
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: workflow edge rows: %w", err)
	}
	return out, nil
}
