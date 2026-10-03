package workgraph

import (
	"context"
	"regexp"
	"strings"
)

// NodeRef is one graph node whose TYPE is known: its type and its id, as an
// edge table stores them.
type NodeRef struct {
	Type string
	ID   string
}

// prColonIDRe is the pull request id of the typed edge tables
// (ai_workflow_artifact_edges, work_graph_pr_review_outcome_edges,
// work_graph_pr_deployment_edges): "{repo_uuid}:{number}", written by
// aiworkflow.prIDFor and workgraphedges.pullRequestID. It is NOT the
// "{repo_uuid}#pr{number}" id of work_graph_edges (prEdgeIDRe).
var prColonIDRe = regexp.MustCompile(`(?i)^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}):(\d+)$`)

// uuidAnywhereRe finds a UUID inside a longer id.
var uuidAnywhereRe = regexp.MustCompile(`(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// NodeDisplayNames returns the display name of each node, in the order given;
// nil means no name is known (CHAOS-8113: the nodes of aiWorkflowDrilldown).
//
// It reads the same three catalogues, with the same statements, as the edge
// ends of workGraphEdges (batchResolveDisplayNames): there is one copy of each
// read. What differs is how a node reaches a read. The ids of work_graph_edges
// carry their form (a "#pr" id, a bare UUID), so batchResolveDisplayNames sorts
// them by form. The ids of the typed edge tables do not: a deployment or an
// incident id is the provider's own opaque id and a pull request id is
// "{repo_uuid}:{number}". Here the node's TYPE selects the read:
//
//   - pr: the pull request's title, for an id in either pull request form.
//   - deployment: "<environment> deploy".
//   - incident: "<title> (<status>)", or "incident (<status>)" with no title.
//   - issue: the id itself when it is a readable key (an issue key). An id that
//     is, or holds, a UUID or that is an opaque hash is not a name.
//   - any other type (a review outcome, an AI workflow run): nil. No catalogue
//     names it, and its id is a provider id or a hash.
//
// A pull request, deployment or incident the catalogue does not name gets nil,
// never its id: those ids hold a repository UUID or an opaque provider id.
//
// Each read is one statement for all the nodes of its type (no N+1), is bound
// to the org, and is best-effort: a failed read is logged and leaves the nodes
// of that type without a name. It never fails the request.
func NodeDisplayNames(ctx context.Context, client QueryClient, orgID string, nodes []NodeRef) []*string {
	out := make([]*string, len(nodes))
	if client == nil || orgID == "" || len(nodes) == 0 {
		return out
	}
	ids := map[string]map[string]struct{}{"pr": {}, "deployment": {}, "incident": {}}
	for _, node := range nodes {
		id := strings.TrimSpace(node.ID)
		if set, ok := ids[nodeTypeKey(node.Type)]; ok && id != "" {
			set[id] = struct{}{}
		}
	}
	names := map[string]map[string]string{"pr": {}, "deployment": {}, "incident": {}}
	if len(ids["pr"]) > 0 {
		resolvePRDisplayNames(ctx, client, orgID, ids["pr"], names["pr"], prEdgeIDRe, prColonIDRe)
	}
	if len(ids["deployment"]) > 0 {
		resolveDeploymentDisplayNames(ctx, client, orgID, ids["deployment"], names["deployment"])
	}
	if len(ids["incident"]) > 0 {
		resolveIncidentDisplayNames(ctx, client, orgID, ids["incident"], names["incident"])
	}
	for i, node := range nodes {
		id := strings.TrimSpace(node.ID)
		if id == "" {
			continue
		}
		typ := nodeTypeKey(node.Type)
		if resolved, ok := names[typ]; ok {
			if name, found := resolved[id]; found && name != "" {
				out[i] = &name
			}
			continue
		}
		if typ == "issue" && !uuidAnywhereRe.MatchString(id) {
			out[i] = displayNameFor(id, nil)
		}
	}
	return out
}

// nodeTypeKey is a node type as the edge tables spell it: lowercase, trimmed.
func nodeTypeKey(nodeType string) string {
	return strings.ToLower(strings.TrimSpace(nodeType))
}
