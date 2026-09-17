package goapiproof

import "sort"

// SankeyRepoFanoutShape, set on a BaselineDefect, narrows that defect's
// blanket "any leaf difference under Paths is covered" rule to the ONE
// transform the sankey repos-join fan-out (repos is
// ReplacingMergeTree(last_synced), joined by the reference plane with no
// FINAL or org_id scoping at all -- restcorpus.go's own
// sankeyRepoDedupParity/investmentFlowRepoDedupParity doc comments) can
// actually produce on a REST sankey response's links (and, where the
// route populates it, node values), instead of admitting any value
// difference under the cited nodes/links paths.
//
// Why the blanket form was not enough, measured against a real capture
// (captured from a production deployed-vs-deployed prove run, GET /api/v1/sankey investment_default_org and
// hotspot_org): once nodes/links are keyed (see OrderInsensitiveList;
// this shape assumes that declaration already resolved position-shift
// noise into per-key findings), the SAME repo ("full-chaos/script-
// manifest") fans out at an EXACT, UNIFORM 2.0x across every edge in its
// own subtree -- the two theme->repo edges targeting it in investment
// mode, and all three edges of its own repo->directory->file->change_type
// chain in hotspot mode. A blanket citation would also admit an
// unrelated regression on any other edge under data.links; this shape
// admits only an edge whose OWN ratio matches its owning repo's verified,
// uniform multiplier.
//
// The shape this type verifies, over the two DECODED sankey subtrees:
//
//  1. A REPO-dimension node (Node.Group is one of RepoNodeGroups, and its
//     name is not one of FallbackAnchorNames -- see below) is an ANCHOR.
//     An anchor's own k is the ONE integer >= 2 that every one of its
//     DIRECTLY incident edges (as source or as target) which differs
//     between the two planes agrees on, within tolerance. An anchor with
//     no differing direct edges, or whose direct edges disagree on k, has
//     NO verified k and explains nothing.
//  2. An anchor's k propagates FORWARD along the candidate graph: an edge
//     whose source is a node already reached from that anchor (the
//     anchor itself, or a node the anchor's own forward walk already
//     reached) is OWNED by that anchor, and reaching its target extends
//     the walk one hop further. An edge whose TARGET is the anchor itself
//     is also owned by it directly (the investment-mode incoming
//     theme->repo edges that establish the anchor's k in the first
//     place), without itself extending the walk past the anchor.
//  3. FallbackAnchorNames excludes the mode's own null-repo fallback
//     label ("Other" for investment mode's normalizeLabel(row.Target,
//     true, "Other"), "Unknown repo" for hotspot mode's
//     normalizeLabel(row.Repo, true, "Unknown repo")) from ever being
//     treated as an anchor: it is a catch-all bucket for an unresolved
//     repo_id, not a specific repos row subject to this mechanism, and
//     mechanically qualifying by Group alone would let it borrow a
//     neighbouring anchor's multiplier dishonestly.
//  4. OPTIONAL, only when NodeValuePath is set (see its own doc comment):
//     an ANCHOR's own node value is admitted when baseline equals
//     candidate times that SAME anchor's own verified k from rule 1,
//     checked directly against the anchor's own value -- not re-summed
//     through the graph. A NON-anchor node (one whose value aggregates
//     over more than one repo, some fanned out and some not) is never
//     admitted by this rule: proving that case needs a separate
//     re-summation or conservation argument this shape does not attempt
//     (RepoFanoutShape's own THEME/TEAM rules are that argument, for the
//     harder GraphQL case where such aggregation is unavoidable), and
//     leaving it uncovered is the safe default, not an oversight.
//
// TWO REPOS FANNING OUT IN THE SAME RESPONSE (ownership overlap), decided
// explicitly rather than by traversal order: ownership (rule 2) is a SET
// per edge -- every anchor whose forward walk or direct-target
// relationship reaches it, not just the first one found. An edge is
// admitted only when, among every owning anchor, the set of DISTINCT k
// values that ACTUALLY VALIDATE against THIS edge's own observed
// baseline/candidate ratio (expected = candidate*k, checked within
// repoFanoutRatioTolerance, exactly as repoFanoutIntegerMultiplier
// already validates a single anchor's own direct edges) has size EXACTLY
// ONE. Zero owning anchors, zero validating k values, or more than one
// DISTINCT validating k value (two anchors reaching the same edge with
// genuinely different multipliers) all leave the edge NOT admitted --
// this is a decline, not a guess, and the underlying sets are built from
// maps keyed by anchor/edge identity, so the verdict cannot depend on
// which anchor's BFS happened to run first.
//
// What this shape CANNOT catch: a repo whose unmerged physical versions
// carry DIFFERENT r.repo text (a rename in progress, not just a
// duplicate) -- that splits a row's contribution across TWO target
// labels instead of multiplying one cleanly, which shows up as a
// structural extra/missing key (a key present on only one side) rather
// than a leaf value difference, and structural differences are never
// covered by ANY BaselineDefect shape (see compare.go's leafDifference
// gate). A non-integer or non-uniform ratio on an anchor's own direct
// edges is the same kind of miss, by design: this shape only ever
// admits the ONE transform the mechanism can cleanly produce. When
// NodeValuePath is set, a NON-anchor node's own value difference is
// ALWAYS outside this shape (rule 4), even when it is entirely explained
// by the SAME fan-out one hop away -- that is left as an uncovered
// finding rather than a guessed re-summation.
type SankeyRepoFanoutShape struct {
	// NodesListPath/LinksListPath are the dotted, index-free paths to the
	// sankey nodes/links LISTS themselves (no trailing field name), e.g.
	// "data.nodes"/"data.links".
	NodesListPath, LinksListPath string
	// LinkValuePath is the leaf path findings carry for a link value
	// difference -- must equal one of the defect's own Paths entries,
	// e.g. "data.links.value".
	LinkValuePath string
	// NodeValuePath is OPTIONAL: the leaf path findings carry for a node
	// value difference -- must equal one of the defect's own Paths
	// entries, e.g. "data.nodes.value". Leave empty for a response whose
	// Node.Value is always null (GET/POST /api/v1/sankey's own
	// sankeyRepoDedupParity entry -- cmd/query-api/internal/sankey's own
	// Node struct doc comment: "_touch_node never sets it" -- citing a
	// value path there would be the appearance of coverage rather than
	// coverage itself, so that entry leaves this field unset and rule 4
	// never runs). investment/flow and investment/flow/repo-team's own
	// response (package investmentflow, reusing this SAME sankey.Response
	// wire type) is different: nodeRunningTotal/nodePresence
	// (investmentflow/builders.go) accumulate a real running total into
	// every node's own Value, so a genuine per-repo fan-out there also
	// moves an anchor's own node value, one hop from an edge this shape
	// already admits -- leaving this field unset on that route would
	// admit only the edge half of a real instance and leave the node half
	// sitting right next to it as an uncovered finding. Unset (the zero
	// value, "") is fully backward compatible: every existing caller that
	// does not set this field keeps its exact prior behaviour, since rule
	// 4 and admittedNodeKeys are both no-ops when it is empty.
	NodeValuePath string
	// RepoNodeGroups names the Node.Group values that mark a node as a
	// repo-dimension anchor, e.g. []string{"project"} for investment
	// mode's target-repo nodes, []string{"repo"} for hotspot mode's root
	// nodes.
	RepoNodeGroups []string
	// FallbackAnchorNames excludes a mode's own null-repo fallback label
	// (see the type doc comment's rule 3) from ever being treated as an
	// anchor.
	FallbackAnchorNames []string
}

// sankeyRepoFanoutPlan is one comparison's fully-evaluated admission
// decision, built once per defect (not per finding) from the two decoded
// sankey nodes/links subtrees.
type sankeyRepoFanoutPlan struct {
	shape *SankeyRepoFanoutShape
	// valid is false when the sankey subtree could not even be read on
	// both sides. A plan that is not valid admits NOTHING -- the safe
	// default, never a guess.
	valid bool
	// admittedLinkKeys is the set of "source\x1ftarget" edge keys this
	// plan admits -- see the type doc comment's overlap rule for how an
	// edge earns a place here.
	admittedLinkKeys map[string]bool
	// admittedNodeKeys is the set of anchor node names rule 4 admits.
	// Always empty when shape.NodeValuePath == "" -- see that field's own
	// doc comment.
	admittedNodeKeys map[string]bool
}

// buildSankeyRepoFanoutPlan evaluates every rule SankeyRepoFanoutShape
// documents against one comparison's decoded baseline/candidate `data`
// values.
func buildSankeyRepoFanoutPlan(shape *SankeyRepoFanoutShape, baselineData, candidateData any) *sankeyRepoFanoutPlan {
	plan := &sankeyRepoFanoutPlan{shape: shape, admittedLinkKeys: map[string]bool{}, admittedNodeKeys: map[string]bool{}}

	candGroups, ok1 := sankeyNodeGroups(candidateData, shape.NodesListPath)
	baseEdges, ok2 := sankeyEdgeInfoMap(baselineData, shape.LinksListPath)
	candEdges, ok3 := sankeyEdgeInfoMap(candidateData, shape.LinksListPath)
	if !ok1 || !ok2 || !ok3 {
		return plan
	}
	plan.valid = true

	isFallback := make(map[string]bool, len(shape.FallbackAnchorNames))
	for _, name := range shape.FallbackAnchorNames {
		isFallback[name] = true
	}
	isRepoGroup := make(map[string]bool, len(shape.RepoNodeGroups))
	for _, group := range shape.RepoNodeGroups {
		isRepoGroup[group] = true
	}

	var anchors []string
	for name, group := range candGroups {
		if isRepoGroup[group] && !isFallback[name] {
			anchors = append(anchors, name)
		}
	}
	// Deterministic order: the VERDICT below never depends on it (every
	// set this function builds is keyed by anchor/edge identity, not by
	// insertion order), but a fixed order keeps this function's own
	// behaviour reproducible for anyone reading it.
	sort.Strings(anchors)

	// Rule 1: each anchor's own verified, uniform k from its DIRECT
	// edges only.
	anchorK := make(map[string]int, len(anchors))
	for _, anchor := range anchors {
		distinct := map[int]bool{}
		for key, candEdge := range candEdges {
			if candEdge.source != anchor && candEdge.target != anchor {
				continue
			}
			baseEdge, ok := baseEdges[key]
			if !ok || baseEdge.value == candEdge.value {
				continue
			}
			if k, ok := repoFanoutIntegerMultiplier(baseEdge.value, candEdge.value); ok {
				distinct[k] = true
			}
		}
		if len(distinct) == 1 {
			for k := range distinct {
				anchorK[anchor] = k
			}
		}
	}

	// Rule 2: forward ownership walk from every anchor with a verified k.
	// ownedBy maps an edge key to the SET of anchors that reach it --
	// the overlap rule reads this set directly, never "whichever anchor
	// got there first".
	ownedBy := map[string]map[string]bool{}
	own := func(key, anchor string) {
		if ownedBy[key] == nil {
			ownedBy[key] = map[string]bool{}
		}
		ownedBy[key][anchor] = true
	}
	for _, anchor := range anchors {
		if _, ok := anchorK[anchor]; !ok {
			continue
		}
		visited := map[string]bool{anchor: true}
		queue := []string{anchor}
		for len(queue) > 0 {
			node := queue[0]
			queue = queue[1:]
			for key, candEdge := range candEdges {
				if candEdge.source != node {
					continue
				}
				own(key, anchor)
				if !visited[candEdge.target] {
					visited[candEdge.target] = true
					queue = append(queue, candEdge.target)
				}
			}
		}
		for key, candEdge := range candEdges {
			if candEdge.target == anchor {
				own(key, anchor)
			}
		}
	}

	// Per-edge admission: exactly one DISTINCT k value validates against
	// THIS edge's own observed ratio, among every owning anchor.
	for key, candEdge := range candEdges {
		baseEdge, ok := baseEdges[key]
		if !ok || baseEdge.value == candEdge.value {
			continue
		}
		owners := ownedBy[key]
		if len(owners) == 0 {
			continue
		}
		validated := map[int]bool{}
		for anchor := range owners {
			k := anchorK[anchor]
			expected := candEdge.value * float64(k)
			if repoFanoutFloatsWithinTolerance(baseEdge.value, expected) {
				validated[k] = true
			}
		}
		if len(validated) == 1 {
			plan.admittedLinkKeys[key] = true
		}
	}

	// Rule 4 (optional): an anchor's OWN node value, checked directly
	// against its OWN verified k -- never attempted for a non-anchor
	// node, and never attempted at all when the shape declares no
	// NodeValuePath.
	if shape.NodeValuePath != "" {
		baseNodeValues, okB := sankeyNodeValuesByName(baselineData, shape.NodesListPath)
		candNodeValues, okC := sankeyNodeValuesByName(candidateData, shape.NodesListPath)
		if okB && okC {
			for anchor, k := range anchorK {
				candValue, hasCand := candNodeValues[anchor]
				baseValue, hasBase := baseNodeValues[anchor]
				if !hasCand || !hasBase {
					continue
				}
				expected := candValue * float64(k)
				if repoFanoutFloatsWithinTolerance(baseValue, expected) {
					plan.admittedNodeKeys[anchor] = true
				}
			}
		}
	}

	return plan
}

// admits reports whether one Finding is covered by this plan.
func (p *sankeyRepoFanoutPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	path := tieredPath(finding.Path)
	switch path {
	case p.shape.LinkValuePath:
		key, ok := parseOrderInsensitiveDetailKey(finding.Detail)
		if !ok {
			return false
		}
		return p.admittedLinkKeys[key]
	case p.shape.NodeValuePath:
		// p.shape.NodeValuePath == "" can never match a real finding's
		// path (tieredPath never produces the empty string), so this case
		// is naturally unreachable when the shape leaves it unset -- no
		// separate guard needed.
		key, ok := parseOrderInsensitiveDetailKey(finding.Detail)
		if !ok {
			return false
		}
		return p.admittedNodeKeys[key]
	}
	return false
}

// sankeyNodeGroups reads a sankey nodes list at listPath (dotted,
// index-free, under `data`) into a name -> group map. ok is false when
// the path does not resolve to a list of objects each carrying a string
// "name" -- the caller treats that as "cannot evaluate the shape at all"
// rather than guessing a partial map. An element whose "group" is null
// (JSON null, e.g. a mode that never sets it) reads as "", which matches
// no RepoNodeGroups entry and is therefore never an anchor -- the safe
// default.
func sankeyNodeGroups(root any, listPath string) (map[string]string, bool) {
	listValue, ok := navigateSegments(root, citedSegments(listPath))
	if !ok {
		return nil, false
	}
	list, ok := listValue.([]any)
	if !ok {
		return nil, false
	}
	out := make(map[string]string, len(list))
	for _, element := range list {
		object, ok := element.(map[string]any)
		if !ok {
			return nil, false
		}
		name, ok := object["name"].(string)
		if !ok {
			return nil, false
		}
		group, _ := object["group"].(string)
		out[name] = group
	}
	return out, true
}

// sankeyNodeValuesByName reads a sankey nodes list at listPath (dotted,
// index-free, under `data`) into a name -> value map, for rule 4's own
// use only -- unlike sankeyNodeValues (repofanout.go, keyed by "id" for
// investmentFull's GraphQL nodes), this response's own Node struct keys
// by "name" (sankey.Node's own json tag). ok is false when the path does
// not resolve to a list of objects each carrying a string "name" -- an
// element whose "value" is null or missing is simply left out of the
// returned map (a null Value is legitimate -- GET/POST /api/v1/sankey's
// own nodes always carry one -- and an anchor with no numeric value on
// either side is never admitted by rule 4, the same safe-default shape
// every other lookup in this package uses).
func sankeyNodeValuesByName(root any, listPath string) (map[string]float64, bool) {
	listValue, ok := navigateSegments(root, citedSegments(listPath))
	if !ok {
		return nil, false
	}
	list, ok := listValue.([]any)
	if !ok {
		return nil, false
	}
	out := make(map[string]float64, len(list))
	for _, element := range list {
		object, ok := element.(map[string]any)
		if !ok {
			return nil, false
		}
		name, ok := object["name"].(string)
		if !ok {
			return nil, false
		}
		value, ok := asFloat(object["value"])
		if !ok {
			continue
		}
		out[name] = value
	}
	return out, true
}
