package goapiproof

import (
	"math"
	"strconv"
	"strings"
)

// RepoFanoutShape, set on a BaselineDefect, narrows that defect's blanket
// "any leaf difference under Paths is covered" rule to the ONE transform
// the repos-join fan-out declared on investmentFull can actually produce,
// instead of admitting any value difference under the cited sankey/coverage
// paths.
//
// Why the blanket form was not enough, measured: the declared citation
// on investmentFull covers data.analytics.sankey.nodes/.edges.value and
// .coverage.teamCoverage/.repoCoverage as a whole subtree, so ANY
// difference under those four paths -- including a real Go sankey
// regression with nothing to do with an unmerged `repos` row -- passed
// as the known defect. It fired for real three times in one day on prod
// (findings 13, 18, 36) and a genuine regression under those same paths
// would have been silently admitted right alongside it.
//
// The shape this type verifies, over the two DECODED `sankey` subtrees:
//
//  1. Every REPO-dimension node's baseline value equals its candidate
//     value times ONE integer k >= 2 (k may differ per repo; a repo
//     whose values agree, or whose ratio is not such an integer, gets no
//     multiplier and stays k == 1 for everything below).
//  2. Every THEME->REPO edge into a repo carries that SAME repo's k.
//  3. Every THEME node's value equals its own candidate value plus the
//     SAME per-repo inflation its own outgoing THEME->REPO edges imply --
//     re-summing the candidate through the repo multipliers, not a fresh
//     recomputation.
//  4. TEAM nodes and TEAM->THEME edges have no repo attribution of their
//     own in the response (a TEAM->THEME edge aggregates over every repo
//     a team's work touches), so they cannot be re-summed per element.
//     They are checked in AGGREGATE instead: the total across every TEAM
//     node, and separately across every TEAM->THEME edge, must equal the
//     candidate total plus the SAME total inflation the REPO nodes imply
//     (conservation: a work unit's effort sums to the same total whether
//     grouped by team, by theme, or by repo -- CompileSankey groups the
//     identical joined row set three different ways). Any TEAM-side
//     difference that does not reconcile at that aggregate -- including
//     one confined to a single edge -- fails the aggregate identity and
//     is reported outside the citation in its entirety, which is the
//     safe direction: it never silently widens admission.
//  5. teamCoverage/repoCoverage are NOT exactly reproducible from the
//     response: they come from sankeycoverage.go's OWN query, over a
//     population resolveSankeyCoverage never exposes per repo (measured:
//     reconstructing repoCoverage from the sankey REPO nodes' own
//     assigned/unassigned split gives 1.0 against an actual 0.9931 on
//     the committed job5 capture -- the two queries read different
//     populations). What IS provable from compileSankeyCoverage's own
//     SQL (sankeycoverage.go): repoCoverage's denominator
//     (`repoTotal`) and numerator (`assignedRepo`) are both
//     `sum(repoEffortCol)`-shaped, i.e. subject to the SAME repos join
//     as every sankey value; and a row with repo_id IS NULL (the
//     unassigned share, `1 - repoCoverage`) can never join `repos` at
//     all (`toString(r.id) = toString(repo_id)`, and NULL never equals
//     anything), so the unassigned share is PROVABLY immune. That gives
//     an exact floor (repoCoverage cannot fall below its candidate
//     value -- the fan-out only inflates assigned rows) and a sound
//     ceiling (assuming the entire assigned share happens to carry the
//     largest observed repo multiplier). teamCoverage shares its
//     denominator with repoCoverage (sankeycoverage.go:210-211,
//     `repoTotalExpr = totalExpr`) but its own numerator
//     (`assignedTeam`) has no such immune share -- team assignment is
//     independent of repo_id nullity -- so its bound is wider on both
//     sides. Both are BOUNDS, not the literal SQL, and are documented as
//     such rather than silently treated as exact.
//
// Anything outside these five rules -- a non-integer ratio, a repo whose
// own edges do not carry its node's k, a THEME/TEAM total that does not
// reconcile, a coverage ratio outside its bound -- is NOT covered:
// classifyBaselineDefects reports it as an ordinary difference outside
// the citation, exactly like any other uncited mismatch.
type RepoFanoutShape struct {
	// NodesListPath/EdgesListPath are the dotted, index-free paths to the
	// sankey nodes/edges LISTS themselves (no trailing ".value"), e.g.
	// "data.analytics.sankey.nodes".
	NodesListPath, EdgesListPath string
	// NodeValuePath/EdgeValuePath are the leaf paths findings carry for a
	// node/edge value difference -- must equal one of the defect's own
	// Paths entries, e.g. "data.analytics.sankey.nodes.value".
	NodeValuePath, EdgeValuePath string
	// TeamCoveragePath/RepoCoveragePath are the coverage leaf paths --
	// must equal two more of the defect's own Paths entries.
	TeamCoveragePath, RepoCoveragePath string
}

// repoFanoutRatioTolerance is this file's own relative tolerance, applied
// to sums the comparator elsewhere already treats as FloatTierB
// (summation-order noise across many rows) -- generous enough to absorb
// that noise, tight enough that a percent-level drift (this file's own
// red test) never rounds away.
const repoFanoutRatioTolerance = 1e-6

// repoFanoutMinMultiplier is condition 1's floor: a multiplier of 1 is
// "no observed difference", never a citable defect instance.
const repoFanoutMinMultiplier = 2

// repoFanoutEdge is one decoded sankey edge.
type repoFanoutEdge struct {
	source, target string
	value          float64
}

// repoFanoutPlan is one comparison's fully-evaluated admission decision,
// built once per defect (not per finding) from the two decoded `sankey`
// subtrees.
type repoFanoutPlan struct {
	shape *RepoFanoutShape
	// valid is false when the sankey subtree could not even be read on
	// both sides (wrong operation, malformed body, ...). A plan that is
	// not valid admits NOTHING -- the safe default, never a guess.
	valid bool

	// repoMultiplier maps a REPO node id to its verified integer k >= 2.
	// A repo absent here is k == 1: either no observed difference, or a
	// difference that failed validation (non-integer, k < 2, or baseline
	// nonzero against a zero candidate) -- both cases must stay
	// unexplained, and defaulting to k == 1 makes every downstream
	// re-sum correctly expect "no change" for it, which a genuine
	// unexplained difference then fails to match.
	repoMultiplier map[string]int
	kMax           int

	admittedNodeKeys map[string]bool // REPO/THEME node id -> admitted
	admittedEdgeKeys map[string]bool // "source\x1ftarget" -> admitted

	teamAggregateAdmits bool
	teamCoverageAdmits  bool
	repoCoverageAdmits  bool
}

// buildRepoFanoutPlan evaluates every rule RepoFanoutShape documents
// against one comparison's decoded baseline/candidate `data` values.
func buildRepoFanoutPlan(shape *RepoFanoutShape, baselineData, candidateData any) *repoFanoutPlan {
	plan := &repoFanoutPlan{
		shape:            shape,
		repoMultiplier:   map[string]int{},
		admittedNodeKeys: map[string]bool{},
		admittedEdgeKeys: map[string]bool{},
	}

	baseNodes, ok1 := sankeyNodeValues(baselineData, shape.NodesListPath)
	candNodes, ok2 := sankeyNodeValues(candidateData, shape.NodesListPath)
	baseEdges, ok3 := sankeyEdgeInfoMap(baselineData, shape.EdgesListPath)
	candEdges, ok4 := sankeyEdgeInfoMap(candidateData, shape.EdgesListPath)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return plan
	}
	plan.valid = true

	// --- Rule 1: per-repo integer multiplier -------------------------
	for id, candValue := range candNodes {
		if dimensionOf(id) != "REPO" {
			continue
		}
		baseValue, present := baseNodes[id]
		if !present {
			continue
		}
		if k, ok := repoFanoutIntegerMultiplier(baseValue, candValue); ok {
			plan.repoMultiplier[id] = k
			if k > plan.kMax {
				plan.kMax = k
			}
		}
	}
	if plan.kMax < 1 {
		plan.kMax = 1
	}
	for id := range candNodes {
		if dimensionOf(id) != "REPO" {
			continue
		}
		if _, ok := plan.repoMultiplier[id]; ok {
			plan.admittedNodeKeys[id] = true
		}
	}

	// --- Rule 2: THEME->REPO edges carry their target repo's k --------
	// Also accumulates, per THEME id, the SAME inflation its own edges
	// imply, and the TEAM<->THEME edge totals rule 4 needs -- computed
	// unconditionally on the resolved k (default 1), never gated on
	// whether the edge itself validated, so an unrelated drift on one
	// edge cannot hide inside a theme total that silently omits it: the
	// theme's re-summed expectation stays "what the repo multipliers
	// alone explain", and a real baseline that also carries the drift
	// diverges from it exactly enough to fail rule 3 too.
	themeInflation := map[string]float64{}
	var teamThemeCandidateTotal, teamThemeBaselineTotal float64
	for key, edge := range candEdges {
		baseEdge, present := baseEdges[key]
		if !present {
			continue
		}
		switch {
		case dimensionOf(edge.source) == "THEME" && dimensionOf(edge.target) == "REPO":
			k := 1
			if kk, ok := plan.repoMultiplier[edge.target]; ok {
				k = kk
			}
			expected := edge.value * float64(k)
			if repoFanoutFloatsWithinTolerance(baseEdge.value, expected) {
				plan.admittedEdgeKeys[key] = true
			}
			themeInflation[edge.source] += (float64(k) - 1) * edge.value
		case dimensionOf(edge.source) == "TEAM" && dimensionOf(edge.target) == "THEME":
			teamThemeCandidateTotal += edge.value
			teamThemeBaselineTotal += baseEdge.value
		}
	}

	// --- Rule 3: THEME nodes re-sum from their own edges ---------------
	for id, candValue := range candNodes {
		if dimensionOf(id) != "THEME" {
			continue
		}
		baseValue, present := baseNodes[id]
		if !present {
			continue
		}
		expected := candValue + themeInflation[id]
		if repoFanoutFloatsWithinTolerance(baseValue, expected) {
			plan.admittedNodeKeys[id] = true
		}
	}

	// --- Rule 4: TEAM nodes/edges, aggregate only -----------------------
	var totalInflation float64
	for id, k := range plan.repoMultiplier {
		totalInflation += (float64(k) - 1) * candNodes[id]
	}
	var teamNodeCandidateTotal, teamNodeBaselineTotal float64
	for id, candValue := range candNodes {
		if dimensionOf(id) != "TEAM" {
			continue
		}
		baseValue, present := baseNodes[id]
		if !present {
			continue
		}
		teamNodeCandidateTotal += candValue
		teamNodeBaselineTotal += baseValue
	}
	plan.teamAggregateAdmits = plan.valid &&
		repoFanoutFloatsWithinTolerance(teamNodeBaselineTotal, teamNodeCandidateTotal+totalInflation) &&
		repoFanoutFloatsWithinTolerance(teamThemeBaselineTotal, teamThemeCandidateTotal+totalInflation)

	// --- Rule 5: coverage bounds -----------------------------------
	repoCovCand, okRC := floatAtDottedPath(candidateData, shape.RepoCoveragePath)
	repoCovBase, okRB := floatAtDottedPath(baselineData, shape.RepoCoveragePath)
	if okRC && okRB {
		lo := repoCovCand
		hi := repoFanoutRepoCoverageCeiling(repoCovCand, plan.kMax)
		plan.repoCoverageAdmits = repoCovBase >= lo-repoFanoutRatioTolerance && repoCovBase <= hi+repoFanoutRatioTolerance
	}
	teamCovCand, okTC := floatAtDottedPath(candidateData, shape.TeamCoveragePath)
	teamCovBase, okTB := floatAtDottedPath(baselineData, shape.TeamCoveragePath)
	if okTC && okTB && okRC {
		totalHiFactor := (1 - repoCovCand) + repoCovCand*float64(plan.kMax)
		lo := teamCovCand
		if totalHiFactor > 0 {
			lo = teamCovCand / totalHiFactor
		}
		hi := teamCovCand * float64(plan.kMax)
		if hi > 1 {
			hi = 1
		}
		plan.teamCoverageAdmits = teamCovBase >= lo-repoFanoutRatioTolerance && teamCovBase <= hi+repoFanoutRatioTolerance
	}

	return plan
}

// repoFanoutRepoCoverageCeiling is rule 5's repoCoverage ceiling: the
// unassigned share (1-a) is provably immune (a repo_id IS NULL row can
// never join `repos`), so the ceiling is reached when the ENTIRE assigned
// share carries the largest observed multiplier.
func repoFanoutRepoCoverageCeiling(candidateRepoCoverage float64, kMax int) float64 {
	a := candidateRepoCoverage
	denominator := (1 - a) + a*float64(kMax)
	if denominator <= 0 {
		return a
	}
	return (a * float64(kMax)) / denominator
}

// admits reports whether one Finding is covered by this plan.
func (p *repoFanoutPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	path := tieredPath(finding.Path)
	switch path {
	case p.shape.TeamCoveragePath:
		return p.teamCoverageAdmits
	case p.shape.RepoCoveragePath:
		return p.repoCoverageAdmits
	case p.shape.NodeValuePath:
		key, ok := parseOrderInsensitiveDetailKey(finding.Detail)
		if !ok {
			return false
		}
		switch dimensionOf(key) {
		case "TEAM":
			return p.teamAggregateAdmits
		default:
			return p.admittedNodeKeys[key]
		}
	case p.shape.EdgeValuePath:
		key, ok := parseOrderInsensitiveDetailKey(finding.Detail)
		if !ok {
			return false
		}
		source, target, ok := strings.Cut(key, "\x1f")
		if !ok {
			return false
		}
		if dimensionOf(source) == "TEAM" && dimensionOf(target) == "THEME" {
			return p.teamAggregateAdmits
		}
		return p.admittedEdgeKeys[key]
	}
	return false
}

// dimensionOf reads a sankey node/edge-endpoint id's dimension prefix
// ("REPO:full-chaos/x" -> "REPO"); an id with no ":" is its own
// (unrecognised) dimension, which admits nothing below.
func dimensionOf(id string) string {
	if idx := strings.IndexByte(id, ':'); idx >= 0 {
		return id[:idx]
	}
	return id
}

// repoFanoutFloatsWithinTolerance mirrors compareNumber's own max(abs,
// rel) shape, at this file's own (looser) tolerance -- see
// repoFanoutRatioTolerance.
func repoFanoutFloatsWithinTolerance(a, b float64) bool {
	tol := math.Max(repoFanoutRatioTolerance, repoFanoutRatioTolerance*math.Max(math.Abs(a), math.Abs(b)))
	return math.Abs(a-b) <= tol
}

// repoFanoutIntegerMultiplier reports whether baseline = candidate * k for
// one integer k >= repoFanoutMinMultiplier, within tolerance. A zero
// candidate can never explain a nonzero baseline via multiplication --
// there is no finite k -- so that case is always unexplained.
func repoFanoutIntegerMultiplier(baseline, candidate float64) (int, bool) {
	if candidate == 0 {
		return 0, false
	}
	ratio := baseline / candidate
	if ratio < float64(repoFanoutMinMultiplier)-0.5 {
		return 0, false
	}
	rounded := math.Round(ratio)
	if rounded < repoFanoutMinMultiplier {
		return 0, false
	}
	if math.Abs(ratio-rounded) > repoFanoutRatioTolerance*rounded {
		return 0, false
	}
	return int(rounded), true
}

// --- decoding helpers ---------------------------------------------------

// navigateSegments walks a decoded JSON value through a chain of object
// keys, exactly as nonNullLeaves does, but returning the value reached
// rather than a leaf count.
func navigateSegments(value any, segments []string) (any, bool) {
	for _, seg := range segments {
		m, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok = m[seg]
		if !ok {
			return nil, false
		}
	}
	return value, true
}

// floatAtDottedPath reads one scalar leaf at a dotted, index-free path
// under `data` (the same form BaselineDefect.Paths uses).
func floatAtDottedPath(root any, dottedPath string) (float64, bool) {
	value, ok := navigateSegments(root, citedSegments(dottedPath))
	if !ok {
		return 0, false
	}
	return asFloat(value)
}

// sankeyNodeValues reads a sankey nodes list at listPath (dotted,
// index-free, under `data`) into an id -> value map. ok is false when the
// path does not resolve to a list of objects each carrying a string "id"
// -- the caller treats that as "cannot evaluate the shape at all" rather
// than guessing a partial map.
func sankeyNodeValues(root any, listPath string) (map[string]float64, bool) {
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
		id, ok := object["id"].(string)
		if !ok {
			return nil, false
		}
		value, ok := asFloat(object["value"])
		if !ok {
			continue
		}
		out[id] = value
	}
	return out, true
}

// sankeyEdgeInfoMap reads a sankey edges list the same way, keyed by
// "source\x1ftarget" -- the SAME join orderInsensitiveKey uses for
// sankey.edges' declared KeyFields (["source","target"]), so a key built
// here lines up directly with parseOrderInsensitiveDetailKey's output.
func sankeyEdgeInfoMap(root any, listPath string) (map[string]repoFanoutEdge, bool) {
	listValue, ok := navigateSegments(root, citedSegments(listPath))
	if !ok {
		return nil, false
	}
	list, ok := listValue.([]any)
	if !ok {
		return nil, false
	}
	out := make(map[string]repoFanoutEdge, len(list))
	for _, element := range list {
		object, ok := element.(map[string]any)
		if !ok {
			return nil, false
		}
		source, ok1 := object["source"].(string)
		target, ok2 := object["target"].(string)
		if !ok1 || !ok2 {
			return nil, false
		}
		value, ok := asFloat(object["value"])
		if !ok {
			continue
		}
		out[source+"\x1f"+target] = repoFanoutEdge{source: source, target: target, value: value}
	}
	return out, true
}

// parseOrderInsensitiveDetailKey recovers the raw pairing key
// compareListByKey embedded in a finding's Detail (`[key=%q] ...`,
// %q-quoted so an id containing a literal quote or backslash still
// round-trips). strconv.QuotedPrefix finds exactly the quoted span
// regardless of what it contains, so this never mis-splits on a quoted
// "] " the way a plain substring search could.
func parseOrderInsensitiveDetailKey(detail string) (string, bool) {
	rest, ok := strings.CutPrefix(detail, "[key=")
	if !ok {
		return "", false
	}
	quoted, err := strconv.QuotedPrefix(rest)
	if err != nil {
		return "", false
	}
	key, err := strconv.Unquote(quoted)
	if err != nil {
		return "", false
	}
	if !strings.HasPrefix(rest[len(quoted):], "] ") {
		return "", false
	}
	return key, true
}
