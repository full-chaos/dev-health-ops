package goapiproof

import (
	"strconv"
	"strings"
)

// TeamCoverageIdentityShape, set on a BaselineDefect, narrows that
// defect's blanket "any leaf difference under Paths is covered" rule to
// a per-leg arithmetic identity between a sankey response's own
// team_coverage leaf(ves) and its own team-group nodes, instead of
// admitting any coverage difference under the cited paths.
//
// The identity, proven from source: coverageStats (cmd/query-api/
// internal/investmentflow/builders.go) computes
// teamCoverage = assignedTeamValue / totalValue over the fetched rows,
// and Python's build_investment_flow_response ports the identical sum
// (investment_flow.py: total_value/assigned_team_value). Separately, a
// TEAM-group node's own value is finishPresenceEdges' max(incoming,
// outgoing) (builders.go); a team node's incoming is always zero (no
// edge ever targets a team label in either _build_team_burden_sankey or
// _build_team_theme_subcategory_repo_sankey), so its value is its
// outgoing total -- the sum of every row's value that named that team,
// exactly the same sum coverageStats' own per-team accumulation
// performs. Summing every team-group node therefore reproduces
// totalValue, and summing every one EXCEPT the unassigned-team bucket
// (UnassignedTeamNodeName, investmentflow/builders.go's own
// unassignedTeamLabel constant, "Unassigned team") reproduces
// assignedTeamValue -- on EACH plane independently, from that plane's
// OWN nodes and that plane's OWN reported ratio. Team-group nodes are
// never truncated or bucketed by either builder (only repoRollupMap
// rolls up repos past TopNRepos into "Other repos"; no equivalent exists
// for teams on either plane), so the identity's inputs are never
// silently clipped.
//
// This shape does NOT re-derive why any individual node differs between
// planes -- that is the sibling supersession-exclusion (KeyedDirectionShape)
// and repos-join fan-out (SankeyRepoFanoutShape) declarations' own job.
// It only checks: (1) the identity
// holds on the baseline body against the baseline's own nodes, and holds
// on the candidate body against the candidate's own nodes -- so
// team_coverage has no source other than the same node values already
// in play; and (2) every team-group node difference in this SAME
// comparison is already covered by some other declared defect that
// fired. When both hold, the team_coverage difference has no source
// other than the covered nodes, and admitting it adds no unverified
// claim.
//
// What this shape CANNOT catch: a node difference this package's other
// shapes did not admit (an unrelated Go regression touching a team
// node, or a genuinely new mechanism) leaves the identity's own inputs
// unexplained, and rule (2) above refuses admission -- the safe
// default, not a gap.
type TeamCoverageIdentityShape struct {
	// NodesListPath is the dotted, index-free path to the sankey nodes
	// list itself, e.g. "data.nodes".
	NodesListPath string
	// TeamGroup is the Node.Group value marking a team-level node --
	// investmentflow/builders.go's own literal "team"
	// (nodePresence.add(teamLabel, "team")).
	TeamGroup string
	// UnassignedTeamNodeName is the exact Node.Name the unassigned-team
	// bucket carries on both planes -- investmentflow/builders.go's own
	// unassignedTeamLabel constant ("Unassigned team"), mirrored
	// byte-for-byte by investment_flow.py's UNASSIGNED_TEAM_LABEL. Never
	// a guessed display string.
	UnassignedTeamNodeName string
	// CoveragePaths are the leaf paths this shape may admit -- must be a
	// subset of the defect's own Paths entries, e.g.
	// []string{"data.team_coverage", "data.coverage.team_coverage"}.
	CoveragePaths []string
}

// teamCoverageIdentityPlan is one comparison's fully-evaluated admission
// decision, built once per defect from the two decoded response bodies,
// the whole comparison's tiered mismatch paths, and the FINAL `covered`
// state every other declared defect already reached.
type teamCoverageIdentityPlan struct {
	// valid is a WHOLE-COMPARISON verdict: true only when the identity
	// holds on both legs AND every team-group node difference here is
	// already covered. false admits NOTHING -- the safe default.
	valid bool
}

// buildTeamCoverageIdentityPlan evaluates TeamCoverageIdentityShape's
// two rules. It must run only after every non-identity defect in this
// comparison has already decided `covered` -- classifyBaselineDefects
// enforces that ordering, never this function.
func buildTeamCoverageIdentityPlan(
	shape *TeamCoverageIdentityShape,
	baselineData, candidateData any,
	mismatches []string,
	findingRefs []int,
	findings []Finding,
	covered []bool,
) *teamCoverageIdentityPlan {
	plan := &teamCoverageIdentityPlan{}

	baseGroups, ok1 := sankeyNodeGroups(baselineData, shape.NodesListPath)
	candGroups, ok2 := sankeyNodeGroups(candidateData, shape.NodesListPath)
	baseValues, ok3 := sankeyNodeValuesByName(baselineData, shape.NodesListPath)
	candValues, ok4 := sankeyNodeValuesByName(candidateData, shape.NodesListPath)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return plan
	}

	baseIdentity, okB := teamCoverageFromNodes(baseGroups, baseValues, shape)
	candIdentity, okC := teamCoverageFromNodes(candGroups, candValues, shape)
	if !okB || !okC {
		return plan
	}

	// Rule 1: the identity holds on EACH leg independently, against that
	// SAME leg's own reported coverage leaves.
	for _, path := range shape.CoveragePaths {
		baseReported, okBR := floatAtDottedPath(baselineData, path)
		candReported, okCR := floatAtDottedPath(candidateData, path)
		if !okBR || !okCR {
			return plan
		}
		if !floatsMatchTiered(baseReported, baseIdentity) {
			return plan
		}
		if !floatsMatchTiered(candReported, candIdentity) {
			return plan
		}
	}

	// Rule 2: every team-group node difference in this comparison
	// (value or presence) is already covered by another declared defect.
	nodeValuePath := shape.NodesListPath + ".value"
	for i, path := range mismatches {
		if path != nodeValuePath && path != shape.NodesListPath {
			continue
		}
		key, ok := findingGroupKey(findings[findingRefs[i]])
		if !ok {
			// Cannot identify which node this finding is about -- refuse
			// rather than guess it is unrelated to the team group.
			return plan
		}
		group, known := baseGroups[key]
		if !known {
			group, known = candGroups[key]
		}
		if !known || group != shape.TeamGroup {
			continue
		}
		if !covered[i] {
			return plan
		}
	}

	plan.valid = true
	return plan
}

// admits reports whether one Finding is covered by this plan. Every
// finding under CoveragePaths shares the SAME whole-comparison verdict,
// exactly like CoverageShiftShape's own admits.
func (p *teamCoverageIdentityPlan) admits(Finding) bool {
	return p != nil && p.valid
}

// teamCoverageFromNodes sums every node whose Group matches
// shape.TeamGroup into total, and every one of those EXCEPT
// shape.UnassignedTeamNodeName into assigned, returning assigned/total.
// ok is false when no team-group node exists at all, or the total is
// non-positive -- there is nothing to check the identity against, so
// the safe default is "cannot verify", never a guessed ratio.
func teamCoverageFromNodes(groups map[string]string, values map[string]float64, shape *TeamCoverageIdentityShape) (float64, bool) {
	var total, assigned float64
	found := false
	for name, group := range groups {
		if group != shape.TeamGroup {
			continue
		}
		value, ok := values[name]
		if !ok {
			continue
		}
		found = true
		total += value
		if name != shape.UnassignedTeamNodeName {
			assigned += value
		}
	}
	if !found || total <= 0 {
		return 0, false
	}
	return assigned / total, true
}

// findingGroupKey recovers the order-insensitive pairing key from a
// finding's Detail, whether it is a paired-element VALUE difference
// (compareListByKey's own "[key=%q] ..." prefix, parsed by
// parseOrderInsensitiveDetailKey) or a whole-element PRESENCE difference
// (compareListByKey's own "key %q present in ..." sentence).
func findingGroupKey(f Finding) (string, bool) {
	if key, ok := parseOrderInsensitiveDetailKey(f.Detail); ok {
		return key, true
	}
	return parsePresenceDetailKey(f.Detail)
}

// parsePresenceDetailKey recovers the raw pairing key from a
// compareListByKey PRESENCE finding's Detail ("key %q present in
// baseline, absent in candidate" / "key %q present in candidate, absent
// in baseline") -- the same %q-quoted key parseOrderInsensitiveDetailKey
// reads from a paired-element VALUE finding's own "[key=%q] ..." prefix,
// just without the surrounding brackets a presence finding never gets.
func parsePresenceDetailKey(detail string) (string, bool) {
	rest, ok := strings.CutPrefix(detail, "key ")
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
	return key, true
}
