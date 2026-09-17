package goapiproof

import (
	"sort"
	"strings"
)

// HotspotListBoundaryShape, set on a BaselineDefect, narrows that
// defect's blanket "any leaf difference under Paths is covered" rule to
// the FULL consequence set a value-multiplying baseline defect produces
// on the hotspot sankey's own two-level structure: a bounded, value-DESC
// FILE list feeding a repo->directory PARENT SUM one level above it.
//
// Established from source: fetch_hotspot_rows (api/queries/sankey.py:
// 136-208) and fetchHotspotRows (sankey/queries.go:401-478) both GROUP BY
// repo, directory, file_path, ORDER BY churn DESC (no secondary sort --
// the SAME unordered-tie property sankeyRepoDedupParity's own
// OrderInsensitiveList entries already declare) and LIMIT the same
// MAX_HOTSPOT_ROWS/maxHotspotRows constant (services/sankey.py:59,
// sankey.go:97) -- never a request-level override; hotspot's query
// params carry no limit field (services/sankey.py's own
// _build_hotspot_flow never reads filters.limit). _build_hotspot_flow
// (services/sankey.py:476-534) and buildHotspotFlow (sankey/builders.go:
// 206-252) both derive a repo->directory LINK value as the SUM of every
// LISTED file's own churn under that directory (_add_edge/addEdge,
// keyed by (source, target)); a directory or file->change_type edge is
// otherwise 1:1 with one file row. A value-multiplying row (the repos-
// join fan-out sankeyRepoDedupParity's own SankeyRepoFanoutShape entry,
// same BaselineDefect list, same Ticket) can therefore (a) cross the
// file-list boundary, entering one plane's list at the cost of whichever
// file currently ranks last on the other plane, exactly as
// LimitDisplacementShape (limitdisplacement.go) already admits for a
// FLAT list, and (b) shift a repo->directory parent link's own value by
// the leaving/entering files' contribution plus the fan-out's own
// inflation of the children still shared by both legs -- a SUM
// consequence LimitDisplacementShape has no notion of, since a flat
// list carries no parent to re-sum. When every one of a directory's
// listed children falls on one leg only, the directory's own node and
// its repo->directory link vanish entirely from the other leg -- a
// PRESENCE difference, not a value one (confirmed live: GET's later run
// keeps a shared child under every affected directory, POST's does not
// for two of them).
//
// The shape this type verifies, over the two DECODED `data.nodes`/
// `data.links` subtrees, reconstructing the flat file list and its
// repo/directory ownership from the graph itself (FileNodeGroup/
// DirectoryNodeGroup/RepoNodeGroup node groups, the directory->file and
// repo->directory edges):
//
//  1. Both reconstructed file lists are EXACTLY Limit long -- the route's
//     own fixed LIMIT, never a per-request value (see the type doc
//     comment above). Anything else refuses the WHOLE plan, the same
//     discipline as LimitDisplacementShape's own rule 1.
//  2. A repository's multiplier k is read ONLY from this comparison's
//     ALREADY-COVERED LinkValuePath findings whose edge source is a
//     repo-group node -- the sibling SankeyRepoFanoutShape entry's own
//     admitted anchor edges (same BaselineDefect list, evaluated first;
//     classifyBaselineDefects' own second pass, compare.go, is what
//     makes this legal). Never a fixed repository, never independently
//     re-derived: a repository with no such covered finding gets no
//     multiplier, and every rule below then treats it as k=1 (no known
//     fan-out), exactly LimitDisplacementShape's own repoRatios
//     discipline.
//  3. A baseline-only (leaving) file is an ENTRANT admission candidate
//     when its own repository carries a verified k>1 and its value
//     divided by k -- its true, undoubled value -- sits at or under the
//     candidate list's own minimum. A candidate-only (entering) file is
//     admitted one-for-one against however many rule 3's baseline side
//     actually admitted, LOWEST-VALUED first, each still required to sit
//     at or under baseline's own stable floor (its minimum excluding
//     every baseline-only file) -- identical to LimitDisplacementShape's
//     own rules 2/3, applied to the reconstructed list instead of a
//     literal response list.
//  4. Once rule 3 decides the file sets: an admitted file's own NODE and
//     its two LINKS (directory->file, file->change_type) are admitted,
//     in the same direction as the file itself. A directory that has NO
//     presence at all on one leg is admitted (node + its own
//     repo->directory link, same direction) only when EVERY one of its
//     listed children on the leg where it still exists was itself
//     admitted by rule 3 -- an unexplained child leaves the whole
//     directory outside. A directory present on BOTH legs whose own
//     repo->directory link VALUE differs is admitted only when it
//     re-sums EXACTLY: baseline = candidate + (leaving children's own
//     baseline values) - (entering children's own candidate values) +
//     (k-1)*(shared children's own candidate values) -- an unadmitted
//     leaving/entering child under that SAME directory leaves the
//     identity unable to close, and the parent link stays outside.
//  5. Everything else -- a presence difference belonging to neither an
//     admitted file nor a fully-admitted directory, a parent link whose
//     identity does not close to the printed value, either list off the
//     boundary -- stays outside. In particular: ORDER BY churn DESC has
//     no secondary sort on EITHER plane, so two files with genuinely
//     equal churn at the boundary can swap position with no doubled
//     repository involved at all; that swap fails rule 3's own
//     repository/multiplier check (no repo of a merely-tied file carries
//     a verified k unless it is independently also the fanned-out repo)
//     and is correctly left outside, never admitted by coincidence.
type HotspotListBoundaryShape struct {
	// NodesListPath/LinksListPath are the dotted, index-free paths to the
	// sankey nodes/links LISTS themselves, e.g. "data.nodes"/"data.links".
	NodesListPath, LinksListPath string
	// LinkValuePath is the leaf path findings carry for a link value
	// difference -- must equal the defect's own Paths entry, e.g.
	// "data.links.value", and the SAME path the sibling
	// SankeyRepoFanoutShape entry's own LinkValuePath already covers.
	LinkValuePath string
	// RepoNodeGroups/DirectoryNodeGroup/FileNodeGroup name the Node.Group
	// values marking a repo-, directory- and file-dimension node, e.g.
	// "repo"/"directory"/"file" for hotspot mode.
	RepoNodeGroup, DirectoryNodeGroup, FileNodeGroup string
	// Limit is the route's own fixed file-list LIMIT (never a per-request
	// override on this route -- see the type doc comment).
	Limit int
}

// hotspotFileRow is one decoded file's projection: its own churn value,
// the directory/repository it hangs from, and the two link keys
// ("source\x1ftarget") a presence admission of this file must also
// admit.
type hotspotFileRow struct {
	value             float64
	dirKey            string
	repo              string
	dirToFileLinkKey  string
	fileToTypeLinkKey string
}

// hotspotDirRow is one decoded directory's projection: its own
// repository and the repo->directory link key, plus every file name
// listed under it on this ONE leg.
type hotspotDirRow struct {
	repo        string
	repoLinkKey string
	children    []string
}

// hotspotListBoundaryPlan is one comparison's fully-evaluated admission
// decision, built once per defect from the two decoded graphs, the whole
// comparison's tiered mismatch paths, and the FINAL `covered` state
// every other declared defect already reached (the sibling
// SankeyRepoFanoutShape entry, specifically).
type hotspotListBoundaryPlan struct {
	shape *HotspotListBoundaryShape
	valid bool
	// admittedBaselineOnly/admittedCandidateOnly hold every admitted
	// PRESENCE key -- file node names, their two link keys, and (for a
	// vanished directory) the directory's own node name and
	// repo->directory link key -- keyed uniformly since a presence
	// finding's raw key is the same shape regardless of what kind of
	// node or link it names.
	admittedBaselineOnly  map[string]bool
	admittedCandidateOnly map[string]bool
	// admittedParentValue holds every repo->directory link key whose own
	// VALUE finding re-sums exactly (rule 4's parent identity).
	admittedParentValue map[string]bool
}

// decodeHotspotGraph reconstructs one leg's file/directory projections
// from its already-decoded node-group and edge maps.
func decodeHotspotGraph(groups map[string]string, edges map[string]repoFanoutEdge, shape *HotspotListBoundaryShape) (map[string]hotspotFileRow, map[string]hotspotDirRow) {
	dirs := map[string]hotspotDirRow{}
	for key, edge := range edges {
		if groups[edge.source] == shape.RepoNodeGroup && groups[edge.target] == shape.DirectoryNodeGroup {
			d := dirs[edge.target]
			d.repo = edge.source
			d.repoLinkKey = key
			dirs[edge.target] = d
		}
	}
	typeLinkOf := map[string]string{}
	for key, edge := range edges {
		if groups[edge.source] == shape.FileNodeGroup {
			typeLinkOf[edge.source] = key
		}
	}
	files := map[string]hotspotFileRow{}
	for key, edge := range edges {
		if groups[edge.source] != shape.DirectoryNodeGroup || groups[edge.target] != shape.FileNodeGroup {
			continue
		}
		d := dirs[edge.source]
		d.children = append(d.children, edge.target)
		dirs[edge.source] = d
		files[edge.target] = hotspotFileRow{
			value:             edge.value,
			dirKey:            edge.source,
			repo:              d.repo,
			dirToFileLinkKey:  key,
			fileToTypeLinkKey: typeLinkOf[edge.target],
		}
	}
	return files, dirs
}

// buildHotspotListBoundaryPlan evaluates every rule HotspotListBoundaryShape
// documents. It must run only after the sibling SankeyRepoFanoutShape
// entry (same BaselineDefect list) has already decided `covered` --
// classifyBaselineDefects' own second pass enforces that ordering, never
// this function.
func buildHotspotListBoundaryPlan(
	shape *HotspotListBoundaryShape,
	baselineData, candidateData any,
	mismatches []string,
	findingRefs []int,
	findings []Finding,
	covered []bool,
) *hotspotListBoundaryPlan {
	plan := &hotspotListBoundaryPlan{
		shape:                 shape,
		admittedBaselineOnly:  map[string]bool{},
		admittedCandidateOnly: map[string]bool{},
		admittedParentValue:   map[string]bool{},
	}

	baseGroups, ok1 := sankeyNodeGroups(baselineData, shape.NodesListPath)
	candGroups, ok2 := sankeyNodeGroups(candidateData, shape.NodesListPath)
	baseEdges, ok3 := sankeyEdgeInfoMap(baselineData, shape.LinksListPath)
	candEdges, ok4 := sankeyEdgeInfoMap(candidateData, shape.LinksListPath)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return plan
	}

	baseFiles, baseDirs := decodeHotspotGraph(baseGroups, baseEdges, shape)
	candFiles, candDirs := decodeHotspotGraph(candGroups, candEdges, shape)

	// Rule 1.
	if len(baseFiles) != shape.Limit || len(candFiles) != shape.Limit {
		return plan
	}
	var baselineOnly, candidateOnly []string
	for name := range baseFiles {
		if _, ok := candFiles[name]; !ok {
			baselineOnly = append(baselineOnly, name)
		}
	}
	for name := range candFiles {
		if _, ok := baseFiles[name]; !ok {
			candidateOnly = append(candidateOnly, name)
		}
	}
	if len(baselineOnly) == 0 || len(baselineOnly) != len(candidateOnly) {
		return plan
	}
	sort.Strings(baselineOnly)
	sort.Strings(candidateOnly)
	plan.valid = true

	// Rule 2: per-repository multiplier from already-covered
	// LinkValuePath findings whose edge source is a repo-group node.
	repoNames := map[string]bool{}
	for _, d := range baseDirs {
		repoNames[d.repo] = true
	}
	for _, d := range candDirs {
		repoNames[d.repo] = true
	}
	repoRatios := map[string][]float64{}
	for i, path := range mismatches {
		if path != shape.LinkValuePath || !covered[i] {
			continue
		}
		key, ok := parseOrderInsensitiveDetailKey(findings[findingRefs[i]].Detail)
		if !ok {
			continue
		}
		source, _, ok := strings.Cut(key, "\x1f")
		if !ok || !repoNames[source] {
			continue
		}
		baseEdge, okB := baseEdges[key]
		candEdge, okC := candEdges[key]
		if !okB || !okC || candEdge.value == 0 {
			continue
		}
		repoRatios[source] = append(repoRatios[source], baseEdge.value/candEdge.value)
	}
	repoMultiplier := map[string]float64{}
	for repo, ratios := range repoRatios {
		consistent := true
		for _, ratio := range ratios[1:] {
			if !repoFanoutFloatsWithinTolerance(ratio, ratios[0]) {
				consistent = false
				break
			}
		}
		if consistent && ratios[0] > 1 {
			repoMultiplier[repo] = ratios[0]
		}
	}

	// Rule 3.
	candMin, hasCandMin := hotspotMin(candFiles, nil)
	if hasCandMin {
		for _, name := range baselineOnly {
			f := baseFiles[name]
			k, ok := repoMultiplier[f.repo]
			if !ok {
				continue
			}
			if f.value/k <= candMin {
				plan.admittedBaselineOnly[name] = true
			}
		}
	}
	admittedEntrants := len(plan.admittedBaselineOnly)
	if admittedEntrants > 0 {
		floor, hasFloor := hotspotMin(baseFiles, baselineOnly)
		if hasFloor {
			sortedCandidateOnly := append([]string(nil), candidateOnly...)
			sort.Slice(sortedCandidateOnly, func(i, j int) bool {
				return candFiles[sortedCandidateOnly[i]].value < candFiles[sortedCandidateOnly[j]].value
			})
			for _, name := range sortedCandidateOnly {
				if len(plan.admittedCandidateOnly) >= admittedEntrants {
					break
				}
				v := candFiles[name].value
				if v > floor {
					break
				}
				plan.admittedCandidateOnly[name] = true
			}
		}
	}

	// Rule 4, file half: an admitted file's own node and two links.
	for _, name := range baselineOnly {
		if !plan.admittedBaselineOnly[name] {
			continue
		}
		f := baseFiles[name]
		plan.admittedBaselineOnly[f.dirToFileLinkKey] = true
		if f.fileToTypeLinkKey != "" {
			plan.admittedBaselineOnly[f.fileToTypeLinkKey] = true
		}
	}
	for _, name := range candidateOnly {
		if !plan.admittedCandidateOnly[name] {
			continue
		}
		f := candFiles[name]
		plan.admittedCandidateOnly[f.dirToFileLinkKey] = true
		if f.fileToTypeLinkKey != "" {
			plan.admittedCandidateOnly[f.fileToTypeLinkKey] = true
		}
	}

	// Rule 4, vanished-directory half: every listed child on the leg
	// where the directory still exists must itself be an admitted file.
	for name, d := range baseDirs {
		if _, stillPresent := candDirs[name]; stillPresent {
			continue
		}
		allAdmitted := len(d.children) > 0
		for _, child := range d.children {
			if !plan.admittedBaselineOnly[child] {
				allAdmitted = false
				break
			}
		}
		if allAdmitted {
			plan.admittedBaselineOnly[name] = true
			plan.admittedBaselineOnly[d.repoLinkKey] = true
		}
	}
	for name, d := range candDirs {
		if _, stillPresent := baseDirs[name]; stillPresent {
			continue
		}
		allAdmitted := len(d.children) > 0
		for _, child := range d.children {
			if !plan.admittedCandidateOnly[child] {
				allAdmitted = false
				break
			}
		}
		if allAdmitted {
			plan.admittedCandidateOnly[name] = true
			plan.admittedCandidateOnly[d.repoLinkKey] = true
		}
	}

	// Rule 4, parent-value half: a directory present on BOTH legs whose
	// own repo->directory link value differs, re-summed exactly from
	// this SAME directory's own admitted leavers/entrants and the
	// repository's own verified inflation of its shared children.
	for name, bd := range baseDirs {
		cd, ok := candDirs[name]
		if !ok {
			continue
		}
		baseEdge, okB := baseEdges[bd.repoLinkKey]
		candEdge, okC := candEdges[cd.repoLinkKey]
		if !okB || !okC || baseEdge.value == candEdge.value {
			continue
		}
		k := 1.0
		if m, ok := repoMultiplier[bd.repo]; ok {
			k = m
		}
		var leaverSum, entrantSum, sharedCandSum float64
		closes := true
		for _, child := range bd.children {
			if _, shared := candFiles[child]; shared {
				continue
			}
			if !plan.admittedBaselineOnly[child] {
				closes = false
				break
			}
			leaverSum += baseFiles[child].value
		}
		if closes {
			for _, child := range cd.children {
				if _, shared := baseFiles[child]; shared {
					sharedCandSum += candFiles[child].value
					continue
				}
				if !plan.admittedCandidateOnly[child] {
					closes = false
					break
				}
				entrantSum += candFiles[child].value
			}
		}
		if !closes {
			continue
		}
		expected := candEdge.value + leaverSum - entrantSum + (k-1)*sharedCandSum
		if repoFanoutFloatsWithinTolerance(baseEdge.value, expected) {
			plan.admittedParentValue[bd.repoLinkKey] = true
		}
	}

	return plan
}

// admits reports whether one Finding is covered by this plan.
func (p *hotspotListBoundaryPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	if tieredPath(finding.Path) == p.shape.LinkValuePath {
		key, ok := parseOrderInsensitiveDetailKey(finding.Detail)
		if !ok {
			return false
		}
		return p.admittedParentValue[key]
	}
	key, ok := parsePresenceDetailKey(finding.Detail)
	if !ok {
		return false
	}
	switch {
	case strings.Contains(finding.Detail, "absent in candidate"):
		return p.admittedBaselineOnly[key]
	case strings.Contains(finding.Detail, "absent in baseline"):
		return p.admittedCandidateOnly[key]
	}
	return false
}

// hotspotMin returns the smallest value among files, excluding any name
// in exclude. ok is false when nothing is left to measure.
func hotspotMin(files map[string]hotspotFileRow, exclude []string) (float64, bool) {
	excluded := make(map[string]bool, len(exclude))
	for _, name := range exclude {
		excluded[name] = true
	}
	found := false
	var min float64
	for name, f := range files {
		if excluded[name] {
			continue
		}
		if !found || f.value < min {
			min = f.value
			found = true
		}
	}
	return min, found
}
