package goapiproof

import (
	"sort"
	"strings"
)

// LimitDisplacementShape, set on a BaselineDefect, narrows that
// defect's blanket "any leaf difference under Paths is covered" rule to
// the ONE structural consequence a value-multiplying baseline defect can
// produce on a LIMIT-bounded, value-DESC list: a row whose value the
// defect's own mechanism inflates can cross the limit boundary, entering
// the baseline's list at the cost of whichever row currently sits at
// candidate's own bottom rank. It admits ONLY that displaced pair, never
// an unrelated presence difference elsewhere in the same list.
//
// Established from source for GET /api/v1/investment/sunburst:
// fetch_investment_sunburst (api/queries/investment.py) and
// FetchInvestmentSunburst (sunburst.go) both GROUP BY theme, subcategory,
// scope, ORDER BY value DESC (no secondary sort -- the SAME unordered-tie
// property investmentSunburstOrderInsensitiveLists already declares),
// then LIMIT :limit identically on both planes. A value-multiplying row
// can therefore change which (theme, subcategory, scope) keys survive
// the LIMIT without EITHER plane's own SQL changing which keys exist.
// The sibling repos-join fan-out entry (KeyedDirectionShape) this shape
// depends on is direction-only -- "however many versions are still live
// for it", no declared multiplier -- so this shape never assumes a fixed
// k; it reads the ACTUAL observed ratio off an already-covered value leaf
// for the SAME repository (scope) in THIS SAME comparison, on the premise
// that one unmerged repos row fans out every slice of that repo by the
// same physical version count.
// A baseline-only row whose repository has no such covered leaf in this
// run carries no verified magnitude -- direction alone cannot place it
// relative to the boundary -- and is never admitted (see the type's own
// admits, and the doc comment on buildLimitDisplacementPlan's repo-
// multiplier pass).
//
// The shape this type verifies, over the two DECODED lists:
//
//  1. Both lists are EXACTLY Limit long -- the actual effective LIMIT
//     this specific request carried, not merely equal to each other.
//     Two equal-length lists both SHORT OF the limit have no boundary at
//     all: a presence pair there is an ordinary missing/extra row, not a
//     rank displacement, and admitting it would be a guess. Limit must
//     be the real value the request used: the query parameter when the
//     request sent one, otherwise the route's own default (see Limit's
//     own doc comment for the citation). A length mismatch, either
//     length off the effective limit, or an unequal count of
//     baseline-only vs. candidate-only keys, refuses the WHOLE plan --
//     a real structural anomaly this mechanism does not explain.
//  2. Each baseline-only row's repository (RepoKeyField) carries a
//     SINGLE, internally-consistent observed multiplier k (>1) from
//     every already-covered ValuePath finding sharing that repository in
//     this comparison. The row's OWN value divided by k -- its candidate-
//     side true value, since no unmerged version exists there -- is <=
//     candidate's own minimum listed value: consistent with genuinely
//     ranking below the limit on the Go side, not a Go regression.
//  3. Each admitted entrant (rule 2) displaces AT MOST ONE row: rule 3
//     admits candidate-only rows one-for-one against however many rule 2
//     actually admitted, LOWEST-VALUED first, each still required to sit
//     at or under the SMALLEST value among baseline's own STABLE rows
//     (the ones baseline shares with candidate, excluding the
//     newly-admitted entrants). A candidate-only row past that count, or
//     past the floor, is left outside -- an unrelated disappearance is
//     never assumed to be this mechanism just because a DIFFERENT row
//     also entered.
//
// UNVERIFIABLE, stated rather than assumed: a candidate-only row's own
// reduced-side counterpart is never independently observed -- Go's true
// value for a baseline-only key below the limit is inferred (rule 2's
// division), never read from a response, because the whole point of the
// mechanism is that the candidate response never lists it.
type LimitDisplacementShape struct {
	// ListPath is the dotted, index-free path to the list itself, e.g.
	// "data".
	ListPath string
	// KeyFields names the fields that together key one row -- the SAME
	// KeyFields this route's own OrderInsensitiveList declares, e.g.
	// []string{"theme", "subcategory", "scope"}.
	KeyFields []string
	// RepoKeyField is the ONE KeyFields entry that identifies the
	// repository a row belongs to, e.g. "scope".
	RepoKeyField string
	// ValueField is the row's numeric field, e.g. "value".
	ValueField string
	// ValuePath is the leaf path a paired-element value mismatch carries
	// -- must equal the sibling defect's own Paths entry whose covered
	// findings this shape reads an observed multiplier from, e.g.
	// "data.value".
	ValuePath string
	// Limit is the effective LIMIT this specific request's own query
	// carried -- the `limit` query parameter's value when the request
	// sent one, otherwise the route's own default. It is a property of
	// ONE request, not the route in general, so a caller composing
	// per-request Options (investmentSunburstParityWithLimit,
	// restcorpus.go) sets a fresh copy of this field for every corpus
	// entry that can reach a different limit.
	Limit int
}

// limitDisplacementRow is one decoded list element's repo/value
// projection.
type limitDisplacementRow struct {
	repo  string
	value float64
}

// limitDisplacementRowSet is one plane's fully-decoded, key-indexed list.
type limitDisplacementRowSet struct {
	order []string
	byKey map[string]limitDisplacementRow
}

// limitDisplacementPlan is one comparison's fully-evaluated admission
// decision, built once per defect from the two decoded lists, the whole
// comparison's tiered mismatch paths, and the FINAL `covered` state
// every other declared defect already reached.
type limitDisplacementPlan struct {
	// valid reports whether the STRUCTURAL preconditions (rule 1) held --
	// false refuses every finding outright. true does not itself admit
	// anything; each key still needs its own place in the two maps
	// below.
	valid                 bool
	admittedBaselineOnly  map[string]bool
	admittedCandidateOnly map[string]bool
}

// buildLimitDisplacementPlan evaluates every rule LimitDisplacementShape
// documents. It must run only after every non-displacement defect in
// this comparison has already decided `covered` -- classifyBaselineDefects
// enforces that ordering, never this function.
func buildLimitDisplacementPlan(
	shape *LimitDisplacementShape,
	baselineData, candidateData any,
	mismatches []string,
	findingRefs []int,
	findings []Finding,
	covered []bool,
) *limitDisplacementPlan {
	plan := &limitDisplacementPlan{admittedBaselineOnly: map[string]bool{}, admittedCandidateOnly: map[string]bool{}}

	baseRows, ok1 := decodeLimitDisplacementRows(baselineData, shape)
	candRows, ok2 := decodeLimitDisplacementRows(candidateData, shape)
	if !ok1 || !ok2 {
		return plan
	}

	// Rule 1.
	if len(baseRows.order) != shape.Limit || len(candRows.order) != shape.Limit {
		return plan
	}
	var baselineOnly, candidateOnly []string
	for _, key := range baseRows.order {
		if _, ok := candRows.byKey[key]; !ok {
			baselineOnly = append(baselineOnly, key)
		}
	}
	for _, key := range candRows.order {
		if _, ok := baseRows.byKey[key]; !ok {
			candidateOnly = append(candidateOnly, key)
		}
	}
	if len(baselineOnly) == 0 || len(baselineOnly) != len(candidateOnly) {
		return plan
	}
	plan.valid = true

	// Observed per-repository multiplier: every ALREADY-COVERED
	// ValuePath finding sharing a repository must agree, within
	// tolerance, on the SAME ratio -- the uniform-per-repo premise the
	// type doc comment states. A repository with no covered finding, or
	// with disagreeing ratios, gets no verified multiplier at all.
	repoRatios := map[string][]float64{}
	for i, path := range mismatches {
		if path != shape.ValuePath || !covered[i] {
			continue
		}
		key, ok := parseOrderInsensitiveDetailKey(findings[findingRefs[i]].Detail)
		if !ok {
			continue
		}
		baseRow, okB := baseRows.byKey[key]
		candRow, okC := candRows.byKey[key]
		if !okB || !okC || candRow.value == 0 {
			continue
		}
		repoRatios[baseRow.repo] = append(repoRatios[baseRow.repo], baseRow.value/candRow.value)
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

	// Rule 2.
	candMin, hasCandMin := limitDisplacementMin(candRows, nil)
	if hasCandMin {
		for _, key := range baselineOnly {
			row := baseRows.byKey[key]
			k, ok := repoMultiplier[row.repo]
			if !ok {
				continue
			}
			reduced := row.value / k
			if reduced <= candMin {
				plan.admittedBaselineOnly[key] = true
			}
		}
	}

	// Rule 3: one displaced row per admitted entrant, lowest-valued
	// first. Zero admitted entrants means zero candidate-only admissions
	// -- a candidate-only row with nothing that displaced it is an
	// unrelated disappearance, never assumed to be this mechanism.
	// Baseline's own stable floor excludes every baselineOnly key
	// (admitted or not): an entrant rule 2 refused is still a NEW key
	// with no place in baseline's prior, stable ranking, so it stays
	// excluded from the floor either way.
	admittedEntrants := len(plan.admittedBaselineOnly)
	if admittedEntrants > 0 {
		floor, hasFloor := limitDisplacementMin(baseRows, baselineOnly)
		if hasFloor {
			sortedCandidateOnly := append([]string(nil), candidateOnly...)
			sort.Slice(sortedCandidateOnly, func(i, j int) bool {
				return candRows.byKey[sortedCandidateOnly[i]].value < candRows.byKey[sortedCandidateOnly[j]].value
			})
			for _, key := range sortedCandidateOnly {
				if len(plan.admittedCandidateOnly) >= admittedEntrants {
					break
				}
				row := candRows.byKey[key]
				if row.value > floor {
					// Ascending order: every remaining row is >= this
					// one, so none of them can be <= floor either.
					break
				}
				plan.admittedCandidateOnly[key] = true
			}
		}
	}

	return plan
}

// admits reports whether one Finding is covered by this plan.
func (p *limitDisplacementPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
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

// decodeLimitDisplacementRows reads shape.ListPath into a key-indexed
// row set. ok is false when the path does not resolve to a list of
// objects each carrying every declared key field, RepoKeyField (as a
// string) and ValueField (as a number), or when two elements on the same
// side share a key -- the same vacuity discipline orderInsensitiveKey's
// own callers use elsewhere in this package.
func decodeLimitDisplacementRows(root any, shape *LimitDisplacementShape) (limitDisplacementRowSet, bool) {
	listValue, ok := navigateSegments(root, citedSegments(shape.ListPath))
	if !ok {
		return limitDisplacementRowSet{}, false
	}
	list, ok := listValue.([]any)
	if !ok {
		return limitDisplacementRowSet{}, false
	}
	out := limitDisplacementRowSet{byKey: make(map[string]limitDisplacementRow, len(list))}
	for _, element := range list {
		key, ok := orderInsensitiveKey(element, shape.KeyFields)
		if !ok {
			return limitDisplacementRowSet{}, false
		}
		if _, duplicate := out.byKey[key]; duplicate {
			return limitDisplacementRowSet{}, false
		}
		object, ok := element.(map[string]any)
		if !ok {
			return limitDisplacementRowSet{}, false
		}
		repo, ok := object[shape.RepoKeyField].(string)
		if !ok {
			return limitDisplacementRowSet{}, false
		}
		value, ok := asFloat(object[shape.ValueField])
		if !ok {
			return limitDisplacementRowSet{}, false
		}
		out.order = append(out.order, key)
		out.byKey[key] = limitDisplacementRow{repo: repo, value: value}
	}
	return out, true
}

// limitDisplacementMin returns the smallest value among rows, excluding
// any key in exclude. ok is false when nothing is left to measure.
func limitDisplacementMin(rows limitDisplacementRowSet, exclude []string) (float64, bool) {
	excluded := make(map[string]bool, len(exclude))
	for _, key := range exclude {
		excluded[key] = true
	}
	found := false
	var min float64
	for _, key := range rows.order {
		if excluded[key] {
			continue
		}
		value := rows.byKey[key].value
		if !found || value < min {
			min = value
			found = true
		}
	}
	return min, found
}
