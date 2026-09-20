package goapiproof

// LeafPairShape, set on a BaselineDefect, replaces that defect's blanket
// "any leaf difference under Paths is covered" rule with an exact-pair
// admission: a leaf finding is covered only when its two decoded leaves are
// exactly one of the declared (baseline, candidate) pairs. It exists for a
// baseline that substitutes a fixed sentinel where this port carries the
// value's real absence -- the Python string "None" for a missing dimension
// value where this port carries "" or null, a 0.0 for an aggregate with no
// rows where this port carries null -- so the citation can say exactly which
// substitution it names and nothing else under the path is covered: a
// different string, a different number, or a swap in the other direction
// stays outside.
type LeafPairShape struct {
	// Pairs are the admitted substitutions. A string leaf matches by text,
	// a number by value, null by null; a leaf of any other type matches
	// nothing.
	Pairs []LeafPair

	// CandidateMayBeAllNull exempts this defect's paths from the
	// empty-result rule (ShapeEmptyResult): a candidate whose subtree under a
	// cited path has no non-null leaf, against a baseline that has one, is
	// normally relabelled structural and never covered. Set it only where a
	// declared pair itself names a null candidate and every leaf under the
	// path can legitimately be null together (a label Go leaves null for every
	// item, against the reference's "None" on the one item with a NULL
	// dimension value). Coverage is still only the exact pairs: any other
	// baseline leaf against null stays outside.
	CandidateMayBeAllNull bool
}

// LeafPair is one admitted (baseline, candidate) pair of decoded leaves.
type LeafPair struct {
	Baseline, Candidate any
}

// leafPairPlan judges each finding from the two decoded leaves the
// comparator recorded on it. The comparator's own gate never offers a
// structural finding to a shape, so only leaf differences reach admits.
type leafPairPlan struct{ shape *LeafPairShape }

func (p *leafPairPlan) admits(finding Finding) bool {
	if p == nil {
		return false
	}
	for _, pair := range p.shape.Pairs {
		if leafEquals(finding.baselineLeaf, pair.Baseline) && leafEquals(finding.candidateLeaf, pair.Candidate) {
			return true
		}
	}
	return false
}

// leafEquals reports whether a decoded leaf equals a declared literal: null
// with null, a string with a string of the same text, a number with a
// number of the same value. Booleans and containers equal nothing here.
func leafEquals(got, want any) bool {
	switch w := want.(type) {
	case nil:
		return got == nil
	case string:
		g, ok := got.(string)
		return ok && g == w
	}
	wantNumber, wantIsNumber := asFloat(want)
	gotNumber, gotIsNumber := asFloat(got)
	return wantIsNumber && gotIsNumber && wantNumber == gotNumber
}
