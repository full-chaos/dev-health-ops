package goapiproof

import (
	"regexp"
	"strconv"
	"strings"
)

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

	// CandidateMayBeAllNull lets a declared pair that names a null candidate
	// keep its leaf shape where the empty-result rule (ShapeEmptyResult) would
	// relabel it: a candidate whose subtree under a cited path has no non-null
	// leaf, against a baseline that has one, is normally relabelled structural
	// and never covered. Set it only where every leaf under the path can
	// legitimately be null together (a label Go leaves null for every item,
	// against the reference's "None" on the one item with a NULL dimension
	// value). Only a finding that is exactly a declared pair is exempted; every
	// other finding under the path is relabelled empty_result as before.
	CandidateMayBeAllNull bool

	// Sibling, when set, additionally requires the CANDIDATE's own object that
	// holds the finding's leaf to satisfy a condition on its boolean fields: a
	// pair is covered only beside the state that explains it (a null percent
	// against the reference's 0.0, only on a delta whose has_data or
	// has_prior_data says a window holds no stored value). A pair whose
	// sibling cannot be found, or does not satisfy the condition, stays outside.
	Sibling *SiblingCondition
}

// SiblingCondition names the candidate object that holds a leaf and the
// boolean fields of it that must hold for a LeafPairShape pair to be covered.
type SiblingCondition struct {
	// ListPath is the dotted, index-free path of the list holding the object,
	// paired with an OrderInsensitiveList declaration whose KeyFields are
	// KeyFields (the finding's "[key=...]" Detail prefix names the element).
	// Empty means the object is the single one at ObjectPath.
	ListPath  string
	KeyFields []string
	// ObjectPath is the dotted path of the object when ListPath is empty,
	// e.g. "data".
	ObjectPath string
	// AllTrue lists boolean fields that must all be true; AnyFalse lists
	// boolean fields of which at least one must be false. Exactly one is set.
	AllTrue  []string
	AnyFalse []string
}

// holds reports whether the candidate object found for finding satisfies the
// condition. Anything that cannot be found, or is not a boolean, fails it.
func (c *SiblingCondition) holds(candidate any, finding Finding) bool {
	var object map[string]any
	if c.ListPath != "" {
		list, ok := listAtDottedPath(candidate, c.ListPath)
		if !ok {
			return false
		}
		if key, keyed := parseOrderInsensitiveDetailKey(finding.Detail); keyed {
			// An order-insensitive list: the element is named by its key.
			for _, element := range list {
				if k, ok := orderInsensitiveKey(element, c.KeyFields); ok && k == key {
					object, _ = element.(map[string]any)
					break
				}
			}
		} else {
			// An ordered list: the comparator paired the elements by index, and the
			// finding's path carries it ("data.deltas[3].delta_pct").
			segments := strings.Split(c.ListPath, ".")
			match := regexp.MustCompile(regexp.QuoteMeta(segments[len(segments)-1]) + `\[(\d+)\]`).FindStringSubmatch(finding.Path)
			if match == nil {
				return false
			}
			index, err := strconv.Atoi(match[1])
			if err != nil || index < 0 || index >= len(list) {
				return false
			}
			object, _ = list[index].(map[string]any)
		}
	} else {
		value, ok := navigateSegments(candidate, citedSegments(c.ObjectPath))
		if !ok {
			return false
		}
		object, _ = value.(map[string]any)
	}
	if object == nil {
		return false
	}
	flag := func(name string) (bool, bool) {
		v, ok := object[name].(bool)
		return v, ok
	}
	if len(c.AllTrue) > 0 {
		for _, name := range c.AllTrue {
			if v, ok := flag(name); !ok || !v {
				return false
			}
		}
		return true
	}
	for _, name := range c.AnyFalse {
		if v, ok := flag(name); ok && !v {
			return true
		}
	}
	return false
}

// LeafPair is one admitted (baseline, candidate) pair of decoded leaves.
type LeafPair struct {
	Baseline, Candidate any
}

// leafPairPlan judges each finding from the two decoded leaves the
// comparator recorded on it. The comparator's own gate never offers a
// structural finding to a shape, so only leaf differences reach admits.
type leafPairPlan struct {
	shape     *LeafPairShape
	candidate any // the decoded candidate body, for Sibling
}

func (p *leafPairPlan) admits(finding Finding) bool {
	if p == nil {
		return false
	}
	for _, pair := range p.shape.Pairs {
		if leafEquals(finding.baselineLeaf, pair.Baseline) && leafEquals(finding.candidateLeaf, pair.Candidate) {
			return p.shape.Sibling == nil || p.shape.Sibling.holds(p.candidate, finding)
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
