package aianalytics

import (
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// opportunityGroup is the detector's rule order: the metric rules first, then each workflow rule in the order
// workflowOpportunities runs it. Equal-score opportunities keep the order the Python detector emits them in.
var opportunityGroup = map[model.AIOpportunityKind]int{
	model.AIOpportunityKindTestGeneration:       0,
	model.AIOpportunityKindHighReviewLoad:       0,
	model.AIOpportunityKindHighRework:           0,
	model.AIOpportunityKindSlowCycle:            0,
	model.AIOpportunityKindUncoveredTestArea:    0,
	model.AIOpportunityKindRepetitiveChange:     1,
	model.AIOpportunityKindDependencyUpdates:    2,
	model.AIOpportunityKindMechanicalMigrations: 3,
	model.AIOpportunityKindDocumentationDrift:   4,
	model.AIOpportunityKindFlakyTestTriage:      5,
}

// metricRuleRank is the order metricRules emits one repository's opportunities in.
var metricRuleRank = map[model.AIOpportunityKind]int{
	model.AIOpportunityKindTestGeneration:    0,
	model.AIOpportunityKindHighReviewLoad:    1,
	model.AIOpportunityKindHighRework:        2,
	model.AIOpportunityKindSlowCycle:         3,
	model.AIOpportunityKindUncoveredTestArea: 4,
}

func teamOf(o opportunity) string {
	if o.teamID == nil {
		return ""
	}
	return *o.teamID
}

// sortedBy sets a workflow opportunity's rule sort key: the count or share its read orders by, then the rest of the
// read's ORDER BY. Within one rule opportunityLess then orders exactly as the read's ORDER BY ... LIMIT 100 does, so
// the rows the read keeps are the rule's first 100 in the served order (CHAOS-14258).
func (o opportunity) sortedBy(value float64, key ...string) opportunity {
	o.value, o.key = value, key
	return o
}

// opportunityLess is the served order (CHAOS-14256): the unclamped score (descending, so the served score is too),
// then the plain parts opportunityId is hashed from and the text, so equal scores never fall back to the order the
// reads return rows in and the limit cut is the same on every call. A NaN score (a stored NaN or Inf measure) comes
// last.
func opportunityLess(a, b opportunity) bool {
	if na, nb := math.IsNaN(a.raw), math.IsNaN(b.raw); na != nb {
		return nb
	}
	if a.raw != b.raw && !math.IsNaN(a.raw) {
		return a.raw > b.raw
	}
	if ga, gb := opportunityGroup[a.kind], opportunityGroup[b.kind]; ga != gb {
		return ga < gb
	}
	if a.value != b.value {
		return a.value > b.value
	}
	if a.repoID != b.repoID {
		return a.repoID < b.repoID
	}
	if ta, tb := teamOf(a), teamOf(b); ta != tb {
		return ta < tb
	}
	for i := 0; i < len(a.key) && i < len(b.key); i++ {
		if a.key[i] != b.key[i] {
			return a.key[i] < b.key[i]
		}
	}
	if ra, rb := metricRuleRank[a.kind], metricRuleRank[b.kind]; ra != rb {
		return ra < rb
	}
	if a.why != b.why {
		return a.why < b.why
	}
	return strings.Join(a.refs, "\n") < strings.Join(b.refs, "\n")
}

// sortPRRefs puts a cluster's pull request refs ("git_pull_requests:<repository>:<number>") in one order before
// firstFive cuts them: groupArray returns them in no fixed order, and they are served as evidence, drilldown chips
// and the last key of opportunityLess. By repository, then number as a number; a ref whose number does not parse
// comes after the numeric ones, by its text.
func sortPRRefs(refs []string) {
	sort.SliceStable(refs, func(i, j int) bool { return prRefLess(refs[i], refs[j]) })
}

func prRefLess(a, b string) bool {
	ra, na, oka := splitPRRef(a)
	rb, nb, okb := splitPRRef(b)
	if ra != rb {
		return ra < rb
	}
	if oka != okb {
		return oka
	}
	if oka && na != nb {
		return na < nb
	}
	return a < b
}

func splitPRRef(ref string) (repo string, number uint64, ok bool) {
	i := strings.LastIndex(ref, ":")
	if i < 0 {
		return ref, 0, false
	}
	n, err := strconv.ParseUint(ref[i+1:], 10, 64)
	return ref[:i], n, err == nil
}
