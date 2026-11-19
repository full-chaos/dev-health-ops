package aianalytics

import (
	"math"
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
	if a.repoID != b.repoID {
		return a.repoID < b.repoID
	}
	if ta, tb := teamOf(a), teamOf(b); ta != tb {
		return ta < tb
	}
	if ra, rb := metricRuleRank[a.kind], metricRuleRank[b.kind]; ra != rb {
		return ra < rb
	}
	if a.why != b.why {
		return a.why < b.why
	}
	return strings.Join(a.refs, "\n") < strings.Join(b.refs, "\n")
}
