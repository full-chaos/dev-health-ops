// Package changefailureevidence maps what a change failure rate stands on
// (changefailure.Outcome.Evidence, CHAOS-9630) to its GraphQL type. Home and
// the operating review both serve the type through this one mapping.
package changefailureevidence

import (
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// Model returns the GraphQL evidence of a rate. nil maps to nil: every other
// metric serves no evidence.
func Model(e *changefailure.Evidence) *model.ChangeFailureEvidence {
	if e == nil {
		return nil
	}
	return &model.ChangeFailureEvidence{
		Basis:                 e.Basis,
		Deployments:           int(e.Deployments),
		DeploymentsWithStatus: int(e.StatusKnown),
		Coverage:              e.Coverage,
		UnknownReason:         e.UnknownReason,
	}
}
