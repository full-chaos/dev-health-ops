package changefailureevidence

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
)

func TestModelMapsEveryFieldByValueAndNilToNil(t *testing.T) {
	if got := Model(nil); got != nil {
		t.Fatalf("Model(nil) = %+v, want nil: a metric other than change failure rate serves no evidence", got)
	}
	basis, reason, coverage := "status", "deployments_without_outcome", 2.0/3.0
	got := Model(&changefailure.Evidence{Basis: &basis, Deployments: 9, StatusKnown: 6, Coverage: &coverage, UnknownReason: &reason})
	if got == nil || got.Basis != &basis || got.Deployments != 9 || got.DeploymentsWithStatus != 6 || got.Coverage != &coverage || got.UnknownReason != &reason {
		t.Fatalf("Model = %+v, want every field carried over (basis, D 9, K 6, coverage, reason)", got)
	}
	empty := Model(&changefailure.Evidence{})
	if empty == nil || empty.Basis != nil || empty.Coverage != nil || empty.UnknownReason != nil || empty.Deployments != 0 || empty.DeploymentsWithStatus != 0 {
		t.Fatalf("Model(empty) = %+v, want zero counts and no basis, coverage or reason", empty)
	}
}
