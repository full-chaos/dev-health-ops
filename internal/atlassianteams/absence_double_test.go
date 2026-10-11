package atlassianteams

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// everyAbsentFactIsGone stands for "the gateway says every candidate is gone":
// the tests of the WRITE (valid_from, retraction, the scope gate, the empty
// answer) are about what the rule does once an absence is proven. The proof
// itself is tested against the recorded answers (absence_test.go).
type everyAbsentFactIsGone struct{}

func (everyAbsentFactIsGone) TeamAbsence(context.Context, string) providersync.SnapshotAbsence {
	return providersync.SnapshotAbsenceProven
}

func (everyAbsentFactIsGone) MembershipAbsence(context.Context, string, string) providersync.SnapshotAbsence {
	return providersync.SnapshotAbsenceProven
}

func (everyAbsentFactIsGone) LinkAbsence(context.Context, string, providersync.ProjectID) providersync.SnapshotAbsence {
	return providersync.SnapshotAbsenceProven
}
