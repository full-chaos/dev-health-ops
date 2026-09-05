package daily

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/icfinalize"
)

// ICFinalizeExecutor adapts icfinalize's run-scoped executor to this package's
// NativeFinalizeFamilyExecutor interface (CHAOS-4290).
//
// The adapter lives HERE rather than icfinalize taking daily.Run directly,
// matching repo_user_commit_native_executor.go's shape: the compute package
// stays free of this one, so the dependency points one way and the family can
// be tested without constructing a Run.
type ICFinalizeExecutor struct {
	inner *icfinalize.Executor
}

// NewICFinalizeExecutor builds the adapter. conn is the ClickHouse connection
// the family reads back from and writes through.
func NewICFinalizeExecutor(conn icfinalize.Conn) *ICFinalizeExecutor {
	return &ICFinalizeExecutor{inner: icfinalize.NewExecutor(conn)}
}

// ComputeFinalizeFamily implements NativeFinalizeFamilyExecutor.
//
// Run carries both the organization and the target day, and both are taken
// from it rather than from separate arguments -- computing one org's day
// against another's scope is the mistake this shape makes unrepresentable.
func (executor *ICFinalizeExecutor) ComputeFinalizeFamily(ctx context.Context, run Run) (int, error) {
	if executor == nil || executor.inner == nil {
		return 0, ErrUnavailable
	}
	return executor.inner.ComputeFinalizeFamily(ctx, icfinalize.RunScope{
		OrganizationID: run.OrganizationID,
		TargetDay:      run.TargetDay,
	})
}

// SetTeamMapper forwards the identity->team resolver.
func (executor *ICFinalizeExecutor) SetTeamMapper(mapper icfinalize.TeamMapper) {
	if executor == nil || executor.inner == nil {
		return
	}
	executor.inner.SetTeamMapper(mapper)
}

var _ NativeFinalizeFamilyExecutor = (*ICFinalizeExecutor)(nil)

// FamilyName re-exports the single source of truth for this family's
// families.json key, so registration sites do not restate the literal.
const ICFinalizeFamilyName = icfinalize.FamilyName
