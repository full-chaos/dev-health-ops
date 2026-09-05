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
//
// Wires a real team resolver (CHAOS-5151's fourth defect: SetTeamMapper
// previously had no caller at all, so every identity fell through to its
// git-backed team_id, typically "unassigned", regardless of real team
// ownership). Reuses team_wellbeing's own, already-tested
// LoadWellbeingTeams + NewMemberResolver (wellbeing_native_clickhouse.go) --
// the SAME production query Python's load_team_resolver_from_store is built
// from -- rather than inventing a second identity->team path.
//
// The mapper is set ONCE here, at construction, not per finalize call: the
// closure it wires takes orgID as an explicit parameter and does a fresh,
// independent ClickHouse read on every invocation, so concurrent
// ComputeFinalizeFamily calls for DIFFERENT organizations sharing this one
// Executor instance never mutate shared state -- only icfinalize.Executor's
// own `teamMapper` field is written, and that write happens exactly once,
// before any concurrent read of it can occur.
func NewICFinalizeExecutor(conn icfinalize.Conn) *ICFinalizeExecutor {
	inner := icfinalize.NewExecutor(conn)
	inner.SetTeamMapper(func(ctx context.Context, orgID string) (icfinalize.TeamResolver, error) {
		teams, err := LoadWellbeingTeams(ctx, conn, orgID)
		if err != nil {
			return nil, err
		}
		resolver := NewMemberResolver(teams)
		return func(identity string) (string, bool) {
			teamID, _ := resolver.ResolveMember(identity)
			return teamID, teamID != ""
		}, nil
	})
	return &ICFinalizeExecutor{inner: inner}
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
