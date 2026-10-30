package workload

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

const ReasonWorkloadEnabled = "workload_enabled"

type EnableInput struct {
	Operator        uuid.UUID
	EnablePrincipal bool
	RequestID       *string
}

type EnablePlan struct {
	PrincipalID       uuid.UUID
	RowDisabled       bool
	PrincipalDisabled bool
	EnableRow         bool
	EnablePrincipal   bool
}

type enableState struct {
	principalID       uuid.UUID
	rowDisabled       bool
	principalKind     string
	principalDisabled bool
	operatorFound     bool
	operatorDisabled  bool
}

func decideEnable(enableState, EnableInput) (EnablePlan, error) { return EnablePlan{}, nil }

func (s *Store) enableAudit(EnablePlan, EnableInput) audit.AuditEvent { return audit.AuditEvent{} }

func enableEvents(EnablePlan) []audit.OutboxEvent { return nil }

func (s *Store) PlanEnable(context.Context, string, EnableInput) (EnablePlan, error) {
	return EnablePlan{}, nil
}

func (s *Store) EnableWorkload(context.Context, string, EnableInput) (EnablePlan, error) {
	return EnablePlan{}, nil
}
