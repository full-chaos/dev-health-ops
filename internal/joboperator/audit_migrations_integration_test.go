//go:build integration

package joboperator

import (
	"context"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/operatorauditschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// TestEveryAuditedActionPassesTheMigratedAuditConstraints is the proof of
// record for the audit action check: against the table the real migrations
// build, the production PostgresAuditor writes a row for every Action in
// AuditedActions as the operator principal, and still refuses an action
// the check does not allow.
func TestEveryAuditedActionPassesTheMigratedAuditConstraints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())

	python := pyoracle.Resolve(t, operatorauditschema.Root())
	command := exec.CommandContext(ctx, python, operatorauditschema.Argv(instance.URI)...)
	command.Env = operatorauditschema.Env()
	output, err := command.CombinedOutput()
	operatorauditschema.CheckApplied(t, python, output, err)

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	auditor, err := NewPostgresAuditor(pool)
	if err != nil {
		t.Fatal(err)
	}
	event := func(action Action) AuditEvent {
		return AuditEvent{
			Principal: OperatorPrincipal, Action: action, ResourceType: "sync_route",
			ResourceID: "dispatch_sync_run", ReasonCode: "operator_test",
			CorrelationID: "corr-" + strings.ReplaceAll(string(action), ".", "-"),
			CreatedAt:     time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC),
		}
	}
	for _, action := range AuditedActions {
		handle, err := auditor.Begin(ctx, event(action))
		if err != nil {
			t.Errorf("%s: audit Begin refused by the migrated table: %v", action, err)
			continue
		}
		if err := handle.Complete(ctx, AuditSucceeded); err != nil {
			t.Errorf("%s: audit Complete: %v", action, err)
		}
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.worker_operator_audits
		WHERE principal_type = 'operator' AND principal_id = 'dho-workers' AND credential_id IS NULL
		  AND status = 'succeeded'`).Scan(&rows); err != nil || rows != len(AuditedActions) {
		t.Fatalf("succeeded operator audit rows = %d (err %v), want %d", rows, err, len(AuditedActions))
	}
	// The migrated predicate allows exactly AuditedActions: every quoted
	// literal in the live constraint definition, compared as a set, so an
	// extra allowed value fails here as surely as a missing one fails above.
	var definition string
	if err := pool.QueryRow(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conname = 'ck_worker_operator_audits_action'`).Scan(&definition); err != nil {
		t.Fatalf("read the migrated action check: %v", err)
	}
	allowed := map[string]bool{}
	for _, match := range regexp.MustCompile(`'([^']+)'`).FindAllStringSubmatch(definition, -1) {
		allowed[match[1]] = true
	}
	want := map[string]bool{}
	for _, action := range AuditedActions {
		want[string(action)] = true
	}
	if !maps(allowed, want) {
		t.Fatalf("migrated action check allows %v, AuditedActions = %v (definition %s)", sortedKeys(allowed), sortedKeys(want), definition)
	}
	// An action outside the check stays refused: the constraint still binds.
	if _, err := auditor.Begin(ctx, event(ActionInspect)); err == nil {
		t.Fatal("the migrated check allowed jobs.inspect, which it must not")
	}
}
