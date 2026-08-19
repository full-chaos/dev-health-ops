package jobruntime

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

func TestPostgresIdempotencyOnlySupportsRegistryPolicies(t *testing.T) {
	t.Parallel()
	store := &PostgresIdempotency{}
	for _, policy := range []string{
		"unique_schedule_occurrence", "maintenance_run_checkpoint",
		"billing_notification", "webhook_delivery",
	} {
		if !store.Supports(policy) {
			t.Fatalf("policy %q must be supported", policy)
		}
	}
	if store.Supports("unregistered_policy") {
		t.Fatal("unregistered policy was accepted")
	}
}

func TestPostgresIdempotencySupportsWorkgraphFamilyPolicies(t *testing.T) {
	t.Parallel()
	store := &PostgresIdempotency{}
	for _, policy := range []string{
		"work_graph_request",
		"investment_request",
		"investment_dispatch",
		"investment_chunk",
		"investment_finalize",
	} {
		if !store.Supports(policy) {
			t.Fatalf("policy %q must be supported", policy)
		}
	}
}

func TestIdempotencyCompletionMapsOnlyExplicitRuntimeOutcomes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		completion Completion
		want       string
	}{
		{name: "success", completion: Completion{Result: ResultSuccess}, want: "succeeded"},
		{name: "duplicate", completion: Completion{Result: ResultDuplicate}, want: "succeeded"},
		{name: "retry", completion: Completion{Result: ResultRetry}, want: "retryable"},
		{name: "discard", completion: Completion{Result: ResultDiscard}, want: "terminal"},
		// An explicit domain-terminal decision must never run again.
		{
			name:       "cancel/domain-terminal",
			completion: Completion{Result: ResultCancel, Category: CategoryTerminalDomain, Terminal: true},
			want:       "terminal",
		},
		// A process drain or budget-lease loss also arrives as ResultCancel,
		// and River retries it: stamping it terminal made Begin return
		// ClaimTerminal on the retry and the adapter cancelled it forever
		// (CHAOS-3865).
		{
			name:       "cancel/drain",
			completion: Completion{Result: ResultCancel, Category: CategoryCancelled},
			want:       "retryable",
		},
	}
	for _, test := range tests {
		got, err := runStatus(test.completion)
		if err != nil || got != test.want {
			t.Fatalf("runStatus(%s) = %q, %v; want %q", test.name, got, err, test.want)
		}
	}
	if _, err := runStatus(Completion{}); err == nil {
		t.Fatal("empty completion was accepted")
	}
}

func TestIdempotencyClaimRequestRejectsUnsafeOrIncompleteDomainLinks(t *testing.T) {
	t.Parallel()
	request := ClaimRequest{
		Kind:           jobcontract.KindRetentionCleanup,
		IdempotencyKey: "retention:worker_job_terminal:2026-07-14",
		Domain:         jobcontract.DomainLink{Type: "maintenance_run", ID: "00000000-0000-4000-8000-000000000002"},
		Policy:         "maintenance_run_checkpoint",
		JobID:          42,
		Attempt:        1,
	}
	if !validClaimRequest(request) {
		t.Fatal("valid contract claim was rejected")
	}
	request.Domain.ID = ""
	if validClaimRequest(request) {
		t.Fatal("missing domain ID was accepted")
	}
}
