package jobruntime

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

func TestPostgresIdempotencyOnlySupportsRegistryPolicies(t *testing.T) {
	t.Parallel()
	store := &PostgresIdempotency{}
	for _, policy := range []string{"unique_schedule_occurrence", "maintenance_run_checkpoint"} {
		if !store.Supports(policy) {
			t.Fatalf("policy %q must be supported", policy)
		}
	}
	if store.Supports("webhook_delivery") {
		t.Fatal("unimplemented external-effect policy was accepted")
	}
}

func TestIdempotencyCompletionMapsOnlyExplicitRuntimeOutcomes(t *testing.T) {
	t.Parallel()
	tests := map[Result]string{
		ResultSuccess:   "succeeded",
		ResultDuplicate: "succeeded",
		ResultRetry:     "retryable",
		ResultDiscard:   "terminal",
		ResultCancel:    "terminal",
	}
	for result, want := range tests {
		got, err := runStatus(Completion{Result: result})
		if err != nil || got != want {
			t.Fatalf("runStatus(%q) = %q, %v; want %q", result, got, err, want)
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
