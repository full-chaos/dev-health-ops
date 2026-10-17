package workerservice

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

func TestFallbackInvariantPreStepRefusesANilService(t *testing.T) {
	step, err := newIssuePRFallbackInvariantPreStep(nil)
	if err == nil || step != nil {
		t.Fatalf("newIssuePRFallbackInvariantPreStep(nil) = %v, %v; want a refusal", step, err)
	}
}

func TestFallbackInvariantPreStepNameIsTheDeclaredOne(t *testing.T) {
	if got := (&issuePRFallbackInvariantPreStep{}).Name(); got != "issue_pr_fallback_invariant" {
		t.Fatalf("Name() = %q", got)
	}
}

type fakeFallbackInvariantChecker struct {
	outcome issuepredges.InvariantOutcome
	err     error
	org     string
}

func (fake *fakeFallbackInvariantChecker) CheckFallbackInvariant(_ context.Context, orgID string) (issuepredges.InvariantOutcome, error) {
	fake.org = orgID
	return fake.outcome, fake.err
}

func invariantClaim() workgraph.Claim {
	return workgraph.Claim{Request: workgraph.Request{OrganizationID: "org-1"}}
}

func TestFallbackInvariantPreStepReportsFailedOnABreach(t *testing.T) {
	checker := &fakeFallbackInvariantChecker{outcome: issuepredges.InvariantOutcome{
		LinksRead: 9, Beside: issueprlinks.FallbacksBesideNative{ByPullRequest: 2, ByIssue: 1, Total: 3},
	}}
	fragment, err := (&issuePRFallbackInvariantPreStep{checker: checker}).Run(context.Background(), invariantClaim())
	var failure *workgraph.StepFailure
	if !errors.As(err, &failure) || fragment != nil {
		t.Fatalf("Run = %v, %v; want a workgraph.StepFailure", fragment, err)
	}
	if checker.org != "org-1" {
		t.Fatalf("checked org %q, want the claim's org", checker.org)
	}
	if failure.Evidence["fallback_beside_native"] != 3 || failure.Evidence["beside_native_pull_request"] != 2 ||
		failure.Evidence["beside_native_issue"] != 1 || failure.Evidence["links_read"] != 9 || failure.Evidence["organization_id"] != "org-1" || failure.Reason == "" {
		t.Fatalf("failure = %+v", failure)
	}
}

func TestFallbackInvariantPreStepPassesWithNoBreach(t *testing.T) {
	checker := &fakeFallbackInvariantChecker{outcome: issuepredges.InvariantOutcome{LinksRead: 9}}
	fragment, err := (&issuePRFallbackInvariantPreStep{checker: checker}).Run(context.Background(), invariantClaim())
	if err != nil || fragment["fallback_beside_native"] != 0 || fragment["links_read"] != 9 || fragment["organization_id"] != "org-1" {
		t.Fatalf("Run = %v, %v; want a passing fragment with 0", fragment, err)
	}
}

// A failed read is not a verdict: it aborts the build like any other step error.
func TestFallbackInvariantPreStepReadFailureIsAnOrdinaryError(t *testing.T) {
	checker := &fakeFallbackInvariantChecker{err: errors.New("clickhouse down")}
	_, err := (&issuePRFallbackInvariantPreStep{checker: checker}).Run(context.Background(), invariantClaim())
	var failure *workgraph.StepFailure
	if err == nil || errors.As(err, &failure) {
		t.Fatalf("Run err = %v, want an ordinary error, not a StepFailure", err)
	}
}
