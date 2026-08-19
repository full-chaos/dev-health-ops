package joboperator

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestAuthenticationReasonSeparatesPrivilegeDenialFromCredentialFailure pins
// the CHAOS-3100 distinction at the unit boundary: an operator told
// "authentication_failed" rotates a token, and an operator told
// "credential_store_forbidden" goes looking at grants and pool wiring. The
// integration half -- that a real 42501 from a real restricted role actually
// produces the second code -- lives in postgres_integration_test.go, because
// only a real server can raise the SQLSTATE.
func TestAuthenticationReasonSeparatesPrivilegeDenialFromCredentialFailure(t *testing.T) {
	denied := authenticationDenied(ReasonCredentialStoreForbidden)

	for _, testCase := range []struct {
		name string
		err  error
		want string
	}{
		{"no error", nil, ReasonAuthenticationFailed},
		{"credential verdict", ErrAuthentication, ReasonAuthenticationFailed},
		{"no matching credential row", pgx.ErrNoRows, ReasonAuthenticationFailed},
		// A raw driver error is deliberately NOT translated here. Only
		// Authenticate decides that a 42501 was a credential-store denial,
		// because only it knows which statement raised it; mapping the
		// SQLSTATE in this function too would let an unrelated denial from
		// some future caller inherit the code.
		{"undecided driver error", &pgconn.PgError{Code: insufficientPrivilege}, ReasonAuthenticationFailed},
		{"privilege denial", denied, ReasonCredentialStoreForbidden},
		{"wrapped privilege denial", fmt.Errorf("configure runtime: %w", denied), ReasonCredentialStoreForbidden},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if reason := AuthenticationReason(testCase.err); reason != testCase.want {
				t.Fatalf("AuthenticationReason(%v) = %q, want %q", testCase.err, reason, testCase.want)
			}
		})
	}
}

// TestPrivilegeDenialStaysAnAuthenticationError keeps the new error type
// compatible with every existing caller: the sentinel comparison is the
// contract Authenticate has always offered, and widening the verdict must not
// quietly turn a denial into an unhandled error somewhere upstream.
func TestPrivilegeDenialStaysAnAuthenticationError(t *testing.T) {
	denied := authenticationDenied(ReasonCredentialStoreForbidden)
	if !errors.Is(denied, ErrAuthentication) {
		t.Fatalf("errors.Is(%v, ErrAuthentication) = false", denied)
	}
	if errors.Is(denied, ErrAuthorization) {
		t.Fatal("a credential-store denial must not be reported as an authorization failure")
	}
}

// TestReasonCodesCarryNoInterpolatedMaterial is the property that lets these
// codes be logged unredacted, following the same rule as
// cmd/dev-health-stream-runner/dependencies.go's DependencyReason: the
// vocabulary is closed and every member is a lowercase constant, so no token,
// role name, statement text or driver message can ever ride out inside one.
func TestReasonCodesCarryNoInterpolatedMaterial(t *testing.T) {
	for _, reason := range []string{ReasonAuthenticationFailed, ReasonCredentialStoreForbidden} {
		if reason == "" || strings.ToLower(reason) != reason || strings.ContainsAny(reason, " \t\n\"'%:;=/\\") {
			t.Fatalf("reason code %q is not a bare lowercase identifier", reason)
		}
	}
	if ReasonAuthenticationFailed == ReasonCredentialStoreForbidden {
		t.Fatal("the two verdicts must be distinguishable by an operator")
	}
	// The error text is allowed to name the reason and nothing else: the
	// driver's message can quote the failing statement, so it must not be
	// carried along.
	denied := authenticationDenied(ReasonCredentialStoreForbidden).Error()
	if !strings.HasSuffix(denied, ReasonCredentialStoreForbidden) ||
		strings.Contains(denied, "internal_service_credentials") {
		t.Fatalf("denial error text = %q", denied)
	}
}
