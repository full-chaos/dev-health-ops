package providerunit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// providerErrorWithSecrets is the shape that makes jobruntime.WithSafeCause
// unusable on this handler's failure returns.
//
// providerfoundation.ProviderError.Error() concatenates the request PATH and a
// bounded snippet of the provider's RESPONSE BODY (types.go, CHAOS-4582), so
// promoting the error's own message -- which is exactly what WithSafeCause does
// -- would put both in a log line on every provider 4xx. The cause must be
// CONSTRUCTED from the class and status instead.
func providerErrorWithSecrets() error {
	return &providerfoundation.ProviderError{
		Class:      providerfoundation.ErrorAuthentication,
		StatusCode: 401,
		Path:       "/repos/octo/hello/actions/artifacts",
		Body:       "token ghp_do_not_log_this_value is invalid",
	}
}

// TestProviderUnitFailuresCarryASafeCause is the CHAOS-4242-shaped gap this
// package had: `rg WithSafeCause internal/jobs/providerunit internal/providersync`
// found NOTHING, so every durable trace of a provider-unit failure was River's
// synthesized "dev-health job failed [retryable]" with an empty trace.
//
// Sync run 115e6246 burned all five attempts for 17 units inside a 77-second
// window and left five identical copies of that string on each. The cause of
// the day's largest sync incident is permanently unrecoverable.
//
// The cause is read through jobruntime.SafeCause -- the same walk Adapter's
// logCause uses -- rather than a local re-implementation, so this proves the
// text actually reaches a log rather than merely existing on the error.
//
// RED CONTROL: on the parent commit every subtest fails at the "no safe cause"
// check, because nothing in this package opted anything in.
func TestProviderUnitFailuresCarryASafeCause(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 7, 0, 37, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name string
		// attempt/maxAttempts select the retry ladder arm.
		attempt, maxAttempts int
		executorErr          error
		wantFragments        []string
		reason               string
	}{
		{
			name: "ordinary retry",
			// The arm ALL FIVE of run 115e6246's attempts took, and the one
			// that left nothing behind.
			attempt: 1, maxAttempts: 5,
			executorErr:   errors.New("dial tcp 10.0.0.2:5432: connect: connection refused"),
			wantFragments: []string{"category=" + retryCauseExecution, "attempt=1/5"},
			reason:        "a restore window, a provider outage and a connection refusal are otherwise the same string",
		},
		{
			name:    "exhausted",
			attempt: 5, maxAttempts: 5,
			executorErr:   errors.New("dial tcp 10.0.0.2:5432: connect: connection refused"),
			wantFragments: []string{"category=provider_unit_exhausted", "attempt=5/5"},
			reason:        "the terminal attempt must say WHICH exhaustion category it recorded",
		},
		{
			name: "deterministic terminal category",
			// all_artifacts_unreadable is one of the categories the ticket
			// names, and it terminalizes on the FIRST attempt by design.
			attempt: 1, maxAttempts: 5,
			executorErr:   providersync.ErrGitHubTestsAllArtifactsUnreadable,
			wantFragments: []string{"category=" + AllArtifactsUnreadableCategory},
			reason:        "the deterministic category IS the diagnosis and must survive into the log",
		},
		{
			name: "provider error class and status",
			// The 4xx/5xx class the ticket names. Class and status are safe;
			// Path and Body are not, and the next test proves they stay out.
			attempt: 1, maxAttempts: 5,
			executorErr: providerErrorWithSecrets(),
			wantFragments: []string{
				"category=" + AuthCategory,
				"provider_error_class=" + string(providerfoundation.ErrorAuthentication),
				"provider_status=401",
			},
			reason: "a 4xx, a 5xx and a transport failure demand different operator responses",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			unit := providerUnit()
			repository := newMemoryUnitRepository(unit)
			handler := &Handler{
				Repository:    repository,
				LeaseDuration: time.Minute,
				Heartbeat:     10 * time.Second,
				Now:           func() time.Time { return now },
				BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
					return providersync.CompleteRouteExecutor{}, testCase.executorErr
				},
			}
			execution := providerExecution(unit, now, testCase.attempt)
			execution.Definition.Kind = jobcontract.KindSyncProviderUnit
			execution.Definition.MaxAttempts = testCase.maxAttempts

			err := handler.Work(context.Background(), execution)
			if err == nil {
				t.Fatal("Work() = nil; this case must fail or it proves nothing")
			}
			cause, ok := jobruntime.SafeCause(err)
			if !ok {
				t.Fatalf("no safe cause on the returned error -- %s", testCase.reason)
			}
			for _, fragment := range testCase.wantFragments {
				if !strings.Contains(cause, fragment) {
					t.Fatalf("cause %q is missing %q -- %s", cause, fragment, testCase.reason)
				}
			}
			// The provider and dataset are on every cause, so an operator can
			// group without reaching for the unit row.
			if !strings.Contains(cause, "provider="+unit.Provider) ||
				!strings.Contains(cause, "dataset="+unit.Dataset) {
				t.Fatalf("cause %q does not identify the pair", cause)
			}
			// Wrapping must not change the error's identity: classify() and
			// every errors.Is upstream still see what they saw before.
			if testCase.executorErr != nil && !errors.Is(err, testCase.executorErr) {
				t.Fatalf("the cause wrapper broke errors.Is on %v", testCase.executorErr)
			}
		})
	}
}

// The cause is CONSTRUCTED, never derived from the error's message. This is the
// whole reason jobruntime.WithSafeCauseText exists alongside WithSafeCause: a
// provider error's own Error() carries the request path and a snippet of the
// response body, and WithSafeCause would promote both verbatim.
//
// Asserted as a negative directly, not as `bad not in cause or good in cause`
// -- that form is vacuous whenever `good` is always present.
func TestProviderUnitSafeCauseNeverCarriesProviderResponseContent(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 7, 0, 37, 0, 0, time.UTC)
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	providerErr := providerErrorWithSecrets()
	handler := &Handler{
		Repository:    repository,
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, providerErr
		},
	}
	execution := providerExecution(unit, now, 1)
	execution.Definition.Kind = jobcontract.KindSyncProviderUnit
	execution.Definition.MaxAttempts = 5

	err := handler.Work(context.Background(), execution)
	if err == nil {
		t.Fatal("Work() = nil; nothing below tests anything")
	}
	cause, ok := jobruntime.SafeCause(err)
	if !ok {
		t.Fatal("no safe cause on a provider-error failure")
	}
	// Precondition: the fixture really does carry both, so the assertions
	// below cannot pass because the secrets were never there.
	if !strings.Contains(providerErr.Error(), "ghp_do_not_log_this_value") ||
		!strings.Contains(providerErr.Error(), "/repos/octo/hello") {
		t.Fatalf("fixture no longer embeds path and body in Error(): %q", providerErr.Error())
	}
	if strings.Contains(cause, "ghp_do_not_log_this_value") {
		t.Fatalf("cause %q leaked the response body", cause)
	}
	if strings.Contains(cause, "/repos/octo/hello") {
		t.Fatalf("cause %q leaked the request path", cause)
	}
	if strings.Contains(cause, providerErr.Error()) {
		t.Fatalf("cause %q is the error's own message; it must be constructed from vetted fields only", cause)
	}
}
