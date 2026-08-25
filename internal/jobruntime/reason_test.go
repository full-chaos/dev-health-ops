package jobruntime

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// testReasonProbe exists only for these tests. It is built the same way
// every real Reason constant is built -- by calling the unexported reason()
// constructor from inside this package -- which is exactly the point: that
// constructor is reachable from any file in jobruntime (subject to the same
// code review as errors.go), but from nowhere else.
var testReasonProbe = reason("test_probe_value")

// TestReasonConstructorRejectsRuntimeValue is the compiled proof that Reason
// cannot be built from a runtime-derived value from outside this package. It
// shells out to the real Go compiler against small programs living under
// testdata/reasonconstructor (a directory the Go toolchain always excludes
// from ./... build/vet/test patterns, so these never affect the package's
// own build) and asserts each one FAILS to compile, for the specific reason
// that matters:
//
//   - bad_error_string.go tries the exact misuse CHAOS-3910 exists to
//     prevent: jobruntime.WithReason(err, err.Error()). Reason is a distinct
//     type, so a string argument is a type mismatch, not a warning.
//   - bad_struct_literal.go tries to build a Reason directly from a string
//     via a keyed struct literal (jobruntime.Reason{value: "..."}). The
//     field is unexported, so this fails even though the caller can name the
//     Reason type itself.
//
// A third program, valid_usage.go, performs the sanctioned call --
// WithReason with a package-level constant -- and MUST compile, as a control
// proving this test would catch a real regression rather than failing for an
// unrelated reason (an import path typo, a moved package, etc).
func TestReasonConstructorRejectsRuntimeValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		file       string
		wantBuild  bool
		wantErrSub string
	}{
		{
			name:      "sanctioned: package-level constant",
			file:      "testdata/reasonconstructor/valid_usage.go",
			wantBuild: true,
		},
		{
			name:       "rejected: err.Error() passed as a Reason",
			file:       "testdata/reasonconstructor/bad_error_string.go",
			wantBuild:  false,
			wantErrSub: "cannot use err.Error()",
		},
		{
			name:       "rejected: Reason built from a string literal outside the package",
			file:       "testdata/reasonconstructor/bad_struct_literal.go",
			wantBuild:  false,
			wantErrSub: "unexported field",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command("go", "build", "-o", os.DevNull, test.file)
			out, err := cmd.CombinedOutput()
			built := err == nil
			if built != test.wantBuild {
				t.Fatalf("go build %s: built=%v, want built=%v (output:\n%s)", test.file, built, test.wantBuild, out)
			}
			if !test.wantBuild && !strings.Contains(string(out), test.wantErrSub) {
				t.Fatalf("go build %s: output %q does not contain %q", test.file, out, test.wantErrSub)
			}
		})
	}
}

// TestSafeErrorNoReasonMatchesLegacyOutput proves a handler that supplies no
// reason produces byte-for-byte today's output: no default-behaviour change
// from adding the Reason field.
func TestSafeErrorNoReasonMatchesLegacyOutput(t *testing.T) {
	t.Parallel()
	safe := &safeError{category: CategoryPermanent}
	const want = "dev-health job failed [permanent]"
	if got := safe.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

// unwrapSafe pulls the *safeError back out of whatever transportError
// returned (a bare *safeError, or one wrapped in *river.JobCancelError).
func unwrapSafe(t *testing.T, err error) error {
	t.Helper()
	var cancelErr interface{ Unwrap() error }
	if errors.As(err, &cancelErr) {
		return cancelErr.Unwrap()
	}
	return err
}

// TestSafeErrorReasonSurvivesToRiverErrorRow proves a handler-supplied
// reason reaches the text that becomes the River error row (safeError.Error()
// is what River persists), while the original cause stays discarded.
func TestSafeErrorReasonSurvivesToRiverErrorRow(t *testing.T) {
	t.Parallel()
	cause := errors.New("dsn=postgres://svc:s3cr3t@db.internal:5432/tenant_42")
	marked := WithReason(Permanent(cause), testReasonProbe)
	choice := classify(context.Background(), marked, 1, 3)
	got := unwrapSafe(t, transportError(choice)).Error()

	if !strings.Contains(got, "test_probe_value") {
		t.Fatalf("Error() = %q, want it to contain the reason %q", got, "test_probe_value")
	}
	if !strings.Contains(got, string(CategoryPermanent)) {
		t.Fatalf("Error() = %q, want it to keep the category %q", got, CategoryPermanent)
	}
	if strings.Contains(got, "s3cr3t") || strings.Contains(got, "postgres://") || strings.Contains(got, "tenant_42") {
		t.Fatalf("Error() = %q, leaked the DSN-bearing/tenant-identifying cause", got)
	}
}

// TestSafeErrorUnreasonedFailureNeverLeaksCause is the DSN/tenant-identifying
// acceptance case with no reason attached at all -- the common path today --
// confirming the invariant errors.go documents still holds unmodified.
func TestSafeErrorUnreasonedFailureNeverLeaksCause(t *testing.T) {
	t.Parallel()
	cause := errors.New("dsn=postgres://svc:s3cr3t@db.internal:5432/tenant_42")
	choice := classify(context.Background(), Permanent(cause), 1, 3)
	got := unwrapSafe(t, transportError(choice)).Error()

	if got != "dev-health job failed [permanent]" {
		t.Fatalf("Error() = %q, want exactly today's category-only output", got)
	}
	if strings.Contains(got, "s3cr3t") || strings.Contains(got, "postgres://") || strings.Contains(got, "tenant_42") {
		t.Fatalf("Error() = %q, leaked the DSN-bearing/tenant-identifying cause", got)
	}
}

// TestCompatibilityBridgeReasonsSurviveToRiverErrorRow proves the three
// CHAOS-4264 Reason constants (used by internal/jobs/metrics/daily's
// retryCompatibilityError) render through the same safe-error pipeline as
// every other bounded reason -- distinguishable in the River attempt log
// without leaking the underlying HTTP/process error text.
func TestCompatibilityBridgeReasonsSurviveToRiverErrorRow(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name   string
		reason Reason
	}{
		{name: "process_signaled", reason: ReasonProcessSignaled},
		{name: "resource_exhausted", reason: ReasonResourceExhausted},
		{name: "ambiguous_refused", reason: ReasonAmbiguousRefused},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			cause := errors.New("daily metrics compatibility runner was terminated by signal 9 (pid 1330111)")
			marked := WithReason(Retryable(cause), testCase.reason)
			choice := classify(context.Background(), marked, 1, 5)
			got := unwrapSafe(t, transportError(choice)).Error()

			if !strings.Contains(got, testCase.reason.String()) {
				t.Fatalf("Error() = %q, want it to contain the reason %q", got, testCase.reason)
			}
			if !strings.Contains(got, string(CategoryRetryable)) {
				t.Fatalf("Error() = %q, want it to keep the retryable category", got)
			}
			if strings.Contains(got, "1330111") || strings.Contains(got, "terminated by signal") {
				t.Fatalf("Error() = %q, leaked the underlying cause text", got)
			}
		})
	}
}

// TestWithReasonIsANoOpOnUnmarkedErrors proves the reason stays strictly
// opt-in: an unclassified handler error (the fail-closed default) is
// unaffected by WithReason, because there is no markedError to attach it to.
func TestWithReasonIsANoOpOnUnmarkedErrors(t *testing.T) {
	t.Parallel()
	plain := errors.New("unclassified")
	decorated := WithReason(plain, testReasonProbe)
	if decorated != plain {
		t.Fatalf("WithReason mutated an unmarked error: %v", decorated)
	}
	choice := classify(context.Background(), decorated, 1, 3)
	got := unwrapSafe(t, transportError(choice)).Error()
	if got != "dev-health job failed [permanent]" {
		t.Fatalf("Error() = %q, want the unchanged fail-closed default", got)
	}
}

// TestAdapterPanicPathCarriesReason drives a real panicking handler through
// Adapter.Work end to end (not just the classify/transportError pair) and
// proves the resulting River-facing error names the recover site via
// ReasonHandlerPanic -- while the recovered value ("panic-secret-do-not-log")
// and any stack detail never appear in the log line or the returned error.
func TestAdapterPanicPathCarriesReason(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	observer := &recordingObserver{}
	claim := &recordingClaim{state: ClaimProceed}
	lease := &recordingLease{}
	handler := HandlerFunc[RetentionCleanupArgs](func(context.Context, *Execution[RetentionCleanupArgs]) error {
		panic("panic-secret-do-not-log")
	})
	adapter := newRetentionAdapter(t, handler, observer, claim, lease, &logs)
	job := retentionJob(t, 1)

	err := adapter.Work(context.Background(), job)
	if err == nil {
		t.Fatal("expected a safe error from a panicking handler")
	}
	got := err.Error()
	if !strings.Contains(got, string(CategoryPanic)) {
		t.Fatalf("Error() = %q, want the panic category", got)
	}
	if !strings.Contains(got, ReasonHandlerPanic.String()) {
		t.Fatalf("Error() = %q, want it to name the recover site via ReasonHandlerPanic (%q)", got, ReasonHandlerPanic)
	}
	combined := logs.String() + got
	if strings.Contains(combined, "panic-secret-do-not-log") {
		t.Fatalf("recovered panic value leaked into logs/error: %s", combined)
	}
}
