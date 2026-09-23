package workersctl

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
)

// mutationFlags are the --reason and --correlation-id every audited
// `dho workers` mutation takes: the same contract for the service's own
// verbs (jobs cancel, queues pause, routes ...) and for the direct-write
// verbs that go through auditedWrite.
type mutationFlags struct {
	reason      *string
	correlation *string
}

func addMutationFlags(flags *flag.FlagSet) mutationFlags {
	return mutationFlags{
		reason:      flags.String("reason", "", "bounded reason code (required unless --dry-run)"),
		correlation: flags.String("correlation-id", "", "bounded correlation ID (required unless --dry-run)"),
	}
}

// valid reports whether the flags meet the audited-mutation contract: a
// write needs a well-formed reason code and correlation id; a --dry-run
// preview writes nothing and needs neither.
func (flags mutationFlags) valid(dryRun bool) bool {
	if dryRun {
		return true
	}
	return joboperator.ValidMutationFlags(*flags.reason, *flags.correlation)
}

// errAuditedWriteFailed marks the audit row of a write that returned a
// non-zero exit code as failed.
var errAuditedWriteFailed = errors.New("operator write failed")

// auditedWrite runs write as one audited operator mutation
// (joboperator.Service.Audited). write prints the verb's own output and
// returns its exit code; a non-zero code completes the audit row failed.
//
//   - If the audit intent row cannot be written (or the request is invalid
//     or unauthorized), write never runs and the service error is printed.
//   - If write ran but its audit row could not be completed, write's own
//     output stands and the failure is logged; a write that succeeded then
//     exits with audit_pending, the code the service's own verbs return.
func auditedWrite(
	ctx context.Context, runtime *operatorRuntime, stderr io.Writer, flags mutationFlags,
	action joboperator.Action, resourceType, resourceID string, write func(context.Context) int,
) int {
	if runtime.service == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	attrs := []slog.Attr{
		slog.String("action", string(action)),
		slog.String("resource_type", resourceType),
		slog.String("resource_id", resourceID),
		slog.String("correlation_id", *flags.correlation),
	}
	ran, code := false, 0
	err := runtime.service.Audited(ctx, joboperator.Mutation{
		Principal: runtime.principal, Action: action,
		ResourceType: resourceType, ResourceID: resourceID,
		ReasonCode: *flags.reason, CorrelationID: *flags.correlation,
	}, func(ctx context.Context) error {
		ran = true
		code = write(ctx)
		if code != 0 {
			return errAuditedWriteFailed
		}
		return nil
	})
	if !ran {
		slog.Default().LogAttrs(ctx, slog.LevelWarn, "dho workers: audited write refused before it ran",
			append(attrs, slog.Any("error", err), slog.Any("cause", errors.Unwrap(err)))...)
		return writeServiceError(stderr, err)
	}
	var serviceError *joboperator.ServiceError
	if errors.As(err, &serviceError) &&
		(serviceError.Code == joboperator.CodeAuditPending || serviceError.Code == joboperator.CodeOutcomeUnknown) {
		slog.Default().LogAttrs(ctx, slog.LevelError, "dho workers: audit row not completed after the write",
			append(attrs, slog.Int("exit_code", code), slog.Any("cause", errors.Unwrap(err)))...)
		if code == 0 {
			return writeServiceError(stderr, err)
		}
	}
	return code
}

// dryRunPreview authorizes a --dry-run preview and runs it. A preview writes
// nothing, so it writes no audit row, but it needs the same authority as the
// write it previews.
func dryRunPreview(
	ctx context.Context, runtime *operatorRuntime, stderr io.Writer,
	action joboperator.Action, resourceType, resourceID string, preview func(context.Context) int,
) int {
	if runtime.service == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	if err := runtime.service.Authorize(ctx, runtime.principal, action, resourceType, resourceID); err != nil {
		return writeServiceError(stderr, err)
	}
	return preview(ctx)
}

// scopeOrAll is an audit resource id: the given scope, or "*" when the write
// is not scoped to one resource.
func scopeOrAll(scope string) string {
	if scope == "" {
		return "*"
	}
	return scope
}
