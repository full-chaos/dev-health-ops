package workgraph

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// runClaimedWork is the claim/lease-renewal/complete skeleton shared by the
// Build and Materialize handlers. It owns every codepath that is IDENTICAL
// between the two kinds: claiming, the claimed-request-mismatch permanent
// cancel (still Ambiguous for both -- an operator needs to see which of
// org/id/kind disagreed with the claimed request, regardless of whether the
// kind bridges to Python), lease renewal around runWork, and Complete.
//
// The two kinds diverge ONLY in what runs inside the lease (runWork) and how
// a non-lease-lost failure from it is classified (classify) -- see
// buildHandler.work and materializeHandler.work for why classify differs.
func runClaimedWork(
	ctx context.Context, store Store, logger *slog.Logger,
	requestID string, kind Kind, organizationID *string, domain jobcontract.DomainLink,
	runWork func(workCtx context.Context, claim Claim) ([]byte, error),
	classify func(ctx context.Context, claim Claim, err error) error,
) error {
	if store == nil || !validUUID(requestID) ||
		organizationID == nil || domain.ID != requestID || domain.Type != domainFor(kind) {
		return jobruntime.Permanent(ErrInvalidState)
	}
	claim, err := store.Claim(ctx, requestID, kind)
	if err != nil {
		if errors.Is(err, ErrInvalidState) {
			return jobruntime.Permanent(err)
		}
		// Park until the lease expires instead of burning an attempt on it. A
		// snooze does not consume one, so the reclaim stays reachable however
		// long the current holder takes to die.
		var active *LeaseActiveError
		if errors.As(err, &active) {
			return jobruntime.RetryableAfter(err, active.RetryAfter)
		}
		return jobruntime.Retryable(err)
	}
	if claim == nil { // a completed request is an idempotent success.
		return nil
	}
	if claim.Request.OrganizationID != *organizationID || claim.Request.ID != requestID || claim.Request.Kind != kind {
		// Loud for the same reason as the lease-renewal permanent-cancel
		// below: this is a permanent cancel too, and without the mismatch
		// itself logged an operator sees only "ambiguous", never which of
		// org/id/kind actually disagreed with the claimed request.
		logger.Error("workgraph handler: permanent cancel, claimed request does not match envelope",
			slog.String("request_id", requestID), slog.String("kind", string(kind)),
			slog.String("organization_id", *organizationID),
			slog.String("claimed_organization_id", claim.Request.OrganizationID),
			slog.String("claimed_request_id", claim.Request.ID),
			slog.String("claimed_kind", string(claim.Request.Kind)),
		)
		_ = releaseAmbiguous(store, ctx, *claim, "claimed request no longer matches River envelope")
		return jobruntime.Permanent(ErrInvalidState)
	}
	// runWork shares ONE lease renewal with the caller: the lease has to
	// cover the whole execution, and a step that ran under an expired lease
	// would be writing outside its fence.
	evidence, err := runWithLeaseRenewal(ctx, claim.LeaseDuration,
		func(renewCtx context.Context) error { return store.Renew(renewCtx, *claim) },
		func(workCtx context.Context) ([]byte, error) { return runWork(workCtx, *claim) },
	)
	if err != nil {
		if errors.Is(err, ErrLeaseLost) {
			return jobruntime.Retryable(err)
		}
		return classify(ctx, *claim, err)
	}
	if err := store.Complete(ctx, *claim, evidence); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			return jobruntime.Retryable(err)
		}
		return jobruntime.Retryable(err)
	}
	return nil
}

// maxAmbiguousDetailBytes mirrors the ledger's own bound. PostgresStore's
// transition refuses a detail outside 1..1024 characters, and alembic 0060
// carries the same CHECK on work_graph_execution_ledger.failure_detail -- an
// overflowing detail would turn a recorded ambiguity into ErrInvalidState and
// lose the discriminator entirely, which is the exact failure this replaces.
const maxAmbiguousDetailBytes = 1024

// compatibilityAmbiguousDetail carries the executor's own classification and
// diagnostic into the ledger row instead of the fixed literal that used to be
// written for every outcome. It never returns the empty string: an empty
// detail is rejected by the same bound above.
func compatibilityAmbiguousDetail(err error) string {
	const fallback = "compatibility execution outcome is unknown"
	if err == nil {
		return fallback
	}
	detail := sanitizeDetail(err.Error(), maxAmbiguousDetailBytes)
	if detail == "" {
		return fallback
	}
	return detail
}

func releaseAmbiguous(store Store, ctx context.Context, claim Claim, detail string) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return store.Ambiguous(releaseCtx, claim, detail)
}

func runWithLeaseRenewal(ctx context.Context, lease time.Duration, renew func(context.Context) error, work func(context.Context) ([]byte, error)) ([]byte, error) {
	if ctx == nil || lease < 3*time.Millisecond || renew == nil || work == nil {
		return nil, ErrInvalidState
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan struct{})
	renewed := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				renewed <- nil
				return
			case <-ctx.Done():
				cancel()
				renewed <- ctx.Err()
				return
			case <-ticker.C:
				if err := renew(ctx); err != nil {
					cancel()
					renewed <- err
					return
				}
			}
		}
	}()
	evidence, workErr := work(workCtx)
	close(stop)
	if renewalErr := <-renewed; renewalErr != nil {
		return nil, renewalErr
	}
	return evidence, workErr
}

// isTransientStepError reports whether err is a connectivity-class failure
// (ClickHouse dial failure, query/context timeout, cancellation) rather than
// a genuine defect -- context.DeadlineExceeded/context.Canceled cover a
// cancelled or timed-out query context, and net.Error covers everything
// clickhouse-go's driver itself returns for a dial failure or a read/write
// timeout (both satisfy net.Error via Timeout()/the underlying *net.OpError).
func isTransientStepError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// buildHandler runs workgraph.build entirely natively: no CompatibilityExecutor
// field exists on this type at all (CHAOS-4924 cutover) -- Python's remaining
// build() compute was already a 0-stats no-op (every stage ported to a native
// pre-step), so there is nothing left to bridge to, and the absence is
// structural (a compile-time fact, not a runtime nil-check) rather than a
// poison executor that would fail loud if ever called. See classify's own
// reasoning in work() for why a step failure here is NEVER Ambiguous.
type buildHandler struct {
	store Store
	// preSteps run first, in order, inside the one lease renewal. See
	// NativePreStep for the ordering invariant.
	preSteps []NativePreStep
	// postSteps run after preSteps, in order, inside the same lease renewal.
	// Empty since CHAOS-4924 retired the last one (issue_issue_edges moved
	// into preSteps once its Python producer was deleted) -- kept as a typed
	// seam, not removed, so a future step that genuinely needs to run last
	// has a declared home. See buildPostStepOrder's own doc comment.
	postSteps []NativePostStep
	logger    *slog.Logger
}

func newBuildHandler(store Store, preSteps []NativePreStep, postSteps []NativePostStep, logger *slog.Logger) (*buildHandler, error) {
	if store == nil {
		return nil, ErrUnavailable
	}
	for _, step := range preSteps {
		// A nil step would be a wiring bug that silently skips ported compute,
		// which is exactly the failure this seam exists to prevent.
		if step == nil {
			return nil, ErrUnavailable
		}
	}
	for _, step := range postSteps {
		if step == nil {
			return nil, ErrUnavailable
		}
	}
	// A nil logger falls back to slog.Default() so a permanent-cancel below
	// is never silently unlogged -- same convention as issueprlinks.Service.
	if logger == nil {
		logger = slog.Default()
	}
	return &buildHandler{store: store, preSteps: preSteps, postSteps: postSteps, logger: logger}, nil
}

func (h *buildHandler) work(ctx context.Context, requestID string, organizationID *string, domain jobcontract.DomainLink) error {
	if h == nil {
		return jobruntime.Permanent(ErrInvalidState)
	}
	return runClaimedWork(ctx, h.store, h.logger, requestID, KindBuild, organizationID, domain,
		func(workCtx context.Context, claim Claim) ([]byte, error) {
			fragments, preStepErr := runPreSteps(workCtx, h.preSteps, claim)
			if preStepErr != nil {
				return nil, preStepErr
			}
			postFragments, postStepErr := runPostSteps(workCtx, h.postSteps, claim)
			if postStepErr != nil {
				return nil, postStepErr
			}
			// No bridge means no base evidence payload to merge fragments
			// into -- mergePreStepEvidence returns its input UNCHANGED when
			// that input is empty, which would silently drop every
			// fragment. "{}" is a valid empty JSON object, so the merge
			// proceeds and every fragment lands as a top-level key.
			return mergePreStepEvidence([]byte("{}"), mergeStepFragments(fragments, postFragments)), nil
		},
		func(_ context.Context, _ Claim, err error) error {
			// Ambiguous exists to record a HALF-APPLIED Python write: the
			// bridge may have succeeded before a later step failed, leaving
			// rows that carry Python's values behind. Build has no bridge,
			// so a step failure here never leaves anything half-applied --
			// there is nothing for an operator to repair via
			// `workerctl workgraph repair`, only a build to retry or fix.
			// This NEVER releases Ambiguous, deliberately, for Build.
			if isTransientStepError(err) {
				return jobruntime.Retryable(err)
			}
			// Loud, not just returned: a permanent-cancel needs the
			// underlying cause (e.g. a pre-step's ClickHouse bind failure)
			// visible without correlating a ledger detail string back to it
			// -- same reasoning the bridge-era permanent-cancel log carried,
			// now without an ambiguous ledger row to also record it on.
			h.logger.Error("workgraph build handler: permanent cancel after step failure",
				slog.String("request_id", requestID), slog.String("organization_id", *organizationID),
				slog.Any("error", err),
			)
			return jobruntime.Permanent(err)
		},
	)
}

// materializeHandler runs investment.materialize through the Python bridge,
// unchanged by the CHAOS-4924 Build cutover -- investment.materialize is its
// own track (CHAOS-4441 landed its own native path separately; this type
// exists for whichever configuration still routes through the bridge).
type materializeHandler struct {
	store         Store
	compatibility CompatibilityExecutor
	logger        *slog.Logger
}

func newMaterializeHandler(store Store, compatibility CompatibilityExecutor, logger *slog.Logger) (*materializeHandler, error) {
	if store == nil || compatibility == nil {
		return nil, ErrUnavailable
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &materializeHandler{store: store, compatibility: compatibility, logger: logger}, nil
}

func (h *materializeHandler) work(ctx context.Context, requestID string, organizationID *string, domain jobcontract.DomainLink) error {
	if h == nil || h.compatibility == nil {
		return jobruntime.Permanent(ErrInvalidState)
	}
	return runClaimedWork(ctx, h.store, h.logger, requestID, KindMaterialize, organizationID, domain,
		func(workCtx context.Context, claim Claim) ([]byte, error) {
			return h.compatibility.Execute(workCtx, claim)
		},
		func(ctx context.Context, claim Claim, err error) error {
			// A failure the executor could positively place as "never sent" or
			// "the bridge declined" carries no possibility of a half-applied
			// side effect, so there is nothing ambiguous to record. Releasing
			// ambiguous here is what wedged CHAOS-4970's chain: 'ambiguous' is a
			// state Claim refuses and joboutbox's strand-repair sweep excludes
			// by construction, and no Go caller exists for the Python /repair
			// endpoint that is the only way out of it -- so a transient DNS
			// blip or a 401 became a permanently dead request. Leaving the lease
			// alone instead keeps the ordinary reclaim path reachable: the next
			// attempt parks on LeaseActiveError until the lease expires, then
			// Claim's expired-lease branch reclaims it, all inside River's own
			// attempt budget.
			if errors.Is(err, ErrCompatibilityNotSent) || errors.Is(err, ErrCompatibilityRefused) {
				return jobruntime.Retryable(err)
			}
			// Loud, not just recorded on the ambiguous ledger row: this is the
			// permanent-cancel path, and the underlying error is exactly what
			// an operator needs to see without having to correlate a ledger
			// detail string back to a cause.
			h.logger.Error("workgraph handler: permanent cancel after ambiguous compatibility outcome",
				slog.String("request_id", requestID), slog.String("kind", string(KindMaterialize)),
				slog.String("organization_id", *organizationID), slog.Any("error", err),
			)
			_ = releaseAmbiguous(h.store, ctx, claim, compatibilityAmbiguousDetail(err))
			return jobruntime.Permanent(err)
		},
	)
}

type BuildHandler struct{ *buildHandler }
type MaterializeHandler struct{ *materializeHandler }

// NewBuildHandler builds the workgraph.build handler. preSteps are native Go
// producers that run before postSteps, in the order given; see NativePreStep
// for why they live inside this execution rather than beside it. There is no
// CompatibilityExecutor here at all -- see buildHandler's own doc comment.
func NewBuildHandler(
	store Store,
	preSteps []NativePreStep, postSteps []NativePostStep,
	logger *slog.Logger,
) (*BuildHandler, error) {
	h, err := newBuildHandler(store, preSteps, postSteps, logger)
	return &BuildHandler{h}, err
}
func NewMaterializeHandler(store Store, executor CompatibilityExecutor, logger *slog.Logger) (*MaterializeHandler, error) {
	h, err := newMaterializeHandler(store, executor, logger)
	return &MaterializeHandler{h}, err
}

func (h *BuildHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.WorkGraphBuildArgs]) error {
	if execution == nil {
		return jobruntime.Permanent(ErrInvalidState)
	}
	return h.work(ctx, execution.Args.Payload.RequestID, execution.OrganizationID, execution.Envelope.Domain)
}
func (h *MaterializeHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.InvestmentMaterializeArgs]) error {
	if execution == nil {
		return jobruntime.Permanent(ErrInvalidState)
	}
	return h.work(ctx, execution.Args.Payload.RequestID, execution.OrganizationID, execution.Envelope.Domain)
}
