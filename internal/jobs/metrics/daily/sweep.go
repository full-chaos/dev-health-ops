package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// maxSweepResponseBytes bounds the endpoint's reply. The body carries swept
// execution ids, so it grows with the sweep limit rather than being fixed.
const maxSweepResponseBytes = 64 * 1024

// defaultSweepLimit bounds one pass. The sweep rides on a dispatch job, so it
// must never become the slow part of it: a stuck-row backlog is drained over
// several dispatches rather than in one long pass holding the job open.
const defaultSweepLimit = 100

// SweepResult is what one dead-claim sweep did.
//
// SweptIDs is part of the result rather than a count because an operator needs
// the list to run the CHAOS-5042 partition repair that has to follow -- a count
// alone would make them re-derive it by hand from the ledger.
type SweepResult struct {
	Swept              int
	SkippedClaimActive int
	SweptIDs           []string
}

// ExecutionSweeper moves metric_compatibility_executions rows stuck 'executing'
// with a provably dead claim into 'ambiguous' (CHAOS-5049).
//
// It cannot authorize a retry, and that is the point rather than a limitation.
// An expired lease proves the writer is GONE; it does not prove the writer wrote
// NOTHING. Families whose readers SUM raw rows rather than argMax by computed_at
// silently inflate on a duplicate write, so retry authorization stays manual.
type ExecutionSweeper interface {
	SweepDeadClaims(ctx context.Context, runID string, limit int) (SweepResult, error)
}

// HTTPExecutionSweeper calls the api's operator-gated sweep endpoint.
type HTTPExecutionSweeper struct {
	client   *http.Client
	endpoint string
	token    string
}

func NewHTTPExecutionSweeper(client *http.Client, config HTTPCompatibilityConfig, token string) (*HTTPExecutionSweeper, error) {
	if client == nil || strings.TrimSpace(token) == "" ||
		!validCompatibilityEndpoint(config.Endpoint, config.AllowInsecureInternal) {
		return nil, ErrUnavailable
	}
	base := strings.TrimSuffix(config.Endpoint, "/")
	return &HTTPExecutionSweeper{
		client:   client,
		endpoint: base + "/metric-executions/v1/sweep-dead-claims",
		token:    token,
	}, nil
}

func (sweeper *HTTPExecutionSweeper) SweepDeadClaims(ctx context.Context, runID string, limit int) (SweepResult, error) {
	if sweeper == nil || sweeper.client == nil {
		return SweepResult{}, ErrUnavailable
	}
	if limit < 1 {
		limit = defaultSweepLimit
	}
	payload := struct {
		RunIDs []string `json:"run_ids"`
		Limit  int      `json:"limit"`
	}{Limit: limit}
	if strings.TrimSpace(runID) != "" {
		payload.RunIDs = []string{runID}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return SweepResult{}, ErrUnavailable
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, sweeper.endpoint, bytes.NewReader(body))
	if err != nil {
		return SweepResult{}, ErrUnavailable
	}
	request.Header.Set("Authorization", "Bearer "+sweeper.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := sweeper.client.Do(request)
	if err != nil {
		return SweepResult{}, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxSweepResponseBytes+1))
	if err != nil || len(data) > maxSweepResponseBytes ||
		response.StatusCode < 200 || response.StatusCode >= 300 {
		return SweepResult{}, ErrUnavailable
	}
	var decoded struct {
		Swept              int      `json:"swept"`
		SkippedClaimActive int      `json:"skipped_claim_active"`
		SweptIDs           []string `json:"swept_ids"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return SweepResult{}, ErrUnavailable
	}
	return SweepResult{
		Swept:              decoded.Swept,
		SkippedClaimActive: decoded.SkippedClaimActive,
		SweptIDs:           decoded.SweptIDs,
	}, nil
}

// SetExecutionSweeper attaches the CHAOS-5049 sweep. Optional by construction:
// a Dispatcher without one behaves exactly as before, so the sweep can be rolled
// out without changing NewDispatcher's signature or any existing caller.
func (handler *Dispatcher) SetExecutionSweeper(sweeper ExecutionSweeper) {
	if handler == nil {
		return
	}
	handler.sweeper = sweeper
}

// SetExecutionSweepObserver attaches the bounded-outcome counter.
func (handler *Dispatcher) SetExecutionSweepObserver(observer jobruntime.DailyMetricsExecutionSweepObserver) {
	if handler == nil {
		return
	}
	handler.sweepObserver = observer
}

// SetExecutionSweepLogger attaches the logger that reports swept ids.
func (handler *Dispatcher) SetExecutionSweepLogger(logger *slog.Logger) {
	if handler == nil {
		return
	}
	handler.sweepLogger = logger
}

// sweepDeadClaimExecutions is opportunistic hygiene and is FAIL-OPEN: it never
// returns an error, so it can never fail the dispatch job it rides on. Making a
// dispatch retry because hygiene failed would turn a stuck-row cleanup into a
// delivery outage -- strictly worse than the rows it exists to clear.
//
// Fail-open, but NOT silent to metrics. Every error path increments
// outcome="failed". A fail-open path with no failure counter is
// indistinguishable from one that is working: if every pass errored, "swept" and
// "skipped_claim_active" would both sit at zero, which reads exactly like "there
// was nothing to sweep" -- the CHAOS-4970 shape reproduced inside the mechanism
// built to report it.
//
// Called BEFORE the partition publish loop, deliberately. The rows it clears
// belong to PREVIOUS runs, so it must not be gated on this run's publishes
// succeeding. The consequence, stated rather than discovered: when a publish
// fails, Work returns Retryable and River re-runs the whole function, so this
// sweep executes again on each attempt. That is safe because it is idempotent --
// a row it moved to 'ambiguous' no longer matches the endpoint's
// `state = 'executing'` predicate, so a second pass simply finds fewer rows --
// and bounded, because each pass is capped at defaultSweepLimit.
func (handler *Dispatcher) sweepDeadClaimExecutions(ctx context.Context, runID string) {
	if handler == nil || handler.sweeper == nil {
		return
	}
	sweepCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	result, err := handler.sweeper.SweepDeadClaims(sweepCtx, runID, defaultSweepLimit)
	if err != nil {
		handler.observeSweep("failed", 1)
		if handler.sweepLogger != nil {
			handler.sweepLogger.Warn(
				"daily metrics dead-claim sweep failed",
				"error_category", "hygiene_failed", "run_id", runID, "error", err,
			)
		}
		return
	}
	handler.observeSweep("swept", result.Swept)
	handler.observeSweep("skipped_claim_active", result.SkippedClaimActive)
	if len(result.SweptIDs) > 0 && handler.sweepLogger != nil {
		// Logged at info with the ids because an operator needs the list to run
		// the CHAOS-5042 partition repair these rows now require.
		handler.sweepLogger.Info(
			"daily metrics dead-claim executions swept to ambiguous",
			"run_id", runID,
			"swept", result.Swept,
			"skipped_claim_active", result.SkippedClaimActive,
			"execution_ids", strings.Join(result.SweptIDs, ","),
		)
	}
}

func (handler *Dispatcher) observeSweep(outcome string, count int) {
	if handler == nil || handler.sweepObserver == nil || count <= 0 {
		return
	}
	_ = handler.sweepObserver.ObserveDailyMetricsExecutionSweep(outcome, count)
}
