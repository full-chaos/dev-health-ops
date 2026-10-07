package investment

// shadowphase.go is the shadow categorization phase of investment.materialize
// (CHAOS-8869, parent CHAOS-8865): after every served write of a run, the same
// bundles are classified a second time by the decision backend (TypeSafe Jev)
// and the results go to work_unit_investment_shadow and
// llm_categorization_attempts only. No reader of the product reads either table.
//
// What the phase can and cannot do to the served run:
//
//   - It starts after the last served write and returns nothing, so no shadow
//     failure (a state, a transport error, a panic, a missing table) can change
//     or remove a served row, the run's Stats or its result.
//   - It runs under a CHILD of the run context with a time budget (default 60 s,
//     never more than MaxShadowBudget). It is never detached: a lost lease means
//     "stop writing", for the shadow tables too.
//   - It delays the request's completion fence by its own duration, and a cancel
//     of the run context inside it (lost lease, soft-stop expiry) still makes the
//     handler retry the request and spend one of its 9 claims. The budget bounds
//     that; the run log line and a counter report it.
//
// Nothing here writes llm_token_usage (the org spend reader has no source
// filter) or the execution ledger evidence. No log line and no column holds
// prompt, response or source text.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// Stop reasons of a shadow phase: a closed set, used as a log field and as a
// metric label.
const (
	ShadowStopDone                 = "done"
	ShadowStopBudget               = "budget"
	ShadowStopCap                  = "cap"
	ShadowStopDeterministicFailure = "deterministic_failure"
	ShadowStopCancelled            = "cancelled"
	ShadowStopTableMissing         = "table_missing"
	ShadowStopStoreError           = "store_error"
	ShadowStopPanic                = "panic"
)

// ShadowStopReasons is the closed set, in a stable order.
func ShadowStopReasons() []string {
	return []string{
		ShadowStopDone, ShadowStopBudget, ShadowStopCap, ShadowStopDeterministicFailure,
		ShadowStopCancelled, ShadowStopTableMissing, ShadowStopStoreError, ShadowStopPanic,
	}
}

// ShadowAttemptRetried labels an attempt that was followed by a retry: it has
// no state of its own. The last attempt of a classification carries the state.
const ShadowAttemptRetried = "retried"

const (
	shadowAttemptKindFirst = "first"
	shadowAttemptKindRetry = "retry"

	// shadowWriteBatch is how many shadow rows are collected before one insert.
	// Rows are written during the phase, not only at its end, so a cancel late
	// in the phase keeps what was already paid for.
	shadowWriteBatch = 500
	// shadowWriteTimeout bounds one shadow insert and the skip-existing read.
	// The phase can add at most its budget plus two of these to a run: the last
	// shadow insert and the attempt insert (see flush in run).
	shadowWriteTimeout = chwrite.AttemptWriteTimeout
	// shadowDefectLogLimit bounds the per-unit ERROR lines of one phase.
	shadowDefectLogLimit = 5

	// shadowRatesVersion names the rate table below. TypeSafe publishes USD
	// 0.042 for each million input tokens and bills no output tokens (read
	// 2026-10-06). Neither API returns an invoice figure, so the cost of an
	// attempt row is the reported usage times this table.
	shadowRatesVersion = "typesafe-systemone-2026-10-06"
	// shadowNanoUSDPerInputToken is the rate as an integer: 0.042 USD / 1e6
	// tokens = 42e-9 USD for each token. Spend is summed in integers, so the
	// same usage gives the same cost on every architecture.
	shadowNanoUSDPerInputToken = 42
)

// shadowClassifier is the decision completer as the phase uses it.
type shadowClassifier interface {
	Classify(ctx context.Context, bundle units.TextBundle) (decision.Classification, error)
}

// shadowStore is the ClickHouse side of the phase: its own skip-existing read
// and the two shadow sinks.
type shadowStore interface {
	FetchExistingShadowKeys(ctx context.Context, orgID string, keys []chquery.InvestmentKey, shadowConfig string) (map[chquery.InvestmentKey]struct{}, error)
	WriteShadowInvestments(ctx context.Context, orgID string, records []chwrite.ShadowRecord) (int, error)
	FlushAttempts(ctx context.Context, orgID string, buffer *chwrite.AttemptBuffer) chwrite.AttemptFlushResult
}

type clickHouseShadowStore struct {
	reader *chquery.Reader
	writer *chwrite.Writer
}

func (store clickHouseShadowStore) FetchExistingShadowKeys(ctx context.Context, orgID string, keys []chquery.InvestmentKey, shadowConfig string) (map[chquery.InvestmentKey]struct{}, error) {
	return store.reader.FetchExistingShadowKeys(ctx, orgID, keys, shadowConfig)
}

func (store clickHouseShadowStore) WriteShadowInvestments(ctx context.Context, orgID string, records []chwrite.ShadowRecord) (int, error) {
	return store.writer.WriteShadowInvestments(ctx, orgID, records)
}

func (store clickHouseShadowStore) FlushAttempts(ctx context.Context, orgID string, buffer *chwrite.AttemptBuffer) chwrite.AttemptFlushResult {
	return store.writer.FlushAttempts(ctx, orgID, buffer)
}

// ShadowPhase is the shadow backend of one Execute: built once, used by one
// Materializer.Run, then closed.
type ShadowPhase struct {
	settings   ShadowSettings
	classifier shadowClassifier
	// identity and config are fixed at construction from configured values
	// only. config is the key column of both tables: it is built ONE time, by
	// shadowConfigStamp, and never from a response field.
	identity decision.Identity
	config   string
	observer ShadowObserver
	logger   *slog.Logger
	closer   func() error
	// writeBatch is how many shadow rows are collected before one insert. Zero
	// selects shadowWriteBatch.
	writeBatch int
}

// shadowConfigStamp is the ONE place the configuration stamp is built.
func shadowConfigStamp(identity decision.Identity) string { return identity.Stamp() }

// systemOneSender is the TypeSafe client as the phase needs it.
type systemOneSender interface {
	PostSystemOneDetailed(ctx context.Context, body []byte) (categorize.SystemOneResult, error)
	Model() string
	Close() error
}

// NewShadowPhase builds the phase over a TypeSafe client. It fails when the
// embedded rubric does not have its pinned digest; the caller then runs with
// no phase.
func NewShadowPhase(settings ShadowSettings, client *categorize.TypeSafeClient, logger *slog.Logger) (*ShadowPhase, error) {
	if client == nil {
		return nil, ErrUnavailable
	}
	return newShadowPhase(settings, client, logger)
}

func newShadowPhase(settings ShadowSettings, sender systemOneSender, logger *slog.Logger) (*ShadowPhase, error) {
	if sender == nil || logger == nil {
		return nil, ErrUnavailable
	}
	completer, err := decision.NewCompleter(shadowTransport{sender: sender}, sender.Model())
	if err != nil {
		return nil, err
	}
	identity := completer.Identity()
	return &ShadowPhase{
		settings: settings.clamped(), classifier: completer,
		identity: identity, config: shadowConfigStamp(identity),
		logger: logger, closer: sender.Close,
	}, nil
}

// SetObserver wires the optional metrics sink. Nil is tolerated.
func (phase *ShadowPhase) SetObserver(observer ShadowObserver) {
	if phase != nil {
		phase.observer = observer
	}
}

// Close releases the client of the phase.
func (phase *ShadowPhase) Close() error {
	if phase == nil || phase.closer == nil {
		return nil
	}
	return phase.closer()
}

// shadowEligible is the production gate of the served path: a bundle under
// minEvidenceChars characters, or with no text source, never reaches a model.
func shadowEligible(bundle units.TextBundle) bool {
	return bundle.TextCharCount >= minEvidenceChars && bundle.TextSourceCount > 0
}

// shadowLedger is the spend of one phase in 1e-9 USD. A send reserves an
// estimate first and is refused when the reservation would pass the cap; the
// reservation is replaced by the billed amount when the response arrives.
type shadowLedger struct {
	mu       sync.Mutex
	limit    int64
	reserved int64
	billed   int64
}

func (ledger *shadowLedger) reserve(amount int64) bool {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.billed+ledger.reserved+amount > ledger.limit {
		return false
	}
	ledger.reserved += amount
	return true
}

func (ledger *shadowLedger) settle(reserved, billed int64) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.reserved -= reserved
	ledger.billed += billed
}

func (ledger *shadowLedger) spent() int64 {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return ledger.billed
}

// shadowReservationNanoUSD is the amount reserved before a send: one token for
// every two bytes of the request body. Real requests have three bytes or more
// for each token, so the estimate is above the bill.
func shadowReservationNanoUSD(body []byte) int64 {
	return int64((len(body)+1)/2) * shadowNanoUSDPerInputToken
}

// shadowBilledNanoUSD is the bill of a response from its checked usage. Output
// tokens are free. A response with no usable usage is priced at zero and its
// attempt row says so by its zero tokens.
func shadowBilledNanoUSD(usage categorize.SystemOneUsage) int64 {
	if !usage.Reported || usage.InputTokens < 0 {
		return 0
	}
	return usage.InputTokens * shadowNanoUSDPerInputToken
}

// shadowExchange is what the transport records for ONE classification. It
// travels in the context of that classification, because the completer is
// shared by every goroutine of the phase.
type shadowExchange struct {
	ledger *shadowLedger
	// capRefused: the send was refused by the spend cap; nothing was sent.
	capRefused bool
	attempts   []categorize.SystemOneAttempt
	usage      categorize.SystemOneUsage
	billed     int64
}

type shadowExchangeKey struct{}

func withShadowExchange(ctx context.Context, exchange *shadowExchange) context.Context {
	return context.WithValue(ctx, shadowExchangeKey{}, exchange)
}

func shadowExchangeFrom(ctx context.Context) *shadowExchange {
	exchange, _ := ctx.Value(shadowExchangeKey{}).(*shadowExchange)
	return exchange
}

var (
	errShadowNoExchange = errors.New("investment shadow: a request outside a shadow classification is refused")
	errShadowSpendCap   = errors.New("investment shadow: the spend cap of the run is reached")
)

// shadowTransport is the decision.Transport of the phase: the TypeSafe client,
// plus the spend reservation before the send and the attempt record after it.
type shadowTransport struct {
	sender systemOneSender
}

func (transport shadowTransport) PostSystemOne(ctx context.Context, body []byte) ([]byte, http.Header, error) {
	exchange := shadowExchangeFrom(ctx)
	if exchange == nil || exchange.ledger == nil {
		return nil, nil, errShadowNoExchange
	}
	reserved := shadowReservationNanoUSD(body)
	if !exchange.ledger.reserve(reserved) {
		exchange.capRefused = true
		return nil, nil, errShadowSpendCap
	}
	billed := int64(0)
	// Deferred, so a panic below the send cannot leave the reservation open.
	defer func() { exchange.ledger.settle(reserved, billed) }()

	result, err := transport.sender.PostSystemOneDetailed(ctx, body)
	if err != nil {
		var failed *categorize.SystemOneError
		if errors.As(err, &failed) {
			exchange.attempts = failed.Attempts
		}
		// The error goes back to the adapter as it is: the adapter reads its
		// class. Nothing here prints it or any layer inside it.
		return nil, nil, err
	}
	billed = shadowBilledNanoUSD(result.Usage)
	exchange.attempts, exchange.usage, exchange.billed = result.Attempts, result.Usage, billed
	return result.Body, result.Header, nil
}

// shadowUnit is one work unit of the phase.
type shadowUnit struct {
	workUnitID string
	bundle     units.TextBundle
}

// shadowResult is one finished classification.
type shadowResult struct {
	unit           shadowUnit
	classification decision.Classification
	err            error
	exchange       *shadowExchange
	panicked       bool
}

// shadowSummary is the outcome of one phase: the fields of its log line.
type shadowSummary struct {
	GatePass        int
	Eligible        int
	SkippedExisting int
	Attempted       int
	States          map[string]int
	Attempts        int
	InputTokens     int64
	OutputTokens    int64
	BilledNanoUSD   int64
	RowsWritten     int
	RowsLost        int
	AttemptRows     int
	AttemptDropped  int
	AttemptWriteErr bool
	PanicsRecovered int
	StopReason      string
	Duration        time.Duration
	latencies       []time.Duration
	attemptLabels   map[string]int
	defectLogs      int
}

// run executes the phase for the bundles of one materialize run. It returns
// no error by design: see the file comment.
func (phase *ShadowPhase) run(ctx context.Context, store shadowStore, cfg Config, entries []preprocessed) shadowSummary {
	started := time.Now()
	summary := shadowSummary{States: map[string]int{}, attemptLabels: map[string]int{}}
	stop := ""
	setStop := func(reason string) {
		if stop == "" {
			stop = reason
		}
	}

	selected := make([]shadowUnit, 0, len(entries))
	for _, entry := range entries {
		if !shadowEligible(entry.result.Bundle) {
			continue
		}
		summary.GatePass++
		if !phase.settings.unitSelected(cfg.OrgID, entry.result.Investment.WorkUnitID) {
			continue
		}
		selected = append(selected, shadowUnit{workUnitID: entry.result.Investment.WorkUnitID, bundle: entry.result.Bundle})
	}
	summary.Eligible = len(selected)

	// The child context: the run context with the budget as its deadline.
	phaseCtx, cancel := context.WithTimeout(ctx, phase.settings.clamped().Budget)
	defer cancel()

	pending := selected
	if len(selected) > 0 {
		keys := make([]chquery.InvestmentKey, 0, len(selected))
		for _, unit := range selected {
			keys = append(keys, chquery.InvestmentKey{WorkUnitID: unit.workUnitID, InputHash: unit.bundle.InputHash})
		}
		readCtx, cancelRead := context.WithTimeout(phaseCtx, shadowWriteTimeout)
		existing, err := store.FetchExistingShadowKeys(readCtx, cfg.OrgID, keys, phase.config)
		cancelRead()
		if err != nil {
			// With no answer about what exists, asking every unit again would
			// pay two times: stop.
			setStop(phase.storeStop(ctx, cfg, "skip_existing_read", err))
			pending = nil
		} else {
			pending = make([]shadowUnit, 0, len(selected))
			for _, unit := range selected {
				if _, ok := existing[chquery.InvestmentKey{WorkUnitID: unit.workUnitID, InputHash: unit.bundle.InputHash}]; ok {
					summary.SkippedExisting++
					continue
				}
				pending = append(pending, unit)
			}
		}
	}

	ledger := &shadowLedger{limit: phase.settings.MaxNanoUSD}
	attempts := chwrite.NewAttemptBuffer(0)
	batchSize := phase.writeBatch
	if batchSize < 1 {
		batchSize = shadowWriteBatch
	}
	records := make([]chwrite.ShadowRecord, 0, min(len(pending), batchSize))
	writesStopped := false
	// flush writes the collected rows. The time the phase can add to a run is
	// bounded by it: a write INSIDE the phase (last false) runs under the phase
	// context, so it ends with the budget, and its rows are then kept for the
	// last write. The LAST write runs under the run context with its own
	// shadowWriteTimeout, so rows that were paid for are written after the
	// budget ended. A write that failed for another reason ends all shadow
	// writes of the phase. A cancelled run context writes nothing.
	flush := func(last bool) {
		if len(records) == 0 || writesStopped {
			return
		}
		if ctx.Err() != nil {
			return
		}
		parent := ctx
		if !last {
			if phaseCtx.Err() != nil {
				return // the phase is over: the last write takes these rows
			}
			parent = phaseCtx
		}
		writeCtx, cancelWrite := context.WithTimeout(parent, shadowWriteTimeout)
		written, err := store.WriteShadowInvestments(writeCtx, cfg.OrgID, records)
		cancelWrite()
		if err != nil {
			if !last && phaseCtx.Err() != nil && ctx.Err() == nil {
				return // the budget ended during the write: the last write takes these rows
			}
			summary.RowsLost += len(records)
			records, writesStopped = nil, true
			setStop(phase.storeStop(ctx, cfg, "shadow_write", err))
			cancel()
			return
		}
		summary.RowsWritten += written
		records = records[:0:0]
	}

	if len(pending) > 0 {
		work := make(chan shadowUnit)
		results := make(chan shadowResult)
		// abandoned is closed when this function returns. After a normal end it
		// has no reader left. After a panic of the loop below, nothing reads
		// results any more: a worker that could only wait on its send ends here.
		abandoned := make(chan struct{})
		defer close(abandoned)
		go func() {
			defer close(work)
			for _, unit := range pending {
				select {
				case work <- unit:
				case <-phaseCtx.Done():
					return
				}
			}
		}()
		var workers sync.WaitGroup
		for worker := 0; worker < phase.settings.Concurrency; worker++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for unit := range work {
					result := phase.classifyOne(phaseCtx, ledger, unit)
					if result.classification.Stop {
						// Stop here, not only in the loop that reads the result:
						// this worker must not start its next unit in between.
						cancel()
					}
					select {
					case results <- result:
					case <-abandoned:
						return
					}
				}
			}()
		}
		go func() {
			workers.Wait()
			close(results)
		}()

		for result := range results {
			switch {
			case result.exchange.capRefused:
				// Nothing was sent, so there is no row: the unit stays for the
				// next run.
				setStop(ShadowStopCap)
				cancel()
				continue
			case result.err != nil:
				// A cancelled or expired context (the only error Classify
				// returns here): the unit was not answered and has no row.
				continue
			}
			if result.panicked {
				summary.PanicsRecovered++
			}
			summary.Attempted++
			record := phase.shadowRecord(cfg, result)
			summary.States[record.State]++
			phase.addAttempts(cfg, result, record, attempts, &summary)
			phase.logClassification(ctx, cfg, result, record, &summary)
			records = append(records, record)
			if result.classification.Stop {
				setStop(ShadowStopDeterministicFailure)
				cancel()
			}
			if len(records) >= batchSize {
				flush(false)
			}
		}
	}
	flush(true)

	if ctx.Err() == nil {
		flushed := store.FlushAttempts(ctx, cfg.OrgID, attempts)
		summary.AttemptRows, summary.AttemptDropped = flushed.Written, flushed.Dropped
		if flushed.Failed {
			summary.AttemptWriteErr = true
			if flushed.TableMissing {
				setStop(ShadowStopTableMissing)
			}
			phase.logger.WarnContext(ctx, "investment shadow attempt rows were not written",
				shadowStoreErrorAttrs(cfg, "attempt_write", flushed.Err)...)
		}
	} else {
		summary.AttemptDropped = attempts.Dropped()
	}

	switch {
	case stop != "":
	case ctx.Err() != nil:
		stop = ShadowStopCancelled
	case phaseCtx.Err() != nil && summary.Attempted+summary.SkippedExisting < summary.Eligible:
		stop = ShadowStopBudget
	default:
		stop = ShadowStopDone
	}
	summary.StopReason = stop
	summary.BilledNanoUSD = ledger.spent()
	summary.Duration = time.Since(started)
	phase.report(ctx, cfg, summary)
	return summary
}

// classifyOne runs one classification with its own recover: a panic in the
// adapter ends that unit as adapter_defect and never reaches the worker
// process.
func (phase *ShadowPhase) classifyOne(ctx context.Context, ledger *shadowLedger, unit shadowUnit) (result shadowResult) {
	exchange := &shadowExchange{ledger: ledger}
	defer func() {
		if recovered := recover(); recovered != nil {
			result = shadowResult{unit: unit, exchange: exchange, panicked: true, classification: decision.Classification{
				State: decision.StateAdapterDefect, Status: decision.StatusForState(decision.StateAdapterDefect),
				SufficiencyLevel: -1, Errors: []string{"adapter_defect:panic"},
			}}
		}
	}()
	classification, err := phase.classifier.Classify(withShadowExchange(ctx, exchange), unit.bundle)
	return shadowResult{unit: unit, classification: classification, err: err, exchange: exchange}
}

// shadowRecord builds the shadow row of one classification. Every string that
// a response can influence passes a bound of shadowsanitize.go first.
func (phase *ShadowPhase) shadowRecord(cfg Config, result shadowResult) chwrite.ShadowRecord {
	classification := result.classification
	state := shadowState(classification.State)
	record := chwrite.ShadowRecord{
		WorkUnitID:           result.unit.workUnitID,
		InputHash:            result.unit.bundle.InputHash,
		ShadowConfig:         phase.config,
		RubricSHA256:         phase.identity.RubricSHA256,
		ModelReturned:        shadowModelID(classification.ModelReturned),
		State:                state,
		CategorizationStatus: decision.StatusForState(state),
		CompleteStrict:       state == decision.StateOK && classification.CompleteStrict,
		Levels:               shadowLevels(classification.Levels),
		LevelProbabilities:   shadowLevelProbabilities(classification.LevelProbabilities),
		SufficiencyLevel:     shadowSufficiency(classification.SufficiencyLevel),
		Warnings:             shadowCodes(classification.Warnings),
		ErrorCodes:           shadowCodes(classification.Errors),
		ServedRunID:          cfg.RunID,
		ComputedAt:           cfg.ComputedAt,
	}
	// Only an ok classification has a mix and cited evidence. Every other state
	// keeps the two maps empty: a failure row must not look like a mix.
	if state == decision.StateOK {
		record.SubcategoryDistribution = classification.Subcategories
		record.EvidenceSpanID = shadowSpanID(classification.EvidenceSpanID)
		record.EvidenceHandle = shadowSpanID(classification.EvidenceHandle)
		if len(classification.EvidenceQuotes) > 0 {
			// The source type and id of the cited quote. The quote TEXT is not
			// stored: the shadow tables hold no source text.
			record.EvidenceSourceType = shadowSourceType(classification.EvidenceQuotes[0].SourceType)
			record.EvidenceSourceID = shadowSourceID(classification.EvidenceQuotes[0].SourceID)
		}
	}
	return record
}

// addAttempts turns the transport record of one classification into attempt
// rows: one for each HTTP attempt. The last one carries the state, the usage
// and the cost.
func (phase *ShadowPhase) addAttempts(cfg Config, result shadowResult, record chwrite.ShadowRecord, buffer *chwrite.AttemptBuffer, summary *shadowSummary) {
	exchange := result.exchange
	for index, attempt := range exchange.attempts {
		row := chwrite.AttemptRecord{
			RunID: cfg.RunID, WorkUnitID: result.unit.workUnitID, Role: chwrite.RoleShadow,
			Config: phase.config, RubricSHA256: phase.identity.RubricSHA256,
			Provider: phase.identity.Provider, APIMode: phase.identity.API, ModelRequested: phase.identity.Model,
			Attempt: shadowAttemptNumber(index), Kind: shadowAttemptKindFirst,
			HTTPStatus: shadowHTTPStatus(attempt.StatusCode), ErrorClass: shadowErrorClass(attempt.Class),
			RequestID: shadowRequestID(attempt.RequestID), RatesVersion: shadowRatesVersion,
			LatencyMS: shadowMillis(attempt.Latency), RetryWaitMS: shadowMillis(attempt.WaitBefore),
			ComputedAt: cfg.ComputedAt,
		}
		if index > 0 {
			row.Kind = shadowAttemptKindRetry
		}
		label := ShadowAttemptRetried
		if index == len(exchange.attempts)-1 {
			label = record.State
			row.State, row.ModelReturned = record.State, record.ModelReturned
			if exchange.usage.Reported {
				row.InputTokens, row.OutputTokens = shadowTokens(exchange.usage.InputTokens), shadowTokens(exchange.usage.OutputTokens)
				row.BilledCostUSD = float64(exchange.billed) / 1e9
				summary.InputTokens += int64(row.InputTokens)
				summary.OutputTokens += int64(row.OutputTokens)
			}
		}
		buffer.Add(row)
		summary.Attempts++
		summary.attemptLabels[label]++
		summary.latencies = append(summary.latencies, attempt.Latency)
	}
}

// storeStop logs a store failure and returns the stop reason it maps to. The
// error TEXT is never logged: a ClickHouse error can quote a row value.
func (phase *ShadowPhase) storeStop(ctx context.Context, cfg Config, step string, err error) string {
	reason := ShadowStopStoreError
	switch {
	case errors.Is(err, chwrite.ErrShadowTableMissing), errors.Is(err, chquery.ErrShadowTableMissing):
		reason = ShadowStopTableMissing
	case ctx.Err() != nil:
		reason = ShadowStopCancelled
	case errors.Is(err, context.DeadlineExceeded):
		reason = ShadowStopBudget
	}
	phase.logger.WarnContext(ctx, "investment shadow store step failed", shadowStoreErrorAttrs(cfg, step, err)...)
	return reason
}

func shadowStoreErrorAttrs(cfg Config, step string, err error) []any {
	attrs := []any{
		slog.String("org_id", cfg.OrgID), slog.String("run_id", cfg.RunID), slog.String("step", step),
		slog.String("error_type", fmt.Sprintf("%T", err)),
		slog.Bool("table_missing", errors.Is(err, chwrite.ErrShadowTableMissing) || errors.Is(err, chquery.ErrShadowTableMissing)),
	}
	var exception *clickhouse.Exception
	if errors.As(err, &exception) {
		attrs = append(attrs, slog.Int("clickhouse_code", int(exception.Code)))
	}
	return attrs
}

// logClassification writes the debug line of one classification, and an ERROR
// line for a defect of the adapter (a bug, not a data state). Scalars only: no
// code, warning, source text, request or response.
func (phase *ShadowPhase) logClassification(ctx context.Context, cfg Config, result shadowResult, record chwrite.ShadowRecord, summary *shadowSummary) {
	requestID := ""
	latency := time.Duration(0)
	for _, attempt := range result.exchange.attempts {
		requestID = shadowRequestID(attempt.RequestID)
		latency += attempt.Latency
	}
	phase.logger.DebugContext(ctx, "investment shadow classification",
		slog.String("org_id", cfg.OrgID), slog.String("run_id", cfg.RunID),
		slog.String("work_unit_id", result.unit.workUnitID),
		slog.String("provider", phase.identity.Provider), slog.String("model", phase.identity.Model),
		slog.String("state", record.State), slog.Int64("latency_ms", latency.Milliseconds()),
		slog.Int64("input_tokens", result.exchange.usage.InputTokens),
		slog.Int64("output_tokens", result.exchange.usage.OutputTokens),
		slog.String("request_id", requestID), slog.Int("attempts", len(result.exchange.attempts)),
	)
	if record.State != decision.StateAdapterDefect {
		return
	}
	summary.defectLogs++
	if summary.defectLogs > shadowDefectLogLimit {
		return
	}
	phase.logger.ErrorContext(ctx, "investment shadow adapter defect",
		slog.String("org_id", cfg.OrgID), slog.String("run_id", cfg.RunID),
		slog.String("work_unit_id", result.unit.workUnitID),
		slog.Bool("panic_recovered", result.panicked),
		slog.Int("error_codes", len(record.ErrorCodes)),
	)
}

// report writes the run log line of the phase, the WARN line of a stop and the
// metrics.
func (phase *ShadowPhase) report(ctx context.Context, cfg Config, summary shadowSummary) {
	attrs := []any{
		slog.String("org_id", cfg.OrgID), slog.String("run_id", cfg.RunID),
		slog.String("provider", phase.identity.Provider), slog.String("model", phase.identity.Model),
		slog.String("stop_reason", summary.StopReason),
		slog.Int("units_gate_pass", summary.GatePass),
		slog.Int("units_eligible", summary.Eligible),
		slog.Int("units_skipped_existing", summary.SkippedExisting),
		slog.Int("units_attempted", summary.Attempted),
		slog.Int("units_not_reached", summary.Eligible-summary.SkippedExisting-summary.Attempted),
	}
	for _, state := range shadowStates() {
		attrs = append(attrs, slog.Int("state_"+state, summary.States[state]))
	}
	attrs = append(attrs,
		slog.Int("attempts", summary.Attempts),
		slog.Int64("input_tokens", summary.InputTokens),
		slog.Int64("output_tokens", summary.OutputTokens),
		slog.Float64("billed_cost_usd", float64(summary.BilledNanoUSD)/1e9),
		slog.String("rates_version", shadowRatesVersion),
		slog.Int64("duration_ms", summary.Duration.Milliseconds()),
		slog.Int64("budget_ms", phase.settings.Budget.Milliseconds()),
		slog.Int("rows_written", summary.RowsWritten),
		slog.Int("rows_lost", summary.RowsLost),
		slog.Int("attempt_rows_written", summary.AttemptRows),
		slog.Int("attempt_rows_dropped", summary.AttemptDropped),
		slog.Bool("attempt_write_failed", summary.AttemptWriteErr),
		slog.Int("panics_recovered", summary.PanicsRecovered),
	)
	phase.logger.InfoContext(ctx, "investment shadow phase complete", attrs...)
	if summary.StopReason != ShadowStopDone {
		phase.logger.WarnContext(ctx, "investment shadow phase stopped",
			slog.String("org_id", cfg.OrgID), slog.String("run_id", cfg.RunID),
			slog.String("stop_reason", summary.StopReason),
			slog.Int("units_not_reached", summary.Eligible-summary.SkippedExisting-summary.Attempted),
			slog.Int64("duration_ms", summary.Duration.Milliseconds()),
			slog.Int64("budget_ms", phase.settings.Budget.Milliseconds()),
		)
	}
	if phase.observer != nil {
		writeErrors := 0
		if summary.AttemptWriteErr {
			writeErrors = 1
		}
		phase.observer.ObserveShadowPhase(ShadowPhaseCounts{
			Model: phase.identity.Model, StopReason: summary.StopReason,
			AttemptsByState: summary.attemptLabels, AttemptLatencies: summary.latencies,
			PanicsRecovered: summary.PanicsRecovered, AttemptWriteErrors: writeErrors,
			AttemptRowsDropped: summary.AttemptDropped,
		})
	}
}

// reportPanic is the last line of defence: a panic of the phase's own
// goroutine (not of a classification, which classifyOne recovers).
func (phase *ShadowPhase) reportPanic(ctx context.Context, cfg Config, recovered any) {
	phase.logger.ErrorContext(ctx, "investment shadow phase panicked",
		slog.String("org_id", cfg.OrgID), slog.String("run_id", cfg.RunID),
		slog.String("panic_type", fmt.Sprintf("%T", recovered)),
	)
	if phase.observer != nil {
		phase.observer.ObserveShadowPhase(ShadowPhaseCounts{
			Model: phase.identity.Model, StopReason: ShadowStopPanic, PanicsRecovered: 1,
		})
	}
}

// runShadow is the one call site of the phase in Materializer.Run. It has no
// result: nothing of the phase can reach the served result.
func (m *Materializer) runShadow(ctx context.Context, cfg Config, entries []preprocessed) {
	if m.shadow == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			m.shadow.reportPanic(ctx, cfg, recovered)
		}
	}()
	m.shadow.run(ctx, clickHouseShadowStore{reader: m.reader, writer: m.writer}, cfg, entries)
}

// SetShadow attaches the shadow phase of this run. Nil (the default) means no
// phase: no client exists and no request is possible.
func (m *Materializer) SetShadow(phase *ShadowPhase) {
	if m != nil {
		m.shadow = phase
	}
}
