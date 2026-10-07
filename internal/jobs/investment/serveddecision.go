package investment

// serveddecision.go is the SERVED mode of the decision backend (TypeSafe Jev)
// in investment.materialize (CHAOS-8874, parent CHAOS-8865). For an org on the
// list of INVESTMENT_SERVED_DECISION_ORG_IDS the categorization call of the run
// is one decision.Completer.Classify in place of the generative
// categorize.CategorizeTextBundle; everything before it (the gates, the
// skip-existing read) and after it (the theme roll-up, the three writes) is
// the one served path.
//
// The switch is OFF by default: with an empty list no client is built and no
// request is possible. Taking the org off the list is the rollback.
//
// What a served decision row is, by state (the two functions marked RULING are
// the two choices that wait for a ruling; each is the one place of its choice):
//
//   - ok: the validated mix, its one cited quote, status ok.
//   - zero_support: the top raw key at 1.0, no quote, servedLowQualityStatus,
//     evidence quality capped at servedLowQualityCap. No generative fallback.
//   - evidence_none: the mix of the levels, no quote, the same status and cap.
//   - a refused, missing or invalid answer, a missing evidence answer, an
//     adapter defect: the invalid_llm_output row of the generative path.
//   - request_failed: servedTransportFailureWritesRow. A deterministic failure
//     (rejected key, unknown model) ends the run, like the generative path.
//
// The stamp of every row is decision.Identity.Stamp, so no decision row can
// match a generative skip-existing key. The run's llm_token_usage row names
// the decision provider and model: the org spend reader groups by them.
//
// No log line and no column holds prompt, response or source text beyond the
// cited quote, which is a span of the org's own source text.

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"sync"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// EnvServedDecisionOrgIDs is the switch: a comma list of org ids, "*" for
// every org. Empty, the default, is off.
const EnvServedDecisionOrgIDs = "INVESTMENT_SERVED_DECISION_ORG_IDS"

const (
	// servedLowQualityCap is the evidence-quality cap of a served row that has
	// a mix and no validated evidence quote. It is the cap of an
	// invalid_llm_output row.
	servedLowQualityCap = 0.3
	// servedTopRawKeyCode marks a zero_support row in its audit column.
	servedTopRawKeyCode = "served_top_raw_key"
	// servedLevelMixCode marks an evidence_none row in its audit column.
	servedLevelMixCode = "served_level_mix"
	// servedLogLimit bounds the per-unit WARN and ERROR lines of one run.
	servedLogLimit = 5
	// servedNoSpendCap is the limit of the ledger of a served run. The served
	// path has no spend cap (the generative path has none); the ledger is kept
	// because the transport bills through it.
	servedNoSpendCap = math.MaxInt64 / 4
)

// servedLowQualityStatus is the status of a served zero_support or
// evidence_none row. RULING 1 (provisional): invalid_llm_output. Such a row is
// not reused by skip-existing, so the unit is asked again by the next run.
func servedLowQualityStatus() string { return categorize.StatusInvalidLLMOutput }

// servedTransportFailureWritesRow says what a unit gets when its request
// failed and the failure is not deterministic. RULING 2 (provisional): false,
// option A. No investment row is written, so the last served row of the unit
// stays the latest one, and the next run asks again. true is option C: the
// llm_task_failed prior row of the generative path.
func servedTransportFailureWritesRow() bool { return false }

// ServedDecisionSettings is the decoded switch.
type ServedDecisionSettings struct {
	// AllOrgs is the "*" entry of the list.
	AllOrgs bool
	// OrgIDs is the allow-list. Empty with AllOrgs false is off.
	OrgIDs map[string]struct{}
}

// ServedDecisionSettingsFromEnv decodes the switch through lookup (production
// passes secrets.GetenvNamed). No value of the variable is an error: an entry
// is an org id or it matches no org.
func ServedDecisionSettingsFromEnv(lookup func(string) string) ServedDecisionSettings {
	settings := ServedDecisionSettings{OrgIDs: map[string]struct{}{}}
	for _, entry := range strings.Split(lookup(EnvServedDecisionOrgIDs), ",") {
		switch entry = strings.TrimSpace(entry); entry {
		case "":
		case shadowAllOrgs:
			settings.AllOrgs = true
		default:
			settings.OrgIDs[entry] = struct{}{}
		}
	}
	return settings
}

// EnabledFor reports whether the org is served by the decision backend. An
// empty org is never: "*" means every organization, not a run with none.
func (settings ServedDecisionSettings) EnabledFor(orgID string) bool {
	if orgID == "" {
		return false
	}
	if settings.AllOrgs {
		return true
	}
	_, ok := settings.OrgIDs[orgID]
	return ok
}

// servedClassifier is the decision completer as the served mode uses it.
type servedClassifier interface {
	Classify(ctx context.Context, bundle units.TextBundle) (decision.Classification, error)
	LevelMix(levels map[string]int) (map[string]float64, bool)
}

// ServedDecision is the served decision backend of one Execute: built once,
// used by one Materializer.Run, then closed.
type ServedDecision struct {
	classifier servedClassifier
	// identity and stamp are fixed at construction from configured values only,
	// never from a response field.
	identity decision.Identity
	stamp    string
	logger   *slog.Logger
	closer   func() error
	ledger   *shadowLedger
	attempts *chwrite.AttemptBuffer

	mu sync.Mutex
	// lowQuality and keptLastRow are keyed by the component index of the run.
	lowQuality  map[int]struct{}
	keptLastRow map[int]struct{}
	states      map[string]int
	attemptRows int
	warnLogs    int
	defectLogs  int
	panics      int
}

// NewServedDecision builds the backend over a TypeSafe client. It fails when
// the embedded rubric does not have its pinned digest.
func NewServedDecision(client *categorize.TypeSafeClient, logger *slog.Logger) (*ServedDecision, error) {
	if client == nil {
		return nil, ErrUnavailable
	}
	return newServedDecision(client, logger)
}

func newServedDecision(sender systemOneSender, logger *slog.Logger) (*ServedDecision, error) {
	if sender == nil || logger == nil {
		return nil, ErrUnavailable
	}
	completer, err := decision.NewCompleter(shadowTransport{sender: sender}, sender.Model())
	if err != nil {
		return nil, err
	}
	identity := completer.Identity()
	return &ServedDecision{
		classifier: completer, identity: identity, stamp: shadowConfigStamp(identity),
		logger: logger, closer: sender.Close,
		ledger: &shadowLedger{limit: servedNoSpendCap}, attempts: chwrite.NewAttemptBuffer(0),
		lowQuality: map[int]struct{}{}, keptLastRow: map[int]struct{}{}, states: map[string]int{},
	}, nil
}

// Close releases the client of the backend.
func (served *ServedDecision) Close() error {
	if served == nil || served.closer == nil {
		return nil
	}
	return served.closer()
}

// servedFailure is the error of a request that gave no usable response. Its
// text holds the failure class only, which is one of a closed set.
type servedFailure struct {
	class         string
	deterministic bool
	calls         int
	inputTokens   int
	outputTokens  int
}

func (failure *servedFailure) Error() string {
	return "investment served decision: the request failed (" + failure.class + ")"
}

// llmFailureOf is the class of a categorization error and whether it recurs on
// every request. A served decision failure carries both; every other error is
// classified by the generative path's own two functions.
func llmFailureOf(err error) (class string, deterministic bool) {
	var failure *servedFailure
	if errors.As(err, &failure) {
		return failure.class, failure.deterministic
	}
	return categorize.FailureClass(err), categorize.IsDeterministicFailure(err)
}

// servedFailureClass reads the class of a request_failed classification from
// its detail code ("request_failed:<class>"). A class outside the code shape
// is "llm_error".
func servedFailureClass(classification decision.Classification) string {
	const prefix = decision.StateRequestFailed + ":"
	for _, code := range classification.Errors {
		if !strings.HasPrefix(code, prefix) {
			continue
		}
		class := strings.TrimPrefix(code, prefix)
		if cut := strings.IndexByte(class, ':'); cut >= 0 {
			class = class[:cut]
		}
		if class != "" && shadowShaped(class, shadowMaxCodeBytes, "_") {
			return class
		}
	}
	return "llm_error"
}

// categorize is the served categorization of one unit: one request, no repair,
// no second backend. It returns the outcome of the unit, or an error when the
// unit has none (a failed request, a cancelled context).
func (served *ServedDecision) categorize(ctx context.Context, cfg Config, entry preprocessed) (categorize.CategorizationOutcome, error) {
	unit := shadowUnit{workUnitID: entry.result.Investment.WorkUnitID, bundle: entry.result.Bundle}
	result := served.classifyOne(ctx, unit)
	if result.err != nil {
		// A cancelled or expired context (the only error Classify returns for a
		// completer that NewCompleter built).
		return categorize.CategorizationOutcome{}, result.err
	}
	classification := result.classification
	state := shadowState(classification.State)
	modelReturned := shadowModelID(classification.ModelReturned)
	rows := attemptRecords(cfg, chwrite.RoleServed, served.identity, served.stamp, unit.workUnitID, result.exchange, state, modelReturned)
	for _, row := range rows {
		served.attempts.Add(row)
	}

	served.mu.Lock()
	served.states[state]++
	served.attemptRows += len(rows)
	if result.panicked {
		served.panics++
	}
	served.mu.Unlock()

	if state == decision.StateRequestFailed {
		failure := &servedFailure{
			class: servedFailureClass(classification), deterministic: classification.Stop,
			calls: classification.LLMCalls, inputTokens: classification.InputTokens, outputTokens: classification.OutputTokens,
		}
		if !failure.deterministic && !servedTransportFailureWritesRow() {
			served.mu.Lock()
			served.keptLastRow[entry.index] = struct{}{}
			served.mu.Unlock()
		}
		served.logFailure(ctx, cfg, unit.workUnitID, failure)
		return categorize.CategorizationOutcome{}, failure
	}

	outcome, lowQuality := served.outcomeFor(classification, state)
	outcome.LLMCalls, outcome.InputTokens, outcome.OutputTokens = classification.LLMCalls, classification.InputTokens, classification.OutputTokens
	outcome.LLMModel = modelReturned
	if lowQuality {
		served.mu.Lock()
		served.lowQuality[entry.index] = struct{}{}
		served.mu.Unlock()
	}
	if state == decision.StateAdapterDefect {
		served.logDefect(ctx, cfg, unit.workUnitID, result.panicked)
	}
	return outcome, nil
}

// classifyOne runs one classification with its own recover: a panic in the
// adapter ends that unit as adapter_defect and never reaches the worker
// process.
func (served *ServedDecision) classifyOne(ctx context.Context, unit shadowUnit) (result shadowResult) {
	exchange := &shadowExchange{ledger: served.ledger}
	defer func() {
		if recovered := recover(); recovered != nil {
			result = shadowResult{unit: unit, exchange: exchange, panicked: true, classification: decision.Classification{
				State: decision.StateAdapterDefect, Status: decision.StatusForState(decision.StateAdapterDefect),
				SufficiencyLevel: -1, Errors: []string{categorize.StatusInvalidLLMOutput, "decision_" + decision.StateAdapterDefect, "adapter_defect:panic"},
			}}
		}
	}()
	classification, err := served.classifier.Classify(withShadowExchange(ctx, exchange), unit.bundle)
	return shadowResult{unit: unit, classification: classification, err: err, exchange: exchange}
}

// outcomeFor maps a classification that has a response to the outcome of its
// unit. lowQuality reports a row with a mix of the model and no validated
// evidence quote. Every code passes the bound of shadowsanitize.go.
func (served *ServedDecision) outcomeFor(classification decision.Classification, state string) (outcome categorize.CategorizationOutcome, lowQuality bool) {
	warnings := shadowCodes(classification.Warnings)
	switch state {
	case decision.StateOK:
		return categorize.CategorizationOutcome{
			Subcategories: classification.Subcategories, EvidenceQuotes: classification.EvidenceQuotes,
			Uncertainty: classification.Uncertainty, Status: categorize.StatusOK,
			Errors: []string{}, Warnings: warnings,
		}, false
	case decision.StateZeroSupport:
		if mix, ok := servedTopRawKeyMix(classification.TopRawKey); ok {
			return servedLowQualityOutcome(mix, state, servedTopRawKeyCode, warnings), true
		}
	case decision.StateEvidenceNone:
		if mix, ok := served.classifier.LevelMix(classification.Levels); ok {
			return servedLowQualityOutcome(mix, state, servedLevelMixCode, warnings), true
		}
	}
	// A refused, missing or invalid answer, a missing evidence answer, an
	// adapter defect, or a zero_support / evidence_none answer with nothing to
	// build a mix from: the invalid_llm_output row of the generative path.
	outcome = categorize.FallbackOutcome(categorize.StatusInvalidLLMOutput)
	outcome.Errors = shadowCodes(append([]string{categorize.StatusInvalidLLMOutput, "decision_" + state}, servedDetails(classification.Errors)...))
	outcome.Warnings = warnings
	return outcome, false
}

// servedDetails is the detail codes of a classification: its errors with the
// status and the "decision_<state>" entry, which the caller writes itself,
// taken off.
func servedDetails(errorCodes []string) []string {
	if len(errorCodes) <= 2 {
		return nil
	}
	return errorCodes[2:]
}

// servedLowQualityOutcome is the outcome of a served row with a mix of the
// model and no validated evidence quote.
func servedLowQualityOutcome(mix map[string]float64, state, code string, warnings []string) categorize.CategorizationOutcome {
	outcome := categorize.FallbackOutcome(servedLowQualityStatus())
	outcome.Subcategories = mix
	outcome.Errors = []string{servedLowQualityStatus(), "decision_" + state, code}
	outcome.Warnings = warnings
	return outcome
}

// servedTopRawKeyMix is the mix of a zero_support row: the whole weight on the
// top raw key. ok is false when the key is not a canonical subcategory.
func servedTopRawKeyMix(key string) (map[string]float64, bool) {
	for _, canonical := range units.SortedSubcategories {
		if key == canonical {
			return categorize.EnsureFullSubcategoryVector(map[string]float64{key: 1}), true
		}
	}
	return nil, false
}

// isLowQuality reports a unit whose row gets the low-quality cap. Nil is
// tolerated: the generative path has no such unit.
func (served *ServedDecision) isLowQuality(index int) bool {
	if served == nil {
		return false
	}
	served.mu.Lock()
	defer served.mu.Unlock()
	_, ok := served.lowQuality[index]
	return ok
}

// keepsLastRow reports a unit that gets no investment row in this run. Nil is
// tolerated.
func (served *ServedDecision) keepsLastRow(index int) bool {
	if served == nil {
		return false
	}
	served.mu.Lock()
	defer served.mu.Unlock()
	_, ok := served.keptLastRow[index]
	return ok
}

func (served *ServedDecision) logFailure(ctx context.Context, cfg Config, workUnitID string, failure *servedFailure) {
	served.mu.Lock()
	served.warnLogs++
	quiet := served.warnLogs > servedLogLimit
	served.mu.Unlock()
	if quiet {
		return
	}
	served.logger.WarnContext(ctx, "investment served decision request failed",
		slog.String("org_id", cfg.OrgID), slog.String("run_id", cfg.RunID),
		slog.String("work_unit_id", workUnitID), slog.String("failure_class", failure.class),
		slog.Bool("deterministic", failure.deterministic),
		slog.Bool("row_written", failure.deterministic || servedTransportFailureWritesRow()),
	)
}

func (served *ServedDecision) logDefect(ctx context.Context, cfg Config, workUnitID string, panicked bool) {
	served.mu.Lock()
	served.defectLogs++
	quiet := served.defectLogs > servedLogLimit
	served.mu.Unlock()
	if quiet {
		return
	}
	served.logger.ErrorContext(ctx, "investment served decision adapter defect",
		slog.String("org_id", cfg.OrgID), slog.String("run_id", cfg.RunID),
		slog.String("work_unit_id", workUnitID), slog.Bool("panic_recovered", panicked),
	)
}

// finish writes the attempt rows of the run in one insert and the run log
// line. It returns nothing: an attempt row that was not written must not fail
// a run. A cancelled run context writes nothing.
func (served *ServedDecision) finish(ctx context.Context, writer *chwrite.Writer, cfg Config) {
	written, dropped, failed := 0, served.attempts.Dropped(), false
	if ctx.Err() == nil {
		flushed := writer.FlushAttempts(ctx, cfg.OrgID, served.attempts)
		written, dropped, failed = flushed.Written, flushed.Dropped, flushed.Failed
		if flushed.Failed {
			served.logger.WarnContext(ctx, "investment served decision attempt rows were not written",
				shadowStoreErrorAttrs(cfg, "attempt_write", flushed.Err)...)
		}
	}
	served.mu.Lock()
	defer served.mu.Unlock()
	attrs := []any{
		slog.String("org_id", cfg.OrgID), slog.String("run_id", cfg.RunID),
		slog.String("provider", served.identity.Provider), slog.String("model", served.identity.Model),
	}
	for _, state := range shadowStates() {
		attrs = append(attrs, slog.Int("state_"+state, served.states[state]))
	}
	attrs = append(attrs,
		slog.Int("units_low_quality", len(served.lowQuality)),
		slog.Int("units_kept_last_row", len(served.keptLastRow)),
		slog.Int("attempts", served.attemptRows),
		slog.Float64("billed_cost_usd", float64(served.ledger.spent())/1e9),
		slog.String("rates_version", shadowRatesVersion),
		slog.Int("attempt_rows_written", written),
		slog.Int("attempt_rows_dropped", dropped),
		slog.Bool("attempt_write_failed", failed),
		slog.Int("panics_recovered", served.panics),
	)
	served.logger.InfoContext(ctx, "investment served decision complete", attrs...)
}

// SetServed attaches the served decision backend of this run. Nil (the
// default) means the generative provider serves the run.
func (m *Materializer) SetServed(served *ServedDecision) {
	if m != nil {
		m.served = served
	}
}

// categorizeEntry is the one categorization call of the served path.
func (m *Materializer) categorizeEntry(ctx context.Context, cfg Config, entry preprocessed) (categorize.CategorizationOutcome, error) {
	if m.served != nil {
		return m.served.categorize(ctx, cfg, entry)
	}
	return categorize.CategorizeTextBundle(ctx, entry.result.Bundle, categorize.CategorizeOptions{
		Provider: m.provider, ProviderName: cfg.ProviderName, Model: cfg.Model,
	})
}

// modelVersion is the stamp of the rows of this run, and its skip-existing key.
func (m *Materializer) modelVersion(cfg Config) string {
	if m.served != nil {
		return m.served.stamp
	}
	return categorize.EffectiveModelVersion(cfg.ProviderName, resolvedModelName(cfg))
}

// usageIdentity is the provider and model of the run's llm_token_usage row.
func (m *Materializer) usageIdentity(cfg Config) (provider, model string) {
	if m.served != nil {
		return m.served.identity.Provider, m.served.identity.Model
	}
	return cfg.ProviderName, resolvedModelName(cfg)
}

// finishServed writes the attempt rows and the run log line of a served
// decision run. It does nothing for a generative run.
func (m *Materializer) finishServed(ctx context.Context, cfg Config) {
	if m.served != nil {
		m.served.finish(ctx, m.writer, cfg)
	}
}
