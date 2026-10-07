package decisioneval

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// RunConfig configures one run of the experiment runner. A plain test run
// never builds one: the entry point is env-gated (see entry_test.go).
type RunConfig struct {
	// OutDir holds ledger.jsonl and runs/<run_id>/raw/... It must be outside
	// any git repository.
	OutDir string

	// Fixtures are the bundles to classify. FixturesPath is read when Fixtures
	// is empty.
	Fixtures     []FixtureRecord
	FixturesPath string
	MaxFixtures  int

	Arms        []string
	Concurrency int
	// DryRun renders and saves the requests and reports the token estimate; it
	// sends nothing and needs no key.
	DryRun bool
	// Live must be true for any real send. Together with a cap > 0 for each
	// provider it is the only way a request leaves the process.
	Live bool
	// Caps are the HARD spend caps in USD for each provider (typesafe, openai).
	// 0 or missing refuses to run live.
	Caps map[string]float64

	Set    string
	Repeat int
	RunID  string

	Rubric *Rubric
	Rates  *RateTable
	// MapName selects a weight map ("" = primary). Alternates are for the
	// development set; scoring re-evaluates maps offline from stored answers.
	MapName string

	Jev       Backend
	Decisions Backend
	Incumbent IncumbentConfig

	// Base is the underlying transport (nil = a direct one). Tests inject an
	// httptest transport; nothing else may change the destination.
	Base http.RoundTripper
	// Timeout is the per-attempt timeout of the decision arms (30 s).
	Timeout time.Duration
	// IncumbentTimeout is the per-attempt timeout of the incumbent client (60 s,
	// the production client's).
	IncumbentTimeout time.Duration
	Sleep            func(ctx context.Context, d time.Duration) bool
	Now              func() time.Time

	// RetryOrphans allows resending fixtures whose attempt was reserved but
	// never completed (the request may have been billed; the reserved cost
	// still counts toward the cap).
	RetryOrphans bool
	// ConsecutiveFailureStop stops an arm after this many request failures in a
	// row (default 5).
	ConsecutiveFailureStop int

	Out io.Writer
}

// ArmSummary counts what one arm did in a run.
type ArmSummary struct {
	Planned int `json:"planned"`
	Sent    int `json:"sent"`
	// Rendered counts requests built and saved by a dry run (nothing sent).
	Rendered      int            `json:"rendered,omitempty"`
	SkippedResume int            `json:"skipped_resume"`
	Gate          int            `json:"gate"`
	NotRun        int            `json:"not_run"`
	States        map[string]int `json:"states"`
	// Stopped names why the arm stopped early ("" = it did not).
	Stopped string `json:"stopped,omitempty"`
}

// RunSummary is the result of Run.
type RunSummary struct {
	RunID  string                 `json:"run_id"`
	Arms   map[string]*ArmSummary `json:"arms"`
	Spend  map[string]float64     `json:"spend_usd"`
	DryRun *DryRunReport          `json:"dry_run,omitempty"`
}

// StoppedArms lists the arms that stopped early with their reason.
func (s RunSummary) StoppedArms() map[string]string {
	out := map[string]string{}
	for arm, a := range s.Arms {
		if a.Stopped != "" {
			out[arm] = a.Stopped
		}
	}
	return out
}

// DryRunArm is the dry-run estimate of one arm.
type DryRunArm struct {
	Requests          int     `json:"requests"`
	EstInputTokensMin int     `json:"est_input_tokens_min"`
	EstInputTokensP50 int     `json:"est_input_tokens_p50"`
	EstInputTokensMax int     `json:"est_input_tokens_max"`
	EstInputTokens    float64 `json:"est_input_tokens_mean"`
	EstCostUSD        float64 `json:"est_cost_usd_input_only"`
	// Dir holds the saved request bodies.
	Dir string `json:"dir"`
}

// DryRunReport is the dry-run result: nothing was sent.
type DryRunReport struct {
	Arms      map[string]*DryRunArm `json:"arms"`
	GateSkips int                   `json:"gate_skips"`
}

// KnownArms lists the arm names.
var KnownArms = []string{ArmIncumbent, ArmIncumbentDefs, ArmJev, ArmDecisions}

func newRunID(now time.Time) string {
	var b [3]byte
	_, _ = rand.Read(b[:])
	return now.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
}

func (c *RunConfig) now() time.Time {
	if c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}

func (c *RunConfig) versions(armPrompt string) Versions {
	return Versions{
		Rubric: c.Rubric.RubricVersion, Map: c.mapVersion(), Adapter: c.Rubric.AdapterVersion, Span: c.Rubric.SpanVersion,
		Eval: EvalVersion, Rates: c.Rates.Version, Prompt: armPrompt, Taxonomy: categorize.TaxonomyVersion,
		RubricSHA256: c.Rubric.SHA256,
	}
}

func (c *RunConfig) mapVersion() string {
	if c.MapName != "" {
		return c.MapName
	}
	return c.Rubric.MapVersion
}

func (c *RunConfig) validate() error {
	if c.Rubric == nil {
		return errors.New("run: rubric is not loaded")
	}
	if c.Rates == nil {
		return errors.New("run: rate table is not loaded")
	}
	if c.OutDir == "" {
		return errors.New("run: output directory is not set")
	}
	if len(c.Arms) == 0 {
		return errors.New("run: no arms")
	}
	for _, a := range c.Arms {
		if !containsString(KnownArms, a) {
			return fmt.Errorf("run: unknown arm %q (known: %s)", a, strings.Join(KnownArms, ", "))
		}
	}
	return nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// armBackend describes the provider facts of an arm.
type armInfo struct {
	provider, apiMode, endpoint, model, promptVersion string
}

func (c *RunConfig) armInfo(arm string) armInfo {
	switch arm {
	case ArmJev:
		return armInfo{ProviderTypeSafe, APIModeSystemOne, c.Jev.Endpoint, c.Jev.Model, c.Rubric.RubricVersion}
	case ArmDecisions:
		return armInfo{ProviderOpenAI, APIModeDecisions, c.Decisions.Endpoint, c.Decisions.Model, c.Rubric.RubricVersion}
	default:
		base := c.Incumbent.BaseURL
		if base == "" {
			base = "https://api.openai.com/v1"
		}
		model := c.Incumbent.Model
		if model == "" {
			model = DefaultIncumbentModel
		}
		return armInfo{ProviderOpenAI, APIModeResponses, strings.TrimRight(base, "/") + "/responses", model, IncumbentPromptVersion(arm == ArmIncumbentDefs, c.Rubric.RubricVersion)}
	}
}

type armState struct {
	mu      sync.Mutex
	stopped string
	consec  int
}

func (a *armState) stop(reason string) {
	a.mu.Lock()
	if a.stopped == "" {
		a.stopped = reason
	}
	a.mu.Unlock()
}

func (a *armState) reason() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stopped
}

func (a *armState) note(failed bool, limit int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !failed {
		a.consec = 0
		return
	}
	a.consec++
	if a.consec >= limit && a.stopped == "" {
		a.stopped = fmt.Sprintf("%d_request_failures_in_a_row", limit)
	}
}

// Run executes the runner. In dry-run mode it only renders requests. In live
// mode it refuses unless Live is set and every provider in use has a cap > 0.
func Run(ctx context.Context, cfg RunConfig) (RunSummary, error) {
	if err := cfg.validate(); err != nil {
		return RunSummary{}, err
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.ConsecutiveFailureStop <= 0 {
		cfg.ConsecutiveFailureStop = 5
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.IncumbentTimeout <= 0 {
		cfg.IncumbentTimeout = 60 * time.Second
	}
	if cfg.Out == nil {
		cfg.Out = io.Discard
	}
	if cfg.RunID == "" {
		cfg.RunID = newRunID(cfg.now())
	}
	if cfg.Set == "" {
		cfg.Set = "adhoc"
	}
	fixtures := cfg.Fixtures
	if len(fixtures) == 0 {
		var err error
		if fixtures, err = LoadFixtures(cfg.FixturesPath, cfg.MaxFixtures); err != nil {
			return RunSummary{}, err
		}
	} else if cfg.MaxFixtures > 0 && len(fixtures) > cfg.MaxFixtures {
		fixtures = fixtures[:cfg.MaxFixtures]
	}
	// Build every bundle before any spend: a bad fixture stops the run here.
	bundles := make([]units.TextBundle, len(fixtures))
	for i, f := range fixtures {
		b, err := f.Bundle()
		if err != nil {
			return RunSummary{}, err
		}
		bundles[i] = b
	}
	if err := RefuseInsideRepo(cfg.OutDir); err != nil {
		return RunSummary{}, err
	}
	if cfg.DryRun {
		return runDry(ctx, cfg, fixtures, bundles)
	}
	return runLive(ctx, cfg, fixtures, bundles)
}

// preflight checks caps and rates for the arms in use before the first send.
func preflight(cfg RunConfig) error {
	if !cfg.Live {
		return errors.New("run: live mode is not enabled: refusing to send (set Live, or use --dry-run)")
	}
	for _, arm := range cfg.Arms {
		info := cfg.armInfo(arm)
		if cfg.Caps[info.provider] <= 0 {
			return fmt.Errorf("run: spend cap for provider %s is %v: refusing to run live (a cap > 0 USD is required)", info.provider, cfg.Caps[info.provider])
		}
		if _, err := cfg.Rates.Lookup(info.provider, info.apiMode, info.model); err != nil {
			return fmt.Errorf("run: %w", err)
		}
		switch arm {
		case ArmJev:
			if cfg.Jev.APIKey.Reveal() == "" || cfg.Jev.Endpoint == "" {
				return errors.New("run: jev arm needs an endpoint and a token")
			}
		case ArmDecisions:
			if cfg.Decisions.APIKey.Reveal() == "" || cfg.Decisions.Endpoint == "" {
				return errors.New("run: decisions arm needs an endpoint and an API key")
			}
		default:
			if cfg.Incumbent.APIKey.Reveal() == "" {
				return errors.New("run: incumbent arms need an API key")
			}
		}
	}
	return nil
}

func runLive(ctx context.Context, cfg RunConfig, fixtures []FixtureRecord, bundles []units.TextBundle) (RunSummary, error) {
	if err := preflight(cfg); err != nil {
		return RunSummary{}, err
	}
	if _, err := os.Stat(filepath.Join(cfg.OutDir, LedgerFile)); err != nil && !os.IsNotExist(err) {
		return RunSummary{}, err
	}
	var prior *LedgerData
	if _, err := os.Stat(filepath.Join(cfg.OutDir, LedgerFile)); err == nil {
		if prior, err = ReadLedger(cfg.OutDir); err != nil {
			return RunSummary{}, err
		}
	} else {
		prior = &LedgerData{}
	}
	if orphans := prior.OrphanReservations(); len(orphans) > 0 && !cfg.RetryOrphans {
		var ids []string
		for _, o := range orphans {
			ids = append(ids, o.AttemptID)
		}
		return RunSummary{}, fmt.Errorf("run: %d attempt(s) were reserved and never completed (the request may have been billed): %s; set RetryOrphans to resend them (their reserved cost counts toward the cap)", len(orphans), strings.Join(ids, ", "))
	}
	done := map[string]bool{}
	for _, c := range prior.Classifications {
		if !c.Retriable() {
			done[c.Key()] = true
		}
	}
	ledger, err := OpenLedger(cfg.OutDir)
	if err != nil {
		return RunSummary{}, err
	}
	defer ledger.Close()
	budget := NewBudget(cfg.Caps, prior.SpentByProvider())
	rec := &Recorder{Ledger: ledger, Budget: budget, Now: cfg.Now}

	runCfg := map[string]string{"concurrency": fmt.Sprint(cfg.Concurrency), "repeat": fmt.Sprint(cfg.Repeat), "map": cfg.mapVersion(),
		"fixtures": fmt.Sprint(len(fixtures))}
	for p, v := range cfg.Caps {
		runCfg["cap_usd_"+p] = fmt.Sprint(v)
	}
	if err := ledger.Append(RunRecord{Kind: KindRun, RunID: cfg.RunID, Started: cfg.now(), Set: cfg.Set, Arms: cfg.Arms, Versions: cfg.versions(""), Config: runCfg}); err != nil {
		return RunSummary{}, err
	}

	decisionClient := NewRecordingClient(cfg.Base, rec, cfg.Timeout)
	incumbentClient := NewRecordingClient(cfg.Base, rec, cfg.IncumbentTimeout)
	sender := &HTTPSender{Client: decisionClient, Sleep: cfg.Sleep}

	summary := RunSummary{RunID: cfg.RunID, Arms: map[string]*ArmSummary{}, Spend: map[string]float64{}}
	for _, arm := range cfg.Arms {
		as := &ArmSummary{States: map[string]int{}}
		summary.Arms[arm] = as
		state := &armState{}
		info := cfg.armInfo(arm)
		weights, _, err := cfg.Rubric.MapWeights(cfg.MapName)
		if err != nil {
			return summary, err
		}
		rate, _ := cfg.Rates.Lookup(info.provider, info.apiMode, info.model)

		sem := make(chan struct{}, cfg.Concurrency)
		var wg sync.WaitGroup
		var mu sync.Mutex
		var firstErr error
		for i := range fixtures {
			f, bundle := fixtures[i], bundles[i]
			key := ResumeKey(f.BundleID, arm, cfg.Rubric.RubricVersion, cfg.Rubric.AdapterVersion, cfg.Repeat)
			mu.Lock()
			as.Planned++
			mu.Unlock()
			if done[key] {
				mu.Lock()
				as.SkippedResume++
				mu.Unlock()
				continue
			}
			if state.reason() != "" || ctx.Err() != nil {
				mu.Lock()
				as.NotRun++
				mu.Unlock()
				continue
			}
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				if r := state.reason(); r != "" {
					mu.Lock()
					as.NotRun++
					mu.Unlock()
					return
				}
				meta := AttemptMeta{
					RunID: cfg.RunID, Set: setOf(cfg, f), FixtureID: f.ID(), BundleID: f.BundleID, InputHash: f.InputHash, Arm: arm, Repeat: cfg.Repeat,
					Provider: info.provider, Endpoint: info.endpoint, APIMode: info.apiMode, ModelRequested: info.model,
					Versions: cfg.versions(info.promptVersion), Rate: rate,
				}
				meta.Stamp = ModelVersionStamp(info.provider, info.apiMode, info.model, meta.Versions)
				crec, err := classify(ctx, cfg, arm, f, bundle, meta, weights, sender, incumbentClient)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					as.NotRun++
					return
				}
				if aerr := ledger.Append(crec); aerr != nil && firstErr == nil {
					firstErr = aerr
				}
				switch {
				case crec.Gate != "":
					as.Gate++
				default:
					as.Sent++
				}
				as.States[crec.State]++
				if crec.StopArm != "" {
					state.stop(crec.StopArm)
				}
				if crec.Gate == "" {
					state.note(crec.State == StateRequestFailed || crec.State == "llm_task_failed", cfg.ConsecutiveFailureStop)
				}
			}()
		}
		wg.Wait()
		as.Stopped = state.reason()
		summary.Spend[info.provider] = budget.Spent(info.provider)
		if firstErr != nil {
			return summary, firstErr
		}
		if err := ctx.Err(); err != nil {
			return summary, err
		}
	}
	return summary, nil
}

func setOf(cfg RunConfig, f FixtureRecord) string {
	if f.Set != "" {
		return f.Set
	}
	return cfg.Set
}

// classify runs one fixture on one arm and builds its ledger record. A gate
// failure sends nothing.
func classify(ctx context.Context, cfg RunConfig, arm string, f FixtureRecord, bundle units.TextBundle, meta AttemptMeta,
	weights []float64, sender Sender, incumbentClient *http.Client) (ClassificationRecord, error) {

	rec := ClassificationRecord{
		Kind: KindClassification, RunID: cfg.RunID, Set: meta.Set, FixtureID: f.ID(), BundleID: f.BundleID, InputHash: f.InputHash,
		Arm: arm, Repeat: cfg.Repeat, Provider: meta.Provider, APIMode: meta.APIMode, Versions: meta.Versions,
		ModelRequested: meta.ModelRequested, StartedAt: cfg.now(),
	}
	if gate := GateStatus(bundle); gate != "" {
		rec.Gate, rec.State, rec.Status = gate, gate, gate
		rec.ErrorCodes = []string{gate}
		rec.FinishedAt = cfg.now()
		return rec, nil
	}
	col := &Collector{Meta: meta}
	switch arm {
	case ArmJev, ArmDecisions:
		backend := cfg.Jev
		if arm == ArmDecisions {
			backend = cfg.Decisions
		}
		rec.AcceptedModels = backend.AcceptedModels
		c, err := DecisionCategorize(ctx, bundle, DecisionDeps{Rubric: cfg.Rubric, Weights: weights, Backend: backend, Sender: sender, Collector: col})
		if err != nil {
			return rec, err
		}
		fillFromOutcome(&rec, c.Outcome)
		rec.State, rec.Interpretation = c.State, c.Interp
		if c.Terminal != nil && c.Terminal.StopArm != "" {
			rec.StopArm = c.Terminal.StopArm
		}
	default:
		icfg := cfg.Incumbent
		icfg.Defs = arm == ArmIncumbentDefs
		icfg.Rubric = cfg.Rubric
		outcome, err := IncumbentCategorize(ctx, bundle, icfg, incumbentClient, col)
		switch {
		case err != nil && ctx.Err() != nil:
			return rec, ctx.Err()
		case err != nil:
			rec.State, rec.Status = categorize.StatusLLMTaskFailed, categorize.StatusLLMTaskFailed
			rec.ErrorCodes = []string{categorize.StatusLLMTaskFailed, categorize.FailureClass(err)}
			if categorize.IsDeterministicFailure(err) {
				rec.StopArm = "deterministic_" + categorize.FailureClass(err)
			}
		default:
			fillFromOutcome(&rec, outcome)
			rec.State = outcome.Status
		}
	}
	usage, cost, latency, attempts, returned := col.Totals()
	rec.AttemptIDs = col.AttemptIDs()
	rec.LLMCalls = attempts
	rec.InputTokens, rec.OutputTokens = int(usage.InputTokens), int(usage.OutputTokens)
	rec.BilledCostUSD, rec.LatencyMs, rec.ModelReturned = cost, latency, returned
	if col.BudgetRefused() && rec.StopArm == "" {
		rec.StopArm = "budget"
	}
	rec.FinishedAt = cfg.now()
	return rec, nil
}

func fillFromOutcome(rec *ClassificationRecord, o categorize.CategorizationOutcome) {
	rec.Status = o.Status
	rec.ErrorCodes = o.Errors
	rec.Warnings = o.Warnings
	rec.Subcategories = o.Subcategories
	rec.Uncertainty = o.Uncertainty
	for _, q := range o.EvidenceQuotes {
		rec.Quotes = append(rec.Quotes, QuoteRecord{Quote: q.Quote, SourceType: q.SourceType, SourceID: q.SourceID})
	}
}

// dryTransport answers every request with a minimal 200 so the unchanged
// incumbent provider reveals its exact first request body, which is captured.
type dryTransport struct {
	mu     sync.Mutex
	bodies map[string][]byte // first body for each bundle
}

type dryKey struct{}

func (d *dryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	req.Body.Close()
	if id, _ := req.Context().Value(dryKey{}).(string); id != "" {
		d.mu.Lock()
		if _, ok := d.bodies[id]; !ok {
			d.bodies[id] = body
		}
		d.mu.Unlock()
	}
	resp := `{"status":"completed","output_text":"{}","usage":{"input_tokens":0,"output_tokens":0}}`
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(resp)), Request: req}, nil
}

func runDry(ctx context.Context, cfg RunConfig, fixtures []FixtureRecord, bundles []units.TextBundle) (RunSummary, error) {
	summary := RunSummary{RunID: cfg.RunID, Arms: map[string]*ArmSummary{}, Spend: map[string]float64{}, DryRun: &DryRunReport{Arms: map[string]*DryRunArm{}}}
	root := filepath.Join(cfg.OutDir, "dryrun", cfg.RunID)
	for _, arm := range cfg.Arms {
		info := cfg.armInfo(arm)
		dir := filepath.Join(root, arm)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return summary, err
		}
		as := &ArmSummary{States: map[string]int{}}
		summary.Arms[arm] = as
		dr := &DryRunArm{Dir: dir}
		summary.DryRun.Arms[arm] = dr
		var tokens []int
		dry := &dryTransport{bodies: map[string][]byte{}}
		for i, f := range fixtures {
			as.Planned++
			if gate := GateStatus(bundles[i]); gate != "" {
				as.Gate++
				summary.DryRun.GateSkips++
				continue
			}
			var body []byte
			switch arm {
			case ArmJev, ArmDecisions:
				backend := cfg.Jev
				if arm == ArmDecisions {
					backend = cfg.Decisions
				}
				built, err := backend.BuildRequest(cfg.Rubric, bundles[i])
				if err != nil {
					return summary, fmt.Errorf("dry run: %s %s: %w", arm, f.ID(), err)
				}
				body = built.Body
			default:
				icfg := cfg.Incumbent
				icfg.Defs = arm == ArmIncumbentDefs
				icfg.Rubric = cfg.Rubric
				client := &http.Client{Transport: dry}
				dctx := context.WithValue(ctx, dryKey{}, f.BundleID)
				_, _ = IncumbentCategorize(dctx, bundles[i], icfg, client, nil)
				body = dry.bodies[f.BundleID]
				if body == nil {
					return summary, fmt.Errorf("dry run: %s %s: no request captured", arm, f.ID())
				}
			}
			if err := os.WriteFile(filepath.Join(dir, f.BundleID+".request.json"), body, 0o600); err != nil {
				return summary, err
			}
			tokens = append(tokens, utf8.RuneCount(body)/4)
			as.Rendered++
		}
		dr.Requests = len(tokens)
		if len(tokens) > 0 {
			sorted := append([]int(nil), tokens...)
			sort.Ints(sorted)
			dr.EstInputTokensMin, dr.EstInputTokensMax = sorted[0], sorted[len(sorted)-1]
			dr.EstInputTokensP50 = sorted[(len(sorted)-1)/2]
			sum := 0
			for _, t := range tokens {
				sum += t
			}
			dr.EstInputTokens = float64(sum) / float64(len(tokens))
			if rate, err := cfg.Rates.Lookup(info.provider, info.apiMode, info.model); err == nil {
				dr.EstCostUSD = rate.Estimate(sum, 0)
			}
		}
		fmt.Fprintf(cfg.Out, "dry-run arm=%s requests=%d est_input_tokens min=%d p50=%d mean=%.0f max=%d est_input_cost_usd=%.6f dir=%s\n",
			arm, dr.Requests, dr.EstInputTokensMin, dr.EstInputTokensP50, dr.EstInputTokens, dr.EstInputTokensMax, dr.EstCostUSD, dir)
	}
	data, _ := json.MarshalIndent(summary, "", "  ")
	_ = os.WriteFile(filepath.Join(root, "summary.json"), data, 0o600)
	return summary, nil
}
