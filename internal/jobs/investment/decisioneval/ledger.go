package decisioneval

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Ledger record kinds.
const (
	KindRun            = "run"
	KindAttempt        = "attempt"
	KindClassification = "classification"
)

// Attempt phases: a "reserved" line is written before the send, a "completed"
// line after the response. The last line of an attempt id wins.
const (
	PhaseReserved  = "reserved"
	PhaseCompleted = "completed"
)

// Attempt-level states (not classification states).
const (
	AttemptHTTPOK         = "http_ok"
	AttemptHTTPError      = "http_error"
	AttemptTransportError = "transport_error"
	AttemptBudgetRefused  = "budget_refused"
)

// Versions groups the version stamps written into every record.
type Versions struct {
	Rubric   string `json:"rubric_version"`
	Map      string `json:"map_version"`
	Adapter  string `json:"adapter_version"`
	Span     string `json:"span_version"`
	Eval     string `json:"eval_version"`
	Rates    string `json:"rates_version"`
	Prompt   string `json:"prompt_version,omitempty"`
	Taxonomy string `json:"taxonomy_version,omitempty"`
	// RubricSHA256 is the digest of the rubric file in use.
	RubricSHA256 string `json:"rubric_sha256,omitempty"`
}

// EvalVersion names the evaluation definition.
const EvalVersion = "decision-eval-v1"

// AttemptRecord is one provider HTTP attempt.
type AttemptRecord struct {
	Kind      string `json:"kind"`
	Phase     string `json:"phase"`
	AttemptID string `json:"attempt_id"`
	RunID     string `json:"run_id"`
	Set       string `json:"set"`
	FixtureID string `json:"fixture_id"`
	BundleID  string `json:"bundle_id"`
	InputHash string `json:"input_hash,omitempty"`
	Arm       string `json:"arm"`
	Repeat    int    `json:"repeat"`

	Provider       string `json:"provider"`
	Endpoint       string `json:"endpoint"`
	APIMode        string `json:"api_mode"`
	ModelRequested string `json:"model_requested"`
	ModelReturned  string `json:"model_returned,omitempty"`
	Versions
	ModelVersionStamp string `json:"model_version_stamp,omitempty"`

	RequestSHA256 string `json:"request_sha256"`
	RequestBytes  int    `json:"request_bytes"`
	QuestionCount int    `json:"question_count"`
	SpanCount     int    `json:"span_count"`
	SpansDropped  int    `json:"spans_dropped"`
	// DelimiterCollision: the source text holds a Decisions delimiter word.
	DelimiterCollision bool `json:"delimiter_collision,omitempty"`

	RequestID  string `json:"request_id,omitempty"`
	HTTPStatus int    `json:"http_status,omitempty"`
	ErrorClass string `json:"error_class,omitempty"`
	// AttemptState is http_ok, http_error, transport_error or budget_refused.
	AttemptState string `json:"attempt_state,omitempty"`

	// Usage is the provider's usage as reported (usage.reported false when the
	// response had none).
	Usage UsageReport `json:"usage"`
	// BilledCostUSD is reported usage x the published rate (cost_basis says
	// how). No invoice figure is available through either API.
	BilledCostUSD float64 `json:"billed_cost_usd"`
	// CostNoCacheDiscountUSD prices every input token at the full input rate.
	CostNoCacheDiscountUSD float64 `json:"cost_no_cache_discount_usd"`
	CostBasis              string  `json:"cost_basis"`
	ReservedCostUSD        float64 `json:"reserved_cost_usd"`

	LatencyMsTotal int64 `json:"latency_ms_total"`
	LatencyMsTTFB  int64 `json:"latency_ms_ttfb"`
	UpstreamMs     int64 `json:"upstream_ms,omitempty"`

	Attempt int `json:"attempt"`
	// RetryOf is the attempt number this attempt retries (0 for none).
	RetryOf int `json:"retry_of"`
	// FallbackUsed is always false on an attempt: a fallback is a separate
	// incumbent classification that the scorer adds (design.md 4.3).
	FallbackUsed bool `json:"fallback_used"`

	RawRequestPath  string `json:"raw_request_path,omitempty"`
	RawResponsePath string `json:"raw_response_path,omitempty"`
	RawHeadersPath  string `json:"raw_headers_path,omitempty"`

	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

// ClassificationRecord is the summary of one classification (one fixture, one
// arm): the final state plus the typed answers after parsing.
type ClassificationRecord struct {
	Kind      string `json:"kind"`
	RunID     string `json:"run_id"`
	Set       string `json:"set"`
	FixtureID string `json:"fixture_id"`
	BundleID  string `json:"bundle_id"`
	InputHash string `json:"input_hash,omitempty"`
	Arm       string `json:"arm"`
	Repeat    int    `json:"repeat"`
	Provider  string `json:"provider,omitempty"`
	APIMode   string `json:"api_mode,omitempty"`
	Versions
	ModelRequested string `json:"model_requested,omitempty"`
	ModelReturned  string `json:"model_returned,omitempty"`
	// AcceptedModels are the extra returned model ids the run accepted.
	AcceptedModels []string `json:"accepted_models,omitempty"`

	// Gate is set (insufficient_evidence / no_text_sources) when the production
	// pre-LLM gate stopped the bundle: no request was sent.
	Gate string `json:"gate,omitempty"`

	// State is the candidate state (design.md 4.1); for a generative arm it is
	// the production status.
	State        string   `json:"state"`
	Status       string   `json:"status"`
	ErrorCodes   []string `json:"error_codes,omitempty"`
	Warnings     []string `json:"warnings,omitempty"`
	AttemptIDs   []string `json:"attempt_ids,omitempty"`
	LLMCalls     int      `json:"llm_calls"`
	InputTokens  int      `json:"input_tokens"`
	OutputTokens int      `json:"output_tokens"`
	// BilledCostUSD and LatencyMs sum every attempt of the classification.
	BilledCostUSD float64 `json:"billed_cost_usd"`
	LatencyMs     int64   `json:"latency_ms"`

	Interpretation *Interpretation    `json:"interpretation,omitempty"`
	Subcategories  map[string]float64 `json:"subcategories,omitempty"`
	Quotes         []QuoteRecord      `json:"quotes,omitempty"`
	Uncertainty    string             `json:"uncertainty,omitempty"`
	// StopArm names why the arm must stop (auth, budget).
	StopArm string `json:"stop_arm,omitempty"`

	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}

// QuoteRecord is one persisted-shape evidence quote.
type QuoteRecord struct {
	Quote      string `json:"quote"`
	SourceType string `json:"source_type"`
	SourceID   string `json:"source_id"`
}

// RunRecord opens a run in the ledger. It holds no secret.
type RunRecord struct {
	Kind     string            `json:"kind"`
	RunID    string            `json:"run_id"`
	Started  time.Time         `json:"started_at"`
	Set      string            `json:"set"`
	Arms     []string          `json:"arms"`
	Versions Versions          `json:"versions"`
	Config   map[string]string `json:"config"`
}

// Ledger appends JSONL records. It is safe for concurrent use.
type Ledger struct {
	mu   sync.Mutex
	f    *os.File
	Path string
	Dir  string
}

// LedgerFile is the file name inside the output directory.
const LedgerFile = "ledger.jsonl"

// OpenLedger opens (creating) dir/ledger.jsonl for append. The directory must
// be outside any git repository: ledger and raw bodies hold provider data.
func OpenLedger(dir string) (*Ledger, error) {
	if err := RefuseInsideRepo(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("ledger: %w", err)
	}
	path := filepath.Join(dir, LedgerFile)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("ledger: %w", err)
	}
	return &Ledger{f: f, Path: path, Dir: dir}, nil
}

// RefuseInsideRepo fails when dir is inside a git working tree.
func RefuseInsideRepo(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("output dir: %w", err)
	}
	for p := abs; ; p = filepath.Dir(p) {
		if _, err := os.Stat(filepath.Join(p, ".git")); err == nil {
			return fmt.Errorf("output dir %s is inside the git repository at %s: ledger and raw provider bodies must stay outside the repo", abs, p)
		}
		if filepath.Dir(p) == p {
			return nil
		}
	}
}

// Append writes one record and syncs it: a reservation must be on disk before
// the request is sent.
func (l *Ledger) Append(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("ledger: marshal: %w", err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("ledger: write: %w", err)
	}
	return l.f.Sync()
}

// Close closes the file.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}

// LedgerData is a ledger read back.
type LedgerData struct {
	Dir  string
	Runs []RunRecord
	// Attempts holds the last line of every attempt id, in first-seen order.
	Attempts        []AttemptRecord
	Classifications []ClassificationRecord
}

// ReadLedger reads dir/ledger.jsonl. A malformed line is an error (a ledger
// that cannot be read in full cannot be trusted for the spend cap).
func ReadLedger(dir string) (*LedgerData, error) {
	f, err := os.Open(filepath.Join(dir, LedgerFile))
	if err != nil {
		return nil, fmt.Errorf("ledger: %w", err)
	}
	defer f.Close()
	data := &LedgerData{Dir: dir}
	index := map[string]int{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 256<<20)
	n := 0
	for sc.Scan() {
		n++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var head struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal([]byte(line), &head); err != nil {
			return nil, fmt.Errorf("ledger line %d: %w", n, err)
		}
		switch head.Kind {
		case KindRun:
			var r RunRecord
			if err := json.Unmarshal([]byte(line), &r); err != nil {
				return nil, fmt.Errorf("ledger line %d: %w", n, err)
			}
			data.Runs = append(data.Runs, r)
		case KindAttempt:
			var a AttemptRecord
			if err := json.Unmarshal([]byte(line), &a); err != nil {
				return nil, fmt.Errorf("ledger line %d: %w", n, err)
			}
			if i, ok := index[a.AttemptID]; ok {
				data.Attempts[i] = a
			} else {
				index[a.AttemptID] = len(data.Attempts)
				data.Attempts = append(data.Attempts, a)
			}
		case KindClassification:
			var c ClassificationRecord
			if err := json.Unmarshal([]byte(line), &c); err != nil {
				return nil, fmt.Errorf("ledger line %d: %w", n, err)
			}
			data.Classifications = append(data.Classifications, c)
		default:
			return nil, fmt.Errorf("ledger line %d: unknown kind %q", n, head.Kind)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("ledger: %w", err)
	}
	return data, nil
}

// AttemptsOf returns the attempts of one classification in attempt order.
func (d *LedgerData) AttemptsOf(c ClassificationRecord) []AttemptRecord {
	byID := map[string]AttemptRecord{}
	for _, a := range d.Attempts {
		byID[a.AttemptID] = a
	}
	var out []AttemptRecord
	for _, id := range c.AttemptIDs {
		if a, ok := byID[id]; ok {
			out = append(out, a)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Attempt < out[j].Attempt })
	return out
}

// ResumeKey identifies a classification for resume: fixture + arm + rubric
// version + adapter version + repeat index.
func ResumeKey(bundleID, arm, rubricVersion, adapterVersion string, repeat int) string {
	return fmt.Sprintf("%s|%s|%s|%s|%d", bundleID, arm, rubricVersion, adapterVersion, repeat)
}

// Key is the resume key of the record.
func (c ClassificationRecord) Key() string {
	return ResumeKey(c.BundleID, c.Arm, c.Rubric, c.Adapter, c.Repeat)
}

// Retriable says whether a classification is worth sending again on resume.
// A transport failure (request_failed, a generative arm's llm_task_failed) and
// an adapter defect are retried. Refused, missing and invalid answers are
// final: a second ask of the same question could hide them (design.md 4.2).
func (c ClassificationRecord) Retriable() bool {
	switch c.State {
	case StateRequestFailed, StateAdapterDefect, "llm_task_failed":
		return true
	}
	return false
}

// OrphanReservations returns attempts whose last line is still "reserved": the
// process stopped between the reservation and the response, so the request may
// have been billed.
func (d *LedgerData) OrphanReservations() []AttemptRecord {
	var out []AttemptRecord
	for _, a := range d.Attempts {
		if a.Phase == PhaseReserved {
			out = append(out, a)
		}
	}
	return out
}

// SpentByProvider sums, for each provider, the billed cost of completed
// attempts plus the reserved cost of orphan reservations.
func (d *LedgerData) SpentByProvider() map[string]float64 {
	spent := map[string]float64{}
	for _, a := range d.Attempts {
		if a.Phase == PhaseCompleted {
			spent[a.Provider] += a.BilledCostUSD
		} else {
			spent[a.Provider] += a.ReservedCostUSD
		}
	}
	return spent
}
