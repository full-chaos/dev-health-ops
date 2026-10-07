package decisioneval

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// AttemptMeta is what the transport writes into every attempt record of one
// classification. It rides in the request context.
type AttemptMeta struct {
	RunID     string
	Set       string
	FixtureID string
	BundleID  string
	InputHash string
	Arm       string
	Repeat    int

	Provider       string
	Endpoint       string
	APIMode        string
	ModelRequested string
	Versions       Versions
	Stamp          string

	QuestionCount      int
	SpanCount          int
	SpansDropped       int
	DelimiterCollision bool

	Rate PriceRate
}

// Collector gathers the attempts of one classification.
type Collector struct {
	Meta AttemptMeta

	mu         sync.Mutex
	n          int
	ids        []string
	prevFailed bool
	usage      UsageReport
	cost       float64
	latencyMs  int64
	lastStatus int
	returned   string
	refused    bool
}

type collectorKey struct{}

// WithCollector attaches a collector to ctx.
func WithCollector(ctx context.Context, c *Collector) context.Context {
	return context.WithValue(ctx, collectorKey{}, c)
}

func collectorFrom(ctx context.Context) *Collector {
	c, _ := ctx.Value(collectorKey{}).(*Collector)
	return c
}

// AttemptIDs returns the attempt ids written so far.
func (c *Collector) AttemptIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.ids...)
}

// Totals returns summed usage, cost and latency of the attempts so far.
func (c *Collector) Totals() (usage UsageReport, costUSD float64, latencyMs int64, attempts int, returnedModel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.usage, c.cost, c.latencyMs, c.n, c.returned
}

// BudgetRefused reports whether the spend cap refused an attempt.
func (c *Collector) BudgetRefused() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.refused
}

// LastHTTPStatus is the status of the last attempt (0 for a transport error).
func (c *Collector) LastHTTPStatus() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastStatus
}

// Recorder writes the ledger and raw files and enforces the spend cap. It
// wraps every provider HTTP attempt, for the decision adapters and for the
// unchanged incumbent provider alike.
type Recorder struct {
	Ledger *Ledger
	Budget *Budget
	Now    func() time.Time
}

func (r *Recorder) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

// RecordingTransport is an http.RoundTripper that records each attempt.
type RecordingTransport struct {
	Base http.RoundTripper
	Rec  *Recorder
}

// NewRecordingClient returns an http.Client with the recording transport and
// the per-attempt timeout of the design (30 s unless overridden).
func NewRecordingClient(base http.RoundTripper, rec *Recorder, timeout time.Duration) *http.Client {
	if base == nil {
		base = &http.Transport{Proxy: nil}
	}
	return &http.Client{
		Transport: &RecordingTransport{Base: base, Rec: rec},
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

var secretHeaders = map[string]bool{"authorization": true, "proxy-authorization": true, "cookie": true, "set-cookie": true}

func headerRequestID(h http.Header) string {
	for _, name := range []string{"x-typesafe-request-id", "x-request-id", "openai-request-id", "request-id"} {
		if v := h.Get(name); v != "" {
			return v
		}
	}
	return ""
}

func formatHeaders(first string, h http.Header) []byte {
	var b bytes.Buffer
	b.WriteString(first + "\n")
	names := make([]string, 0, len(h))
	for n := range h {
		if !secretHeaders[strings.ToLower(n)] {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for _, n := range names {
		for _, v := range h[n] {
			b.WriteString(n + ": " + v + "\n")
		}
	}
	return b.Bytes()
}

func writeRaw(dir, rel string, data []byte) error {
	path := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func maxOutputTokensOf(body []byte) int {
	var probe struct {
		MaxOutputTokens int `json:"max_output_tokens"`
	}
	_ = json.Unmarshal(body, &probe)
	return probe.MaxOutputTokens
}

// RoundTrip records one attempt around the base transport.
func (t *RecordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	col := collectorFrom(req.Context())
	if col == nil {
		// A request outside a classification would escape the ledger and the
		// cap. Fail loudly.
		return nil, errors.New("decisioneval: request without an attempt collector refused")
	}
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("decisioneval: read request body: %w", err)
		}
	}
	rec := t.Rec
	meta := col.Meta

	col.mu.Lock()
	col.n++
	n := col.n
	retryOf := 0
	if col.prevFailed {
		retryOf = n - 1
	}
	col.mu.Unlock()

	sum := sha256.Sum256(body)
	attemptID := fmt.Sprintf("%s/%s/%s/r%d/a%d", meta.RunID, meta.Arm, meta.BundleID, meta.Repeat, n)
	base := fmt.Sprintf("runs/%s/raw/%s/%s/r%d-a%d", meta.RunID, meta.Arm, meta.BundleID, meta.Repeat, n)
	est := meta.Rate.Estimate(utf8.RuneCount(body)/4, maxOutputTokensOf(body))

	a := AttemptRecord{
		Kind: KindAttempt, Phase: PhaseReserved, AttemptID: attemptID, RunID: meta.RunID, Set: meta.Set,
		FixtureID: meta.FixtureID, BundleID: meta.BundleID, InputHash: meta.InputHash, Arm: meta.Arm, Repeat: meta.Repeat,
		Provider: meta.Provider, Endpoint: meta.Endpoint, APIMode: meta.APIMode, ModelRequested: meta.ModelRequested,
		Versions: meta.Versions, ModelVersionStamp: meta.Stamp,
		RequestSHA256: hex.EncodeToString(sum[:]), RequestBytes: len(body), QuestionCount: meta.QuestionCount,
		SpanCount: meta.SpanCount, SpansDropped: meta.SpansDropped, DelimiterCollision: meta.DelimiterCollision,
		ReservedCostUSD: est, CostBasis: "reserved_estimate", Attempt: n, RetryOf: retryOf, StartedAt: rec.now(),
	}

	fail := func(state, class string) {
		col.mu.Lock()
		col.ids = append(col.ids, attemptID)
		col.prevFailed = true
		col.lastStatus = 0
		col.refused = col.refused || state == AttemptBudgetRefused
		col.mu.Unlock()
		a.Phase, a.AttemptState, a.ErrorClass, a.CostBasis, a.FinishedAt = PhaseCompleted, state, class, "no_charge_no_response", rec.now()
		_ = rec.Ledger.Append(a)
	}

	resID, err := rec.Budget.Reserve(meta.Provider, est)
	if err != nil {
		fail(AttemptBudgetRefused, "budget_refused")
		return nil, err
	}
	a.RawRequestPath = base + ".request.json"
	if err := writeRaw(rec.Ledger.Dir, a.RawRequestPath, body); err != nil {
		rec.Budget.Settle(resID, 0)
		fail(AttemptTransportError, "raw_write_failed")
		return nil, fmt.Errorf("decisioneval: write raw request: %w", err)
	}
	// The reservation is on disk before the request leaves.
	if err := rec.Ledger.Append(a); err != nil {
		rec.Budget.Settle(resID, 0)
		return nil, err
	}
	col.mu.Lock()
	col.ids = append(col.ids, attemptID)
	col.mu.Unlock()

	start := time.Now()
	out := req.Clone(req.Context())
	out.Body = io.NopCloser(bytes.NewReader(body))
	out.ContentLength = int64(len(body))
	out.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	resp, err := t.Base.RoundTrip(out)
	ttfb := time.Since(start)
	a.LatencyMsTTFB = ttfb.Milliseconds()
	a.FinishedAt = rec.now()
	if err != nil {
		a.LatencyMsTotal = time.Since(start).Milliseconds()
		a.Phase, a.AttemptState, a.ErrorClass, a.CostBasis = PhaseCompleted, AttemptTransportError, transportClass(err), "no_charge_no_response"
		a.RawHeadersPath = base + ".headers.txt"
		// The error text is kept (no header or token can be in it).
		_ = writeRaw(rec.Ledger.Dir, a.RawHeadersPath, []byte("ERROR "+err.Error()+"\n"))
		col.mu.Lock()
		col.prevFailed = true
		col.lastStatus = 0
		col.latencyMs += a.LatencyMsTotal
		col.mu.Unlock()
		rec.Budget.Settle(resID, 0)
		_ = rec.Ledger.Append(a)
		return nil, err
	}
	respBody, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	a.LatencyMsTotal = time.Since(start).Milliseconds()
	a.FinishedAt = rec.now()
	a.HTTPStatus = resp.StatusCode
	a.RequestID = headerRequestID(resp.Header)
	if v := resp.Header.Get("x-envoy-upstream-service-time"); v != "" {
		a.UpstreamMs, _ = strconv.ParseInt(v, 10, 64)
	}
	a.RawResponsePath = base + ".response.json"
	a.RawHeadersPath = base + ".headers.txt"
	_ = writeRaw(rec.Ledger.Dir, a.RawResponsePath, respBody)
	_ = writeRaw(rec.Ledger.Dir, a.RawHeadersPath, formatHeaders("STATUS "+strconv.Itoa(resp.StatusCode), resp.Header))

	a.Phase = PhaseCompleted
	billed := 0.0
	switch {
	case readErr != nil:
		a.AttemptState, a.ErrorClass, a.CostBasis = AttemptTransportError, "body_read_failed", "no_charge_no_response"
	case resp.StatusCode != http.StatusOK:
		a.AttemptState, a.ErrorClass, a.CostBasis = AttemptHTTPError, httpClass(resp.StatusCode), "no_charge_non_200"
	default:
		a.AttemptState = AttemptHTTPOK
		model, usage := usageFromBody(respBody)
		a.ModelReturned, a.Usage = model, usage
		if usage.Reported {
			billed, a.CostNoCacheDiscountUSD = meta.Rate.Cost(usage)
			a.CostBasis = "usage_x_published_rate"
		} else {
			// A 200 without usage is billed at the reservation: the cap must
			// hold even when the provider does not report.
			billed, a.CostNoCacheDiscountUSD = est, est
			a.CostBasis = "estimated_no_usage"
		}
	}
	a.BilledCostUSD = billed
	col.mu.Lock()
	col.prevFailed = a.AttemptState != AttemptHTTPOK
	col.lastStatus = resp.StatusCode
	col.usage.InputTokens += a.Usage.InputTokens
	col.usage.OutputTokens += a.Usage.OutputTokens
	col.usage.CachedInputTokens += a.Usage.CachedInputTokens
	col.usage.Reported = col.usage.Reported || a.Usage.Reported
	col.cost += billed
	col.latencyMs += a.LatencyMsTotal
	if a.ModelReturned != "" {
		col.returned = a.ModelReturned
	}
	col.mu.Unlock()
	rec.Budget.Settle(resID, billed)
	if err := rec.Ledger.Append(a); err != nil {
		return nil, err
	}
	if readErr != nil {
		return nil, fmt.Errorf("decisioneval: read response body: %w", readErr)
	}
	resp.Body = io.NopCloser(bytes.NewReader(respBody))
	return resp, nil
}

func httpClass(status int) string {
	switch {
	case status >= 500 && status != 529:
		return "http_5xx"
	default:
		return "http_" + strconv.Itoa(status)
	}
}

func transportClass(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	var ne interface{ Timeout() bool }
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	return "transport"
}
