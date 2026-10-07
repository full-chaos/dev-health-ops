package investment

// Helpers of the shadow-phase tests. No test here or in the files that use
// these helpers calls the real TypeSafe API: every request goes to a local
// test endpoint (httptest) that answers from the request it received.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// shadowTestKeyValue is the bearer value of the test client. Plain words on
// purpose: it must not look like a credential to a secret scanner.
const shadowTestKeyValue = "plain words used as a test value"

// shadowSourceSentinel is a word that is in the text of every test bundle and
// nowhere else: a log line or a column that holds it holds source text.
const shadowSourceSentinel = "quokkasentinel"

// jevReply describes what the test endpoint answers to one request.
type jevReply struct {
	status  int
	headers map[string]string
	// body, when set, is sent as it is. Otherwise an answer set is built from
	// the request with the fields below.
	body []byte
	// supported maps a support key ("quality.bugfix") to its level; every other
	// key gets level 0.
	supported    map[string]int
	evidenceNone bool
	model        string
	noUsage      bool
	inputTokens  int
	extraAnswers map[string]string
	// wait blocks the reply until the request context ends or wait is closed.
	wait <-chan struct{}
}

func okReply() jevReply {
	return jevReply{status: http.StatusOK, supported: map[string]int{"quality.bugfix": 3, "quality.reliability": 1}, inputTokens: 2383}
}

// fakeJev is the local test endpoint of POST /v1/systemone.
type fakeJev struct {
	server *httptest.Server
	mu     sync.Mutex
	// reply decides the answer of request number n (from 1).
	reply    func(n int, body []byte) jevReply
	requests int
	bodies   [][]byte
	bearers  []string
	onFirst  func()
}

func newFakeJev(t *testing.T, reply func(n int, body []byte) jevReply) *fakeJev {
	t.Helper()
	fake := &fakeJev{reply: reply}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)
	return fake
}

func (fake *fakeJev) handle(w http.ResponseWriter, r *http.Request) {
	var buffer bytes.Buffer
	_, _ = buffer.ReadFrom(r.Body)
	body := buffer.Bytes()
	fake.mu.Lock()
	fake.requests++
	n := fake.requests
	fake.bodies = append(fake.bodies, body)
	fake.bearers = append(fake.bearers, r.Header.Get("Authorization"))
	onFirst := fake.onFirst
	fake.mu.Unlock()
	if n == 1 && onFirst != nil {
		onFirst()
	}
	if r.Method != http.MethodPost || r.URL.Path != "/v1/systemone" {
		http.Error(w, "unexpected route", http.StatusTeapot)
		return
	}
	reply := okReply()
	if fake.reply != nil {
		reply = fake.reply(n, body)
	}
	if reply.wait != nil {
		select {
		case <-reply.wait:
		case <-r.Context().Done():
			return
		}
	}
	for name, value := range reply.headers {
		w.Header().Set(name, value)
	}
	if reply.status == 0 {
		reply.status = http.StatusOK
	}
	if reply.body == nil && reply.status == http.StatusOK {
		reply.body = jevAnswerBody(body, reply)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(reply.status)
	_, _ = w.Write(reply.body)
}

func (fake *fakeJev) count() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.requests
}

// jevAnswerBody builds a /v1/systemone answer set for the questions of the
// request: one score answer for each score question and one choice answer for
// the evidence question.
func jevAnswerBody(request []byte, reply jevReply) []byte {
	var parsed struct {
		Model     string `json:"model"`
		Questions map[string]struct {
			Type     string          `json:"type"`
			Criteria json.RawMessage `json:"criteria"`
		} `json:"questions"`
	}
	if err := json.Unmarshal(request, &parsed); err != nil {
		panic(fmt.Sprintf("the test endpoint got a request that is not JSON: %v", err))
	}
	answers := map[string]any{}
	for id, question := range parsed.Questions {
		switch question.Type {
		case "score":
			var levels []string
			_ = json.Unmarshal(question.Criteria, &levels)
			level := 0
			if strings.HasPrefix(id, "support__") {
				key := strings.Replace(strings.TrimPrefix(id, "support__"), "__", ".", 1)
				level = reply.supported[key]
			} else {
				level = len(levels) - 1 // sufficiency: the top level
			}
			probabilities := map[string]float64{}
			for index := range levels {
				probabilities[fmt.Sprint(index)] = 0
			}
			probabilities[fmt.Sprint(level)] = 1
			answers[id] = map[string]any{"type": "score", "score": float64(level), "confidence": 0.95, "probabilities": probabilities}
		case "choice":
			var options map[string]any
			_ = json.Unmarshal(question.Criteria, &options)
			names := make([]string, 0, len(options))
			for name := range options {
				names = append(names, name)
			}
			sort.Strings(names)
			choice := "none"
			if !reply.evidenceNone {
				for _, name := range names {
					if name != "none" {
						choice = name
						break
					}
				}
			}
			probabilities := map[string]float64{}
			for _, name := range names {
				probabilities[name] = 0
			}
			probabilities[choice] = 1
			answers[id] = map[string]any{"type": "choice", "choice": choice, "confidence": 0.9, "probabilities": probabilities}
		}
	}
	for id, answerType := range reply.extraAnswers {
		answers[id] = map[string]any{"type": answerType}
	}
	model := reply.model
	if model == "" {
		model = parsed.Model
	}
	out := map[string]any{"model": model, "answers": answers}
	if !reply.noUsage {
		out["usage"] = map[string]any{"input_tokens": reply.inputTokens, "output_tokens": 368}
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		panic(err)
	}
	return encoded
}

// newShadowTestClient builds the real TypeSafe client against the local test
// endpoint.
func newShadowTestClient(t *testing.T, fake *fakeJev, logger *slog.Logger) *categorize.TypeSafeClient {
	t.Helper()
	client, err := categorize.NewTypeSafeClient(categorize.TypeSafeClientConfig{
		APIKey: secrets.NewHidden(shadowTestKeyValue), BaseURL: fake.server.URL,
		Logger: logger, Timeout: 10 * time.Second, UnsafeAllowAnyBaseURLForTest: true,
	})
	if err != nil {
		t.Fatalf("build the test client: %v", err)
	}
	return client
}

func shadowTestSettings() ShadowSettings {
	return ShadowSettings{
		Provider: "typesafe", AllOrgs: true, SamplePercent: 100, Concurrency: 2,
		Budget: 30 * time.Second, MaxNanoUSD: defaultShadowMaxNanoUSD,
	}
}

// newTestShadowPhase builds the real phase (real client, real completer, the
// embedded rubric) against the local test endpoint.
func newTestShadowPhase(t *testing.T, fake *fakeJev, settings ShadowSettings, logger *slog.Logger) *ShadowPhase {
	t.Helper()
	phase, err := NewShadowPhase(settings, newShadowTestClient(t, fake, logger), logger)
	if err != nil {
		t.Fatalf("build the shadow phase: %v", err)
	}
	t.Cleanup(func() { _ = phase.Close() })
	return phase
}

// shadowTestBundle builds a bundle through units.BuildTextBundle (the real
// producer). chars is roughly the size of the issue text.
func shadowTestBundle(t *testing.T, workUnitID string, sentences int) units.TextBundle {
	t.Helper()
	description := strings.Repeat("The export worker times out for large workspaces "+shadowSourceSentinel+" and needs a streamed writer. ", sentences)
	bundle, err := units.BuildTextBundle(units.BuildTextBundleInput{
		WorkUnitID: workUnitID,
		IssueIDs:   []string{"ENG-" + workUnitID},
		WorkItemMap: map[string]map[string]any{
			"ENG-" + workUnitID: {"title": "Export job times out " + workUnitID, "description": description, "type": "Bug"},
		},
	})
	if err != nil {
		t.Fatalf("build bundle %s: %v", workUnitID, err)
	}
	return bundle
}

func shadowTestEntries(t *testing.T, workUnitIDs ...string) []preprocessed {
	t.Helper()
	entries := make([]preprocessed, 0, len(workUnitIDs))
	for index, id := range workUnitIDs {
		entries = append(entries, shadowEntry(index, id, shadowTestBundle(t, id, 6)))
	}
	return entries
}

func shadowEntry(index int, workUnitID string, bundle units.TextBundle) preprocessed {
	return preprocessed{index: index, result: MaterializeComponentResult{
		Bundle: bundle, Investment: chwrite.InvestmentRecord{WorkUnitID: workUnitID},
	}}
}

func shadowTestConfig() Config {
	return Config{OrgID: "org-shadow-test", RunID: "run-shadow-test", ComputedAt: time.Now().UTC().Truncate(time.Millisecond)}
}

// memoryShadowStore is the store of the unit tests: it keeps what the phase
// wrote and can fail each step.
type memoryShadowStore struct {
	mu          sync.Mutex
	existing    map[chquery.InvestmentKey]struct{}
	readErr     error
	writeErr    error
	flushResult *chwrite.AttemptFlushResult
	readConfigs []string
	records     []chwrite.ShadowRecord
	attempts    []chwrite.AttemptRecord
	writes      int
	flushes     int
	// cancelledCalls counts store calls made with a context that was already
	// cancelled.
	cancelledCalls int
	delegate       *chwrite.Writer
}

func (store *memoryShadowStore) FetchExistingShadowKeys(ctx context.Context, _ string, _ []chquery.InvestmentKey, shadowConfig string) (map[chquery.InvestmentKey]struct{}, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.readConfigs = append(store.readConfigs, shadowConfig)
	if store.readErr != nil {
		return nil, store.readErr
	}
	return store.existing, nil
}

func (store *memoryShadowStore) WriteShadowInvestments(ctx context.Context, _ string, records []chwrite.ShadowRecord) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.writes++
	if ctx.Err() != nil {
		store.cancelledCalls++
	}
	if store.writeErr != nil {
		return 0, store.writeErr
	}
	store.records = append(store.records, records...)
	return len(records), nil
}

func (store *memoryShadowStore) FlushAttempts(ctx context.Context, orgID string, buffer *chwrite.AttemptBuffer) chwrite.AttemptFlushResult {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.flushes++
	if ctx.Err() != nil {
		store.cancelledCalls++
	}
	if store.flushResult != nil {
		return *store.flushResult
	}
	// The real FlushAttempts over a capturing connection, so the buffer is
	// drained by production code and the rows are the rows it would insert.
	capture := &attemptCaptureConn{}
	writer, err := chwrite.NewWriter(capture)
	if err != nil {
		panic(err)
	}
	result := writer.FlushAttempts(ctx, orgID, buffer)
	store.attempts = append(store.attempts, capture.rows...)
	return result
}

func (store *memoryShadowStore) recordByUnit(t *testing.T, workUnitID string) chwrite.ShadowRecord {
	t.Helper()
	for _, record := range store.records {
		if record.WorkUnitID == workUnitID {
			return record
		}
	}
	t.Fatalf("no shadow row for work unit %s (rows: %d)", workUnitID, len(store.records))
	return chwrite.ShadowRecord{}
}

// attemptCaptureConn is a ClickHouse connection that keeps the rows of the
// llm_categorization_attempts batch it is given.
type attemptCaptureConn struct {
	rows []chwrite.AttemptRecord
}

func (conn *attemptCaptureConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	if !strings.Contains(query, "INSERT INTO llm_categorization_attempts") {
		return nil, fmt.Errorf("unexpected batch: %s", query)
	}
	return &attemptCaptureBatch{conn: conn}, nil
}

type attemptCaptureBatch struct {
	recordingBatch
	conn *attemptCaptureConn
}

// Append reads the values in the column order of chwrite.WriteAttempts. A
// change of that order fails the type assertions here.
func (batch *attemptCaptureBatch) Append(values ...any) error {
	if len(values) != 24 {
		return fmt.Errorf("attempt row has %d values, want 24", len(values))
	}
	batch.conn.rows = append(batch.conn.rows, chwrite.AttemptRecord{
		RunID: values[1].(string), WorkUnitID: values[2].(string), Role: values[3].(string),
		Config: values[4].(string), RubricSHA256: values[5].(string), Provider: values[6].(string),
		APIMode: values[7].(string), ModelRequested: values[8].(string), ModelReturned: values[9].(string),
		Attempt: values[10].(uint8), Kind: values[11].(string), HTTPStatus: values[12].(uint16),
		ErrorClass: values[13].(string), State: values[14].(string), RequestID: values[15].(string),
		InputTokens: values[16].(uint32), OutputTokens: values[17].(uint32), CachedInputTokens: values[18].(uint32),
		BilledCostUSD: values[19].(float64), RatesVersion: values[20].(string),
		LatencyMS: values[21].(uint32), RetryWaitMS: values[22].(uint32), ComputedAt: values[23].(time.Time),
	})
	return nil
}
