package investment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// fakeOpenAI serves the Responses API (synchronous calls) and the Batch API
// over the same answers: the answer to a prompt is MockProvider's, so a batch
// line and a synchronous call for the same unit carry the same text.
type fakeOpenAI struct {
	t *testing.T

	mu sync.Mutex
	// statuses are the batch statuses of successive polls; the last repeats.
	statuses []string
	polls    int
	// lineFor returns the output line of one uploaded request, or "" for none.
	lineFor func(customID, prompt string) string
	// syncText overrides the synchronous answer to a prompt, when set.
	syncText func(prompt string) string
	// failCreate answers the batch create with this status.
	failCreate int
	// failPoll answers every poll with this status.
	failPoll int
	// outputOnCancel is the output file a cancelled batch has.
	outputOnCancel string

	uploaded   []batchLine
	creates    int
	output     string
	cancelled  int
	syncCalls  int
	syncInputs []string
}

type batchLine struct {
	CustomID string `json:"custom_id"`
	Body     struct {
		Input string `json:"input"`
	} `json:"body"`
}

const (
	fakeInputTokens  = 100
	fakeOutputTokens = 50
)

func newFakeOpenAI(t *testing.T) (*fakeOpenAI, *httptest.Server) {
	t.Helper()
	fake := &fakeOpenAI{t: t, statuses: []string{"completed"}}
	fake.lineFor = func(customID, prompt string) string { return okLine(customID, mockAnswer(t, prompt)) }
	server := httptest.NewServer(http.HandlerFunc(fake.serve))
	t.Cleanup(server.Close)
	return fake, server
}

func mockAnswer(t *testing.T, prompt string) string {
	t.Helper()
	completion, err := categorize.MockProvider{}.Complete(context.Background(), categorize.CategorizationRequest(prompt))
	if err != nil {
		t.Fatal(err)
	}
	return completion.Text
}

func okLine(customID, text string) string {
	body, _ := json.Marshal(map[string]any{
		"output_text": text,
		"usage":       map[string]any{"input_tokens": fakeInputTokens, "output_tokens": fakeOutputTokens},
	})
	return fmt.Sprintf(`{"id":"line-%s","custom_id":%q,"response":{"status_code":200,"request_id":"req-%s","body":%s},"error":null}`,
		customID, customID, customID, body)
}

func (f *fakeOpenAI) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/responses":
		var body struct {
			Input string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.syncCalls++
		f.syncInputs = append(f.syncInputs, body.Input)
		text := mockAnswer(f.t, body.Input)
		if f.syncText != nil {
			text = f.syncText(body.Input)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"output_text": text,
			"usage":       map[string]any{"input_tokens": fakeInputTokens, "output_tokens": fakeOutputTokens},
		})
	case r.Method == http.MethodPost && r.URL.Path == "/files":
		_, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		reader := multipart.NewReader(r.Body, params["boundary"])
		for {
			part, err := reader.NextPart()
			if err != nil {
				break
			}
			if part.FormName() != "file" {
				continue
			}
			content, _ := io.ReadAll(part)
			var lines []string
			for _, raw := range bytes.Split(content, []byte("\n")) {
				var line batchLine
				if err := json.Unmarshal(raw, &line); err != nil {
					f.t.Errorf("uploaded line: %v", err)
					continue
				}
				f.uploaded = append(f.uploaded, line)
				if out := f.lineFor(line.CustomID, line.Body.Input); out != "" {
					lines = append(lines, out)
				}
			}
			f.output = strings.Join(lines, "\n") + "\n"
		}
		_, _ = w.Write([]byte(`{"id":"file-in"}`))
	case r.Method == http.MethodPost && r.URL.Path == "/batches":
		if f.failCreate != 0 {
			w.WriteHeader(f.failCreate)
			_, _ = w.Write([]byte(`{"error":{"message":"Incorrect API key provided","code":"invalid_api_key"}}`))
			return
		}
		f.creates++
		_, _ = w.Write([]byte(`{"id":"batch_1","status":"validating"}`))
	case r.Method == http.MethodGet && r.URL.Path == "/batches/batch_1":
		if f.failPoll != 0 {
			w.WriteHeader(f.failPoll)
			return
		}
		index := min(f.polls, len(f.statuses)-1)
		status := f.statuses[index]
		if f.cancelled > 0 {
			status = "cancelled"
		}
		f.polls++
		batch := map[string]any{"id": "batch_1", "status": status}
		switch {
		case status == "completed":
			batch["output_file_id"] = "file-out"
		case f.cancelled > 0 && f.outputOnCancel != "":
			batch["output_file_id"] = "file-cancel"
		}
		_ = json.NewEncoder(w).Encode(batch)
	case r.Method == http.MethodGet && r.URL.Path == "/files/file-out/content":
		_, _ = w.Write([]byte(f.output))
	case r.Method == http.MethodGet && r.URL.Path == "/files/file-cancel/content":
		_, _ = w.Write([]byte(f.outputOnCancel))
	case r.Method == http.MethodPost && r.URL.Path == "/batches/batch_1/cancel":
		f.cancelled++
		_, _ = w.Write([]byte(`{"id":"batch_1","status":"cancelling"}`))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func fakeProvider(baseURL string) *categorize.OpenAIProvider {
	return categorize.NewOpenAIProvider(categorize.OpenAIProviderConfig{
		APIKey: secrets.NewHidden("batch-mode-test-placeholder"), BaseURL: baseURL, Model: "gpt-5-nano",
	})
}

func batchTestConfig(mode string) Config {
	return Config{
		OrgID: "org-batch-test", RunID: "run-batch-test", ProviderName: "openai", Model: "gpt-5-nano",
		LLMConcurrency: 4, LLMBatchMode: mode, LLMBatchMinItems: 2,
		LLMBatchPollInterval: time.Millisecond, LLMBatchTimeout: 5 * time.Second,
	}
}

type categorizeResult struct {
	outcomes map[int]categorize.CategorizationOutcome
	stats    Stats
	err      error
}

func runCategorize(t *testing.T, ctx context.Context, provider categorize.Provider, cfg Config, pending []preprocessed) categorizeResult {
	t.Helper()
	materializer, err := NewMaterializer(unusedReader(t), unusedWriter(t), provider, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	result := categorizeResult{outcomes: map[int]categorize.CategorizationOutcome{}, stats: Stats{LLMFailureCounts: map[string]int{}}}
	result.err = materializer.categorizePending(ctx, cfg, pending, result.outcomes, &result.stats)
	return result
}

func batchPending(t *testing.T, count int) []preprocessed {
	t.Helper()
	ids := make([]string, count)
	for index := range ids {
		ids[index] = fmt.Sprintf("wu%d", index)
	}
	return shadowTestEntries(t, ids...)
}

// The synchronous baseline of the same units against the same answers.
func syncBaseline(t *testing.T, pending []preprocessed, syncText func(string) string) (categorizeResult, *fakeOpenAI) {
	t.Helper()
	fake, server := newFakeOpenAI(t)
	fake.syncText = syncText
	return runCategorize(t, context.Background(), fakeProvider(server.URL), batchTestConfig(LLMBatchModeSync), pending), fake
}

func requireSameCategorization(t *testing.T, got, want categorizeResult) {
	t.Helper()
	if !reflect.DeepEqual(got.outcomes, want.outcomes) {
		t.Errorf("outcomes differ from the synchronous run:\n batch %+v\n  sync %+v", got.outcomes, want.outcomes)
	}
	if !reflect.DeepEqual(got.stats, want.stats) {
		t.Errorf("stats differ from the synchronous run:\n batch %+v\n  sync %+v", got.stats, want.stats)
	}
	if (got.err == nil) != (want.err == nil) {
		t.Errorf("error: batch %v, sync %v", got.err, want.err)
	}
}

func TestBatchModeAnswersEachUnitAsTheSynchronousCallDoes(t *testing.T) {
	pending := batchPending(t, 3)
	want, _ := syncBaseline(t, pending, nil)
	if len(want.outcomes) != 3 || want.stats.LLMCalls != 3 {
		t.Fatalf("the baseline answered nothing: %+v", want)
	}

	fake, server := newFakeOpenAI(t)
	fake.statuses = []string{"validating", "in_progress", "finalizing", "completed"}
	got := runCategorize(t, context.Background(), fakeProvider(server.URL), batchTestConfig(LLMBatchModeProvider), pending)
	requireSameCategorization(t, got, want)
	if fake.syncCalls != 0 {
		t.Fatalf("%d synchronous calls on the batch path", fake.syncCalls)
	}
	// Four polls to the terminal status, and the retrieve of the fetch.
	if len(fake.uploaded) != 3 || fake.polls != 5 {
		t.Fatalf("uploaded %d lines, polled %d times", len(fake.uploaded), fake.polls)
	}
	for position, line := range fake.uploaded {
		entry := pending[position]
		if line.CustomID != fmt.Sprintf("run-batch-test-%d", entry.index) {
			t.Errorf("custom id %q", line.CustomID)
		}
		if line.Body.Input != categorize.BuildPrompt(entry.result.Bundle.SourceBlock) {
			t.Errorf("line %d prompt is not the synchronous prompt", position)
		}
	}
}

func TestBatchModeRepairsAnInvalidLineWithOneSynchronousCall(t *testing.T) {
	pending := batchPending(t, 2)
	brokenPrompt := categorize.BuildPrompt(pending[0].result.Bundle.SourceBlock)
	const invalid = `{"subcategories": {}}`
	want, _ := syncBaseline(t, pending, func(prompt string) string {
		if prompt == brokenPrompt {
			return invalid
		}
		return mockAnswer(t, prompt)
	})
	if want.outcomes[0].LLMCalls != 2 {
		t.Fatalf("the baseline made no repair call: %+v", want.outcomes[0])
	}

	fake, server := newFakeOpenAI(t)
	fake.lineFor = func(customID, prompt string) string {
		if prompt == brokenPrompt {
			return okLine(customID, invalid)
		}
		return okLine(customID, mockAnswer(t, prompt))
	}
	got := runCategorize(t, context.Background(), fakeProvider(server.URL), batchTestConfig(LLMBatchModeProvider), pending)
	requireSameCategorization(t, got, want)
	if fake.syncCalls != 1 || fake.syncInputs[0] == brokenPrompt {
		t.Fatalf("%d synchronous calls (%v), want the one repair call", fake.syncCalls, fake.syncCalls)
	}
}

// deadEndpoint accepts a connection and closes it before any answer: every
// synchronous request is a transport failure.
func deadEndpoint(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("no hijacker")
		}
		conn, _, err := hijacker.Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// The rows a batch timeout writes are the rows a synchronous transport
// failure writes: no outcome for any unit (post-processing gives each the
// llm_task_failed fallback row), the same failure count and calls. Only the
// failure class differs, on purpose: a timeout that won is batch_timeout, not
// llm_error, so it is visible in the run's evidence.
func TestBatchModeTimeoutFailsUnitsAsASynchronousTransportFailure(t *testing.T) {
	pending := batchPending(t, 3)
	want := runCategorize(t, context.Background(), fakeProvider(deadEndpoint(t)), batchTestConfig(LLMBatchModeSync), pending)
	if want.err != nil || len(want.outcomes) != 0 || want.stats.LLMFailureCounts["llm_error"] != 3 {
		t.Fatalf("the transport-failure baseline is not what the test assumes: %+v", want)
	}

	fake, server := newFakeOpenAI(t)
	fake.statuses = []string{"in_progress"}
	cfg := batchTestConfig(LLMBatchModeProvider)
	cfg.LLMBatchTimeout = 50 * time.Millisecond
	got := runCategorize(t, context.Background(), fakeProvider(server.URL), cfg, pending)
	if !reflect.DeepEqual(got.stats.LLMFailureCounts, map[string]int{"batch_timeout": 3}) {
		t.Fatalf("failure counts %v, want batch_timeout for every unit", got.stats.LLMFailureCounts)
	}
	got.stats.LLMFailureCounts = want.stats.LLMFailureCounts
	requireSameCategorization(t, got, want)
	if fake.cancelled != 1 {
		t.Fatalf("cancelled %d times, want 1", fake.cancelled)
	}
	// One batch for the run: the wait never submits again.
	if fake.creates != 1 || len(fake.uploaded) != len(pending) || fake.polls < 2 {
		t.Fatalf("creates %d, uploaded lines %d, polls %d", fake.creates, len(fake.uploaded), fake.polls)
	}
}

func TestBatchModeCancelledRunCancelsTheProviderBatchAndCountsBilledLines(t *testing.T) {
	pending := batchPending(t, 2)
	fake, server := newFakeOpenAI(t)
	fake.statuses = []string{"in_progress"}
	fake.outputOnCancel = okLine("run-batch-test-0", "{}") + "\n"
	ctx, cancel := context.WithCancel(context.Background())
	provider := fakeProvider(server.URL)
	go func() {
		for {
			fake.mu.Lock()
			polled := fake.polls
			fake.mu.Unlock()
			if polled >= 2 {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	got := runCategorize(t, ctx, provider, batchTestConfig(LLMBatchModeProvider), pending)
	if got.err != nil || len(got.outcomes) != 0 {
		t.Fatalf("result %+v", got)
	}
	if fake.cancelled != 1 {
		t.Fatalf("the provider batch was cancelled %d times after the run context ended", fake.cancelled)
	}
	if got.stats.LLMCalls != 1 || got.stats.LLMInputTokens != fakeInputTokens || got.stats.LLMOutputTokens != fakeOutputTokens {
		t.Fatalf("billed lines of the cancelled batch not counted: %+v", got.stats)
	}
	if got.stats.LLMFailures != 2 {
		t.Fatalf("failures %d, want 2", got.stats.LLMFailures)
	}
}

func TestBatchModeTimeoutCountsBilledLines(t *testing.T) {
	pending := batchPending(t, 2)
	fake, server := newFakeOpenAI(t)
	fake.statuses = []string{"in_progress"}
	fake.outputOnCancel = okLine("run-batch-test-0", "{}") + "\n" + okLine("run-batch-test-1", "{}") + "\n"
	cfg := batchTestConfig(LLMBatchModeProvider)
	cfg.LLMBatchTimeout = 30 * time.Millisecond
	got := runCategorize(t, context.Background(), fakeProvider(server.URL), cfg, pending)
	if got.stats.LLMCalls != 2 || got.stats.LLMInputTokens != 2*fakeInputTokens || len(got.outcomes) != 0 {
		t.Fatalf("stats %+v outcomes %d", got.stats, len(got.outcomes))
	}
}

func TestBatchModeMissingAndFailedLinesFailOnlyTheirUnits(t *testing.T) {
	pending := batchPending(t, 4)
	fake, server := newFakeOpenAI(t)
	fake.lineFor = func(customID, prompt string) string {
		switch customID {
		case "run-batch-test-1":
			return ""
		case "run-batch-test-2":
			return `{"id":"l2","custom_id":"run-batch-test-2","response":null,"error":{"code":"server_error","message":"upstream"}}`
		case "run-batch-test-3":
			return `{"id":"l3","custom_id":"run-batch-test-3","response":{"status_code":200,"body":{"usage":{"input_tokens":7,"output_tokens":3}}},"error":null}`
		}
		return okLine(customID, mockAnswer(t, prompt))
	}
	got := runCategorize(t, context.Background(), fakeProvider(server.URL), batchTestConfig(LLMBatchModeProvider), pending)
	if got.err != nil {
		t.Fatal(got.err)
	}
	if _, ok := got.outcomes[0]; !ok || len(got.outcomes) != 1 {
		t.Fatalf("outcomes %v", got.outcomes)
	}
	if got.stats.LLMFailures != 3 {
		t.Fatalf("failures %+v", got.stats.LLMFailureCounts)
	}
	// One answered line plus the billed empty line.
	if got.stats.LLMCalls != 2 || got.stats.LLMInputTokens != fakeInputTokens+7 || got.stats.LLMOutputTokens != fakeOutputTokens+3 {
		t.Fatalf("stats %+v", got.stats)
	}
}

func TestBatchModeEndedBatchFailsEveryUnit(t *testing.T) {
	for _, status := range []string{"failed", "expired", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			pending := batchPending(t, 2)
			fake, server := newFakeOpenAI(t)
			fake.statuses = []string{"in_progress", status}
			got := runCategorize(t, context.Background(), fakeProvider(server.URL), batchTestConfig(LLMBatchModeProvider), pending)
			if got.err != nil || len(got.outcomes) != 0 || got.stats.LLMFailureCounts["server_error"] != 2 {
				t.Fatalf("result %+v", got)
			}
			if fake.cancelled != 0 {
				t.Fatal("an ended batch was cancelled")
			}
		})
	}
}

func TestBatchModeDeterministicSubmitFailureAbortsTheRun(t *testing.T) {
	pending := batchPending(t, 3)
	fake, server := newFakeOpenAI(t)
	fake.failCreate = http.StatusUnauthorized
	got := runCategorize(t, context.Background(), fakeProvider(server.URL), batchTestConfig(LLMBatchModeProvider), pending)
	var deterministic *workgraph.DeterministicError
	if !errors.As(got.err, &deterministic) || deterministic.Class != workgraph.ClassLLMDeterministic {
		t.Fatalf("err = %v", got.err)
	}
	if got.stats.LLMFailures != 1 || len(got.outcomes) != 0 || fake.syncCalls != 0 {
		t.Fatalf("stats %+v sync calls %d", got.stats, fake.syncCalls)
	}
}

func TestBatchModePollFailureCancelsAndFailsEveryUnit(t *testing.T) {
	pending := batchPending(t, 2)
	fake, server := newFakeOpenAI(t)
	fake.failPoll = http.StatusBadGateway
	got := runCategorize(t, context.Background(), fakeProvider(server.URL), batchTestConfig(LLMBatchModeProvider), pending)
	if got.err != nil || len(got.outcomes) != 0 || got.stats.LLMFailureCounts["server_error"] != 2 {
		t.Fatalf("result %+v", got)
	}
	if fake.cancelled != 1 {
		t.Fatalf("cancelled %d", fake.cancelled)
	}
}

func TestBatchModeUsesTheSynchronousPath(t *testing.T) {
	t.Run("auto below the minimum", func(t *testing.T) {
		pending := batchPending(t, 2)
		fake, server := newFakeOpenAI(t)
		cfg := batchTestConfig(LLMBatchModeAuto)
		cfg.LLMBatchMinItems = 3
		got := runCategorize(t, context.Background(), fakeProvider(server.URL), cfg, pending)
		if got.err != nil || len(got.outcomes) != 2 || fake.syncCalls != 2 || len(fake.uploaded) != 0 {
			t.Fatalf("result %+v sync %d uploaded %d", got, fake.syncCalls, len(fake.uploaded))
		}
	})
	t.Run("auto at the minimum uses the batch", func(t *testing.T) {
		pending := batchPending(t, 3)
		fake, server := newFakeOpenAI(t)
		cfg := batchTestConfig(LLMBatchModeAuto)
		cfg.LLMBatchMinItems = 3
		got := runCategorize(t, context.Background(), fakeProvider(server.URL), cfg, pending)
		if got.err != nil || len(got.outcomes) != 3 || fake.syncCalls != 0 || len(fake.uploaded) != 3 {
			t.Fatalf("result %+v sync %d uploaded %d", got, fake.syncCalls, len(fake.uploaded))
		}
	})
	t.Run("auto with a provider that has no batch api", func(t *testing.T) {
		got := runCategorize(t, context.Background(), categorize.MockProvider{}, batchTestConfig(LLMBatchModeAuto), batchPending(t, 3))
		if got.err != nil || len(got.outcomes) != 3 {
			t.Fatalf("result %+v", got)
		}
	})
	t.Run("sync", func(t *testing.T) {
		pending := batchPending(t, 3)
		fake, server := newFakeOpenAI(t)
		got := runCategorize(t, context.Background(), fakeProvider(server.URL), batchTestConfig(LLMBatchModeSync), pending)
		if got.err != nil || fake.syncCalls != 3 || len(fake.uploaded) != 0 {
			t.Fatalf("result %+v", got)
		}
	})
}

// The provider job id is logged at submit, before the batch ends, and an
// outcome other than completed is a WARN line.
func TestBatchModeLogsTheJobAtSubmitAndATimeoutLoudly(t *testing.T) {
	pending := batchPending(t, 2)
	fake, server := newFakeOpenAI(t)
	fake.statuses = []string{"in_progress"}
	logs := &syncBuffer{}
	materializer, err := NewMaterializer(unusedReader(t), unusedWriter(t), fakeProvider(server.URL), debugLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	cfg := batchTestConfig(LLMBatchModeProvider)
	cfg.LLMBatchTimeout = 30 * time.Millisecond
	stats := Stats{LLMFailureCounts: map[string]int{}}
	if err := materializer.categorizePending(context.Background(), cfg, pending, map[int]categorize.CategorizationOutcome{}, &stats); err != nil {
		t.Fatal(err)
	}
	text := logs.String()
	submitted := strings.Index(text, `msg="investment llm batch submitted"`)
	complete := strings.Index(text, `msg="investment llm batch complete"`)
	if submitted < 0 || complete < submitted || !strings.Contains(text[submitted:complete], "provider_job_id=batch_1") {
		t.Fatalf("no submit line with the job id before the end line:\n%s", text)
	}
	var endLine string
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, `msg="investment llm batch complete"`) {
			endLine = line
		}
	}
	if !strings.Contains(endLine, "level=WARN") || !strings.Contains(endLine, "outcome=timeout") {
		t.Fatalf("the timeout end line is not a WARN line with outcome=timeout: %q", endLine)
	}
}
