package categorize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const fakeBatchKey = "batch-test-placeholder"

// fakeBatchAPI is an OpenAI Batch API stand-in: files, batches, file content,
// cancel. failures[path] lists the status codes the next calls to path answer
// with before it answers normally.
type fakeBatchAPI struct {
	t *testing.T

	mu          sync.Mutex
	calls       []string
	uploads     [][]byte
	uploadForm  map[string]string
	uploadName  string
	creates     []map[string]any
	batch       map[string]any
	files       map[string]string
	failures    map[string][]int
	garbageOn   map[string]bool
	authHeaders []string
	cancelled   []string
}

func newFakeBatchAPI(t *testing.T) (*fakeBatchAPI, *httptest.Server) {
	t.Helper()
	fake := &fakeBatchAPI{
		t:         t,
		batch:     map[string]any{"id": "batch_1", "status": "validating"},
		files:     map[string]string{},
		failures:  map[string][]int{},
		garbageOn: map[string]bool{},
	}
	server := httptest.NewServer(http.HandlerFunc(fake.serve))
	t.Cleanup(server.Close)
	return fake, server
}

func (f *fakeBatchAPI) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := r.Method + " " + r.URL.Path
	f.calls = append(f.calls, key)
	f.authHeaders = append(f.authHeaders, r.Header.Get("Authorization"))
	if queue := f.failures[key]; len(queue) > 0 {
		f.failures[key] = queue[1:]
		w.WriteHeader(queue[0])
		_, _ = w.Write([]byte(`{"error":{"message":"injected","type":"server_error"}}`))
		return
	}
	if f.garbageOn[key] {
		_, _ = w.Write([]byte("not json"))
		return
	}
	switch {
	case key == "POST /files":
		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "multipart/form-data" {
			f.t.Errorf("upload content type %q", r.Header.Get("Content-Type"))
		}
		reader := multipart.NewReader(r.Body, params["boundary"])
		f.uploadForm = map[string]string{}
		for {
			part, err := reader.NextPart()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				f.t.Errorf("upload part: %v", err)
				return
			}
			body, _ := io.ReadAll(part)
			if part.FormName() == "file" {
				f.uploadName = part.FileName()
				f.uploads = append(f.uploads, body)
			} else {
				f.uploadForm[part.FormName()] = string(body)
			}
		}
		_, _ = w.Write([]byte(`{"id":"file-in-1","object":"file","purpose":"batch"}`))
	case key == "POST /batches":
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.creates = append(f.creates, body)
		_, _ = w.Write([]byte(`{"id":"batch_1","status":"validating"}`))
	case key == "GET /batches/batch_1":
		_ = json.NewEncoder(w).Encode(f.batch)
	case key == "POST /batches/batch_1/cancel":
		f.cancelled = append(f.cancelled, "batch_1")
		_, _ = w.Write([]byte(`{"id":"batch_1","status":"cancelling"}`))
	case strings.HasPrefix(key, "GET /files/") && strings.HasSuffix(key, "/content"):
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/files/"), "/content")
		content, ok := f.files[id]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(content))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeBatchAPI) callList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func batchTestProvider(baseURL string) *OpenAIProvider {
	return NewOpenAIProvider(OpenAIProviderConfig{
		APIKey: secrets.NewHidden(fakeBatchKey), BaseURL: baseURL + "/", Model: "gpt-5-nano",
	})
}

func batchTestItems() []BatchItem {
	return []BatchItem{
		{CustomID: "run1-0", Request: CategorizationRequest(BuildPrompt("[pr:1] Fix the login timeout"))},
		{CustomID: "run1-1", Request: CategorizationRequest(BuildPrompt("[issue:2] Rotate keys <now> & é"))},
	}
}

func TestAsBatchProviderOnlyOpenAI(t *testing.T) {
	cases := map[string]Provider{
		"openai": NewOpenAIProvider(OpenAIProviderConfig{}),
		"local":  NewLocalProvider(LocalProviderConfig{}),
		"ollama": NewOllamaProvider(OllamaProviderConfig{}),
		"mock":   MockProvider{},
		"none":   NoneProvider{},
	}
	for name, provider := range cases {
		_, ok := AsBatchProvider(provider)
		if ok != (name == "openai") {
			t.Errorf("%s: AsBatchProvider = %v", name, ok)
		}
	}
}

func TestBatchSubmitUploadsOneJSONLFileAndCreatesTheBatch(t *testing.T) {
	fake, server := newFakeBatchAPI(t)
	provider := batchTestProvider(server.URL)
	items := batchTestItems()

	submission, err := provider.SubmitBatch(context.Background(), items)
	if err != nil {
		t.Fatal(err)
	}
	if want := (BatchSubmission{ProviderJobID: "batch_1", InputFileID: "file-in-1", ItemCount: 2}); submission != want {
		t.Fatalf("submission = %+v, want %+v", submission, want)
	}
	if got := fake.callList(); !reflect.DeepEqual(got, []string{"POST /files", "POST /batches"}) {
		t.Fatalf("calls = %v", got)
	}
	for _, header := range fake.authHeaders {
		if header != "Bearer "+fakeBatchKey {
			t.Fatalf("Authorization = %q", header)
		}
	}
	if fake.uploadForm["purpose"] != "batch" || fake.uploadName != openAIBatchFileName {
		t.Fatalf("upload purpose %q name %q", fake.uploadForm["purpose"], fake.uploadName)
	}
	lines := strings.Split(string(fake.uploads[0]), "\n")
	if len(lines) != len(items) {
		t.Fatalf("upload has %d lines, want %d", len(lines), len(items))
	}
	for index, raw := range lines {
		var line struct {
			CustomID string          `json:"custom_id"`
			Method   string          `json:"method"`
			URL      string          `json:"url"`
			Body     json.RawMessage `json:"body"`
		}
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("line %d: %v", index, err)
		}
		if line.CustomID != items[index].CustomID || line.Method != "POST" || line.URL != "/v1/responses" {
			t.Fatalf("line %d envelope = %+v", index, line)
		}
		// The line body is byte for byte the synchronous request body.
		if sync := syncRequestBody(t, items[index].Request); !bytes.Equal(line.Body, sync) {
			t.Fatalf("line %d body differs from the synchronous body:\nbatch %s\nsync  %s", index, line.Body, sync)
		}
	}
	create := fake.creates[0]
	want := map[string]any{
		"input_file_id": "file-in-1", "endpoint": "/v1/responses", "completion_window": "24h",
		"metadata": map[string]any{"source": "investment_materialize"},
	}
	if !reflect.DeepEqual(create, want) {
		t.Fatalf("create body = %v, want %v", create, want)
	}
}

// syncRequestBody captures the body Complete sends for request.
func syncRequestBody(t *testing.T, request CompletionRequest) []byte {
	t.Helper()
	var captured []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"output_text":"{}"}`))
	}))
	defer server.Close()
	if _, err := batchTestProvider(server.URL).Complete(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	return captured
}

func TestBatchSubmitRefusesInvalidItemsWithoutACall(t *testing.T) {
	fake, server := newFakeBatchAPI(t)
	provider := batchTestProvider(server.URL)
	good := CategorizationRequest(BuildPrompt("x"))
	cases := map[string][]BatchItem{
		"empty":           nil,
		"blank id":        {{CustomID: "  ", Request: good}},
		"id with a space": {{CustomID: "run 1", Request: good}},
		"empty prompt":    {{CustomID: "run1-0", Request: CompletionRequest{}}},
		"duplicate id":    {{CustomID: "run1-0", Request: good}, {CustomID: "run1-0", Request: good}},
	}
	for name, items := range cases {
		if _, err := provider.SubmitBatch(context.Background(), items); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if calls := fake.callList(); len(calls) != 0 {
		t.Fatalf("calls = %v", calls)
	}
}

func TestBatchPollMapsStatusAndCounts(t *testing.T) {
	fake, server := newFakeBatchAPI(t)
	provider := batchTestProvider(server.URL)
	cases := []struct {
		batch map[string]any
		want  BatchState
	}{
		{map[string]any{"status": "in_progress", "request_counts": map[string]any{"total": 3, "completed": 1, "failed": 0}},
			BatchState{ProviderJobID: "batch_1", Status: BatchJobRunning, ProviderStatus: "in_progress", TotalCount: 3, CompletedCount: 1}},
		{map[string]any{"status": "completed", "output_file_id": "file-out", "error_file_id": "file-err",
			"request_counts": map[string]any{"total": 3, "completed": 2, "failed": 1}},
			BatchState{ProviderJobID: "batch_1", Status: BatchJobSucceeded, ProviderStatus: "completed", TotalCount: 3,
				CompletedCount: 2, FailedCount: 1, OutputFileID: "file-out", ErrorFileID: "file-err"}},
		{map[string]any{"status": "failed", "request_counts": nil},
			BatchState{ProviderJobID: "batch_1", Status: BatchJobFailed, ProviderStatus: "failed"}},
		{map[string]any{"status": "expired"}, BatchState{ProviderJobID: "batch_1", Status: BatchJobExpired, ProviderStatus: "expired"}},
		{map[string]any{"status": "cancelling"}, BatchState{ProviderJobID: "batch_1", Status: BatchJobCancelled, ProviderStatus: "cancelling"}},
		{map[string]any{"status": "cancelled"}, BatchState{ProviderJobID: "batch_1", Status: BatchJobCancelled, ProviderStatus: "cancelled"}},
	}
	for _, tc := range cases {
		fake.mu.Lock()
		fake.batch = tc.batch
		fake.mu.Unlock()
		state, err := provider.PollBatch(context.Background(), "batch_1")
		if err != nil {
			t.Fatal(err)
		}
		if state != tc.want {
			t.Errorf("poll %v = %+v, want %+v", tc.batch["status"], state, tc.want)
		}
		if state.Status.Terminal() != (tc.want.Status != BatchJobRunning) {
			t.Errorf("%s: Terminal() = %v", state.Status, state.Status.Terminal())
		}
	}
}

func TestBatchFetchReadsOutputThenErrorFileAndLeavesMissingIDsOut(t *testing.T) {
	fake, server := newFakeBatchAPI(t)
	provider := batchTestProvider(server.URL)
	fake.batch = map[string]any{"status": "completed", "output_file_id": "file-out", "error_file_id": "file-err"}
	fake.files["file-out"] = `{"id":"r1","custom_id":"run1-1","response":{"status_code":200,"request_id":"q1","body":{"output":[{"type":"message","content":[{"type":"output_text","text":"{\"b\": 1, \"a\": 2}"}]}],"usage":{"input_tokens":11,"output_tokens":7,"input_tokens_details":{"cached_tokens":3}}}},"error":null}` + "\n"
	fake.files["file-err"] = `{"id":"r2","custom_id":"run1-0","response":{"status_code":400,"request_id":"q2","body":{"error":{"message":"bad"}}},"error":null}` + "\n"

	results, err := provider.FetchBatchResults(context.Background(), "batch_1")
	if err != nil {
		t.Fatal(err)
	}
	eleven, seven, three, fourHundred, twoHundred := 11, 7, 3, 400, 200
	want := []BatchItemResult{
		{CustomID: "run1-1", Text: `{"a":2,"b":1}`, LineID: "r1", RequestID: "q1", StatusCode: &twoHundred,
			InputTokens: &eleven, OutputTokens: &seven, CachedInputTokens: &three},
		{CustomID: "run1-0", ErrorCode: "http_400", ErrorMessage: "Batch response did not contain completion text",
			LineID: "r2", RequestID: "q2", StatusCode: &fourHundred},
	}
	if !reflect.DeepEqual(results, want) {
		t.Fatalf("results:\n got %+v\nwant %+v", results, want)
	}
	if !results[0].Succeeded() || results[1].Succeeded() {
		t.Fatal("Succeeded() is wrong")
	}
	if got := fake.callList(); !reflect.DeepEqual(got, []string{"GET /batches/batch_1", "GET /files/file-out/content", "GET /files/file-err/content"}) {
		t.Fatalf("calls = %v", got)
	}
}

func TestBatchFetchWithNoFilesIsEmpty(t *testing.T) {
	fake, server := newFakeBatchAPI(t)
	fake.batch = map[string]any{"status": "expired"}
	results, err := batchTestProvider(server.URL).FetchBatchResults(context.Background(), "batch_1")
	if err != nil || len(results) != 0 {
		t.Fatalf("results %v err %v", results, err)
	}
}

func TestBatchFetchFailsOnALineThatIsNotJSON(t *testing.T) {
	fake, server := newFakeBatchAPI(t)
	fake.batch = map[string]any{"status": "completed", "output_file_id": "file-out"}
	fake.files["file-out"] = "{\"custom_id\":\"run1-0\"}\nnot json\n"
	if _, err := batchTestProvider(server.URL).FetchBatchResults(context.Background(), "batch_1"); err == nil {
		t.Fatal("no error")
	}
}

func TestBatchCancelPostsToTheCancelPath(t *testing.T) {
	fake, server := newFakeBatchAPI(t)
	if err := batchTestProvider(server.URL).CancelBatch(context.Background(), "batch_1"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fake.cancelled, []string{"batch_1"}) {
		t.Fatalf("cancelled = %v", fake.cancelled)
	}
}

func TestBatchCallFailuresClassifyLikeTheSynchronousCall(t *testing.T) {
	t.Run("401 on create is deterministic and not retried", func(t *testing.T) {
		fake, server := newFakeBatchAPI(t)
		fake.failures["POST /batches"] = []int{401, 401}
		_, err := batchTestProvider(server.URL).SubmitBatch(context.Background(), batchTestItems())
		if err == nil || !IsDeterministicFailure(err) {
			t.Fatalf("err = %v, deterministic = %v", err, IsDeterministicFailure(err))
		}
		if got := fake.callList(); !reflect.DeepEqual(got, []string{"POST /files", "POST /batches"}) {
			t.Fatalf("calls = %v", got)
		}
	})
	t.Run("a create is never retried: a retry could start a second billed batch", func(t *testing.T) {
		fake, server := newFakeBatchAPI(t)
		fake.failures["POST /batches"] = []int{502}
		_, err := batchTestProvider(server.URL).SubmitBatch(context.Background(), batchTestItems())
		if err == nil || IsDeterministicFailure(err) {
			t.Fatalf("err = %v", err)
		}
		if got := fake.callList(); !reflect.DeepEqual(got, []string{"POST /files", "POST /batches"}) {
			t.Fatalf("calls = %v", got)
		}
	})
	t.Run("one 500 on poll is retried once", func(t *testing.T) {
		fake, server := newFakeBatchAPI(t)
		fake.failures["GET /batches/batch_1"] = []int{500}
		state, err := batchTestProvider(server.URL).PollBatch(context.Background(), "batch_1")
		if err != nil || state.Status != BatchJobRunning {
			t.Fatalf("state %+v err %v", state, err)
		}
		if got := len(fake.callList()); got != 2 {
			t.Fatalf("%d calls, want 2", got)
		}
	})
	t.Run("two 500s fail, not deterministic", func(t *testing.T) {
		fake, server := newFakeBatchAPI(t)
		fake.failures["GET /batches/batch_1"] = []int{500, 500}
		_, err := batchTestProvider(server.URL).PollBatch(context.Background(), "batch_1")
		if err == nil || IsDeterministicFailure(err) {
			t.Fatalf("err = %v", err)
		}
		if got := len(fake.callList()); got != 2 {
			t.Fatalf("%d calls, want 2", got)
		}
	})
	t.Run("an unreadable 200 is not retried", func(t *testing.T) {
		fake, server := newFakeBatchAPI(t)
		fake.garbageOn["POST /batches"] = true
		_, err := batchTestProvider(server.URL).SubmitBatch(context.Background(), batchTestItems())
		if err == nil || IsDeterministicFailure(err) {
			t.Fatalf("err = %v", err)
		}
		if got := fake.callList(); !reflect.DeepEqual(got, []string{"POST /files", "POST /batches"}) {
			t.Fatalf("calls = %v", got)
		}
	})
}

func TestBatchOneSpanPerCallNamesTheOperation(t *testing.T) {
	recorder := recordSpans(t)
	fake, server := newFakeBatchAPI(t)
	provider := batchTestProvider(server.URL)
	fake.batch = map[string]any{"status": "completed", "output_file_id": "file-out", "error_file_id": "file-err"}
	fake.files["file-out"] = ""
	fake.files["file-err"] = ""
	fake.failures["POST /batches/batch_1/cancel"] = []int{500}

	ctx := context.Background()
	if _, err := provider.SubmitBatch(ctx, batchTestItems()); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.PollBatch(ctx, "batch_1"); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.FetchBatchResults(ctx, "batch_1"); err != nil {
		t.Fatal(err)
	}
	if err := provider.CancelBatch(ctx, "batch_1"); err != nil {
		t.Fatal(err)
	}

	type seen struct {
		operation string
		attempt   int64
		class     string
	}
	var got []seen
	for _, span := range llmSpans(recorder) {
		attrs := spanAttrs(span)
		if attrs[llmAttrProvider].AsString() != "openai" || attrs[llmAttrModel].AsString() != "gpt-5-nano" {
			t.Fatalf("span attributes %v", attrs)
		}
		got = append(got, seen{attrs[llmAttrBatchOp].AsString(), attrs[llmAttrAttempt].AsInt64(), attrs[llmAttrClass].AsString()})
	}
	want := []seen{
		{"upload", 1, ""}, {"create", 1, ""}, {"retrieve", 1, ""}, {"retrieve", 1, ""},
		{"content", 1, ""}, {"content", 1, ""}, {"cancel", 1, "server"}, {"cancel", 2, ""},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("spans:\n got %v\nwant %v", got, want)
	}
}

func TestSyncSpanHasNoBatchOperation(t *testing.T) {
	recorder := recordSpans(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"output_text":"{}"}`))
	}))
	defer server.Close()
	if _, err := batchTestProvider(server.URL).Complete(context.Background(), CategorizationRequest("p")); err != nil {
		t.Fatal(err)
	}
	spans := llmSpans(recorder)
	if len(spans) != 1 {
		t.Fatalf("%d spans", len(spans))
	}
	if _, ok := spanAttrs(spans[0])[llmAttrBatchOp]; ok {
		t.Fatal("a synchronous call carries a batch operation")
	}
}

func TestBatchLineWithABodyThatIsNotAResponsesBodySaysWhy(t *testing.T) {
	results, err := parseOpenAIBatchLines([]byte(`{"id":"l1","custom_id":"run1-0","response":{"status_code":200,"body":"plain text"},"error":null}`))
	if err != nil || len(results) != 1 {
		t.Fatalf("results %v err %v", results, err)
	}
	if results[0].ErrorCode != "http_200" || !strings.Contains(results[0].ErrorMessage, "not a Responses API body") {
		t.Fatalf("result %+v", results[0])
	}
}

func TestBatchLineDecodeErrorCarriesNoProviderContent(t *testing.T) {
	_, err := parseOpenAIBatchLines([]byte(`{"custom_id": Qsecretvalue}`))
	if err == nil || strings.Contains(err.Error(), "Q") || strings.Contains(err.Error(), "secretvalue") {
		t.Fatalf("err = %v", err)
	}
}
