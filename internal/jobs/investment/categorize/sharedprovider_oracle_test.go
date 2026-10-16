package categorize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/llm"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// No-regression guard for serving OpenAI through the shared llm package: the
// two oracle files are dev-health-go llm/testdata's recordings of what this
// package's own OpenAIProvider (the ops port, at the header's ops_sha) sent and
// returned for a fixed case table. Each case runs here through the path
// production now builds (SharedProvider behind the ops Provider and
// BatchProvider) and must give the same wire bytes and the same caller-relied
// answer. The files are pinned by digest: a changed byte is a new recording.
const (
	sharedSyncOracleDigest  = "42480d41492fbaa147f4fe78f88be5a8e1edabf994cb2a6dfacb7e8151b3b6b5"
	sharedBatchOracleDigest = "1dc0cda0ae0dfd79b36835d28783a45fb9f042a25c5710d90b68ec30f0d0aac8"
	sharedOracleOpsSHA      = "ab09982a0135bdd3adb211780e90be3679485e23"
	// oracleAPIKey is no credential: the scripted servers accept anything.
	oracleAPIKey = "oracle-placeholder"
)

// sharedOracleDir is llm/testdata of the dev-health-go module this build uses.
// The oracles are read where they were recorded, not copied: the batch oracle
// carries the frozen-Python answers, and a copy under this repository's
// testdata would be a Python golden the record verb never made.
func sharedOracleDir(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/full-chaos/dev-health-go").Output()
	dir := strings.TrimSpace(string(out))
	if err != nil || dir == "" {
		t.Fatalf("locate the dev-health-go module: %v (%q)", err, dir)
	}
	return filepath.Join(dir, "llm", "testdata")
}

func loadSharedOracle(t *testing.T, name, digest string, into any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(sharedOracleDir(t), name))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != digest {
		t.Fatalf("%s digest = %s, want %s: an oracle is re-recorded, never edited", name, got, digest)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(into); err != nil {
		t.Fatal(err)
	}
}

func decodeOracleSchema(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	if string(raw) == "null" || len(raw) == 0 {
		return nil
	}
	var schema map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&schema); err != nil {
		t.Fatal(err)
	}
	return schema
}

type sharedSyncStep struct {
	Status int               `json:"status"`
	Header map[string]string `json:"header"`
	Body   string            `json:"body"`
}

type sharedSyncCase struct {
	Name            string           `json:"name"`
	Model           string           `json:"model"`
	ConfigFloor     int              `json:"config_max_output_tokens"`
	System          string           `json:"system"`
	Prompt          string           `json:"prompt"`
	FormatName      string           `json:"format_name"`
	Schema          json.RawMessage  `json:"schema"`
	RequestFloor    int              `json:"request_max_output_tokens"`
	Steps           []sharedSyncStep `json:"steps"`
	SentBodies      []string         `json:"sent_bodies"`
	Calls           int              `json:"calls"`
	ResultText      string           `json:"result_text"`
	ResultModel     string           `json:"result_model"`
	InputTokens     *int             `json:"input_tokens"`
	OutputTokens    *int             `json:"output_tokens"`
	CachedTokens    *int             `json:"cached_input_tokens"`
	ErrorClass      string           `json:"error_class"`
	RetryAfterNanos int64            `json:"retry_after_nanos"`
}

// scriptedSyncServer answers the case's steps in order (the last one repeats)
// and records every request body.
func scriptedSyncServer(t *testing.T, steps []sharedSyncStep) (*httptest.Server, func() []string) {
	t.Helper()
	var (
		mu     sync.Mutex
		bodies []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		index := min(len(bodies), len(steps)-1)
		bodies = append(bodies, string(raw))
		mu.Unlock()
		step := steps[index]
		for key, value := range step.Header {
			w.Header().Set(key, value)
		}
		w.WriteHeader(step.Status)
		_, _ = w.Write([]byte(step.Body))
	}))
	t.Cleanup(server.Close)
	return server, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), bodies...)
	}
}

func samePointer(a, b *int) bool {
	return (a == nil) == (b == nil) && (a == nil || *a == *b)
}

func TestSharedOpenAIReplaysTheOpsPortSyncOracle(t *testing.T) {
	var oracle struct {
		Header map[string]string `json:"header"`
		Cases  []sharedSyncCase  `json:"cases"`
	}
	loadSharedOracle(t, "openai_sync.oracle.json", sharedSyncOracleDigest, &oracle)
	if oracle.Header["ops_sha"] != sharedOracleOpsSHA || len(oracle.Cases) != 21 {
		t.Fatalf("oracle header %v with %d cases: not the 21-case recording at %s", oracle.Header, len(oracle.Cases), sharedOracleOpsSHA)
	}
	compared := 0
	for _, oc := range oracle.Cases {
		t.Run(oc.Name, func(t *testing.T) {
			request := CompletionRequest{
				Prompt: oc.Prompt, SystemMessage: oc.System, ResponseFormatName: oc.FormatName,
				JSONSchema: decodeOracleSchema(t, oc.Schema), MaxOutputTokens: oc.RequestFloor,
			}

			server, sent := scriptedSyncServer(t, oc.Steps)
			shared, err := NewSharedProvider(SharedProviderConfig{
				Kind: ProviderKindOpenAI, APIKey: secrets.NewHidden(oracleAPIKey), BaseURL: server.URL,
				Model: oc.Model, MaxOutputTokens: oc.ConfigFloor,
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := shared.Complete(context.Background(), request)

			bodies := sent()
			if len(bodies) != oc.Calls {
				t.Fatalf("calls = %d, the ops port made %d", len(bodies), oc.Calls)
			}
			for i := range bodies {
				if bodies[i] != oc.SentBodies[i] {
					t.Errorf("request %d differs from the ops port's bytes:\n shared %s\n ops    %s", i, bodies[i], oc.SentBodies[i])
				}
			}
			class, _ := llm.ClassOf(err)
			if string(class) != oc.ErrorClass {
				t.Errorf("error class = %q, ops port %q (err %v)", class, oc.ErrorClass, err)
			}
			if oc.ErrorClass != "" {
				var typed *llm.Error
				if !errors.As(err, &typed) || int64(typed.RetryAfter) != oc.RetryAfterNanos {
					t.Errorf("err %v: RetryAfter differs from the ops port's %v", err, time.Duration(oc.RetryAfterNanos))
				}
			} else {
				if err != nil {
					t.Fatalf("err = %v, the ops port succeeded", err)
				}
				if result.Text != oc.ResultText || result.Model != oc.ResultModel {
					t.Errorf("result %q/%q, ops port %q/%q", result.Text, result.Model, oc.ResultText, oc.ResultModel)
				}
				if !samePointer(result.InputTokens, oc.InputTokens) || !samePointer(result.OutputTokens, oc.OutputTokens) ||
					!samePointer(result.CachedInputTokens, oc.CachedTokens) {
					t.Errorf("usage %v/%v/%v differs from the ops port's %v/%v/%v",
						result.InputTokens, result.OutputTokens, result.CachedInputTokens, oc.InputTokens, oc.OutputTokens, oc.CachedTokens)
				}
			}

			// The same case through the ops port still in this package: the
			// ops-side failure vocabulary every caller switches on is unchanged.
			oldServer, _ := scriptedSyncServer(t, oc.Steps)
			old := NewOpenAIProvider(OpenAIProviderConfig{
				APIKey: secrets.NewHidden(oracleAPIKey), BaseURL: oldServer.URL, Model: oc.Model, MaxOutputTokens: oc.ConfigFloor,
			})
			oldResult, oldErr := old.Complete(context.Background(), request)
			if got, want := FailureClass(err), FailureClass(oldErr); got != want {
				t.Errorf("FailureClass = %q, ops port %q", got, want)
			}
			if got, want := IsDeterministicFailure(err), IsDeterministicFailure(oldErr); got != want {
				t.Errorf("IsDeterministicFailure = %v, ops port %v", got, want)
			}
			gotClass, gotLLM := ClassifyLLMError(err)
			wantClass, wantLLM := ClassifyLLMError(oldErr)
			if gotClass != wantClass || gotLLM != wantLLM {
				t.Errorf("ClassifyLLMError = %v/%v, ops port %v/%v", gotClass, gotLLM, wantClass, wantLLM)
			}
			if oldErr == nil && (oldResult.Text != result.Text || oldResult.Model != result.Model) {
				t.Errorf("result %q/%q, live ops port %q/%q", result.Text, result.Model, oldResult.Text, oldResult.Model)
			}
			compared++
		})
	}
	if compared != len(oracle.Cases) {
		t.Fatalf("compared %d of %d cases", compared, len(oracle.Cases))
	}
}

type sharedBatchOracle struct {
	Header map[string]string `json:"header"`
	Lines  []struct {
		Name         string          `json:"name"`
		Model        string          `json:"model"`
		ConfigFloor  int             `json:"config_max_output_tokens"`
		CustomID     string          `json:"custom_id"`
		System       string          `json:"system"`
		Prompt       string          `json:"prompt"`
		FormatName   string          `json:"format_name"`
		Schema       json.RawMessage `json:"schema"`
		RequestFloor int             `json:"request_max_output_tokens"`
		OpsLine      string          `json:"ops_line"`
	} `json:"lines"`
	Parse []struct {
		Name       string          `json:"name"`
		Text       string          `json:"text"`
		OpsError   bool            `json:"ops_error"`
		OpsResults json.RawMessage `json:"ops_results"`
	} `json:"parse"`
	Statuses map[string]map[string]string `json:"statuses"`
}

// batchServer is a scripted OpenAI Batch API: it records the uploaded file,
// its name and the create body, and answers a retrieve with status and an
// output file whose content is output.
type batchServer struct {
	server   *httptest.Server
	mu       sync.Mutex
	upload   string
	fileName string
	create   map[string]any
}

func newBatchServer(t *testing.T, status, output string) *batchServer {
	t.Helper()
	b := &batchServer{}
	b.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/files":
			_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			reader := multipart.NewReader(r.Body, params["boundary"])
			for {
				part, err := reader.NextPart()
				if err != nil {
					break
				}
				if part.FormName() == "file" {
					raw, _ := io.ReadAll(part)
					b.mu.Lock()
					b.upload, b.fileName = string(raw), part.FileName()
					b.mu.Unlock()
				}
			}
			_, _ = w.Write([]byte(`{"id":"file-in"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/batches":
			var create map[string]any
			_ = json.NewDecoder(r.Body).Decode(&create)
			b.mu.Lock()
			b.create = create
			b.mu.Unlock()
			_, _ = w.Write([]byte(`{"id":"batch-1"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/batches/batch-1":
			encoded, _ := json.Marshal(map[string]any{"id": "batch-1", "status": status, "output_file_id": "file-out"})
			_, _ = w.Write(encoded)
		case r.Method == http.MethodGet && r.URL.Path == "/files/file-out/content":
			_, _ = w.Write([]byte(output))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(b.server.Close)
	return b
}

func sharedBatchFor(t *testing.T, baseURL, model string, configFloor int) BatchProvider {
	t.Helper()
	shared, err := NewSharedProvider(SharedProviderConfig{
		Kind: ProviderKindOpenAI, APIKey: secrets.NewHidden(oracleAPIKey), BaseURL: baseURL, Model: model, MaxOutputTokens: configFloor,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, ok := AsBatchProvider(shared)
	if !ok {
		t.Fatal("the shared OpenAI provider has no Batch API")
	}
	return batch
}

func TestSharedOpenAIReplaysTheOpsPortBatchOracle(t *testing.T) {
	var oracle sharedBatchOracle
	loadSharedOracle(t, "openai_batch.oracle.json", sharedBatchOracleDigest, &oracle)
	if oracle.Header["ops_sha"] != sharedOracleOpsSHA || len(oracle.Lines) != 8 || len(oracle.Parse) != 21 || len(oracle.Statuses) != 12 {
		t.Fatalf("header %v lines=%d parse=%d statuses=%d: not the 8/21/12 recording", oracle.Header,
			len(oracle.Lines), len(oracle.Parse), len(oracle.Statuses))
	}
	compared := 0
	for _, lc := range oracle.Lines {
		t.Run("lines/"+lc.Name, func(t *testing.T) {
			server := newBatchServer(t, "completed", "")
			batch := sharedBatchFor(t, server.server.URL, lc.Model, lc.ConfigFloor)
			submission, err := batch.SubmitBatch(context.Background(), []BatchItem{{
				CustomID: lc.CustomID,
				Request: CompletionRequest{
					Prompt: lc.Prompt, SystemMessage: lc.System, ResponseFormatName: lc.FormatName,
					JSONSchema: decodeOracleSchema(t, lc.Schema), MaxOutputTokens: lc.RequestFloor,
				},
			}})
			if err != nil {
				t.Fatal(err)
			}
			server.mu.Lock()
			defer server.mu.Unlock()
			if server.upload != lc.OpsLine {
				t.Errorf("uploaded line differs from the ops port's bytes:\n shared %s\n ops    %s", server.upload, lc.OpsLine)
			}
			// The ops port's file name and metadata (openaibatch.go at ops_sha).
			if server.fileName != "investment-categorization-batch.jsonl" {
				t.Errorf("file name = %q, ops port investment-categorization-batch.jsonl", server.fileName)
			}
			metadata, _ := server.create["metadata"].(map[string]any)
			if len(metadata) != 1 || metadata["source"] != "investment_materialize" {
				t.Errorf("batch metadata = %v, ops port {source: investment_materialize}", server.create["metadata"])
			}
			if server.create["endpoint"] != "/v1/responses" || server.create["completion_window"] != "24h" || server.create["input_file_id"] != "file-in" {
				t.Errorf("batch create = %v", server.create)
			}
			if submission != (BatchSubmission{ProviderJobID: "batch-1", InputFileID: "file-in", ItemCount: 1}) {
				t.Errorf("submission = %+v", submission)
			}
			compared++
		})
	}
	for _, pc := range oracle.Parse {
		t.Run("parse/"+pc.Name, func(t *testing.T) {
			server := newBatchServer(t, "completed", pc.Text)
			batch := sharedBatchFor(t, server.server.URL, "gpt-5-nano", 0)
			results, err := batch.FetchBatchResults(context.Background(), "batch-1")
			if (err != nil) != pc.OpsError {
				t.Fatalf("error = %v, ops port error = %v", err, pc.OpsError)
			}
			if err == nil {
				var ops []BatchItemResult
				if uerr := json.Unmarshal(pc.OpsResults, &ops); uerr != nil {
					t.Fatal(uerr)
				}
				got, _ := json.Marshal(results)
				want, _ := json.Marshal(ops)
				if !bytes.Equal(got, want) {
					t.Errorf("results differ from the ops port:\n shared %s\n ops    %s", got, want)
				}
			}
			compared++
		})
	}
	for status, want := range oracle.Statuses {
		t.Run("status/"+status, func(t *testing.T) {
			server := newBatchServer(t, status, "")
			batch := sharedBatchFor(t, server.server.URL, "gpt-5-nano", 0)
			state, err := batch.PollBatch(context.Background(), "batch-1")
			if err != nil {
				t.Fatal(err)
			}
			if state.ProviderStatus != status || state.OutputFileID != "file-out" {
				t.Errorf("state = %+v", state)
			}
			if status == "cancelling" {
				// Named improvement: a cancel in progress can still deliver
				// results, so polling goes on (the ops port folded it into
				// cancelled and stopped).
				if string(state.Status) == want["ops"] || state.Status != BatchJobCancelling || state.Status.Terminal() {
					t.Errorf("cancelling = %q terminal=%v, want the non-terminal cancelling (ops port %q)",
						state.Status, state.Status.Terminal(), want["ops"])
				}
			} else if string(state.Status) != want["ops"] {
				t.Errorf("status %q -> %q, ops port %q", status, state.Status, want["ops"])
			}
			compared++
		})
	}
	if total := len(oracle.Lines) + len(oracle.Parse) + len(oracle.Statuses); compared != total {
		t.Fatalf("compared %d of %d cases", compared, total)
	}
	if !strings.Contains(oracle.Header["producer"], "ops Go port") {
		t.Fatalf("producer %q is not the ops port", oracle.Header["producer"])
	}
}
