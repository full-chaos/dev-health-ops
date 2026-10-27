package investment

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The planted token is a marker, never a real credential. These tests reach no
// network: the clef client is built over a fake round tripper.
const (
	testClefToken   = "cfut_PLANTED0123456789abcdefTOKENmark"
	testClefAccount = "0123456789abcdef0123456789abcdef"
	testClefBase    = "https://api.cloudflare.com/client/v4/accounts/" + testClefAccount + "/ai"
	testClefPost    = testClefBase + "/run/@cf/cloudflare/clef"
)

type clefNetwork struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	reply    func(call int, body []byte) (int, []byte)
	// header is added to every response.
	header http.Header
}

func (network *clefNetwork) RoundTrip(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	network.mu.Lock()
	network.requests = append(network.requests, req)
	network.bodies = append(network.bodies, body)
	call := len(network.requests)
	network.mu.Unlock()
	status, payload := network.reply(call, body)
	header := http.Header{}
	for key, values := range network.header {
		header[key] = values
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(bytes.NewReader(payload))}, nil
}

func (network *clefNetwork) count() int {
	network.mu.Lock()
	defer network.mu.Unlock()
	return len(network.requests)
}

// clefAnswers answers every question of the request like the clef API does.
func clefAnswers(model string) func(int, []byte) (int, []byte) {
	return func(_ int, body []byte) (int, []byte) {
		return http.StatusOK, jevAnswerBody(body, jevReply{supported: map[string]int{"quality.bugfix": 3}, inputTokens: 2644, model: model})
	}
}

// clefBYO builds the production orgCategorizeBYO over stubs and the fake network.
type clefBYO struct {
	network   *clefNetwork
	cfg       llmorgsettings.OrgConfig
	logs      *bytes.Buffer
	clefBuilt int
	provBuilt int
	guards    []*llmbudget.Guard
	store     llmbudget.Store
}

func newClefBYO(reply func(int, []byte) (int, []byte)) *clefBYO {
	return &clefBYO{
		network: &clefNetwork{reply: reply},
		cfg: llmorgsettings.OrgConfig{Purpose: llmorgsettings.PurposeCategorize, Provider: "clef", Model: "clef",
			BaseURL: testClefBase, APIKey: secrets.NewHidden(testClefToken)},
		logs: &bytes.Buffer{},
	}
}

func (stub *clefBYO) build() *orgCategorizeBYO {
	logger := slog.New(slog.NewJSONHandler(stub.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return &orgCategorizeBYO{
		logger:  logger,
		optedIn: func(context.Context, string) (bool, error) { return true, nil },
		gate: func(context.Context, string) llmorgsettings.Decision {
			return llmorgsettings.Decision{Kind: llmorgsettings.Own}
		},
		config: func(context.Context, string) (llmorgsettings.OrgConfig, bool, error) {
			return stub.cfg, true, nil
		},
		newProvider: func(categorize.ProviderKind, string, string, string) (categorize.Provider, error) {
			stub.provBuilt++
			return &stubOrgModel{model: stub.cfg.Model, reply: answerAllQuestions}, nil
		},
		newClef: func(cfg categorize.ClefClientConfig) (*categorize.TypeSafeClient, error) {
			stub.clefBuilt++
			cfg.HTTPClient = &http.Client{Transport: stub.network}
			cfg.Logger = logger
			return categorize.NewClefClient(cfg)
		},
		clefBudget: func(orgID, model, postURL string) *llmbudget.Guard {
			if stub.store == nil {
				return nil
			}
			guard := newClefGuard(stub.store, orgID, model, postURL)
			stub.guards = append(stub.guards, guard)
			return guard
		},
	}
}

// The org's clef account serves the categorization: the request goes to its
// account URL with its token, carries the SystemOne body as the jev backend
// builds it (only the model differs), and the rows carry the byo-clef identity.
func TestAnOrgClefConfigIsServedThroughTheClefTarget(t *testing.T) {
	stub := newClefBYO(clefAnswers("clef"))
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = served.Close() }()
	m := &Materializer{}
	m.SetServed(served)
	bundle := shadowTestBundle(t, "u1", 6)
	outcome, err := m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", bundle))
	if err != nil || outcome.LLMModel != "clef" {
		t.Fatalf("categorize: %+v %v", outcome, err)
	}
	req := stub.network.requests[0]
	if req.Method != http.MethodPost || req.URL.String() != testClefPost || req.Header.Get("Authorization") != "Bearer "+testClefToken {
		t.Fatalf("request = %s %s auth %q", req.Method, req.URL, req.Header.Get("Authorization"))
	}
	rubric, err := decision.LoadRubric()
	if err != nil {
		t.Fatal(err)
	}
	built, err := decision.BuildRequest(rubric, "clef", bundle)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stub.network.bodies[0], built.Body) {
		t.Fatal("the clef request body is not the SystemOne body of the decision adapter")
	}
	if provider, model := m.usageIdentity(Config{}); provider != "byo-clef" || model != "clef" {
		t.Fatalf("usage identity = %q/%q", provider, model)
	}
	if stamp := m.modelVersion(Config{}); !strings.Contains(stamp, "provider=byo-clef;") || !strings.Contains(stamp, "model=clef;") {
		t.Fatalf("stamp = %q", stamp)
	}
	if stub.provBuilt != 0 {
		t.Fatal("a chat provider was built for a clef config")
	}
}

// The target is chosen by the saved provider only, never by the host of the
// base_url, and every refused clef config builds nothing and sends nothing.
func TestTheClefTargetIsChosenByTheProviderAndEveryRefusalSendsNothing(t *testing.T) {
	other := newClefBYO(clefAnswers("clef"))
	other.cfg.Provider, other.cfg.Model = "openai", "gpt-x"
	if served, err := other.build().Build(context.Background(), "org-1"); err != nil || served == nil {
		t.Fatalf("an openai config with the clef URL: %v", err)
	}
	if other.clefBuilt != 0 || other.provBuilt != 1 || other.network.count() != 0 {
		t.Fatalf("the host chose the target: clef built %d, chat built %d, sent %d", other.clefBuilt, other.provBuilt, other.network.count())
	}
	for name, tc := range map[string]struct {
		mutate func(*llmorgsettings.OrgConfig)
		want   string
	}{
		"model not served": {func(c *llmorgsettings.OrgConfig) { c.Model = "clef-pro" }, llmorgsettings.CodeCategorizeBYOModelUnsupported},
		"model traversal":  {func(c *llmorgsettings.OrgConfig) { c.Model = "clef/../x" }, llmorgsettings.CodeCategorizeBYOModelUnsupported},
		"jev host":         {func(c *llmorgsettings.OrgConfig) { c.BaseURL = "https://api.typesafe.ai" }, "invalid_base_url"},
		"other host": {func(c *llmorgsettings.OrgConfig) {
			c.BaseURL = "https://example.com/client/v4/accounts/" + testClefAccount + "/ai"
		}, "invalid_base_url"},
		"trailing dot host": {func(c *llmorgsettings.OrgConfig) {
			c.BaseURL = "https://api.cloudflare.com./client/v4/accounts/" + testClefAccount + "/ai"
		}, "invalid_base_url"},
		"no base url":     {func(c *llmorgsettings.OrgConfig) { c.BaseURL = "" }, "invalid_base_url"},
		"token too short": {func(c *llmorgsettings.OrgConfig) { c.APIKey = secrets.NewHidden("short") }, llmorgsettings.CodeCategorizeBYOUnavailable},
		"token missing":   {func(c *llmorgsettings.OrgConfig) { c.APIKey = secrets.NewHidden("") }, llmorgsettings.CodeCategorizeBYOUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			stub := newClefBYO(clefAnswers("clef"))
			tc.mutate(&stub.cfg)
			served, err := stub.build().Build(context.Background(), "org-1")
			var refusal *BYORefusal
			if !errors.As(err, &refusal) || served != nil || refusal.Code != tc.want {
				t.Fatalf("Build = %v, %v; want the refusal %s", served, err, tc.want)
			}
			if stub.network.count() != 0 || stub.provBuilt != 0 {
				t.Fatalf("a refused config sent %d requests / built %d chat providers", stub.network.count(), stub.provBuilt)
			}
			if strings.Contains(err.Error(), testClefToken) || strings.Contains(stub.logs.String(), testClefToken) {
				t.Fatal("the token is in the refusal or the log")
			}
		})
	}
}

// The response model rules of the org path hold for clef: another model's
// answer and an answer that names no model are refused (11076), the account's
// own dated suffix is accepted.
func TestAClefAnswerFromAnotherOrNoModelIsRefused(t *testing.T) {
	cases := map[string]struct {
		model   string
		wantErr bool
	}{"same": {"clef", false}, "dated suffix": {"clef-20261024", false}, "jev": {"jev-1.13.0", true}, "flash": {"clef-flash", true}}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			stub := newClefBYO(clefAnswers(tc.model))
			served, err := stub.build().Build(context.Background(), "org-1")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = served.Close() }()
			m := &Materializer{}
			m.SetServed(served)
			_, err = m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6)))
			if (err != nil) != tc.wantErr {
				t.Fatalf("model %q: err = %v, want error %v", tc.model, err, tc.wantErr)
			}
		})
	}
	stub := newClefBYO(func(_ int, body []byte) (int, []byte) {
		return http.StatusOK, bytes.Replace(jevAnswerBody(body, jevReply{supported: map[string]int{"quality.bugfix": 3}, inputTokens: 1}), []byte(`"model":"clef"`), []byte(`"model":""`), 1)
	})
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = served.Close() }()
	m := &Materializer{logger: testLogger()}
	m.SetServed(served)
	outcomes := map[int]categorize.CategorizationOutcome{}
	stats := Stats{LLMFailureCounts: map[string]int{}}
	cfg := shadowTestConfig()
	cfg.LLMConcurrency = 1
	err = m.categorizePending(context.Background(), cfg, shadowTestEntries(t, "u1", "u2"), outcomes, &stats)
	if err == nil || !strings.Contains(err.Error(), llmorgsettings.CodeCategorizeBYOModelUnreported) || stub.network.count() != 1 {
		t.Fatalf("an empty model: err=%v requests=%d, want the closed code after one request", err, stub.network.count())
	}
}

// Failures and a planted token: the token is in no error text and no log line,
// whatever the peer sends back.
func TestTheClefTokenIsInNoErrorOrLogOfTheServedPath(t *testing.T) {
	for name, reply := range map[string]func(int, []byte) (int, []byte){
		"server error": func(int, []byte) (int, []byte) { return 500, []byte(testClefToken) },
		"auth":         func(int, []byte) (int, []byte) { return 403, []byte("denied " + testClefToken) },
		"not json":     func(int, []byte) (int, []byte) { return 200, []byte(testClefToken) },
	} {
		t.Run(name, func(t *testing.T) {
			stub := newClefBYO(reply)
			served, err := stub.build().Build(context.Background(), "org-1")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = served.Close() }()
			m := &Materializer{logger: testLogger()}
			m.SetServed(served)
			_, runErr := m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6)))
			texts := []string{stub.logs.String()}
			if runErr != nil {
				texts = append(texts, runErr.Error())
			}
			for _, text := range texts {
				for i := 0; i+12 <= len(testClefToken); i++ {
					if strings.Contains(text, testClefToken[i:i+12]) {
						t.Fatalf("a piece of the token is in %q", text)
					}
				}
			}
		})
	}
}

type admitRecorder struct {
	mu       sync.Mutex
	admits   []llmbudget.AdmitRequest
	settles  int
	capped   bool
	refusals int
}

func (store *admitRecorder) Admit(_ context.Context, request llmbudget.AdmitRequest) (llmbudget.Admission, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.admits = append(store.admits, request)
	if request.MaxCost == nil {
		if store.capped {
			store.refusals++
			return llmbudget.Admission{}, errors.New("BYO LLM pricing is unavailable for the configured budget.")
		}
		return llmbudget.Admission{Unbudgeted: true}, nil
	}
	return llmbudget.Admission{ID: "r1", BudgetConfigured: store.capped}, nil
}

func (store *admitRecorder) Settle(context.Context, llmbudget.SettleRequest) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.settles++
	return nil
}

// Each clef request is priced by the URL it is really sent to, for both
// models; a request the book does not price is refused under a cap.
func TestAClefRequestIsReservedAtItsRealPriceForBothModels(t *testing.T) {
	for _, model := range []string{"clef", "clef-flash"} {
		t.Run(model, func(t *testing.T) {
			stub := newClefBYO(clefAnswers(model))
			stub.cfg.Model = model
			recorder := &admitRecorder{capped: true}
			stub.store = recorder
			served, err := stub.build().Build(context.Background(), "org-1")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = served.Close() }()
			m := &Materializer{}
			m.SetServed(served)
			if _, err := m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6))); err != nil {
				t.Fatalf("categorize: %v", err)
			}
			if len(recorder.admits) != 1 || recorder.admits[0].MaxCost == nil || recorder.admits[0].Provider != "clef" ||
				recorder.admits[0].Model != model || recorder.settles != 1 {
				t.Fatalf("admits = %+v settles %d: the call was not priced and settled", recorder.admits, recorder.settles)
			}
			if !strings.HasSuffix(recorder.admits[0].BaseURL, "/ai/run/@cf/cloudflare/"+model) {
				t.Fatalf("the guard priced %q, not the request URL", recorder.admits[0].BaseURL)
			}
		})
	}
	recorder := &admitRecorder{capped: true}
	guard := newClefGuard(recorder, "org-1", "clef", "https://api.cloudflare.com/client/v4/accounts/"+testClefAccount+"/ai")
	err := guard.Run(context.Background(), llmbudget.Call{IdempotencyKey: "k", MaxInputTokens: 1000}, func(context.Context) (llmbudget.Usage, error) {
		t.Fatal("an unpriced call was invoked under a cap")
		return llmbudget.Usage{}, nil
	})
	if err == nil || recorder.refusals != 1 {
		t.Fatalf("an unpriced clef call under a cap: err=%v refusals=%d", err, recorder.refusals)
	}
}

// No test of this package builds the clef client other than through the
// helper above, which always supplies the fake network.
func TestNoTestBuildsAClefClientOutsideTheFakeNetworkHelper(t *testing.T) {
	needle := "categorize.New" + "ClefClient("
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if n := strings.Count(string(data), needle); n > 0 && file != "clefbyo_test.go" {
			t.Fatalf("%s calls the clef client constructor %d times", file, n)
		} else if file == "clefbyo_test.go" && n != 1 {
			t.Fatalf("clefbyo_test.go calls the constructor %d times, want the one helper", n)
		}
	}
}
