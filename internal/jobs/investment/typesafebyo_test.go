package investment

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The planted keys are markers, never real credentials. These tests reach no
// network: the TypeSafe client is built over a fake round tripper.
const (
	testTypeSafeKey      = "tsk_PLANTED0123456789abcdefKEYmark"
	testPlatformJevKey   = "tsk_PLATFORMplanted0123456789abcdef"
	testTypeSafePost     = "https://api.typesafe.ai/v1/systemone"
	testTypeSafeModel    = "jev-1.13.0"
	testTypeSafeInTokens = 2644
)

// newTypeSafeTestClient is the one place a test of this package builds an
// org TypeSafe client: always over the fake network.
func newTypeSafeTestClient(cfg categorize.OrgTypeSafeClientConfig, network *clefNetwork, logger *slog.Logger) (*categorize.TypeSafeClient, error) {
	cfg.HTTPClient = &http.Client{Transport: network}
	cfg.Logger = logger
	return categorize.NewOrgTypeSafeClient(cfg)
}

func typeSafeAnswers(model string) func(int, []byte) (int, []byte) {
	return func(_ int, body []byte) (int, []byte) {
		return http.StatusOK, jevAnswerBody(body, jevReply{supported: map[string]int{"quality.bugfix": 3}, inputTokens: testTypeSafeInTokens, model: model})
	}
}

// typeSafeBYO builds the production orgCategorizeBYO over stubs and the fake network.
type typeSafeBYO struct {
	network   *clefNetwork
	cfg       llmorgsettings.OrgConfig
	logs      *bytes.Buffer
	tsBuilt   int
	clefBuilt int
	provBuilt int
	store     llmbudget.Store
}

func newTypeSafeBYO(reply func(int, []byte) (int, []byte)) *typeSafeBYO {
	return &typeSafeBYO{
		network: &clefNetwork{reply: reply, header: http.Header{"X-Typesafe-Request-Id": {"req-typesafe-1"}}},
		cfg: llmorgsettings.OrgConfig{Purpose: llmorgsettings.PurposeCategorize, Provider: "typesafe", Model: testTypeSafeModel,
			APIKey: secrets.NewHidden(testTypeSafeKey)},
		logs: &bytes.Buffer{},
	}
}

func (stub *typeSafeBYO) build() *orgCategorizeBYO {
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
		newClef: func(categorize.ClefClientConfig) (*categorize.TypeSafeClient, error) {
			stub.clefBuilt++
			return nil, errors.New("a typesafe config builds no clef client")
		},
		newTypeSafe: func(cfg categorize.OrgTypeSafeClientConfig) (*categorize.TypeSafeClient, error) {
			stub.tsBuilt++
			return newTypeSafeTestClient(cfg, stub.network, logger)
		},
		typeSafeBudget: func(orgID, model string) *llmbudget.Guard {
			if stub.store == nil {
				return nil
			}
			return newTypeSafeGuard(stub.store, orgID, model)
		},
	}
}

func (stub *typeSafeBYO) categorizeOne(t *testing.T) (*ServedDecision, *Materializer, error) {
	t.Helper()
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	m := &Materializer{logger: testLogger()}
	m.SetServed(served)
	_, err = m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6)))
	return served, m, err
}

// The org's TypeSafe account serves the categorization: the request goes to the
// fixed endpoint with the org's key (never the platform key or URL from the
// environment), carries the SystemOne body as the jev backend builds it, and
// the rows carry the byo-typesafe identity and the org credential source.
func TestAnOrgTypeSafeConfigIsServedThroughTheFixedEndpointWithTheOrgKey(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", testPlatformJevKey)
	t.Setenv("TYPESAFE_BASE_URL", "https://platform.example.invalid")
	t.Setenv("TYPESAFE_MODEL", "jev-1.14.0")
	stub := newTypeSafeBYO(typeSafeAnswers(testTypeSafeModel))
	served, m, err := stub.categorizeOne(t)
	defer func() { _ = served.Close() }()
	if err != nil {
		t.Fatalf("categorize: %v", err)
	}
	if stub.network.count() != 1 {
		t.Fatalf("requests = %d, want 1", stub.network.count())
	}
	req := stub.network.requests[0]
	if req.Method != http.MethodPost || req.URL.String() != testTypeSafePost || req.Header.Get("Authorization") != "Bearer "+testTypeSafeKey {
		t.Fatalf("request = %s %s, bearer is org key %v", req.Method, req.URL, req.Header.Get("Authorization") == "Bearer "+testTypeSafeKey)
	}
	if testTypeSafePost != llmorgsettings.TypeSafePostURL {
		t.Fatalf("TypeSafePostURL = %q", llmorgsettings.TypeSafePostURL)
	}
	rubric, err := decision.LoadRubric()
	if err != nil {
		t.Fatal(err)
	}
	built, err := decision.BuildRequest(rubric, testTypeSafeModel, shadowTestBundle(t, "u1", 6))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stub.network.bodies[0], built.Body) {
		t.Fatal("the TypeSafe request body is not the SystemOne body of the decision adapter")
	}
	if provider, model := m.usageIdentity(Config{}); provider != "byo-typesafe" || model != testTypeSafeModel {
		t.Fatalf("usage identity = %q/%q", provider, model)
	}
	if source := m.usageCredentialSource(); source != "org" {
		t.Fatalf("credential source = %q, want org", source)
	}
	if stamp := m.modelVersion(Config{}); !strings.Contains(stamp, "provider=byo-typesafe;") || !strings.Contains(stamp, "model="+testTypeSafeModel+";") {
		t.Fatalf("stamp = %q", stamp)
	}
	if stub.tsBuilt != 1 || stub.provBuilt != 0 || stub.clefBuilt != 0 {
		t.Fatalf("built typesafe %d, chat %d, clef %d", stub.tsBuilt, stub.provBuilt, stub.clefBuilt)
	}
}

// The attempt row of an org TypeSafe call carries the provider, the model, the
// request id and the TypeSafe rate; it names no clef endpoint.
func TestAnOrgTypeSafeAttemptRowCarriesTheProviderAndTheTypeSafeRate(t *testing.T) {
	stub := newTypeSafeBYO(typeSafeAnswers(testTypeSafeModel))
	served, _, err := stub.categorizeOne(t)
	defer func() { _ = served.Close() }()
	if err != nil {
		t.Fatalf("categorize: %v", err)
	}
	rows := servedAttempts(t, served)
	if len(rows) != 1 {
		t.Fatalf("attempt rows = %d", len(rows))
	}
	row := rows[0]
	if row.Provider != "byo-typesafe" || row.APIMode != "systemone" || row.ModelRequested != testTypeSafeModel || row.ModelReturned != testTypeSafeModel ||
		row.HTTPStatus != http.StatusOK || row.RequestID != "req-typesafe-1" || row.InputTokens != testTypeSafeInTokens {
		t.Fatalf("attempt row = %+v", row)
	}
	if want := float64(testTypeSafeInTokens*42) / 1e9; row.BilledCostUSD != want || row.RatesVersion != "typesafe-systemone-2026-10-06" {
		t.Fatalf("billed %.9f under %q, want %.9f under typesafe-systemone-2026-10-06", row.BilledCostUSD, row.RatesVersion, want)
	}
	if row.EndpointType != "" || row.GatewayID != "" {
		t.Fatalf("endpoint %q gateway %q, want empty", row.EndpointType, row.GatewayID)
	}
}

// Every refused TypeSafe config builds nothing and sends nothing: an unlisted
// or moving model, any base_url, a missing or short key.
func TestEveryRefusedOrgTypeSafeConfigSendsNothing(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*llmorgsettings.OrgConfig)
		want   string
		built  int
	}{
		"latest alias":   {func(c *llmorgsettings.OrgConfig) { c.Model = "jev-latest" }, llmorgsettings.CodeCategorizeBYOModelUnsupported, 0},
		"preview alias":  {func(c *llmorgsettings.OrgConfig) { c.Model = "jev-preview" }, llmorgsettings.CodeCategorizeBYOModelUnsupported, 0},
		"unlisted model": {func(c *llmorgsettings.OrgConfig) { c.Model = "jev-1.14.0" }, llmorgsettings.CodeCategorizeBYOModelUnsupported, 0},
		"clef model":     {func(c *llmorgsettings.OrgConfig) { c.Model = "clef" }, llmorgsettings.CodeCategorizeBYOModelUnsupported, 0},
		"jev base url":   {func(c *llmorgsettings.OrgConfig) { c.BaseURL = "https://api.typesafe.ai" }, "invalid_base_url", 0},
		"other base url": {func(c *llmorgsettings.OrgConfig) { c.BaseURL = "https://example.com/v1" }, "invalid_base_url", 0},
		"blank base url": {func(c *llmorgsettings.OrgConfig) { c.BaseURL = " " }, "invalid_base_url", 0},
		"key too short":  {func(c *llmorgsettings.OrgConfig) { c.APIKey = secrets.NewHidden("short") }, llmorgsettings.CodeCategorizeBYOUnavailable, 1},
		"key missing":    {func(c *llmorgsettings.OrgConfig) { c.APIKey = secrets.NewHidden("") }, llmorgsettings.CodeCategorizeBYOUnavailable, 1},
		"spelled, alias": {func(c *llmorgsettings.OrgConfig) { c.Provider = " TypeSafe "; c.Model = "jev-latest" }, llmorgsettings.CodeCategorizeBYOModelUnsupported, 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TYPESAFE_API_KEY", testPlatformJevKey)
			stub := newTypeSafeBYO(typeSafeAnswers(testTypeSafeModel))
			tc.mutate(&stub.cfg)
			served, err := stub.build().Build(context.Background(), "org-1")
			var refusal *BYORefusal
			if !errors.As(err, &refusal) || served != nil || refusal.Code != tc.want {
				t.Fatalf("Build = %v, %v; want the refusal %s", served, err, tc.want)
			}
			if stub.network.count() != 0 || stub.provBuilt != 0 || stub.clefBuilt != 0 || stub.tsBuilt != tc.built {
				t.Fatalf("sent %d, chat %d, clef %d, typesafe %d (want %d)", stub.network.count(), stub.provBuilt, stub.clefBuilt, stub.tsBuilt, tc.built)
			}
			if strings.Contains(err.Error(), testTypeSafeKey) || strings.Contains(stub.logs.String(), testTypeSafeKey) {
				t.Fatal("the key is in the refusal or the log")
			}
		})
	}
}

// A failed call (bad key, server error, not JSON) ends the unit; no piece of the
// key is in any error or log line, and no platform jev request is made.
func TestTheOrgTypeSafeKeyIsInNoErrorOrLogAndABadKeyStopsTheRun(t *testing.T) {
	for name, reply := range map[string]func(int, []byte) (int, []byte){
		"bad key":      func(int, []byte) (int, []byte) { return 401, []byte(`{"error":"invalid key ` + testTypeSafeKey + `"}`) },
		"forbidden":    func(int, []byte) (int, []byte) { return 403, []byte("denied " + testTypeSafeKey) },
		"server error": func(int, []byte) (int, []byte) { return 500, []byte(testTypeSafeKey) },
		"not json":     func(int, []byte) (int, []byte) { return 200, []byte(testTypeSafeKey) },
	} {
		t.Run(name, func(t *testing.T) {
			stub := newTypeSafeBYO(reply)
			served, _, runErr := stub.categorizeOne(t)
			defer func() { _ = served.Close() }()
			if runErr == nil {
				t.Fatal("a failed TypeSafe call gave an outcome")
			}
			for _, req := range stub.network.requests {
				if req.URL.String() != testTypeSafePost || req.Header.Get("Authorization") != "Bearer "+testTypeSafeKey {
					t.Fatalf("a request went to %s with another bearer", req.URL)
				}
			}
			for _, text := range []string{stub.logs.String(), runErr.Error()} {
				for i := 0; i+12 <= len(testTypeSafeKey); i++ {
					if strings.Contains(text, testTypeSafeKey[i:i+12]) {
						t.Fatalf("a piece of the key is in %q", text)
					}
				}
			}
		})
	}
}

// Each TypeSafe request is reserved at its real price on the URL it is sent
// to, under the "typesafe:" idempotency prefix; every listed model is priced,
// and an unpriced one is refused under a cap before any request.
func TestAnOrgTypeSafeRequestIsReservedAtItsRealPrice(t *testing.T) {
	stub := newTypeSafeBYO(typeSafeAnswers(testTypeSafeModel))
	recorder := &admitRecorder{capped: true}
	stub.store = recorder
	served, _, err := stub.categorizeOne(t)
	defer func() { _ = served.Close() }()
	if err != nil {
		t.Fatalf("categorize: %v", err)
	}
	if len(recorder.admits) != 1 || recorder.settles != 1 {
		t.Fatalf("admits %d settles %d", len(recorder.admits), recorder.settles)
	}
	admit := recorder.admits[0]
	if admit.MaxCost == nil || admit.Provider != "typesafe" || admit.Model != testTypeSafeModel || admit.BaseURL != testTypeSafePost ||
		!strings.HasPrefix(admit.IdempotencyKey, "typesafe:") {
		t.Fatalf("admit = %+v: not priced on the request URL under the typesafe prefix", admit)
	}
	for _, model := range llmorgsettings.TypeSafeModels() {
		if price, err := llmbudget.PriceAt("typesafe", model, llmorgsettings.TypeSafePostURL, admit.Now); err != nil || price == nil {
			t.Fatalf("listed model %s is unpriced: %v %v", model, price, err)
		}
	}
	capped := &admitRecorder{capped: true}
	guard := newTypeSafeGuard(capped, "org-1", "jev-1.14.0")
	err = guard.Run(context.Background(), llmbudget.Call{IdempotencyKey: "k", MaxInputTokens: 1000}, func(context.Context) (llmbudget.Usage, error) {
		t.Fatal("an unpriced call was invoked under a cap")
		return llmbudget.Usage{}, nil
	})
	if err == nil || capped.refusals != 1 {
		t.Fatalf("an unpriced TypeSafe call under a cap: err=%v refusals=%d", err, capped.refusals)
	}
}

// The clef path keeps its own reservation prefix.
func TestAClefReservationKeepsTheClefPrefix(t *testing.T) {
	stub := newClefBYO(clefAnswers("clef"))
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
		t.Fatal(err)
	}
	if len(recorder.admits) != 1 || !strings.HasPrefix(recorder.admits[0].IdempotencyKey, "clef:") {
		t.Fatalf("admits = %+v", recorder.admits)
	}
}

// No test of this package builds the org TypeSafe client other than through
// the helper above, which always supplies the fake network.
func TestNoTestBuildsAnOrgTypeSafeClientOutsideTheFakeNetworkHelper(t *testing.T) {
	needle := "categorize.New" + "OrgTypeSafeClient("
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if n := strings.Count(string(data), needle); n > 0 && file != "typesafebyo_test.go" {
			t.Fatalf("%s calls the org TypeSafe client constructor %d times", file, n)
		} else if file == "typesafebyo_test.go" && n != 1 {
			t.Fatalf("typesafebyo_test.go calls the constructor %d times, want the one helper", n)
		}
	}
}

// An opted-in org whose categorize config names TypeSafe with a listed model and
// its own key gets its own backend; it is not refused as an unknown provider.
func TestAnOrgTypeSafeConfigIsNotRefusedAsAnUnknownProvider(t *testing.T) {
	stub := &byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Own}, found: true,
		cfg: llmorgsettings.OrgConfig{Purpose: llmorgsettings.PurposeCategorize, Provider: "typesafe", Model: "jev-1.13.0",
			APIKey: secrets.NewHidden("tsk_PLANTED0123456789abcdefKEYmark")}}
	served, err := stub.build().Build(context.Background(), "org-1")
	var refusal *BYORefusal
	if errors.As(err, &refusal) {
		t.Fatalf("Build refused the org's TypeSafe config: %s", refusal.Code)
	}
	if err != nil || served == nil {
		t.Fatalf("Build = %v, %v", served, err)
	}
	defer func() { _ = served.Close() }()
	if provider, model := served.Identity(); provider != "byo-typesafe" || model != "jev-1.13.0" {
		t.Fatalf("identity = %q/%q, want byo-typesafe/jev-1.13.0", provider, model)
	}
	if served.CredentialSource() != "org" {
		t.Fatalf("credential source = %q, want org", served.CredentialSource())
	}
}
