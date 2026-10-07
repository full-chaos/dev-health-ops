package investment

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
)

// roundTripRecorder is a fake HTTP transport: it records every request and
// answers from the request it got. Nothing leaves the process.
type roundTripRecorder struct {
	mu       sync.Mutex
	requests []*http.Request
}

func (recorder *roundTripRecorder) RoundTrip(request *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(request.Body)
	recorder.mu.Lock()
	recorder.requests = append(recorder.requests, request)
	recorder.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(bytes.NewReader(jevAnswerBody(body, okReply()))), Request: request,
	}, nil
}

func (recorder *roundTripRecorder) count() int {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return len(recorder.requests)
}

func clearShadowEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		EnvShadowProvider, EnvShadowOrgIDs, EnvShadowSamplePercent, EnvShadowConcurrency,
		EnvShadowMaxSeconds, EnvShadowMaxUSDPerRun, "TYPESAFE_API_KEY", "TYPESAFE_BASE_URL", "TYPESAFE_MODEL",
	} {
		t.Setenv(name, "")
	}
}

func shadowTestExecutor(t *testing.T, logs *syncBuffer) (*NativeExecutor, *roundTripRecorder) {
	t.Helper()
	executor, err := NewNativeExecutor(unusedReader(t), unusedWriter(t), debugLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	recorder := &roundTripRecorder{}
	executor.SetShadowHTTPClientForTest(&http.Client{Transport: recorder})
	return executor, recorder
}

// Default OFF. With every flag off -- and also with the key present -- the
// executor builds no phase, so no client exists and no request is possible.
// Each "almost on" configuration is off too, and none of them tries to build a
// client (a build attempt without a key would log its refusal).
func TestWithTheSwitchOffNoShadowClientIsBuilt(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		org  string
	}{
		{"nothing set", nil, "org-a"},
		{"only the key", map[string]string{"TYPESAFE_API_KEY": shadowTestKeyValue}, "org-a"},
		{"key and org list, no provider", map[string]string{"TYPESAFE_API_KEY": shadowTestKeyValue, EnvShadowOrgIDs: "*"}, "org-a"},
		{"provider with no org list", map[string]string{EnvShadowProvider: "typesafe"}, "org-a"},
		{"provider with another org", map[string]string{EnvShadowProvider: "typesafe", EnvShadowOrgIDs: "org-b"}, "org-a"},
		{"provider and all orgs, run with no org", map[string]string{EnvShadowProvider: "typesafe", EnvShadowOrgIDs: "*"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearShadowEnv(t)
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			logs := &syncBuffer{}
			executor, recorder := shadowTestExecutor(t, logs)
			if phase := executor.newShadow(tc.org); phase != nil {
				t.Fatalf("a shadow phase was built: %+v", phase.settings)
			}
			if recorder.count() != 0 {
				t.Fatal("a request was sent")
			}
			if strings.Contains(logs.String(), "TypeSafe client") || logs.String() != "" {
				t.Fatalf("an off phase must build nothing and say nothing:\n%s", logs.String())
			}
		})
	}
}

// With only the shadow switch on (provider, org list, key) the executor builds
// the phase over a client for api.typesafe.ai and the completer reaches a send.
func TestWithOnlyTheShadowSwitchOnTheExecutorBuildsThePhaseAndReachesASend(t *testing.T) {
	clearShadowEnv(t)
	t.Setenv(EnvShadowProvider, "typesafe")
	t.Setenv(EnvShadowOrgIDs, "org-a")
	t.Setenv("TYPESAFE_API_KEY", shadowTestKeyValue)
	logs := &syncBuffer{}
	executor, recorder := shadowTestExecutor(t, logs)
	phase := executor.newShadow("org-a")
	if phase == nil {
		t.Fatalf("no phase was built:\n%s", logs.String())
	}
	defer func() { _ = phase.Close() }()
	if phase.settings.Budget != DefaultShadowBudget || phase.settings.Concurrency != 4 || phase.config != decision.IdentityFor("").Stamp() {
		t.Fatalf("phase = %+v config %q", phase.settings, phase.config)
	}
	store := &memoryShadowStore{}
	cfg := shadowTestConfig()
	cfg.OrgID = "org-a"
	summary := phase.run(t.Context(), store, cfg, shadowTestEntries(t, "u1"))
	if recorder.count() != 1 {
		t.Fatalf("sends = %d, want 1 (summary %+v)", recorder.count(), summary)
	}
	request := recorder.requests[0]
	if request.URL.String() != "https://api.typesafe.ai/v1/systemone" || request.Method != http.MethodPost {
		t.Fatalf("the send went to %s %s", request.Method, request.URL)
	}
	if request.Header.Get("Authorization") != "Bearer "+shadowTestKeyValue {
		t.Fatal("the send did not carry the configured key")
	}
	if len(store.records) != 1 || store.records[0].State != decision.StateOK {
		t.Fatalf("rows = %+v", store.records)
	}
}

// A phase that was asked for and cannot be built is OFF with one loud line. It
// is never an error of the run.
func TestAShadowPhaseThatCannotBeBuiltIsOffAndSaysWhy(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"no key", map[string]string{EnvShadowProvider: "typesafe", EnvShadowOrgIDs: "*"}, "the TypeSafe client cannot be built"},
		{"a key under 12 bytes", map[string]string{EnvShadowProvider: "typesafe", EnvShadowOrgIDs: "*", "TYPESAFE_API_KEY": "short"}, "the TypeSafe client cannot be built"},
		{"another host", map[string]string{EnvShadowProvider: "typesafe", EnvShadowOrgIDs: "*", "TYPESAFE_API_KEY": shadowTestKeyValue, "TYPESAFE_BASE_URL": "https://example.invalid"}, "the TypeSafe client cannot be built"},
		{"a model alias", map[string]string{EnvShadowProvider: "typesafe", EnvShadowOrgIDs: "*", "TYPESAFE_API_KEY": shadowTestKeyValue, "TYPESAFE_MODEL": "jev-latest"}, "the TypeSafe client cannot be built"},
		{"another provider", map[string]string{EnvShadowProvider: "openai", EnvShadowOrgIDs: "*", "TYPESAFE_API_KEY": shadowTestKeyValue}, "a setting cannot be used"},
		{"a budget that is not a number", map[string]string{EnvShadowProvider: "typesafe", EnvShadowOrgIDs: "*", "TYPESAFE_API_KEY": shadowTestKeyValue, EnvShadowMaxSeconds: "soon"}, "a setting cannot be used"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearShadowEnv(t)
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			logs := &syncBuffer{}
			executor, recorder := shadowTestExecutor(t, logs)
			if phase := executor.newShadow("org-a"); phase != nil {
				t.Fatal("a phase was built")
			}
			if recorder.count() != 0 {
				t.Fatal("a request was sent")
			}
			out := logs.String()
			if strings.Count(out, "investment shadow phase is off") != 1 || !strings.Contains(out, tc.want) {
				t.Fatalf("want one line with %q:\n%s", tc.want, out)
			}
			if strings.Contains(out, shadowTestKeyValue) || strings.Contains(out, "example.invalid") {
				t.Fatalf("the line holds a configured value:\n%s", out)
			}
		})
	}
}

// typesafe is a decision backend. It is in the closed set of kinds and it can
// never be the SERVED provider: not by LLM_PROVIDER, not by the scope, not by
// auto-detection of its key -- also when every shadow switch is on.
func TestTypeSafeCanNeverBeTheServedProvider(t *testing.T) {
	clearShadowEnv(t)
	t.Setenv(EnvShadowProvider, "typesafe")
	t.Setenv(EnvShadowOrgIDs, "*")
	t.Setenv("TYPESAFE_API_KEY", shadowTestKeyValue)
	for _, name := range []string{"LLM_PROVIDER", "LLM_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "LOCAL_LLM_BASE_URL", "DASHSCOPE_API_KEY", "QWEN_API_KEY", "OLLAMA_MODEL", "OLLAMA_BASE_URL", "LMSTUDIO_BASE_URL"} {
		t.Setenv(name, "")
	}
	if categorize.IsProviderKindImplemented(categorize.ProviderKindTypeSafe) || slices.Contains(categorize.ImplementedProviderKinds(), categorize.ProviderKindTypeSafe) {
		t.Fatal("typesafe is in the set of kinds the served path can construct")
	}
	if _, err := categorize.ResolveProviderKind("typesafe"); err != nil {
		t.Fatalf("typesafe left the closed set of kinds: %v", err)
	}
	for _, requested := range []string{"typesafe", "TypeSafe", " typesafe "} {
		if provider, kind, err := resolveProviderFromEnv(requested, ""); err == nil || provider != nil {
			t.Fatalf("served resolution of %q built %v (%q)", requested, provider, kind)
		}
	}
	// Auto-detection with only the TypeSafe key present must not pick it.
	if provider, kind, err := resolveProviderFromEnv("auto", ""); err == nil && kind == categorize.ProviderKindTypeSafe {
		t.Fatalf("auto-detection made typesafe the served provider: %v", provider)
	}
	t.Setenv("LLM_PROVIDER", "typesafe")
	if provider, _, err := resolveProviderFromEnv("auto", ""); err == nil || provider != nil {
		t.Fatalf("LLM_PROVIDER=typesafe built a served provider: %v", provider)
	}
}

// The label sets of the collector are the phase's own sets, and the model of
// the evaluated configuration has its own series.
func TestTheMetricLabelSetsAreThePhasesOwnSets(t *testing.T) {
	if !reflect.DeepEqual(jobruntime.InvestmentShadowStopReasons(), ShadowStopReasons()) {
		t.Fatalf("stop reasons differ:\n collector %v\n phase     %v", jobruntime.InvestmentShadowStopReasons(), ShadowStopReasons())
	}
	if want := append(shadowStates(), ShadowAttemptRetried); !reflect.DeepEqual(jobruntime.InvestmentShadowAttemptStates(), want) {
		t.Fatalf("attempt states differ:\n collector %v\n phase     %v", jobruntime.InvestmentShadowAttemptStates(), want)
	}
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	observer := CollectorShadowObserver{Collector: collector}
	for _, reason := range ShadowStopReasons() {
		states := map[string]int{}
		for _, state := range append(shadowStates(), ShadowAttemptRetried) {
			states[state] = 1
		}
		observer.ObserveShadowPhase(ShadowPhaseCounts{Model: decision.DefaultModel, StopReason: reason, AttemptsByState: states})
	}
	rendered := collector.PrometheusText()
	for _, reason := range ShadowStopReasons() {
		if !strings.Contains(rendered, `dev_health_investment_shadow_phase_stops_total{reason="`+reason+`"} 1`) {
			t.Errorf("stop reason %q was not recorded by the collector", reason)
		}
	}
	for _, state := range append(shadowStates(), ShadowAttemptRetried) {
		want := `dev_health_investment_shadow_attempts_total{role="shadow",provider="` + decision.ProviderName + `",model="` + decision.DefaultModel + `",state="` + state + `"} ` + "8"
		if !strings.Contains(rendered, want) {
			t.Errorf("missing series: %s", want)
		}
	}
	CollectorShadowObserver{}.ObserveShadowPhase(ShadowPhaseCounts{StopReason: ShadowStopDone}) // a nil collector is tolerated
}

// The two test seams of the shadow path (a client for any host, an HTTP client
// for the executor) are used by test files only. The walk must cover the
// module, or it proves nothing.
func TestNoProductionCodeUsesAShadowTestSeam(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found at %s: %v", root, err)
	}
	// identifier -> the only non-test files that may name it.
	allowed := map[string][]string{
		"UnsafeAllowAnyBaseURLForTest":           {"internal/jobs/investment/categorize/typesafeclient.go"},
		"SetShadowHTTPClientForTest":             {"internal/jobs/investment/nativeexecutor.go"},
		"NewTypeSafeClientFromEnvWithHTTPClient": {"internal/jobs/investment/categorize/typesafeclient.go", "internal/jobs/investment/nativeexecutor.go"},
		"shadowHTTPClient":                       {"internal/jobs/investment/nativeexecutor.go"},
	}
	seen := map[string]int{}
	files := 0
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return walkErr
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			files++
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			ast.Inspect(parsed, func(node ast.Node) bool {
				identifier, ok := node.(*ast.Ident)
				if !ok {
					return true
				}
				if files, guarded := allowed[identifier.Name]; guarded {
					seen[identifier.Name]++
					if !slices.Contains(files, rel) {
						t.Errorf("%s names the test seam %s", rel, identifier.Name)
					}
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if files < 1000 {
		t.Fatalf("the walk read %d non-test Go files: it did not cover the module", files)
	}
	for name := range allowed {
		if seen[name] == 0 {
			t.Errorf("the seam %s was not found anywhere: the guard looks for a name that no longer exists", name)
		}
	}
	// The executor's seam must reach the client constructor and nothing else,
	// and production wiring must leave it nil.
	source, err := os.ReadFile(filepath.Join(root, "internal/jobs/investment/nativeexecutor.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(source), "executor.shadowHTTPClient") != 2 {
		t.Fatalf("executor.shadowHTTPClient is used %d times in nativeexecutor.go, want 2 (the setter and the constructor call)",
			strings.Count(string(source), "executor.shadowHTTPClient"))
	}
}
