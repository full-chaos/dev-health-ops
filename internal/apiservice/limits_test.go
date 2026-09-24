package apiservice

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func envLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) { value, ok := values[name]; return value, ok }
}

// developmentEnvironment is rate_limit.py's _is_dev_or_test: the first of
// ENVIRONMENT, APP_ENV, ENV that is set and non-empty decides, unset is
// production.
func TestDevelopmentEnvironmentFollowsRateLimitPy(t *testing.T) {
	for name, tc := range map[string]struct {
		env  map[string]string
		want bool
	}{
		"nothing set is production":           {nil, false},
		"ENVIRONMENT=test":                    {map[string]string{"ENVIRONMENT": "test"}, true},
		"ENVIRONMENT=Development (case)":      {map[string]string{"ENVIRONMENT": " Development "}, true},
		"ENVIRONMENT=dev":                     {map[string]string{"ENVIRONMENT": "dev"}, true},
		"ENVIRONMENT=local":                   {map[string]string{"ENVIRONMENT": "local"}, true},
		"ENVIRONMENT=testing":                 {map[string]string{"ENVIRONMENT": "testing"}, true},
		"ENVIRONMENT=production":              {map[string]string{"ENVIRONMENT": "production"}, false},
		"ENVIRONMENT=staging":                 {map[string]string{"ENVIRONMENT": "staging"}, false},
		"APP_ENV used when ENVIRONMENT unset": {map[string]string{"APP_ENV": "dev"}, true},
		"ENV used last":                       {map[string]string{"ENV": "local"}, true},
		"ENVIRONMENT wins over APP_ENV":       {map[string]string{"ENVIRONMENT": "production", "APP_ENV": "dev"}, false},
		"an empty ENVIRONMENT falls through":  {map[string]string{"ENVIRONMENT": "", "APP_ENV": "dev"}, true},
	} {
		if got := developmentEnvironment(envLookup(tc.env)); got != tc.want {
			t.Errorf("%s: developmentEnvironment = %v, want %v", name, got, tc.want)
		}
	}
}

// Outside development a deployment that serves the limited routes (it has its
// database) and has no Valkey refuses to start: an in-process limiter is per
// replica, so it is no limit. Development starts (it fails later on the
// unreachable database, a different reason); no database at all (the
// pre-bootstrap shape) starts regardless.
func TestConfigureRefusesToStartWithoutTheSharedLimiterOutsideDevelopment(t *testing.T) {
	cfg := config.Config{APIAddress: "127.0.0.1:0", APIDatabaseURI: secrets.NewValue("postgres://api:pw@127.0.0.1:1/x?connect_timeout=1")}
	t.Setenv("ENVIRONMENT", "production")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	components, err := configure(ctx, cfg, health.NewRegistry(time.Second), quietLogger())
	closeComponents(components)
	var coded interface{ DependencyReason() string }
	if !errors.As(err, &coded) || coded.DependencyReason() != "api_rate_limiter_shared_store_required" {
		t.Fatalf("production without VALKEY_URI: err = %v, want reason api_rate_limiter_shared_store_required", err)
	}
	if !strings.Contains(err.Error(), "VALKEY_URI") {
		t.Fatalf("the refusal does not say what to set: %v", err)
	}

	t.Setenv("ENVIRONMENT", "development")
	components, err = configure(ctx, cfg, health.NewRegistry(time.Second), quietLogger())
	closeComponents(components)
	if errors.As(err, &coded) && coded.DependencyReason() == "api_rate_limiter_shared_store_required" {
		t.Fatal("development refused for the missing shared limiter")
	}

	t.Setenv("ENVIRONMENT", "production")
	components, err = configure(ctx, config.Config{APIAddress: "127.0.0.1:0"}, health.NewRegistry(time.Second), quietLogger())
	closeComponents(components)
	if err != nil {
		t.Fatalf("the pre-bootstrap shape (no database) must start in production: %v", err)
	}
}

// Without a Valkey client and without an explicit store the api counts in
// process ("memory"); an explicit store wins.
func TestLimitStoreSelection(t *testing.T) {
	store, err := limitStore(Deps{})
	if err != nil || store.Backend() != "memory" {
		t.Fatalf("no Valkey: store %v (%v), want the in-process one", store, err)
	}
	explicit := httpapi.NewMemoryCounters(nil)
	if got, _ := limitStore(Deps{Limits: explicit}); got != explicit {
		t.Fatal("an explicit Deps.Limits was not used")
	}
}

type downStore struct{}

func (downStore) Increment(context.Context, httpapi.Hit) (int64, error) {
	return 0, errors.New("dial tcp 10.0.0.9:6379: connection refused")
}
func (downStore) Backend() string { return "redis" }

// A Valkey outage on a limited route is the Python api's unhandled-error 500
// (slowapi has no swallow_errors), in the api's Python wire shape, marked as
// unhandled -- and the body carries nothing of the failure.
func TestLimitedRouteAnswersPythonsUnhandledError500WhenTheStoreIsDown(t *testing.T) {
	reached := false
	handler := httpapi.LimitWith(httpapi.NewKeyedLimiter(downStore{}, httpapi.Limit{ID: "admin_org_invite", Count: 10, Window: time.Hour}),
		func(*http.Request) string { return "admin-user:secret" }, WriteError)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true }))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/admin/orgs/x/invites", nil))
	if recorder.Code != http.StatusInternalServerError || recorder.Body.String() != `{"detail":"Internal Server Error"}` {
		t.Fatalf("store down answered %d %q, want 500 {\"detail\":\"Internal Server Error\"}", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get(policy.UnhandledErrorHeader) != "1" {
		t.Fatal("the 500 is not marked unhandled")
	}
	if strings.Contains(recorder.Body.String(), "connection refused") || strings.Contains(recorder.Body.String(), "secret") {
		t.Fatalf("the body leaks the failure or the caller key: %q", recorder.Body.String())
	}
	if reached {
		t.Fatal("the handler ran although the limit store was down")
	}
}

// The store error counter is on the api's operator /metrics: configure
// registers it as a metrics source, and an error on a limited route shows
// up in a scrape (the api installs no OTel meter provider, so a global OTel
// counter would be invisible).
func TestStoreErrorCounterIsScrapedFromTheOperatorEndpoint(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	components, err := configure(context.Background(), config.Config{APIAddress: "127.0.0.1:0"}, registry, quietLogger())
	closeComponents(components)
	if err != nil {
		t.Fatal(err)
	}
	handler := httpapi.LimitWith(httpapi.NewKeyedLimiter(downStore{}, httpapi.Limit{ID: "scrape_probe", Count: 1, Window: time.Hour}),
		func(*http.Request) string { return "k" }, WriteError)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/x", nil))
	var scrape strings.Builder
	outcomes, err := registry.WriteMetricsPartial(&scrape)
	if err != nil {
		t.Fatal(err)
	}
	for _, outcome := range outcomes {
		if outcome.Err != nil {
			t.Fatalf("metrics source %s failed: %v", outcome.Source, outcome.Err)
		}
	}
	if !strings.Contains(scrape.String(), `dev_health_api_rate_limit_store_errors_total{limit="scrape_probe"} 1`) {
		t.Fatalf("the operator scrape lacks the store error series:\n%s", scrape.String())
	}
}
