package authapi

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

type limitClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *limitClock) now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *limitClock) advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

type limitFixture struct {
	*admitFixture
	clock        *limitClock
	tokenA       string
	tokenB       string
	stranger     string
	rejectionsAt func() string
}

// newLimitFixture mounts the PDP routes behind the real route chain with the
// service-default route bucket (20/s, burst 40 would starve; the ceiling here is the
// chart default) and per-caller buckets of limit, for two listed callers.
func newLimitFixture(t *testing.T, limit PDPCallerLimit) *limitFixture {
	t.Helper()
	h := newAuthorizeHandlerFixture(t)
	a, b := session.WirePrincipalID(uuid.New()), session.WirePrincipalID(uuid.New())
	clock := &limitClock{t: time.Unix(1_800_000_000, 0)}
	limit.Now = clock.now
	routes, err := AuthorizeRoutes(AuthorizeDeps{
		Callers: h.callers(a, b), References: h.references(), State: h.state, Evaluator: authorization.NewEvaluator(), Audit: h.audit.write,
		ProfileRows: authorizeProfileDoc(t, PatternAuthorize, PatternAuthorizeBatch), CallerLimit: limit,
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := httpapi.NewServer(httpapi.ServerOptions{
		Address: "127.0.0.1:0", Logger: slog.New(slog.DiscardHandler), Routes: routes,
		RequestTimeout: 5 * time.Second, MaxBodyBytes: 1 << 20, RateLimit: 100000, RateLimitBurst: 100000,
	})
	if err != nil {
		t.Fatal(err)
	}
	f := &limitFixture{admitFixture: &admitFixture{authorizeHandlerFixture: h, routes: routes, server: server.Handler()}, clock: clock}
	h.subject = a
	f.tokenA = h.workloadToken(nil)
	h.subject = b
	f.tokenB = h.workloadToken(nil)
	h.subject = session.WirePrincipalID(uuid.New())
	f.stranger = h.workloadToken(nil)
	return f
}

func (f *limitFixture) code(pattern, token string, payload []byte) int {
	rec, _ := f.do(f.server, pattern, token, payload, false)
	return rec.Code
}

func (f *limitFixture) batch(n int) []byte {
	q := f.question("settings.write", "organization", "acme", f.org)
	items := make([]map[string]any, n)
	for i := range items {
		items[i] = q
	}
	return mustJSON(f.t, map[string]any{"schema_version": "authorization_batch_request.v1", "requests": items})
}

func rejectionsText(t *testing.T) string {
	t.Helper()
	var sb strings.Builder
	if err := httpapi.RateLimitRejections.WritePrometheus(&sb); err != nil {
		t.Fatal(err)
	}
	return sb.String()
}

func rejectionCount(t *testing.T, id string) int {
	t.Helper()
	for _, line := range strings.Split(rejectionsText(t), "\n") {
		if strings.HasPrefix(line, `dev_health_api_rate_limit_rejections_total{limit="`+id+`"}`) {
			n, err := strconv.Atoi(strings.Fields(line)[1])
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatalf("no rejection series for %s in:\n%s", id, rejectionsText(t))
	return 0
}

func TestAuthorizeCallersDoNotShareABucket(t *testing.T) {
	f := newLimitFixture(t, PDPCallerLimit{PerSecond: 10, Burst: 20})
	limited := 0
	for i := 0; i < 200; i++ {
		if f.code(PatternAuthorize, f.tokenA, f.payload(PatternAuthorize)) == http.StatusTooManyRequests {
			limited++
		}
	}
	if limited != 180 {
		t.Errorf("caller A: %d of 200 answered 429, want 180 (burst 20, no refill)", limited)
	}
	if got := f.code(PatternAuthorize, f.tokenB, f.payload(PatternAuthorize)); got != http.StatusOK {
		t.Errorf("caller B first request after A flooded: %d, want 200", got)
	}
}

func TestAuthorizeCallerOverLimitGets429WithRetryAfterAndNoBodyRead(t *testing.T) {
	f := newLimitFixture(t, PDPCallerLimit{PerSecond: 0.5, Burst: 1})
	if got := f.code(PatternAuthorize, f.tokenA, f.payload(PatternAuthorize)); got != http.StatusOK {
		t.Fatalf("first request %d, want 200", got)
	}
	reads, audits := f.state.reads, len(f.audit.calls)
	before := rejectionCount(t, pdpAuthorizeCallerLimitID)
	rec, body := f.do(f.server, PatternAuthorize, f.tokenA, f.payload(PatternAuthorize), false)
	if rec.Code != http.StatusTooManyRequests || !strings.Contains(rec.Body.String(), `"rate_limited"`) {
		t.Fatalf("second request: %d %s, want 429 rate_limited", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Retry-After"); got != "2" {
		t.Errorf("Retry-After = %q, want 2 (one token at 0.5/s)", got)
	}
	if body.n != 0 || f.state.reads != reads || len(f.audit.calls) != audits {
		t.Errorf("refused request read %d body bytes, %d state txs, %d audit rows; want 0, 0, 0", body.n, f.state.reads-reads, len(f.audit.calls)-audits)
	}
	if got := rejectionCount(t, pdpAuthorizeCallerLimitID); got != before+1 {
		t.Errorf("rejections = %d, want %d", got, before+1)
	}
	for _, line := range strings.Split(rejectionsText(t), "\n") {
		if strings.HasPrefix(line, "dev_health_api_rate_limit_rejections_total{") && !strings.HasPrefix(line, "dev_health_api_rate_limit_rejections_total{limit=") {
			t.Errorf("rejection series carries a label beyond limit: %s", line)
		}
		if strings.Contains(line, "pdp") && (strings.Contains(line, "wl_") || strings.Contains(line, "principal")) {
			t.Errorf("rejection series names a caller: %s", line)
		}
	}
}

func TestAuthorizeCallerBucketRefills(t *testing.T) {
	f := newLimitFixture(t, PDPCallerLimit{PerSecond: 1, Burst: 1})
	f.code(PatternAuthorize, f.tokenA, f.payload(PatternAuthorize))
	if got := f.code(PatternAuthorize, f.tokenA, f.payload(PatternAuthorize)); got != http.StatusTooManyRequests {
		t.Fatalf("second request %d, want 429", got)
	}
	f.clock.advance(time.Second)
	if got := f.code(PatternAuthorize, f.tokenA, f.payload(PatternAuthorize)); got != http.StatusOK {
		t.Errorf("after one refill interval: %d, want 200", got)
	}
}

func TestAuthorizeLimitIsPerRoute(t *testing.T) {
	f := newLimitFixture(t, PDPCallerLimit{PerSecond: 1, Burst: 1})
	f.code(PatternAuthorize, f.tokenA, f.payload(PatternAuthorize))
	if got := f.code(PatternAuthorize, f.tokenA, f.payload(PatternAuthorize)); got != http.StatusTooManyRequests {
		t.Fatalf("authorize over its limit: %d, want 429", got)
	}
	if got := f.code(PatternAuthorizeBatch, f.tokenA, f.payload(PatternAuthorizeBatch)); got != http.StatusOK {
		t.Errorf("batch route of the same caller: %d, want 200", got)
	}
}

func TestAuthorizeBatchCostsItsDecisionCount(t *testing.T) {
	f := newLimitFixture(t, PDPCallerLimit{PerSecond: 1, Burst: 100})
	if got := f.code(PatternAuthorizeBatch, f.tokenA, f.batch(60)); got != http.StatusOK {
		t.Fatalf("batch of 60 on a full bucket: %d, want 200", got)
	}
	// 40 tokens left: a batch of 41 is refused and spends nothing more than the gate's one token.
	rec, _ := f.do(f.server, PatternAuthorizeBatch, f.tokenA, f.batch(41), false)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("batch of 41 with 40 tokens: %d %s, want 429 with Retry-After", rec.Code, rec.Body)
	}
	if got := f.code(PatternAuthorizeBatch, f.tokenA, f.batch(39)); got != http.StatusOK {
		t.Errorf("batch of 39 with 39 tokens left: %d, want 200", got)
	}
	if got := f.code(PatternAuthorizeBatch, f.tokenA, f.batch(1)); got != http.StatusTooManyRequests {
		t.Errorf("a batch of 1 on an empty bucket: %d, want 429", got)
	}
	if got := f.code(PatternAuthorizeBatch, f.tokenB, f.batch(100)); got != http.StatusOK {
		t.Errorf("caller B batch of 100 (the largest) on its own full bucket: %d, want 200", got)
	}
}

func TestAuthorizeRefusedCallerNeverTouchesABucket(t *testing.T) {
	f := newLimitFixture(t, PDPCallerLimit{PerSecond: 1, Burst: 1})
	before := rejectionCount(t, pdpAuthorizeCallerLimitID)
	for i := 0; i < 5; i++ {
		rec, _ := f.do(f.server, PatternAuthorize, f.stranger, f.payload(PatternAuthorize), false)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("unlisted caller: %d, want 401", rec.Code)
		}
	}
	if got := rejectionCount(t, pdpAuthorizeCallerLimitID); got != before {
		t.Errorf("unlisted caller counted %d rejections, want 0", got-before)
	}
	if got := f.code(PatternAuthorize, f.tokenA, f.payload(PatternAuthorize)); got != http.StatusOK {
		t.Errorf("listed caller after the refused ones: %d, want 200", got)
	}
}

func TestAuthorizeZeroCallerLimitSetsNoLimit(t *testing.T) {
	f := newLimitFixture(t, PDPCallerLimit{})
	for i := 0; i < 50; i++ {
		if got := f.code(PatternAuthorize, f.tokenA, f.payload(PatternAuthorize)); got != http.StatusOK {
			t.Fatalf("request %d: %d, want 200", i, got)
		}
	}
}

func TestAuthorizeCallerRejectionSeriesExistAtZero(t *testing.T) {
	// The process-wide counter is shared by every test, so the zero start is read on an id only this test uses.
	const id = "auth_svc_pdp_test_zero_start"
	if text := rejectionsText(t); strings.Contains(text, id) {
		t.Fatalf("series %s exists before its limiter is built:\n%s", id, text)
	}
	limiter := newCallerLimiter(id, []string{"s"}, PDPCallerLimit{PerSecond: 1, Burst: 1})
	if got := rejectionCount(t, id); got != 0 {
		t.Fatalf("series %s starts at %d, want 0", id, got)
	}
	if ok, _ := limiter.take("s", 1); !ok {
		t.Fatal("first token refused")
	}
	if ok, _ := limiter.take("s", 1); ok {
		t.Fatal("second token granted")
	}
	// take does not count; the handler does, once per 429.
	if got := rejectionCount(t, id); got != 0 {
		t.Errorf("series %s = %d after take, want 0", id, got)
	}

	newLimitFixture(t, PDPCallerLimit{PerSecond: 1, Burst: 1})
	for _, real := range []string{pdpAuthorizeCallerLimitID, pdpAuthorizeBatchCallerLimitID} {
		rejectionCount(t, real)
	}
}
