package authapi

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// cacheMarks is the cache's watermark read in these cells.
type cacheMarks struct {
	mu     sync.Mutex
	latest map[string]int64
}

func (m *cacheMarks) Latest(typ, id string) (int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.latest[typ+":"+id]
	return n, ok
}

func (m *cacheMarks) set(typ, id string, n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latest[typ+":"+id] = n
}

// orderedCache records Begin calls into the shared order log.
type orderedCache struct {
	*decisioncache.Cache
	order *[]string
}

func (o orderedCache) Begin() decisioncache.Stamp {
	*o.order = append(*o.order, "begin")
	return o.Cache.Begin()
}

// depSnapshot is a fake snapshot that also says what it read, as decisionstate's does.
type depSnapshot struct {
	*fakeSnapshot
	deps []decisioncache.Dependency
	sets []string
}

func (s depSnapshot) Dependencies() []decisioncache.Dependency { return s.deps }
func (s depSnapshot) SetTypes() []string                       { return s.sets }

// cacheState wraps the handler's fake state: Unfed values, snapshots with dependencies, an order log and a hook inside
// the transaction.
type cacheState struct {
	*fakeDecisionState
	unfed      map[snapKey]UnfedState
	unfedReads int
	deps       []decisioncache.Dependency
	sets       []string
	order      []string
	inside     func()
	clock      *int64
}

func (c *cacheState) Read(ctx context.Context, fn func(DecisionTx) error) error {
	c.order = append(c.order, "read")
	c.reads++
	if c.beginErr != nil {
		return c.beginErr
	}
	if c.inside != nil {
		c.inside()
	}
	return fn(cacheTx{fakeDecisionTx{c.fakeDecisionState}, c})
}

type cacheTx struct {
	fakeDecisionTx
	c *cacheState
}

func (t cacheTx) Unfed(_ context.Context, principal uuid.UUID, org string) (UnfedState, error) {
	t.c.unfedReads++
	u := t.c.unfed[snapKey{principal, org}]
	u.NowUnixNano = *t.c.clock
	return u, nil
}

func (t cacheTx) Snapshot(ctx context.Context, principal uuid.UUID, org string) (authorization.GrantSnapshot, error) {
	snap, err := t.fakeDecisionTx.Snapshot(ctx, principal, org)
	if err != nil {
		return nil, err
	}
	fake := *snap.(*fakeSnapshot)
	fake.now = *t.c.clock
	return depSnapshot{&fake, t.c.deps, t.c.sets}, nil
}

type cacheFixture struct {
	*authorizeHandlerFixture
	cs     *cacheState
	cache  *decisioncache.Cache
	marks  *cacheMarks
	now    int64
	ready  bool
	reader *sdkmetric.ManualReader
	logs   *bytes.Buffer
}

func (c *cacheFixture) advance(d time.Duration) { c.now += int64(d) }

// newCacheFixture is the handler fixture with the decision cache in use: Ready, one active admin with a membership
// read at mbr_ revision 3, a principal at revision 2 and the resource_grant set tagged.
func newCacheFixture(t *testing.T) *cacheFixture {
	t.Helper()
	h := newAuthorizeHandlerFixture(t)
	c := &cacheFixture{authorizeHandlerFixture: h, marks: &cacheMarks{latest: map[string]int64{}}, now: authorizeNow, ready: true,
		reader: sdkmetric.NewManualReader(), logs: &bytes.Buffer{}}
	c.cs = &cacheState{
		fakeDecisionState: h.state,
		unfed: map[snapKey]UnfedState{
			{h.principal, h.org}: {UserFound: true, UserActive: true, Superuser: new(bool), MembershipRoleFound: true, MembershipRole: "admin"},
			{h.principal, ""}:    {UserFound: true, UserActive: true, Superuser: new(bool)},
		},
		deps: []decisioncache.Dependency{{AggregateType: "principal", AggregateID: "prn_" + h.principal.String(), Revision: 2},
			{AggregateType: "membership", AggregateID: "mbr_one", Revision: 3}},
		sets:  []string{"membership", "resource_grant"},
		clock: &c.now,
	}
	cache, err := decisioncache.New(decisioncache.Config{Catalog: policycatalog.Default(), Watermarks: c.marks,
		Now: func() time.Time { return time.Unix(0, c.now) }})
	if err != nil {
		t.Fatal(err)
	}
	c.cache = cache
	c.rebuild(t, orderedCache{cache, &c.cs.order})
	return c
}

func (c *cacheFixture) rebuild(t *testing.T, cache DecisionCache) {
	t.Helper()
	routes, err := AuthorizeRoutes(AuthorizeDeps{
		Callers: c.callers(), References: c.references(), State: c.cs, Evaluator: authorization.NewEvaluator(),
		ProfileRows: authorizeProfileDoc(t, PatternAuthorize, PatternAuthorizeBatch),
		Cache:       cache, Ready: func() bool { return c.ready },
		Logger:      slog.New(slog.NewJSONHandler(c.logs, nil)), Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(c.reader)).Meter("test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := httpapi.NewServer(httpapi.ServerOptions{
		Address: "127.0.0.1:0", Logger: slog.New(slog.DiscardHandler), Routes: routes,
		RequestTimeout: 5 * time.Second, MaxBodyBytes: 1 << 20, RateLimit: 1000, RateLimitBurst: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	c.routes = map[string]http.Handler{PatternAuthorize: server.Handler(), PatternAuthorizeBatch: server.Handler()}
}

// lowAction is a low-risk decision_bound organization action the admin role holds.
const lowAction = "analytics.read"

func (c *cacheFixture) ask() wireDecision {
	c.t.Helper()
	rec := c.one(c.question(lowAction, "organization", "acme", c.org))
	if rec.Code != http.StatusOK {
		c.t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	return decodeWire[wireDecision](c.t, rec)
}

func (c *cacheFixture) counter(name, value string) int64 {
	c.t.Helper()
	var rm metricdata.ResourceMetrics
	if err := c.reader.Collect(context.Background(), &rm); err != nil {
		c.t.Fatal(err)
	}
	var n int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			for _, p := range m.Data.(metricdata.Sum[int64]).DataPoints {
				if v, ok := p.Attributes.Value(attribute.Key("result")); ok && v.AsString() == value {
					n += p.Value
				}
			}
		}
	}
	return n
}

func TestAHitServesTheStoredAllowWithoutReadingTheSnapshot(t *testing.T) {
	c := newCacheFixture(t)
	first := c.ask()
	second := c.ask()
	if !first.Allowed || !second.Allowed {
		t.Fatalf("decisions %+v, %+v, want two allows", first, second)
	}
	if c.state.snapReads != 1 || c.cs.unfedReads != 2 || c.cs.reads != 2 {
		t.Fatalf("snapshot reads %d, unfed reads %d, transactions %d; want 1, 2, 2 (the hit reads the session and the unfed values only)",
			c.state.snapReads, c.cs.unfedReads, c.cs.reads)
	}
	if first.DecisionID == second.DecisionID {
		t.Fatal("a hit reused the decision id")
	}
	for _, tc := range []struct {
		name, value string
		want        int64
	}{
		{MetricDecisionCacheLookups, "hit", 1}, {MetricDecisionCacheLookups, "miss", 1}, {MetricDecisionCachePuts, "stored", 1},
	} {
		if got := c.counter(tc.name, tc.value); got != tc.want {
			t.Errorf("%s{result=%s} = %d, want %d", tc.name, tc.value, got, tc.want)
		}
	}
	if !strings.Contains(c.logs.String(), `"cache":"hit"`) || !strings.Contains(c.logs.String(), `"cache":"miss"`) {
		t.Fatalf("the decision log lines carry no cache marker: %s", c.logs)
	}
}

// acp condition 2: the session is checked before the cache, so a revoked session never gets a stored allow.
func TestARevokedSessionFailsBeforeTheCache(t *testing.T) {
	c := newCacheFixture(t)
	c.ask()
	s := c.state.sessions[c.sid]
	s.Live = false
	c.state.sessions[c.sid] = s
	assertError(t, c.one(c.question(lowAction, "organization", "acme", c.org)), http.StatusUnauthorized, reasonPrincipalReferenceInvalid)
	if got := c.counter(MetricDecisionCacheLookups, "hit"); got != 0 {
		t.Fatalf("%d hits for a revoked session", got)
	}
}

// The stamp is taken before the transaction: an event of a tagged set that lands while the decision is read makes the
// stored allow miss.
func TestBeginComesBeforeTheReadAndAnEventDuringTheReadMisses(t *testing.T) {
	c := newCacheFixture(t)
	c.cs.inside = func() {
		if err := c.cache.Invalidate(context.Background(), "resource_grant", "grt_new_deny", 1); err != nil {
			t.Fatal(err)
		}
		c.cs.inside = nil
	}
	c.ask()
	if got := c.cs.order[:2]; !slices.Equal(got, []string{"begin", "read"}) {
		t.Fatalf("order %q, want begin before read", got)
	}
	c.ask()
	if c.state.snapReads != 2 {
		t.Fatalf("%d snapshot reads: the allow read across a resource_grant event was served", c.state.snapReads)
	}
}

// pm Q1: the values no feed event covers are read on every call and are part of the key.
func TestAChangeOfAnUnfedValueMisses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*UnfedState)
	}{
		{"user deactivated", func(u *UnfedState) { u.UserActive = false }},
		{"superuser granted", func(u *UnfedState) { yes := true; u.Superuser = &yes }},
		{"public membership role", func(u *UnfedState) { u.MembershipRole = "member" }},
		{"public membership removed", func(u *UnfedState) { u.MembershipRoleFound, u.MembershipRole = false, "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCacheFixture(t)
			c.ask()
			k := snapKey{c.principal, c.org}
			u := c.cs.unfed[k]
			tc.change(&u)
			c.cs.unfed[k] = u
			c.ask()
			if c.state.snapReads != 2 {
				t.Fatalf("%d snapshot reads: a stored allow was served across an unfed change", c.state.snapReads)
			}
		})
	}
}

func TestARevisionBumpOfADependencyMisses(t *testing.T) {
	c := newCacheFixture(t)
	c.ask()
	c.marks.set("membership", "mbr_one", 4)
	c.ask()
	if c.state.snapReads != 2 {
		t.Fatalf("%d snapshot reads, want 2: the membership moved past the revision read", c.state.snapReads)
	}
}

func TestANewDenyGrantMissesThroughTheSetTag(t *testing.T) {
	c := newCacheFixture(t)
	c.ask()
	if err := c.cache.Invalidate(context.Background(), "resource_grant", "grt_new_deny", 1); err != nil {
		t.Fatal(err)
	}
	c.ask()
	if c.state.snapReads != 2 {
		t.Fatalf("%d snapshot reads, want 2", c.state.snapReads)
	}
}

// pm Q3 and acp condition 1: with the cache in use a decision_bound allow expires 30 s after the decision, and a hit
// returns the stored expiry.
func TestADecisionBoundAllowCarriesThirtySecondsAndAHitTheStoredExpiry(t *testing.T) {
	c := newCacheFixture(t)
	want := time.Unix(0, authorizeNow).Add(decisioncache.TTL).UTC().Format(expiresAtLayout)
	if d := c.ask(); d.ExpiresAt != want {
		t.Fatalf("expires_at %s, want decision time + 30 s %s", d.ExpiresAt, want)
	}
	c.advance(29 * time.Second)
	if d := c.ask(); d.ExpiresAt != want {
		t.Fatalf("a hit 29 s later: expires_at %s, want the stored %s", d.ExpiresAt, want)
	}
	if c.state.snapReads != 1 {
		t.Fatalf("%d snapshot reads, want 1", c.state.snapReads)
	}
}

// pm Q4: while the feed is not ready, the cache is neither read nor written, and decision_bound keeps the decision time.
func TestNotReadyBypassesTheCache(t *testing.T) {
	c := newCacheFixture(t)
	c.ready = false
	d := c.ask()
	c.ask()
	if c.state.snapReads != 2 || c.cache.Len() != 0 {
		t.Fatalf("snapshot reads %d, entries %d; want 2 and 0", c.state.snapReads, c.cache.Len())
	}
	if want := time.Unix(0, authorizeNow).UTC().Format(expiresAtLayout); d.ExpiresAt != want {
		t.Fatalf("expires_at %s while not ready, want the decision time %s", d.ExpiresAt, want)
	}
	if got := c.counter(MetricDecisionCacheLookups, "bypass"); got != 2 {
		t.Fatalf("bypass = %d, want 2", got)
	}
	c.ready = true
	c.ask()
	c.ask()
	if c.state.snapReads != 3 {
		t.Fatalf("%d snapshot reads after ready, want 3 (one miss, one hit)", c.state.snapReads)
	}
}

func TestWithoutACacheTheRouteIsUnchanged(t *testing.T) {
	c := newCacheFixture(t)
	c.rebuild(t, nil)
	d := c.ask()
	c.ask()
	if c.state.snapReads != 2 || c.cs.unfedReads != 0 {
		t.Fatalf("snapshot reads %d, unfed reads %d; want 2 and 0", c.state.snapReads, c.cs.unfedReads)
	}
	if want := time.Unix(0, authorizeNow).UTC().Format(expiresAtLayout); d.ExpiresAt != want {
		t.Fatalf("expires_at %s without a cache, want %s", d.ExpiresAt, want)
	}
}

func TestADelegatedSessionBypassesTheCache(t *testing.T) {
	c := newCacheFixture(t)
	s := c.state.sessions[c.sid]
	s.HasActor = true
	c.state.sessions[c.sid] = s
	c.ask()
	if c.cache.Len() != 0 || c.counter(MetricDecisionCacheLookups, "bypass") != 1 {
		t.Fatalf("entries %d, bypass %d; want 0 and 1", c.cache.Len(), c.counter(MetricDecisionCacheLookups, "bypass"))
	}
}

func TestABatchMixesAHitAndAMissAndAFailedReadPutsNothing(t *testing.T) {
	c := newCacheFixture(t)
	c.ask()
	rec := c.batch(c.question(lowAction, "organization", "acme", c.org), c.question("settings.write", "organization", "acme", c.org))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if got := c.counter(MetricDecisionCacheLookups, "hit"); got != 1 {
		t.Fatalf("hits %d, want 1", got)
	}
	before := c.cache.Len()
	c.cs.sessionErr = errors.New("store down")
	if rec := c.batch(c.question("members.read", "organization", "acme", c.org)); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", rec.Code)
	}
	if c.cache.Len() != before {
		t.Fatalf("entries %d after a failed transaction, want %d", c.cache.Len(), before)
	}
}

// acp condition 2 census: every value a decision reads is marked key, dep, tag or gate. A new method of the snapshot or
// of the transaction fails this cell until it has a row.
func TestEveryDecisionReadIsMarked(t *testing.T) {
	marks := map[string]string{
		"Snapshot.PrincipalStatus":    "dep principal + user; Unfed user active",
		"Snapshot.PlatformRole":       "Unfed superuser, or tag platform_role_assignment when owned",
		"Snapshot.OrganizationStatus": "dep organization",
		"Snapshot.Membership":         "dep membership + tag membership; Unfed public role",
		"Snapshot.Grants":             "dep resource_grant + tag resource_grant",
		"Snapshot.Workload":           "never true on this route (decisionstate)",
		"Snapshot.ServiceAccount":     "never true on this route (decisionstate)",
		"Snapshot.NowUnixNano":        "gate: the tx clock, read every call",
		"DecisionTx.Session":          "gate: read every call before the cache",
		"DecisionTx.Snapshot":         "dep and tag through DependencySnapshot; Begin before Read",
		"DecisionTx.Unfed":            "key: decisioncache.Key.Unfed, read every call",
	}
	var seen []string
	for name, typ := range map[string]reflect.Type{
		"Snapshot":   reflect.TypeOf((*authorization.GrantSnapshot)(nil)).Elem(),
		"DecisionTx": reflect.TypeOf((*DecisionTx)(nil)).Elem(),
	} {
		for i := range typ.NumMethod() {
			m := name + "." + typ.Method(i).Name
			seen = append(seen, m)
			if marks[m] == "" {
				t.Errorf("%s has no census row: mark it key, dep, tag or gate", m)
			}
		}
	}
	for m := range marks {
		if !slices.Contains(seen, m) {
			t.Errorf("census row %s names no method", m)
		}
	}
}
