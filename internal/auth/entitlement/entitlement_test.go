package entitlement_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/entitlement"
)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time          { return c.now }
func (c *clock) advance(d time.Duration) { c.now = c.now.Add(d) }

type fakeSource struct {
	mu      sync.Mutex
	content entitlement.Content
	err     error
	calls   atomic.Int64
	gate    chan struct{}
}

func (f *fakeSource) Load(_ context.Context, _ uuid.UUID) (entitlement.Content, error) {
	f.calls.Add(1)
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.content, f.err
}

func (f *fakeSource) set(c entitlement.Content, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.content, f.err = c, err
}

func team() entitlement.Content {
	return entitlement.Content{Tier: "team", Features: map[string]bool{"webhooks": true, "sso_saml": false}}
}

func newAdapter(t *testing.T, src entitlement.Source, clk *clock, maxOrgs int) *entitlement.Adapter {
	t.Helper()
	a, err := entitlement.New(entitlement.Config{Source: src, Now: clk.Now, MaxOrgs: maxOrgs})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestCheck_NoSnapshotDeniesEntitlementUnavailable(t *testing.T) {
	src := &fakeSource{err: errors.New("down")}
	a := newAdapter(t, src, &clock{now: time.Unix(1_000, 0)}, 8)
	got := a.Check(context.Background(), uuid.New(), "webhooks")
	if got.Allowed || got.Reason != "entitlement_unavailable" {
		t.Fatalf("got %+v", got)
	}
}

func TestCheck_ValidSnapshotAllowsGrantedFeatureOnly(t *testing.T) {
	src := &fakeSource{content: team()}
	a := newAdapter(t, src, &clock{now: time.Unix(1_000, 0)}, 8)
	org := uuid.New()
	if got := a.Check(context.Background(), org, "webhooks"); !got.Allowed || got.Reason != "entitled" {
		t.Fatalf("granted: %+v", got)
	}
	if got := a.Check(context.Background(), org, "sso_saml"); got.Allowed || got.Reason != "entitlement_not_granted" {
		t.Fatalf("false feature: %+v", got)
	}
}

func TestCheck_UnknownFeatureDenies(t *testing.T) {
	src := &fakeSource{content: team()}
	a := newAdapter(t, src, &clock{now: time.Unix(1_000, 0)}, 8)
	got := a.Check(context.Background(), uuid.New(), "no_such_feature")
	if got.Allowed || got.Reason != "entitlement_not_granted" {
		t.Fatalf("got %+v", got)
	}
}

func TestCheck_ExpiredSnapshotDeniesAndIsNotExtendedOnSourceOutage(t *testing.T) {
	clk := &clock{now: time.Unix(1_000, 0)}
	src := &fakeSource{content: team()}
	a := newAdapter(t, src, clk, 8)
	org := uuid.New()
	if !a.Check(context.Background(), org, "webhooks").Allowed {
		t.Fatal("first read must allow")
	}
	src.set(entitlement.Content{}, errors.New("down"))
	clk.advance(entitlement.MaxTTL + time.Second)
	for i := 0; i < 3; i++ {
		got := a.Check(context.Background(), org, "webhooks")
		if got.Allowed || got.Reason != "entitlement_unavailable" {
			t.Fatalf("outage read %d: %+v", i, got)
		}
		clk.advance(time.Second)
	}
}

func TestRefresh_SourceErrorKeepsOriginalExpiry(t *testing.T) {
	clk := &clock{now: time.Unix(1_000, 0)}
	src := &fakeSource{content: team()}
	a := newAdapter(t, src, clk, 8)
	org := uuid.New()
	if err := a.Refresh(context.Background(), org); err != nil {
		t.Fatal(err)
	}
	src.set(entitlement.Content{}, errors.New("down"))
	clk.advance(30 * time.Second)
	if err := a.Refresh(context.Background(), org); err == nil {
		t.Fatal("refresh must report the source error")
	}
	if !a.Check(context.Background(), org, "webhooks").Allowed {
		t.Fatal("snapshot still inside its first expiry must keep serving")
	}
	clk.advance(31 * time.Second)
	if got := a.Check(context.Background(), org, "webhooks"); got.Allowed {
		t.Fatalf("expiry was extended by a failed refresh: %+v", got)
	}
}

func TestNew_RefusesTTLAboveBound(t *testing.T) {
	for _, ttl := range []time.Duration{entitlement.MaxTTL + time.Nanosecond, time.Hour, -time.Second} {
		if _, err := entitlement.New(entitlement.Config{Source: &fakeSource{}, TTL: ttl}); err == nil {
			t.Fatalf("a TTL of %v must be refused", ttl)
		}
	}
}

func TestSnapshot_TTLAtBoundExpiresAtBound(t *testing.T) {
	clk := &clock{now: time.Unix(1_000, 0)}
	src := &fakeSource{content: team()}
	a, err := entitlement.New(entitlement.Config{Source: src, Now: clk.Now, TTL: entitlement.MaxTTL})
	if err != nil {
		t.Fatal(err)
	}
	org := uuid.New()
	a.Check(context.Background(), org, "webhooks")
	src.set(entitlement.Content{Tier: "community", Features: map[string]bool{}}, nil)
	clk.advance(entitlement.MaxTTL - time.Second)
	if !a.Check(context.Background(), org, "webhooks").Allowed {
		t.Fatal("snapshot must serve inside the bound")
	}
	clk.advance(time.Second)
	if got := a.Check(context.Background(), org, "webhooks"); got.Allowed {
		t.Fatalf("snapshot must expire at the bound: %+v", got)
	}
}

func TestSnapshot_RevisionChangesWithTierOrFeature(t *testing.T) {
	base := team()
	r0 := entitlement.Revision(base)
	if r0 == "" {
		t.Fatal("empty revision")
	}
	if entitlement.Revision(team()) != r0 {
		t.Fatal("revision is not deterministic")
	}
	tier := team()
	tier.Tier = "enterprise"
	feature := team()
	feature.Features["sso_saml"] = true
	if entitlement.Revision(tier) == r0 || entitlement.Revision(feature) == r0 {
		t.Fatal("revision must change with tier and with a feature")
	}
}

func TestDecision_ExposesRevision(t *testing.T) {
	src := &fakeSource{content: team()}
	a := newAdapter(t, src, &clock{now: time.Unix(1_000, 0)}, 8)
	org := uuid.New()
	got := a.Check(context.Background(), org, "webhooks")
	if got.Revision != entitlement.Revision(team()) {
		t.Fatalf("revision %q", got.Revision)
	}
	denied := a.Check(context.Background(), org, "sso_saml")
	if denied.Revision != got.Revision {
		t.Fatal("a not-granted decision carries the snapshot revision too")
	}
}

func TestDecision_UnavailableCarriesNoRevision(t *testing.T) {
	src := &fakeSource{err: errors.New("down")}
	a := newAdapter(t, src, &clock{now: time.Unix(1_000, 0)}, 8)
	if got := a.Check(context.Background(), uuid.New(), "webhooks"); got.Revision != "" {
		t.Fatalf("revision %q", got.Revision)
	}
}

type claimKey struct{}

func TestCheck_PlanTierNeverReadFromTokenClaims(t *testing.T) {
	typ := reflect.TypeOf((*entitlement.Adapter)(nil)).MethodByName
	m, ok := typ("Check")
	if !ok {
		t.Fatal("no Check")
	}
	want := []reflect.Type{
		reflect.TypeOf((*entitlement.Adapter)(nil)), reflect.TypeOf((*context.Context)(nil)).Elem(),
		reflect.TypeOf(uuid.UUID{}), reflect.TypeOf(""),
	}
	if m.Type.NumIn() != len(want) {
		t.Fatalf("Check takes %d inputs; only context, org id and feature are allowed", m.Type.NumIn())
	}
	for i, w := range want {
		if m.Type.In(i) != w {
			t.Fatalf("input %d is %v", i, m.Type.In(i))
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(entitlement.Snapshot{}), reflect.TypeOf(entitlement.Decision{}), reflect.TypeOf(entitlement.Content{})} {
		for i := 0; i < typ.NumField(); i++ {
			if n := typ.Field(i).Name; n == "Claims" || n == "Token" {
				t.Fatalf("%v has a %s field", typ, n)
			}
		}
	}
	src := &fakeSource{content: entitlement.Content{Tier: "community", Features: map[string]bool{}}}
	a := newAdapter(t, src, &clock{now: time.Unix(1_000, 0)}, 8)
	ctx := context.WithValue(context.Background(), claimKey{}, map[string]any{"tier": "enterprise", "licensed_features": []string{"webhooks"}})
	if got := a.Check(ctx, uuid.New(), "webhooks"); got.Allowed {
		t.Fatalf("a planted claim granted a feature: %+v", got)
	}
}

func TestAdapter_SingleflightPerOrg(t *testing.T) {
	src := &fakeSource{content: team(), gate: make(chan struct{})}
	a := newAdapter(t, src, &clock{now: time.Unix(1_000, 0)}, 8)
	org := uuid.New()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.Check(context.Background(), org, "webhooks")
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(src.gate)
	wg.Wait()
	if n := src.calls.Load(); n != 1 {
		t.Fatalf("source called %d times for one org", n)
	}
}

func TestAdapter_BoundedOrgMap(t *testing.T) {
	src := &fakeSource{content: team()}
	a := newAdapter(t, src, &clock{now: time.Unix(1_000, 0)}, 2)
	orgs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for _, o := range orgs {
		a.Check(context.Background(), o, "webhooks")
	}
	before := src.calls.Load()
	a.Check(context.Background(), orgs[2], "webhooks")
	a.Check(context.Background(), orgs[1], "webhooks")
	if src.calls.Load() != before {
		t.Fatal("the two newest orgs must stay cached")
	}
	a.Check(context.Background(), orgs[0], "webhooks")
	if src.calls.Load() != before+1 {
		t.Fatal("the oldest org must have been evicted at the bound")
	}
}

func TestNew_RequiresSource(t *testing.T) {
	if _, err := entitlement.New(entitlement.Config{}); err == nil {
		t.Fatal("a missing source must fail")
	}
}
