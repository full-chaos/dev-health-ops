package decisioncache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/revisionfeed"
)

var _ revisionfeed.Invalidator = (*Cache)(nil)

type marks struct {
	mu     sync.Mutex
	latest map[string]int64
}

func (m *marks) Latest(typ, id string) (int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.latest[typ+":"+id]
	return n, ok
}

func (m *marks) set(typ, id string, n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latest[typ+":"+id] = n
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type rig struct {
	c     *Cache
	marks *marks
	clock *clock
}

func newRig(t *testing.T, max int) *rig {
	t.Helper()
	r := &rig{marks: &marks{latest: map[string]int64{}}, clock: &clock{now: time.Unix(1_800_000_000, 0)}}
	c, err := New(Config{Catalog: policycatalog.Default(), Watermarks: r.marks, Now: r.clock.Now, MaxEntries: max})
	if err != nil {
		t.Fatal(err)
	}
	r.c = c
	return r
}

// action picks a catalog action by risk, cache class and resource kind, so the cells follow the catalog.
func action(t *testing.T, risk, class, kind string) policycatalog.Action {
	t.Helper()
	for _, a := range policycatalog.Default().Actions() {
		if a.RiskClass == risk && a.CacheClass == class && a.ResourceKind == kind {
			return a
		}
	}
	t.Fatalf("the catalog has no %s risk %s action on %s", risk, class, kind)
	return policycatalog.Action{}
}

func lowOrgAction(t *testing.T) string {
	return action(t, policycatalog.RiskLow, policycatalog.CacheDecisionBound, policycatalog.ScopeOrganization).ID
}

func baseKey(t *testing.T) Key {
	return Key{
		PrincipalID: "prn_1", SessionID: "ses_1",
		ActorChain:     []authorization.Hop{{ActorPrincipalID: "prn_actor", PermittedActions: []string{"b.read", "a.read"}}},
		OrganizationID: "org_1", Action: lowOrgAction(t), ResourceKind: policycatalog.ScopeOrganization, ResourceKey: "",
		Assurance: "aal2", PolicyRevision: policycatalog.Default().Version(), EntitlementRevision: 0,
	}
}

func allow(k Key) authorization.Decision {
	return authorization.Decision{Allowed: true, Reason: authorization.ReasonAllowedByRole, PolicyRevision: k.PolicyRevision}
}

func read(r *rig) Read {
	return Read{Dependencies: []Dependency{{AggregateType: "principal", AggregateID: "prn_1", Revision: 3},
		{AggregateType: "membership", AggregateID: "mem_1", Revision: 7}}, SetTypes: []string{"resource_grant"}, Stamp: r.c.Begin()}
}

func mustStore(t *testing.T, r *rig, k Key, d authorization.Decision, rd Read) {
	t.Helper()
	if got := r.c.Put(k, d, rd); got != Stored {
		t.Fatalf("Put = %q, want stored", got)
	}
}

func hit(t *testing.T, r *rig, k Key) authorization.Decision {
	t.Helper()
	d, ok := r.c.Get(k)
	if !ok {
		t.Fatal("Get missed, want a hit")
	}
	return d
}

func miss(t *testing.T, r *rig, k Key, why string) {
	t.Helper()
	if _, ok := r.c.Get(k); ok {
		t.Fatalf("Get hit, want a miss: %s", why)
	}
}

func TestTheBoundsAreOneConstantEach(t *testing.T) {
	if TTL != 30*time.Second || DefaultMaxEntries != 10000 {
		t.Fatalf("TTL %s, DefaultMaxEntries %d, want 30s and 10000 (acp Q2)", TTL, DefaultMaxEntries)
	}
}

func TestAStoredAllowHitsOnTheSameKey(t *testing.T) {
	r := newRig(t, 0)
	k := baseKey(t)
	mustStore(t, r, k, allow(k), read(r))
	if d := hit(t, r, k); !d.Allowed || d.Reason != authorization.ReasonAllowedByRole {
		t.Fatalf("hit = %+v", d)
	}
	same := baseKey(t)
	same.ActorChain = []authorization.Hop{{ActorPrincipalID: "prn_actor", PermittedActions: []string{"a.read", "b.read", "a.read"}}}
	hit(t, r, same) // the chain is normalized: action order and duplicates do not change the key
}

func TestEachKeyComponentVariedMisses(t *testing.T) {
	other := action(t, policycatalog.RiskLow, policycatalog.CacheDecisionBound, policycatalog.ScopeOrganization).ID + ".other"
	for _, tc := range []struct {
		name string
		vary func(*Key)
	}{
		{"principal", func(k *Key) { k.PrincipalID = "prn_2" }},
		{"session", func(k *Key) { k.SessionID = "ses_2" }},
		{"actor chain actor", func(k *Key) { k.ActorChain[0].ActorPrincipalID = "prn_actor_2" }},
		{"actor chain permitted actions", func(k *Key) { k.ActorChain[0].PermittedActions = []string{"a.read"} }},
		{"actor chain absent", func(k *Key) { k.ActorChain = nil }},
		{"actor chain nested", func(k *Key) { k.ActorChain = append(k.ActorChain, authorization.Hop{ActorPrincipalID: "prn_3"}) }},
		{"organization", func(k *Key) { k.OrganizationID = "org_2" }},
		{"action", func(k *Key) { k.Action = other }},
		{"resource kind", func(k *Key) { k.ResourceKind = "repository" }},
		{"resource key", func(k *Key) { k.ResourceKey = "acme/web" }},
		{"assurance", func(k *Key) { k.Assurance = "aal3" }},
		{"policy revision", func(k *Key) { k.PolicyRevision++ }},
		{"entitlement revision", func(k *Key) { k.EntitlementRevision = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, 0)
			k := baseKey(t)
			mustStore(t, r, k, allow(k), read(r))
			varied := baseKey(t)
			tc.vary(&varied)
			miss(t, r, varied, tc.name+" differs")
			hit(t, r, k)
		})
	}
}

func TestAFieldBoundaryCannotForgeAnotherKey(t *testing.T) {
	r := newRig(t, 0)
	k := baseKey(t)
	k.PrincipalID, k.SessionID = "prn_1,ses", "1"
	mustStore(t, r, k, allow(k), read(r))
	forged := baseKey(t)
	forged.PrincipalID, forged.SessionID = "prn_1", "ses,1"
	miss(t, r, forged, "two fields that join to the same text")
}

func TestARevisionBumpOfADependencyMisses(t *testing.T) {
	r := newRig(t, 0)
	k := baseKey(t)
	mustStore(t, r, k, allow(k), read(r))
	r.marks.set("membership", "mem_1", 7)
	hit(t, r, k)
	r.marks.set("membership", "mem_1", 8)
	miss(t, r, k, "the membership the decision read moved to 8")
	r.marks.set("membership", "mem_1", 7)
	miss(t, r, k, "the entry was dropped, not only hidden")
}

func TestInvalidateEvictsTheEntriesOfTheAggregate(t *testing.T) {
	r := newRig(t, 0)
	k := baseKey(t)
	mustStore(t, r, k, allow(k), Read{Dependencies: []Dependency{{AggregateType: "principal", AggregateID: "prn_1", Revision: 3}}, Stamp: r.c.Begin()})
	if err := r.c.Invalidate(t.Context(), "principal", "prn_9", 4); err != nil {
		t.Fatal(err)
	}
	hit(t, r, k) // another principal
	if err := r.c.Invalidate(t.Context(), "principal", "prn_1", 3); err != nil {
		t.Fatal(err)
	}
	hit(t, r, k) // the revision the decision read
	if err := r.c.Invalidate(t.Context(), "principal", "prn_1", 4); err != nil {
		t.Fatal(err)
	}
	miss(t, r, k, "principal prn_1 moved to 4")
}

// acp Q3: absence is a dependency. A decision that read the principal's grants carries a type tag for the set, and a grant
// the snapshot never saw (a new deny) evicts it.
func TestANewGrantOfATaggedSetEvictsTheAllow(t *testing.T) {
	r := newRig(t, 0)
	k := baseKey(t)
	mustStore(t, r, k, allow(k), read(r))
	if err := r.c.Invalidate(t.Context(), "resource_grant", "grt_new_deny", 1); err != nil {
		t.Fatal(err)
	}
	miss(t, r, k, "a new resource_grant arrived after the decision read the set")
}

func TestAnEntryWithoutTheTypeTagSurvivesAnotherTypesEvent(t *testing.T) {
	r := newRig(t, 0)
	k := baseKey(t)
	mustStore(t, r, k, allow(k), Read{Dependencies: []Dependency{{AggregateType: "principal", AggregateID: "prn_1", Revision: 3}}, Stamp: r.c.Begin()})
	if err := r.c.Invalidate(t.Context(), "resource_grant", "grt_new", 1); err != nil {
		t.Fatal(err)
	}
	hit(t, r, k)
}

// The type tag on the watermark path: a Put whose read began before a set event, arriving after that event's eviction, is
// stored and misses (its stamp is older than the event).
func TestALatePutOfATaggedSetMissesAfterTheEvent(t *testing.T) {
	r := newRig(t, 0)
	k := baseKey(t)
	rd := read(r)
	if err := r.c.Invalidate(t.Context(), "resource_grant", "grt_new_deny", 1); err != nil {
		t.Fatal(err)
	}
	mustStore(t, r, k, allow(k), rd)
	miss(t, r, k, "the read began before the resource_grant event")
	k2 := baseKey(t)
	k2.SessionID = "ses_2"
	mustStore(t, r, k2, allow(k2), read(r)) // a read that began after it hits
	hit(t, r, k2)
}

func TestInvalidateAllClearsAndALatePutMisses(t *testing.T) {
	r := newRig(t, 0)
	k := baseKey(t)
	rd := read(r)
	mustStore(t, r, k, allow(k), rd)
	if err := r.c.InvalidateAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	miss(t, r, k, "InvalidateAll")
	mustStore(t, r, k, allow(k), rd)
	miss(t, r, k, "the read began before the InvalidateAll")
}

// acp condition 3: a decision read before an event, Put after that event's eviction (the consumer invalidates BEFORE it
// moves the mark), is stored, and Get misses once the mark moved. The consumer is the real one.
func TestAPutBetweenTheEvictionAndTheMarkMissesOnceTheMarkMoves(t *testing.T) {
	clk := &clock{now: time.Unix(1_800_000_000, 0)}
	inv := &lateInvalidator{}
	consumer, err := revisionfeed.NewConsumer(revisionfeed.Config{Invalidator: inv})
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(Config{Catalog: policycatalog.Default(), Watermarks: consumer.Watermarks(), Now: clk.Now})
	if err != nil {
		t.Fatal(err)
	}
	inv.c = c
	ctx := t.Context()
	if _, err := consumer.Handle(ctx, provisioning("principal", "prn_1", 1)); err != nil {
		t.Fatal(err)
	}
	k := baseKey(t)
	rd := Read{Dependencies: []Dependency{{AggregateType: "principal", AggregateID: "prn_1", Revision: 1}}, Stamp: c.Begin()}
	var put PutResult
	var hitBeforeMark bool
	inv.after = func() {
		put = c.Put(k, allow(k), rd)
		_, hitBeforeMark = c.Get(k)
	}
	if _, err := consumer.Handle(ctx, provisioning("principal", "prn_1", 2)); err != nil {
		t.Fatal(err)
	}
	if put != Stored {
		t.Fatalf("the late Put = %q, want stored", put)
	}
	if !hitBeforeMark {
		t.Fatal("the late entry missed before the mark moved; the cell does not reach the watermark bound")
	}
	if n, _ := consumer.Watermarks().Latest("principal", "prn_1"); n != 2 {
		t.Fatalf("mark %d, want 2", n)
	}
	if _, ok := c.Get(k); ok {
		t.Fatal("the entry read at revision 1 hit after the mark moved to 2")
	}
}

type lateInvalidator struct {
	c     *Cache
	after func()
}

func (l *lateInvalidator) Invalidate(ctx context.Context, typ, id string, rev int64) error {
	if l.c == nil {
		return nil
	}
	err := l.c.Invalidate(ctx, typ, id, rev)
	if l.after != nil {
		l.after()
		l.after = nil
	}
	return err
}

func (l *lateInvalidator) InvalidateAll(ctx context.Context) error { return l.c.InvalidateAll(ctx) }

func provisioning(typ, id string, rev int64) contracts.ProvisioningEvent {
	return contracts.ProvisioningEvent{AggregateType: typ, AggregateID: id, Payload: contracts.ProvisioningPayload{Revision: contracts.Revision(rev)}}
}

func TestADecisionBoundEntryDiesAtThirtySecondsWithTheFeedDown(t *testing.T) {
	r := newRig(t, 0)
	k := baseKey(t)
	mustStore(t, r, k, allow(k), read(r))
	r.clock.advance(TTL - time.Nanosecond)
	hit(t, r, k)
	r.clock.advance(time.Nanosecond)
	miss(t, r, k, "30 s passed; no event arrived")
}

func TestAHitReturnsTheStoredExpiryNotNowPlusTTL(t *testing.T) {
	r := newRig(t, 0)
	k := baseKey(t)
	want := r.clock.Now().Add(TTL).UnixNano()
	mustStore(t, r, k, allow(k), read(r))
	r.clock.advance(TTL - time.Second)
	if d := hit(t, r, k); d.ExpiresAtUnixNano != want {
		t.Fatalf("expires %d near expiry, want the stored %d (acp condition 1)", d.ExpiresAtUnixNano, want)
	}
}

func repoKey(t *testing.T) Key {
	k := baseKey(t)
	a := action(t, policycatalog.RiskLow, policycatalog.CacheResourceRevisionBound, "repository")
	k.Action, k.ResourceKind, k.ResourceKey = a.ID, "repository", "acme/api"
	return k
}

func TestAResourceRevisionBoundEntryDiesAtTheDecisionsExpiry(t *testing.T) {
	r := newRig(t, 0)
	k := repoKey(t)
	exp := r.clock.Now().Add(10 * time.Second).UnixNano()
	d := allow(k)
	d.CacheClass, d.ExpiresAtUnixNano = authorization.CacheResourceRevisionBound, exp
	mustStore(t, r, k, d, read(r))
	r.clock.advance(5 * time.Second)
	if got := hit(t, r, k); got.ExpiresAtUnixNano != exp {
		t.Fatalf("expires %d, want the decision's %d", got.ExpiresAtUnixNano, exp)
	}
	r.clock.advance(5 * time.Second)
	miss(t, r, k, "the decision's expiry passed")
}

func TestAnExpiryPastThirtySecondsIsCutToThirty(t *testing.T) {
	r := newRig(t, 0)
	k := repoKey(t)
	d := allow(k)
	d.CacheClass, d.ExpiresAtUnixNano = authorization.CacheResourceRevisionBound, r.clock.Now().Add(time.Hour).UnixNano()
	want := r.clock.Now().Add(TTL).UnixNano()
	mustStore(t, r, k, d, read(r))
	if got := hit(t, r, k); got.ExpiresAtUnixNano != want {
		t.Fatalf("expires %d, want now+30s %d", got.ExpiresAtUnixNano, want)
	}
	r.clock.advance(TTL)
	miss(t, r, k, "30 s passed")
}

func TestWhatIsNeverStored(t *testing.T) {
	high := action(t, policycatalog.RiskHigh, policycatalog.CacheNone, policycatalog.ScopeOrganization).ID
	for _, tc := range []struct {
		name string
		make func(t *testing.T, r *rig) (Key, authorization.Decision)
		want PutResult
	}{
		{"deny", func(t *testing.T, r *rig) (Key, authorization.Decision) {
			k := baseKey(t)
			return k, authorization.Decision{Reason: authorization.ReasonRoleLacksAction, PolicyRevision: k.PolicyRevision}
		}, NotStoredDeny},
		{"deny that claims a class", func(t *testing.T, r *rig) (Key, authorization.Decision) {
			k := baseKey(t)
			return k, authorization.Decision{Reason: authorization.ReasonRoleLacksAction, PolicyRevision: k.PolicyRevision, CacheClass: authorization.CacheDecisionBound}
		}, NotStoredDeny},
		{"allow with an audit obligation", func(t *testing.T, r *rig) (Key, authorization.Decision) {
			k := baseKey(t)
			d := allow(k)
			d.Audit = &authorization.DelegationAudit{ActorPrincipalID: "prn_actor", OrganizationID: "org_1", Action: k.Action, Reason: authorization.AuditReasonOwnerExempt}
			return k, d
		}, NotStoredAudit},
		{"high risk allow", func(t *testing.T, r *rig) (Key, authorization.Decision) {
			k := baseKey(t)
			k.Action = high
			return k, allow(k)
		}, NotStoredHighRisk},
		{"high risk allow that claims decision_bound", func(t *testing.T, r *rig) (Key, authorization.Decision) {
			k := baseKey(t)
			k.Action = high
			d := allow(k)
			d.CacheClass = authorization.CacheDecisionBound
			return k, d
		}, NotStoredHighRisk},
		{"class none", func(t *testing.T, r *rig) (Key, authorization.Decision) {
			k := baseKey(t)
			d := allow(k)
			d.CacheClass = authorization.CacheNone
			return k, d
		}, NotStoredClassNone},
		{"unknown class", func(t *testing.T, r *rig) (Key, authorization.Decision) {
			k := baseKey(t)
			d := allow(k)
			d.CacheClass = "forever"
			return k, d
		}, NotStoredClassNone},
		{"unknown action", func(t *testing.T, r *rig) (Key, authorization.Decision) {
			k := baseKey(t)
			k.Action = "no.such.action"
			return k, allow(k)
		}, NotStoredUnknownAction},
		{"resource_revision_bound without an expiry", func(t *testing.T, r *rig) (Key, authorization.Decision) {
			k := repoKey(t)
			d := allow(k)
			d.CacheClass = authorization.CacheResourceRevisionBound
			return k, d
		}, NotStoredExpired},
		{"already expired", func(t *testing.T, r *rig) (Key, authorization.Decision) {
			k := repoKey(t)
			d := allow(k)
			d.CacheClass, d.ExpiresAtUnixNano = authorization.CacheResourceRevisionBound, r.clock.Now().UnixNano()
			return k, d
		}, NotStoredExpired},
		{"policy revision differs from the key", func(t *testing.T, r *rig) (Key, authorization.Decision) {
			k := baseKey(t)
			d := allow(k)
			d.PolicyRevision++
			return k, d
		}, NotStoredInvalid},
		{"no principal", func(t *testing.T, r *rig) (Key, authorization.Decision) {
			k := baseKey(t)
			k.PrincipalID = ""
			return k, allow(k)
		}, NotStoredInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, 0)
			k, d := tc.make(t, r)
			if got := r.c.Put(k, d, read(r)); got != tc.want {
				t.Fatalf("Put = %q, want %q", got, tc.want)
			}
			miss(t, r, k, "nothing was stored")
		})
	}
}

func TestAMalformedReadIsNotStored(t *testing.T) {
	for _, dep := range []Dependency{{AggregateType: "", AggregateID: "x", Revision: 1}, {AggregateType: "principal", AggregateID: "", Revision: 1},
		{AggregateType: "principal", AggregateID: "x", Revision: 0}} {
		r := newRig(t, 0)
		k := baseKey(t)
		if got := r.c.Put(k, allow(k), Read{Dependencies: []Dependency{dep}, Stamp: r.c.Begin()}); got != NotStoredInvalid {
			t.Fatalf("Put with %+v = %q, want invalid", dep, got)
		}
	}
}

func TestTheReturnedDecisionIsACopy(t *testing.T) {
	r := newRig(t, 0)
	k := baseKey(t)
	d := allow(k)
	d.Obligations = &authorization.Obligations{TenantScope: "org_1", MinimumAssurance: "aal2", RepositoryAllowlist: []string{"acme/api"}}
	mustStore(t, r, k, d, read(r))
	d.Obligations.RepositoryAllowlist[0] = "acme/other"
	got := hit(t, r, k)
	got.Obligations.RepositoryAllowlist[0] = "acme/third"
	if again := hit(t, r, k); again.Obligations.RepositoryAllowlist[0] != "acme/api" {
		t.Fatalf("stored obligations changed to %q", again.Obligations.RepositoryAllowlist[0])
	}
}

func TestTheCacheIsBoundedLeastRecentlyUsedFirst(t *testing.T) {
	r := newRig(t, 3)
	keys := make([]Key, 4)
	for i := range keys {
		keys[i] = baseKey(t)
		keys[i].SessionID = fmt.Sprintf("ses_%d", i)
	}
	for _, k := range keys[:3] {
		mustStore(t, r, k, allow(k), read(r))
	}
	hit(t, r, keys[0]) // used: keys[1] is now the least recently used
	mustStore(t, r, keys[3], allow(keys[3]), read(r))
	if n := r.c.Len(); n != 3 {
		t.Fatalf("%d entries, want the bound 3", n)
	}
	miss(t, r, keys[1], "least recently used")
	hit(t, r, keys[0])
	hit(t, r, keys[2])
	hit(t, r, keys[3])
}

func TestTheDefaultBoundIsTenThousand(t *testing.T) {
	r := newRig(t, 0)
	for i := range DefaultMaxEntries + 5 {
		k := baseKey(t)
		k.SessionID = fmt.Sprintf("ses_%d", i)
		mustStore(t, r, k, allow(k), Read{Stamp: r.c.Begin()})
	}
	if n := r.c.Len(); n != DefaultMaxEntries {
		t.Fatalf("%d entries, want %d", n, DefaultMaxEntries)
	}
}

func TestAClosedCacheRefusesAndReturnsErrClosed(t *testing.T) {
	r := newRig(t, 0)
	k := baseKey(t)
	mustStore(t, r, k, allow(k), read(r))
	r.c.Close()
	miss(t, r, k, "closed")
	if got := r.c.Put(k, allow(k), read(r)); got != NotStoredClosed {
		t.Fatalf("Put on a closed cache = %q", got)
	}
	if err := r.c.Invalidate(t.Context(), "principal", "prn_1", 9); !errors.Is(err, ErrClosed) {
		t.Fatalf("Invalidate = %v, want ErrClosed", err)
	}
	if err := r.c.InvalidateAll(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("InvalidateAll = %v, want ErrClosed", err)
	}
}

func TestNewRefusesMissingInputs(t *testing.T) {
	if _, err := New(Config{Watermarks: &marks{}}); err == nil {
		t.Fatal("no catalog accepted")
	}
	if _, err := New(Config{Catalog: policycatalog.Default()}); err == nil {
		t.Fatal("no watermarks accepted")
	}
	if _, err := New(Config{Catalog: policycatalog.Default(), Watermarks: &marks{}, MaxEntries: -1}); err == nil {
		t.Fatal("a negative bound accepted")
	}
}

func TestConcurrentGetPutInvalidate(t *testing.T) {
	r := newRig(t, 64)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				k := baseKey(t)
				k.SessionID = fmt.Sprintf("ses_%d_%d", g, i%40)
				switch i % 5 {
				case 0, 1:
					r.c.Put(k, allow(k), read(r))
				case 2:
					r.c.Get(k)
				case 3:
					_ = r.c.Invalidate(context.Background(), "membership", "mem_1", int64(8+i))
				default:
					r.marks.set("principal", "prn_1", int64(i))
					r.clock.advance(time.Millisecond)
					if i%50 == 4 {
						_ = r.c.InvalidateAll(context.Background())
					}
				}
			}
		}()
	}
	wg.Wait()
	if n := r.c.Len(); n > 64 {
		t.Fatalf("%d entries over the bound 64", n)
	}
}

// acp on point 2: a Put without a stamp of this cache is refused, so a caller cannot skip Begin.
func TestAPutWithoutAStampOfThisCacheIsRefused(t *testing.T) {
	r := newRig(t, 0)
	other := newRig(t, 0)
	k := baseKey(t)
	for name, rd := range map[string]Read{
		"zero stamp":    {Dependencies: read(r).Dependencies},
		"foreign stamp": {Dependencies: read(r).Dependencies, Stamp: other.c.Begin()},
	} {
		if got := r.c.Put(k, allow(k), rd); got != NotStoredStamp {
			t.Fatalf("%s: Put = %q, want %q", name, got, NotStoredStamp)
		}
		miss(t, r, k, name)
	}
	mustStore(t, r, k, allow(k), read(r))
}

// acp on point 4 (condition 1): the 30 s cut is in the STORED decision, and every hit returns that value.
func TestTheCutExpiryIsTheStoredOneOnEveryHit(t *testing.T) {
	r := newRig(t, 0)
	k := repoKey(t)
	put := r.clock.Now()
	d := allow(k)
	d.CacheClass, d.ExpiresAtUnixNano = authorization.CacheResourceRevisionBound, put.Add(60*time.Second).UnixNano()
	mustStore(t, r, k, d, read(r))
	first := hit(t, r, k).ExpiresAtUnixNano
	if limit := put.Add(TTL).UnixNano(); first > limit {
		t.Fatalf("a hit returned expires_at %d, after Put time + 30 s %d", first, limit)
	}
	r.clock.advance(20 * time.Second)
	if again := hit(t, r, k).ExpiresAtUnixNano; again != first {
		t.Fatalf("a later hit returned %d, the first %d: the value is not the stored one", again, first)
	}
}

// The Put reasons are a closed set: they may be metric labels.
func TestThePutResultsAreAClosedSet(t *testing.T) {
	want := []string{"stored", "deny", "audit_obligation", "high_risk", "class_none", "unknown_action", "expired", "invalid", "stamp", "closed"}
	got := PutResults()
	if len(got) != len(want) {
		t.Fatalf("PutResults = %q, want %q", got, want)
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("PutResults = %q, want %q", got, want)
		}
	}
}
