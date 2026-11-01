// Package decisioncache is the server-side allow cache of the policy decision point (ACP 5.8, TRD section 14). It is a
// library: no route and no wiring use it yet. It stores allows only, bounded three ways, each enough alone to drop a
// stale allow after a revoke:
//
//   - expiry: an entry dies at its stored expiry on the injected clock, at most TTL after the Put, with or without the feed;
//   - eviction: as a revisionfeed.Invalidator it drops the entries that read an aggregate when that aggregate moves, and
//     the entries that read a set of an aggregate type (a type tag) when any aggregate of that type moves, because a new
//     row (a new deny grant) has an id no entry holds: absence is a dependency;
//   - watermark: Get misses when a dependency's watermark is above the revision the decision read, or when an event of a
//     tagged type (or InvalidateAll) came after the read began (Begin), so a Put that lands after its event's eviction is
//     never served.
//
// Never stored: a deny, an allow of a high-risk action or of cache class none, and an allow that carries an Audit
// obligation (the owner exemption of a delegated session), so the decision point never skips its audit emission.
//
// Named limit: the key carries EntitlementRevision, and it is 0 today. No entitlement aggregate exists (the entitlement
// snapshot table is dormant), so no feed event moves it; when one exists it becomes a dependency like the others.
package decisioncache

import (
	"container/list"
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const (
	// TTL is the bound of a decision_bound entry (TRD section 14, low risk) and the ceiling of every entry.
	TTL = 30 * time.Second
	// DefaultMaxEntries is the size bound when Config.MaxEntries is 0; the least recently used entry goes first.
	DefaultMaxEntries = 10000
)

// ErrClosed is the only error of Invalidate and InvalidateAll.
var ErrClosed = errors.New("decisioncache: the cache is closed")

// Watermarks is the latest revision seen per aggregate (revisionfeed.Watermarks).
type Watermarks interface {
	Latest(aggregateType, aggregateID string) (int64, bool)
}

// Config is the cache's inputs. Catalog and Watermarks are required; Now defaults to time.Now.
type Config struct {
	Catalog    *policycatalog.Catalog
	Watermarks Watermarks
	Now        func() time.Time
	MaxEntries int
}

// Key is what a decision depends on besides state. Two requests that differ in any field never share an entry.
type Key struct {
	PrincipalID string
	SessionID   string
	// ActorChain is the 5.4 chain of a delegated session, empty when acting as itself. Hop order counts; the permitted
	// actions of a hop are a set.
	ActorChain          []authorization.Hop
	OrganizationID      string
	Action              string
	ResourceKind        string
	ResourceKey         string
	Assurance           string
	PolicyRevision      int
	EntitlementRevision int64
}

func (k Key) encode() string {
	b := make([]byte, 0, 128)
	for _, s := range []string{k.PrincipalID, k.SessionID, k.OrganizationID, k.Action, k.ResourceKind, k.ResourceKey, k.Assurance} {
		b = strconv.AppendQuote(b, s)
		b = append(b, ',')
	}
	b = strconv.AppendInt(b, int64(k.PolicyRevision), 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, k.EntitlementRevision, 10)
	b = append(b, ",["...)
	for _, hop := range k.ActorChain {
		b = strconv.AppendQuote(b, hop.ActorPrincipalID)
		b = append(b, '{')
		actions := slices.Clone(hop.PermittedActions)
		slices.Sort(actions)
		for _, a := range slices.Compact(actions) {
			b = strconv.AppendQuote(b, a)
			b = append(b, ',')
		}
		b = append(b, '}')
	}
	return string(append(b, ']'))
}

// Dependency is one aggregate the decision read, at the revision it read.
type Dependency struct {
	AggregateType string
	AggregateID   string
	Revision      int64
}

// Stamp is the cache's event count when a read began (Begin).
type Stamp uint64

// Read is what the decision read: the aggregates, the aggregate types it read as a set (absence counts), and the Stamp
// taken before the read.
type Read struct {
	Dependencies []Dependency
	SetTypes     []string
	Stamp        Stamp
}

// PutResult says whether Put stored the decision, and why not.
type PutResult string

const (
	Stored                 PutResult = "stored"
	NotStoredDeny          PutResult = "deny"
	NotStoredAudit         PutResult = "audit_obligation"
	NotStoredHighRisk      PutResult = "high_risk"
	NotStoredClassNone     PutResult = "class_none"
	NotStoredUnknownAction PutResult = "unknown_action"
	NotStoredExpired       PutResult = "expired"
	NotStoredInvalid       PutResult = "invalid"
	NotStoredClosed        PutResult = "closed"
)

type entry struct {
	key      string
	decision authorization.Decision
	expires  int64
	deps     []Dependency
	types    []string
	stamp    Stamp
}

// Cache is safe for concurrent use.
type Cache struct {
	catalog *policycatalog.Catalog
	marks   Watermarks
	now     func() time.Time
	max     int

	mu      sync.Mutex
	closed  bool
	lru     *list.List // front = most recently used; values are *entry
	entries map[string]*list.Element
	byAgg   map[string]map[*list.Element]struct{}
	byType  map[string]map[*list.Element]struct{}
	gen     Stamp
	allGen  Stamp
	typeGen map[string]Stamp
}

// New validates cfg and returns an empty cache.
func New(cfg Config) (*Cache, error) {
	if cfg.Catalog == nil || cfg.Watermarks == nil {
		return nil, errors.New("decisioncache: a catalog and watermarks are required")
	}
	if cfg.MaxEntries < 0 {
		return nil, errors.New("decisioncache: MaxEntries must not be negative")
	}
	c := &Cache{catalog: cfg.Catalog, marks: cfg.Watermarks, now: cfg.Now, max: cfg.MaxEntries}
	if c.now == nil {
		c.now = time.Now
	}
	if c.max == 0 {
		c.max = DefaultMaxEntries
	}
	c.reset()
	return c, nil
}

func (c *Cache) reset() {
	c.lru = list.New()
	c.entries = map[string]*list.Element{}
	c.byAgg = map[string]map[*list.Element]struct{}{}
	c.byType = map[string]map[*list.Element]struct{}{}
	if c.typeGen == nil {
		c.typeGen = map[string]Stamp{}
	}
}

// Begin returns the Stamp a caller takes BEFORE it reads the state a decision depends on, and passes in Read.
func (c *Cache) Begin() Stamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen
}

// Put stores an allow that may be reused, and says why when it does not.
func (c *Cache) Put(k Key, d authorization.Decision, r Read) PutResult {
	if !d.Allowed {
		return NotStoredDeny
	}
	if d.Audit != nil {
		return NotStoredAudit
	}
	if k.PrincipalID == "" || k.Action == "" || d.PolicyRevision != k.PolicyRevision {
		return NotStoredInvalid
	}
	for _, dep := range r.Dependencies {
		if dep.AggregateType == "" || dep.AggregateID == "" || dep.Revision < 1 {
			return NotStoredInvalid
		}
	}
	act, err := c.catalog.Action(k.Action)
	if err != nil {
		return NotStoredUnknownAction
	}
	if act.RiskClass != policycatalog.RiskLow {
		return NotStoredHighRisk
	}
	class := d.CacheClass
	if class == "" {
		class = authorization.CacheClass(act.CacheClass)
	}
	now := c.now().UnixNano()
	expires := now + int64(TTL)
	switch {
	case act.CacheClass == policycatalog.CacheNone || class == authorization.CacheNone:
		return NotStoredClassNone
	case class == authorization.CacheDecisionBound:
		if d.ExpiresAtUnixNano > 0 {
			expires = min(expires, d.ExpiresAtUnixNano)
		}
	case class == authorization.CacheResourceRevisionBound:
		if d.ExpiresAtUnixNano <= 0 {
			return NotStoredExpired
		}
		expires = min(expires, d.ExpiresAtUnixNano)
	default:
		return NotStoredClassNone
	}
	if expires <= now {
		return NotStoredExpired
	}

	e := &entry{key: k.encode(), decision: copyDecision(d), expires: expires, deps: slices.Clone(r.Dependencies),
		types: slices.Clone(r.SetTypes), stamp: r.Stamp}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return NotStoredClosed
	}
	if old, ok := c.entries[e.key]; ok {
		c.remove(old)
	}
	el := c.lru.PushFront(e)
	c.entries[e.key] = el
	for _, dep := range e.deps {
		index(c.byAgg, contracts.AggregateKey(dep.AggregateType, dep.AggregateID), el)
	}
	for _, t := range e.types {
		index(c.byType, t, el)
	}
	for c.lru.Len() > c.max {
		c.remove(c.lru.Back())
	}
	return Stored
}

// Get returns a stored allow for k, with ExpiresAtUnixNano set to the STORED expiry (never now plus TTL). It misses on
// expiry, on a dependency whose watermark is above the revision read, and on an event of a tagged type or an
// InvalidateAll after the read began; such an entry is dropped.
func (c *Cache) Get(k Key) (authorization.Decision, bool) {
	key := k.encode()
	now := c.now().UnixNano()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return authorization.Decision{}, false
	}
	el, ok := c.entries[key]
	if !ok {
		return authorization.Decision{}, false
	}
	e := el.Value.(*entry)
	if now >= e.expires || c.stale(e) {
		c.remove(el)
		return authorization.Decision{}, false
	}
	c.lru.MoveToFront(el)
	d := copyDecision(e.decision)
	d.ExpiresAtUnixNano = e.expires
	return d, true
}

func (c *Cache) stale(e *entry) bool {
	if c.allGen > e.stamp {
		return true
	}
	for _, t := range e.types {
		if c.typeGen[t] > e.stamp {
			return true
		}
	}
	for _, dep := range e.deps {
		if latest, seen := c.marks.Latest(dep.AggregateType, dep.AggregateID); seen && latest > dep.Revision {
			return true
		}
	}
	return false
}

// Invalidate drops the entries that read the aggregate below revision, and every entry that read a set of its type.
func (c *Cache) Invalidate(_ context.Context, aggregateType, aggregateID string, revision int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	c.gen++
	c.typeGen[aggregateType] = c.gen
	for el := range c.byAgg[contracts.AggregateKey(aggregateType, aggregateID)] {
		for _, dep := range el.Value.(*entry).deps {
			if dep.AggregateType == aggregateType && dep.AggregateID == aggregateID && dep.Revision < revision {
				c.remove(el)
				break
			}
		}
	}
	for el := range c.byType[aggregateType] {
		c.remove(el)
	}
	return nil
}

// InvalidateAll drops every entry; a read that began before it is never served.
func (c *Cache) InvalidateAll(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	c.gen++
	c.allGen = c.gen
	c.reset()
	return nil
}

// Close drops every entry; afterwards Get misses, Put stores nothing and the invalidations return ErrClosed.
func (c *Cache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.reset()
}

// Len is the number of entries held (expired ones included until a Get or the bound drops them).
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

func (c *Cache) remove(el *list.Element) {
	e := el.Value.(*entry)
	if c.entries[e.key] != el {
		return
	}
	delete(c.entries, e.key)
	c.lru.Remove(el)
	for _, dep := range e.deps {
		unindex(c.byAgg, contracts.AggregateKey(dep.AggregateType, dep.AggregateID), el)
	}
	for _, t := range e.types {
		unindex(c.byType, t, el)
	}
}

func index(m map[string]map[*list.Element]struct{}, k string, el *list.Element) {
	set := m[k]
	if set == nil {
		set = map[*list.Element]struct{}{}
		m[k] = set
	}
	set[el] = struct{}{}
}

func unindex(m map[string]map[*list.Element]struct{}, k string, el *list.Element) {
	if set := m[k]; set != nil {
		delete(set, el)
		if len(set) == 0 {
			delete(m, k)
		}
	}
}

func copyDecision(d authorization.Decision) authorization.Decision {
	if d.Obligations != nil {
		o := *d.Obligations
		o.RepositoryAllowlist = slices.Clone(o.RepositoryAllowlist)
		d.Obligations = &o
	}
	return d
}
