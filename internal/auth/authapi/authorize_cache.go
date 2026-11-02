package authapi

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// The decision cache metrics. lookups and puts carry one closed label, result.
const (
	MetricDecisionCacheLookups = "devhealth_auth_decision_cache_lookups_total"
	MetricDecisionCachePuts    = "devhealth_auth_decision_cache_puts_total"
	MetricDecisionCacheEntries = "devhealth_auth_decision_cache_entries"
)

// The lookup results: the decision was served from the cache, decided and offered to it, or decided without it (no cache,
// the feed not ready, or a delegated session).
const (
	cacheHit    = "hit"
	cacheMiss   = "miss"
	cacheBypass = "bypass"
)

// DecisionCache is the decision cache the routes ask (decisioncache.Cache).
type DecisionCache interface {
	Begin() decisioncache.Stamp
	Get(decisioncache.Key) (authorization.Decision, bool)
	Put(decisioncache.Key, authorization.Decision, decisioncache.Read) decisioncache.PutResult
	Len() int
}

// UnfedState is what a decision reads that no feed event covers: go-api's public.users and public.memberships rows. It is
// read on every call while the cache is in use and is part of the key, so a change in it is never served from the cache.
type UnfedState struct {
	UserFound  bool
	UserActive bool
	// Superuser is nil when the platform role comes from auth's own assignments, which the feed covers.
	Superuser           *bool
	MembershipRoleFound bool
	MembershipRole      string
	// NowUnixNano is the transaction's clock.
	NowUnixNano int64
}

func (u UnfedState) key() string {
	b := make([]byte, 0, 64)
	b = strconv.AppendBool(append(b, "user="...), u.UserFound)
	b = strconv.AppendBool(append(b, ",active="...), u.UserActive)
	b = append(b, ",superuser="...)
	if u.Superuser == nil {
		b = append(b, "fed"...)
	} else {
		b = strconv.AppendBool(b, *u.Superuser)
	}
	b = strconv.AppendBool(append(b, ",role="...), u.MembershipRoleFound)
	return string(strconv.AppendQuote(append(b, ','), u.MembershipRole))
}

// DependencySnapshot is a snapshot that says which aggregates it read, at which revision, and which aggregate types it
// read as a set. A snapshot without it is never stored.
type DependencySnapshot interface {
	Dependencies() []decisioncache.Dependency
	SetTypes() []string
}

type cacheMetrics struct {
	lookups metric.Int64Counter
	puts    metric.Int64Counter
}

func newCacheMetrics(meter metric.Meter, cache DecisionCache) (cacheMetrics, error) {
	var (
		m   cacheMetrics
		err error
	)
	if m.lookups, err = meter.Int64Counter(MetricDecisionCacheLookups,
		metric.WithDescription("Authorize decisions by decision cache result (hit, miss, bypass).")); err != nil {
		return m, err
	}
	if m.puts, err = meter.Int64Counter(MetricDecisionCachePuts,
		metric.WithDescription("Decision cache Puts by result (stored, or the reason it did not store).")); err != nil {
		return m, err
	}
	if cache != nil {
		if _, err = meter.Int64ObservableGauge(MetricDecisionCacheEntries,
			metric.WithDescription("Entries the decision cache holds."),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(int64(cache.Len()))
				return nil
			})); err != nil {
			return m, err
		}
	}
	return m, nil
}

// lookup and put count nothing on a zero cacheMetrics (handlers built without AuthorizeRoutes).
func (m cacheMetrics) lookup(ctx context.Context, result string) {
	if m.lookups == nil {
		return
	}
	m.lookups.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

func (m cacheMetrics) put(ctx context.Context, result decisioncache.PutResult) {
	if m.puts == nil {
		return
	}
	m.puts.Add(ctx, 1, metric.WithAttributes(attribute.String("result", string(result))))
}

// cacheKey is the item's key: the request, the session, the catalog the evaluator decides under, and the unfed values.
// This route decides no delegated session from the cache, so the actor chain is empty; the reference carries no
// assurance and no entitlement aggregate exists.
func cacheKey(item authorizeItem, u UnfedState) decisioncache.Key {
	q := item.question
	k := decisioncache.Key{
		PrincipalID: session.WirePrincipalID(item.principal.ID), SessionID: item.principal.SessionID,
		OrganizationID: q.OrganizationID, Action: q.Action, ResourceKind: q.ResourceType,
		PolicyRevision: policycatalog.Default().Version(), Unfed: u.key(),
	}
	if q.ResourceType == "repository" {
		k.ResourceKey = q.ResourceKey
	}
	return k
}

// decisionBound reports an allow whose class is decision_bound: the decision's own class, else the catalog's.
func decisionBound(action string, d authorization.Decision) bool {
	if d.CacheClass != "" {
		return d.CacheClass == authorization.CacheDecisionBound
	}
	act, err := policycatalog.Default().Action(action)
	return err == nil && act.CacheClass == policycatalog.CacheDecisionBound
}
