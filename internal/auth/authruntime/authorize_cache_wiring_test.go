package authruntime

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// acp condition 4: no cache with the feed off.
func TestTheFeedOffBuildsNoDecisionCache(t *testing.T) {
	cfg, pool, schema := feedFixture(t, map[string]string{authconfig.EnvValkeyURI: testValkeyURI, authconfig.EnvRevisionFeed: "off"})
	parts, err := revisionFeed(cfg, discard(), pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	if parts.decisions != nil {
		t.Fatal("a decision cache was built with the feed off")
	}
}

func storedAllow(t *testing.T, c *decisioncache.Cache, principal string) decisioncache.Key {
	t.Helper()
	var action string
	for _, a := range policycatalog.Default().Actions() {
		if a.RiskClass == policycatalog.RiskLow && a.CacheClass == policycatalog.CacheDecisionBound {
			action = a.ID
			break
		}
	}
	k := decisioncache.Key{PrincipalID: principal, SessionID: "ses_1", OrganizationID: uuid.NewString(), Action: action,
		ResourceKind: "organization", PolicyRevision: policycatalog.Default().Version()}
	d := authorization.Decision{Allowed: true, Reason: authorization.ReasonAllowedByRole, PolicyRevision: k.PolicyRevision}
	read := decisioncache.Read{Dependencies: []decisioncache.Dependency{{AggregateType: "principal", AggregateID: principal, Revision: 1}},
		Stamp: c.Begin()}
	if got := c.Put(k, d, read); got != decisioncache.Stored {
		t.Fatalf("Put = %q", got)
	}
	return k
}

// The consumer's invalidator is the cache (no longer nopInvalidator), the cache reads the consumer's watermarks, and a
// resubscribe clears it (acp condition 4).
func TestTheFeedOnWiresTheCacheIntoTheConsumer(t *testing.T) {
	cfg, pool, schema := feedFixture(t, map[string]string{authconfig.EnvValkeyURI: testValkeyURI, authconfig.EnvRevisionFeed: "on"})
	parts, err := revisionFeed(cfg, discard(), pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	d := parts.decisions
	if d == nil || d.cache == nil || d.ready == nil || d.consumer == nil {
		t.Fatalf("decision parts %+v, want a cache, a readiness and the consumer", d)
	}
	if d.ready() {
		t.Fatal("ready before any subscription")
	}
	ctx := context.Background()
	k := storedAllow(t, d.cache, "prn_one")
	if _, err := d.consumer.Handle(ctx, contracts.ProvisioningEvent{AggregateType: "principal", AggregateID: "prn_one",
		Payload: contracts.ProvisioningPayload{Revision: 2}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := d.cache.Get(k); ok {
		t.Fatal("an event of the principal did not reach the cache")
	}
	k = storedAllow(t, d.cache, "prn_two")
	if err := d.consumer.Resubscribed(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := d.cache.Get(k); ok || d.cache.Len() != 0 {
		t.Fatalf("a resubscribe left %d entries", d.cache.Len())
	}
}

// The routes take the cache and the readiness when the feed is on.
func TestTheAuthorizeRoutesTakeTheDecisionCache(t *testing.T) {
	pool, schema, signer := authorizeTestDeps(t)
	cfg, fpool, fschema := feedFixture(t, map[string]string{authconfig.EnvValkeyURI: testValkeyURI, authconfig.EnvRevisionFeed: "on"})
	parts, err := revisionFeed(cfg, discard(), fpool, fschema)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authorizeRoutes(authconfig.Config{}, discard(), pool, schema, signer, parts.decisions); err != nil {
		t.Fatal(err)
	}
}
