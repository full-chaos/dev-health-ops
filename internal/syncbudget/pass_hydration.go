package syncbudget

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// acp lock-1 ruling (CHAOS-14250): while a dispatch pass holds its bucket locks, nothing calls out. PagerDuty credential
// hydration is the one outbound step of a budget estimate, so a pass hydrates its PagerDuty credentials BEFORE it takes
// the locks (PreHydratePagerDuty, once per credential, with the network) into a PassHydrations record, and every
// estimate under the locks reads that record instead of the network (WithLockedHydrations). A credential the record
// does not hold, or holds past its token's valid-until, takes main's hydration-failure path (the unit gets no
// estimate), with a WARN and a count of LockedHydrationSkipCounter once per credential per pass: observed, never a
// failed pass (D6465).

// ExpiringCredentialHydrator is a CredentialHydrator that also returns the instant until which the token it hydrates
// holds, refreshing one that would not hold until `until` (providerfoundation.PagerDutyOAuthHydrator).
type ExpiringCredentialHydrator interface {
	HydrateUntil(ctx context.Context, lease providerfoundation.LeaseGuard, scope providerfoundation.TenantScope,
		credential providerfoundation.Credential, until time.Time) (providerfoundation.Credential, time.Time, error)
}

// ErrPagerDutyHydrationUnderLock is the cause joined to errPagerDutyHydration (main's hydration-failure error) when an
// estimate under the bucket locks finds no hydration that still holds in its pass record.
var ErrPagerDutyHydrationUnderLock = errors.New("PagerDuty credential hydration needs the network under the bucket lock")

// PassHydrations is one dispatch pass's record of its PagerDuty credential hydrations, keyed by credential id: the
// instant until which a hydrated token holds, or the pre-step's hydration failure.
type PassHydrations struct {
	now     func() time.Time
	mu      sync.Mutex
	until   map[string]time.Time
	failure map[string]error
	skipped map[string]bool
}

// NewPassHydrations is an empty record read on clock now (nil: time.Now).
func NewPassHydrations(now func() time.Time) *PassHydrations {
	if now == nil {
		now = time.Now
	}
	return &PassHydrations{now: now, until: map[string]time.Time{}, failure: map[string]error{}, skipped: map[string]bool{}}
}

// Skips is how many credentials this record answered with the guard (a miss or an expired hydration) at least once.
func (record *PassHydrations) Skips() int {
	record.mu.Lock()
	defer record.mu.Unlock()
	return len(record.skipped)
}

func (record *PassHydrations) holds(credentialID string) bool {
	record.mu.Lock()
	defer record.mu.Unlock()
	_, hydrated := record.until[credentialID]
	_, failed := record.failure[credentialID]
	return hydrated || failed
}

func (record *PassHydrations) put(credentialID string, until time.Time, err error) {
	record.mu.Lock()
	defer record.mu.Unlock()
	if err != nil {
		record.failure[credentialID] = err
		return
	}
	record.until[credentialID] = until
}

type lockedHydrationsKey struct{}

// WithLockedHydrations marks ctx as under a dispatch pass's bucket locks: a budget estimate on it hydrates no PagerDuty
// credential over the network and reads record instead.
func WithLockedHydrations(ctx context.Context, record *PassHydrations) context.Context {
	return context.WithValue(ctx, lockedHydrationsKey{}, record)
}

func lockedHydrations(ctx context.Context) *PassHydrations {
	record, _ := ctx.Value(lockedHydrationsKey{}).(*PassHydrations)
	return record
}

// answer is a locked estimate's hydration: a pre-step failure is returned as it is (the error main's hydration
// returned for it); a hydration whose token still holds succeeds; anything else (no hydration, or one past its
// valid-until) takes main's hydration-failure path with no network call, and the first such answer for a credential in
// the pass logs a WARN and counts once (the pass estimates a unit more than once: observeRun, then enforceRun).
func (record *PassHydrations) answer(ctx context.Context, logger *synclog.Logger, orgID, credentialID string) error {
	record.mu.Lock()
	failure, failed := record.failure[credentialID]
	until, hydrated := record.until[credentialID]
	record.mu.Unlock()
	if failed {
		return failure
	}
	if hydrated && record.now().Before(until) {
		return nil
	}
	reason := LockedHydrationSkipMiss
	if hydrated {
		reason = LockedHydrationSkipExpired
	}
	record.mu.Lock()
	first := !record.skipped[credentialID]
	record.skipped[credentialID] = true
	record.mu.Unlock()
	if first {
		countLockedHydrationSkip(ctx, reason)
		logger.Warn(ctx, synclog.MsgDispatchSyncRunBudgetHydrationSkippedUnderLock,
			synclog.Org(synclog.ParseID(orgID)), synclog.Text(synclog.KeyReason, synclog.ParseLabel(reason)))
	}
	return errors.Join(errPagerDutyHydration, ErrPagerDutyHydrationUnderLock)
}

// LockedHydrationSkipCounter counts the PagerDuty hydrations an estimate under the bucket locks did not run, by
// reason (observed, never enforced).
const LockedHydrationSkipCounter = "sync_budget_locked_hydration_skipped_total"

// Reasons of LockedHydrationSkipCounter.
const (
	LockedHydrationSkipMiss    = "miss"
	LockedHydrationSkipExpired = "expired"
)

func countLockedHydrationSkip(ctx context.Context, reason string) {
	counter, err := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/syncbudget").Int64Counter(
		LockedHydrationSkipCounter,
		metric.WithUnit("{hydration}"),
		metric.WithDescription("PagerDuty credential hydrations a budget estimate under the sync dispatch bucket locks did not run (no network under the lock), by reason"))
	if err != nil {
		return
	}
	if reason == LockedHydrationSkipExpired {
		counter.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", LockedHydrationSkipExpired)))
		return
	}
	counter.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", LockedHydrationSkipMiss)))
}

// PassUnit is the (organization, run, integration) of PagerDuty units a pass's locked estimates read: units of one
// group resolve one credential.
type PassUnit struct {
	OrgID         string
	RunID         string
	IntegrationID string
}

// PreHydratePagerDuty is a dispatch pass's pre-step, run before the pass takes its bucket locks: for each group it
// resolves the credential load resolves for the group's PagerDuty units (the integration's credential, the run's
// stamp, resolveRunAuth) and hydrates each credential once, with the network, into record. A hydration must hold
// until `until` (the pass's deadline): an OAuth token that would need a refresh before it is refreshed here. A group
// whose credential does not resolve records nothing: the locked estimate meets the same failure before hydrating.
func (loader Loader) PreHydratePagerDuty(ctx context.Context, record *PassHydrations, groups []PassUnit, until time.Time) {
	if record == nil || loader.DB == nil || loader.Decryptor == nil {
		return
	}
	// The locked estimate logs what resolving the credential logs (a fingerprint mismatch); the pre-step does not.
	loader.Logger = synclog.Discard()
	credentials := credentialCache{}
	for _, group := range groups {
		integration, err := loader.integration(ctx, group.IntegrationID, group.OrgID)
		if err != nil {
			continue
		}
		run, err := loader.runAuth(ctx, group.RunID, group.OrgID)
		if err != nil {
			continue
		}
		credentialID, mapping, err := loader.resolveRunAuth(ctx, credentials, run, group.OrgID, integration.id, integration.credentialID, "pagerduty")
		if err != nil {
			continue
		}
		pagerDutyMapping, isObject := mapping.(*object)
		if !isObject {
			continue
		}
		key := ""
		if credentialID != nil {
			key = *credentialID
		}
		if record.holds(key) {
			continue
		}
		validUntil, err := loader.hydratePagerDutyOnce(ctx, group.OrgID, key, pagerDutyMapping, pagerDutyStep{prehydrate: true, until: until})
		record.put(key, validUntil, err)
	}
}
