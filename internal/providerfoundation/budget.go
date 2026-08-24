package providerfoundation

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	valkeygo "github.com/valkey-io/valkey-go"
)

type BudgetKey struct {
	Provider, OrgID, Host, CostClass string
	Limit                            int
	TTL                              time.Duration
}

// SyncBudgetKey exactly mirrors Python BudgetGuard's advisory-lock key.
//
// PARTIALLY WIRED as of CHAOS-4175 family 3 (native dispatch_sync_run):
// SyncBudgetKey.String() IS now called, from
// internal/syncdispatchruntime/budget_limits.go's budgetKeyFor -- the SAME
// field order (provider, org_id, host, credential_fingerprint, dimension,
// route_family) verified against Python's own _budget_key, reused directly
// rather than re-deriving a third copy of the join. AdvisoryLockID() and
// PostgresBudgetLocker below remain UNWIRED, still deliberately: dispatch's
// own admission loop only ever has the STRING form of a budget key by the
// point it needs to lock (budget_enforce.go's sortedBudgetKeys, already
// sort.Strings'd), never a reconstructed []SyncBudgetKey, so reusing
// PostgresBudgetLocker.Lock would mean re-deriving structs just to re-sort
// them the same way it already does internally -- a worse fit than a
// second, string-keyed lock-acquire function
// (internal/syncdispatchruntime/budget_advisory_locks.go's
// acquireBudgetAdvisoryLocks) using the IDENTICAL SHA-256-truncated-to-63-bit
// algorithm PostgresBudgetLocker.AdvisoryLockID() implements here. Not a
// fork of a reuse candidate that needed behavioral adaptation -- a
// deliberately separate implementation for a shape PostgresBudgetLocker
// never fits, verified equivalent, not assumed.
type SyncBudgetKey struct {
	Provider, OrgID, Host, CredentialFingerprint, Dimension, RouteFamily string
}

func (k SyncBudgetKey) String() string {
	return strings.Join([]string{k.Provider, k.OrgID, k.Host, k.CredentialFingerprint, k.Dimension, k.RouteFamily}, ":")
}

func (k SyncBudgetKey) AdvisoryLockID() int64 {
	digest := sha256.Sum256([]byte(k.String()))
	return int64(binary.BigEndian.Uint64(digest[:8]) & ((uint64(1) << 63) - 1))
}

// PostgresBudgetLocker uses the same PostgreSQL advisory locks as the Python
// guard. It owns no parallel reservation table: the unit's authoritative
// DISPATCHING/RUNNING lease is the durable in-flight reservation seen by both
// runtimes.
type PostgresBudgetLocker struct{}

func (PostgresBudgetLocker) Lock(ctx context.Context, tx pgx.Tx, keys []SyncBudgetKey) error {
	if tx == nil {
		return ErrBudgetUnavailable
	}
	ordered := append([]SyncBudgetKey(nil), keys...)
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].String() < ordered[right].String() })
	for _, key := range ordered {
		if key.Provider == "" || key.OrgID == "" || key.Dimension == "" || key.RouteFamily == "" {
			return ErrBudgetUnavailable
		}
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", key.AdvisoryLockID()); err != nil {
			return ErrBudgetUnavailable
		}
	}
	return nil
}

func (k BudgetKey) Validate() error {
	if k.Provider == "" || k.OrgID == "" || k.CostClass == "" || k.Limit < 1 || k.TTL <= 0 {
		return ErrBudgetUnavailable
	}
	return nil
}
func (k BudgetKey) String() string {
	return strings.Join([]string{"provider_budget", keyPart(k.Provider), keyPart(k.OrgID), keyPart(k.Host), keyPart(k.CostClass)}, ":")
}
func keyPart(value string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return "_"
}

type Reservation interface{ Release(context.Context) error }
type BudgetStore interface {
	Acquire(context.Context, BudgetKey) (Reservation, error)
}

// BudgetWaitObserver records real-execution latency on the provider
// cost-budget acquire and backoff-gate wait paths as worker_budget_wait_seconds.
// provider and costClass are exactly the bounded, credential-free pair
// BudgetKey.Validate already enforces (never an org, host, or credential). A
// nil Observer on ValkeyBudgetStore/ValkeyBackoffGate keeps both types working
// with no metrics, matching every other optional dependency in this package.
type BudgetWaitObserver interface {
	ObserveProviderBudgetWait(provider, costClass string, wait time.Duration) error
}

// ValkeyBudgetStore uses a single Lua admission/release protocol, so Go
// workers share a provider/org/host/cost limit across processes. The key
// vocabulary is deliberately stable and has no credentials or request data.
type ValkeyBudgetStore struct {
	Client valkeygo.Client
	// Observer records how long Acquire spent asking the shared Valkey store
	// for a reservation — round-trip and lock-contention latency, not a
	// client-side backoff sleep (this store never sleeps; a denial is
	// returned immediately as ErrBudgetContended). It is observed on both
	// the granted and the denied path, since both represent real time this
	// call spent waiting on the budget subsystem.
	Observer BudgetWaitObserver
}

const budgetAcquireLua = `local current=redis.call('GET',KEYS[1]); if not current then current=0 else current=tonumber(current) end; if current>=tonumber(ARGV[1]) then return 0 end; redis.call('INCR',KEYS[1]); redis.call('PEXPIRE',KEYS[1],ARGV[2]); return 1`
const budgetReleaseLua = `local current=redis.call('GET',KEYS[1]); if not current then return 0 end; current=tonumber(current); if current<=1 then redis.call('DEL',KEYS[1]); return 0 end; redis.call('DECR',KEYS[1]); return current-1`

func (s ValkeyBudgetStore) Acquire(ctx context.Context, key BudgetKey) (Reservation, error) {
	if s.Client == nil || key.Validate() != nil {
		return nil, ErrBudgetUnavailable
	}
	started := time.Now()
	response := valkeygo.NewLuaScriptNoSha(budgetAcquireLua).Exec(ctx, s.Client, []string{key.String()}, []string{strconv.Itoa(key.Limit), strconv.FormatInt(key.TTL.Milliseconds(), 10)})
	allowed, err := response.AsInt64()
	s.observeWait(key, started)
	if err != nil {
		return nil, ErrBudgetUnavailable
	}
	if allowed != 1 {
		return nil, ErrBudgetContended
	}
	return &valkeyReservation{client: s.Client, key: key.String()}, nil
}

func (s ValkeyBudgetStore) observeWait(key BudgetKey, started time.Time) {
	if s.Observer == nil {
		return
	}
	_ = s.Observer.ObserveProviderBudgetWait(key.Provider, key.CostClass, time.Since(started))
}

type valkeyReservation struct {
	client valkeygo.Client
	key    string
	once   sync.Once
	err    error
}

func (r *valkeyReservation) Release(ctx context.Context) error {
	r.once.Do(func() {
		r.err = valkeygo.NewLuaScriptNoSha(budgetReleaseLua).Exec(ctx, r.client, []string{r.key}, nil).Error()
	})
	if r.err != nil {
		return ErrBudgetUnavailable
	}
	return nil
}

// BackoffGate uses the exact existing Python key: rate_limit:<provider>:<org
// or _>:<host or _>. This is the coexistence contract with Celery workers.
type BackoffGate interface {
	Wait(context.Context) (time.Duration, error)
	Penalize(context.Context, time.Duration) error
}
type ValkeyBackoffGate struct {
	Client                valkeygo.Client
	Provider, OrgID, Host string
	MaxBackoff            time.Duration
	Now                   func() time.Time
	// CostClass is the same bounded class this gate's calling client is
	// budgeted under. It exists solely to label worker_budget_wait_seconds:
	// the gate's own Valkey key never includes it (see key(), which is
	// intentionally identical to the Python rate_limit:<provider>:<org or
	// _>:<host or _> contract).
	CostClass string
	// Observer records the real backoff duration this Wait call computed and
	// returned to its caller — the actual time the caller now knows it must
	// not send another request, not a call-latency measurement.
	Observer BudgetWaitObserver
}

// applied is returned via tostring, not as a bare Lua number. Redis/Valkey's
// Lua-to-RESP conversion truncates a returned Lua number to an integer reply
// (dropping the millisecond fraction this value legitimately carries as a
// Unix-seconds timestamp), and valkey-go's .AsFloat64() only parses
// string-typed replies — so a bare `return applied` both loses precision on
// the wire and fails client-side decoding ("message type int64 is not a
// string") against a real server. tostring keeps the full value and matches
// the bulk-string reply Penalize's caller already expects.
const backoffPenalizeLua = `local old=tonumber(redis.call('GET',KEYS[1]) or '0'); local proposed=tonumber(ARGV[1]); local applied=math.max(old,proposed); redis.call('SET',KEYS[1],applied,'EX',ARGV[2]); return tostring(applied)`

func (g ValkeyBackoffGate) key() string {
	return fmt.Sprintf("rate_limit:%s:%s:%s", keyPart(g.Provider), keyPart(g.OrgID), keyPart(g.Host))
}
func (g ValkeyBackoffGate) now() time.Time {
	if g.Now != nil {
		return g.Now()
	}
	return time.Now()
}
func (g ValkeyBackoffGate) Wait(ctx context.Context) (time.Duration, error) {
	if g.Client == nil {
		return 0, ErrBudgetUnavailable
	}
	raw, err := g.Client.Do(ctx, g.Client.B().Get().Key(g.key()).Build()).AsFloat64()
	if valkeygo.IsValkeyNil(err) {
		g.observeWait(0)
		return 0, nil
	}
	if err != nil {
		return 0, ErrBudgetUnavailable
	}
	wait := time.Duration((raw - float64(g.now().UnixMilli())/1000) * float64(time.Second))
	if wait < 0 {
		wait = 0
	}
	g.observeWait(wait)
	return wait, nil
}

func (g ValkeyBackoffGate) observeWait(wait time.Duration) {
	if g.Observer == nil {
		return
	}
	_ = g.Observer.ObserveProviderBudgetWait(g.Provider, g.CostClass, wait)
}
func (g ValkeyBackoffGate) Penalize(ctx context.Context, delay time.Duration) error {
	if g.Client == nil {
		return ErrBudgetUnavailable
	}
	if g.MaxBackoff <= 0 {
		g.MaxBackoff = 5 * time.Minute
	}
	if delay < 0 {
		delay = 0
	}
	if delay > g.MaxBackoff {
		delay = g.MaxBackoff
	}
	expiration := int((2 * g.MaxBackoff).Seconds())
	_, err := valkeygo.NewLuaScriptNoSha(backoffPenalizeLua).Exec(ctx, g.Client, []string{g.key()}, []string{strconv.FormatFloat(float64(g.now().Add(delay).UnixMilli())/1000, 'f', 3, 64), strconv.Itoa(expiration)}).AsFloat64()
	if err != nil {
		return ErrBudgetUnavailable
	}
	return nil
}

type Metrics struct {
	mu                         sync.Mutex
	requests                   map[string]uint64
	budgetDenied               map[string]uint64
	budgetReleaseErrors        map[string]uint64
	inventoryPageCap           map[string]uint64
	perRunTruncation           map[string]uint64
	artifactSkipped            map[string]uint64
	snapshotDiscarded          map[string]uint64
	resumeReanchor             map[string]uint64
	unitTerminalWithRows       map[string]uint64
	incidentEntitlementRefused map[string]uint64
	allArtifactsUnreadable     map[string]uint64
}

func NewMetrics() *Metrics {
	return &Metrics{
		requests:                   map[string]uint64{},
		budgetDenied:               map[string]uint64{},
		budgetReleaseErrors:        map[string]uint64{},
		inventoryPageCap:           map[string]uint64{},
		perRunTruncation:           map[string]uint64{},
		artifactSkipped:            map[string]uint64{},
		snapshotDiscarded:          map[string]uint64{},
		resumeReanchor:             map[string]uint64{},
		unitTerminalWithRows:       map[string]uint64{},
		incidentEntitlementRefused: map[string]uint64{},
		allArtifactsUnreadable:     map[string]uint64{},
	}
}

// metricDatasetVocabulary is the closed set of dataset labels, mirroring
// providersync's checked-in dataset capability registry. providerfoundation
// cannot import providersync (providersync imports it), so the list is
// duplicated here and a drift guard in providersync asserts every registered
// dataset is present -- a new dataset that silently reported as "other" would
// be a metric that quietly stops distinguishing the thing it exists to
// distinguish.
var metricDatasetVocabulary = map[string]struct{}{
	"repo-metadata": {}, "commits": {}, "commit-stats": {}, "files": {},
	"blame": {}, "prs": {}, "pr-reviews": {}, "pr-comments": {},
	"cicd": {}, "tests": {}, "deployments": {}, "security": {},
	"work-items": {}, "work-item-labels": {}, "work-item-projects": {},
	"work-item-history": {}, "work-item-comments": {},
	"incidents": {}, "feature-flags": {},
	"services": {}, "business-services": {}, "escalation-policies": {},
	"schedules": {}, "on-calls": {}, "users": {}, "teams": {},
	"incident-alerts": {}, "incident-log-entries": {}, "incident-notes": {},
}

// MetricDatasetLabel bounds the dataset label the same way metricProvider
// bounds the provider one: by ALLOWLIST, not by syntax. A syntactic bound
// still lets a faulty producer or a hostile tenant mint one series per
// distinct well-formed string, which is unbounded metric-map growth wearing a
// character-class disguise. Anything unregistered collapses to "other", so the
// series count is fixed by this file rather than by whatever reaches a claim.
func MetricDatasetLabel(value string) string {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if _, known := metricDatasetVocabulary[lowered]; !known {
		return "other"
	}
	return lowered
}
func metricProvider(value string) string {
	switch strings.ToLower(value) {
	case "github", "gitlab", "jira", "linear", "launchdarkly", "pagerduty":
		return strings.ToLower(value)
	default:
		return "other"
	}
}
func (m *Metrics) RecordRequest(provider string, class ErrorClass) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[metricProvider(provider)+":"+string(class)]++
}
func (m *Metrics) RecordBudgetDenied(provider string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.budgetDenied[metricProvider(provider)]++
}
func (m *Metrics) RecordBudgetReleaseError(provider string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.budgetReleaseErrors[metricProvider(provider)]++
}

// RecordInventoryPageCap counts one inventory phase that stopped at its
// cumulative page budget before the provider ran out of pages. Since
// CHAOS-4130 this is a SUCCESSFUL unit with lower-bound coverage rather than a
// cancellation, which means the log line it used to share with a failure is no
// longer enough on its own -- nothing else in the pipeline reports that a
// window was only partly walked.
func (m *Metrics) RecordInventoryPageCap(provider, dataset string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inventoryPageCap[metricProvider(provider)+":"+MetricDatasetLabel(dataset)]++
}

// metricPerRunComponentVocabulary is the closed set of per-run truncation
// components, mirroring providersync's incomplete vocabulary. An unknown
// component collapses to "other" so a route cannot open an unbounded label
// dimension by inventing a component name (CHAOS-4142).
var metricPerRunComponentVocabulary = map[string]struct{}{
	"run_jobs": {}, "run_artifacts": {}, "run_reports": {},
}

// MetricPerRunComponentLabel bounds the per-run component label.
func MetricPerRunComponentLabel(value string) string {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if _, known := metricPerRunComponentVocabulary[lowered]; !known {
		return "other"
	}
	return lowered
}

// metricPerRunCauseVocabulary separates the two reasons a per-run truncation
// fires. They classify oppositely -- an item cap advances the watermark, a page
// budget withholds it -- so an operator must be able to tell them apart on the
// metric without reading route code (CHAOS-4142).
var metricPerRunCauseVocabulary = map[string]struct{}{
	"per_run_cap": {}, "per_run_page_budget": {},
}

// MetricPerRunCauseLabel bounds the per-run cause label.
func MetricPerRunCauseLabel(value string) string {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if _, known := metricPerRunCauseVocabulary[lowered]; !known {
		return "other"
	}
	return lowered
}

// RecordPerRunTruncation counts ONE workflow run committed with only the first
// cap-worth of its items, by bounded provider, dataset, and component.
//
// This is the per-run twin of RecordInventoryPageCap, and is deliberately a
// SEPARATE series rather than a new label on that one. An inventory page cap
// leaves part of the window unwalked and withholds the watermark; a per-run
// truncation walks the whole window and advances it. Folding them together
// would mix a coverage-stalling condition with a bounded, self-limiting one,
// so an alert could not tell "this source is going stale" from "this source
// has one enormous run in it" (CHAOS-4142).
func (m *Metrics) RecordPerRunTruncation(provider, dataset, component, cause string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.perRunTruncation[metricProvider(provider)+":"+MetricDatasetLabel(dataset)+
		":"+MetricPerRunComponentLabel(component)+":"+MetricPerRunCauseLabel(cause)]++
}

// metricArtifactSkipReasonVocabulary is the closed set of reasons a single
// provider artifact was skipped while the rest of the inventory continued. An
// unknown reason collapses to "other" so a route cannot open an unbounded
// label dimension (CHAOS-4177).
var metricArtifactSkipReasonVocabulary = map[string]struct{}{
	"unreadable_archive":   {},
	"artifact_unavailable": {},
}

// MetricArtifactSkipReasonLabel bounds the artifact skip reason label.
func MetricArtifactSkipReasonLabel(value string) string {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if _, known := metricArtifactSkipReasonVocabulary[lowered]; !known {
		return "other"
	}
	return lowered
}

// RecordArtifactSkipped counts ONE provider artifact that could not be read
// and was skipped, by bounded provider, dataset, and reason.
//
// This is deliberately a SEPARATE series from RecordPerRunTruncation rather
// than a new cause label on it. That series carries the per_run_cap cause,
// which is the one bounded, self-limiting condition allowed to ADVANCE the
// watermark: the walk saw the whole window and the boundary was positively
// observed. (Its other cause, per_run_page_budget, withholds -- the series is
// not uniformly advancing, which is precisely why its cause label matters.)
//
// A skipped artifact is neither: its contents were never observed, so it
// always withholds -- report_member is absent from
// githubTestsWatermarkAdvancingPairs entirely, at the component level rather
// than per cause. Folding it in as a third cause on the truncation series
// would put a permanently-withholding condition beside a
// conditionally-advancing one, so an operator could no longer read the series
// as "bounded truncation" at all (CHAOS-4177).
func (m *Metrics) RecordArtifactSkipped(provider, dataset, reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.artifactSkipped[metricProvider(provider)+":"+MetricDatasetLabel(dataset)+
		":"+MetricArtifactSkipReasonLabel(reason)]++
}

// metricSnapshotDiscardReasonVocabulary is the closed set of reasons a prepared
// route snapshot may be discarded. Bounded because it becomes a Prometheus
// label; an unknown reason collapses to "other" rather than opening the
// dimension.
var metricSnapshotDiscardReasonVocabulary = map[string]struct{}{
	"manifest_mismatch": {}, "manifest_mismatch_unreplayable": {},
	"manifest_mismatch_partially_committed": {},
}

// MetricSnapshotDiscardReasonLabel bounds a discard reason.
func MetricSnapshotDiscardReasonLabel(value string) string {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if _, known := metricSnapshotDiscardReasonVocabulary[lowered]; !known {
		return "other"
	}
	return lowered
}

// RecordPreparedSnapshotDiscarded counts one persisted prepared-route snapshot
// that could not be used and was thrown away.
//
// It exists because the alternative to discarding is a unit that retries the
// same unusable document forever -- stuck, and silent, because nothing else in
// the pipeline reports "recovery keeps refusing the same snapshot". A discard
// is the safe outcome, but it is not a free one: the route re-runs from the
// claim, so the provider is fetched again. A rate that is anything but a brief
// spike after a deploy means something is wrong with the manifest contract.
//
// The two reasons are NOT interchangeable. `manifest_mismatch` is the expected,
// self-healing case: a document written before a destination was added, thrown
// away and replayed. `manifest_mismatch_unreplayable` is the same staleness on
// a document that also contains a recovery-BLOCKED effect, which cannot be
// safely redone -- that unit stops rather than replays, and needs a person.
func (m *Metrics) RecordPreparedSnapshotDiscarded(provider, dataset, reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshotDiscarded[metricProvider(provider)+":"+MetricDatasetLabel(dataset)+
		":"+MetricSnapshotDiscardReasonLabel(reason)]++
}

// metricResumePhaseVocabulary is the closed set of paginated phases whose
// resume cursor carries a positional index. An unknown phase collapses to
// "other" so a route cannot open an unbounded label dimension (CHAOS-4177).
var metricResumePhaseVocabulary = map[string]struct{}{
	"runs": {}, "artifacts": {},
}

// MetricResumePhaseLabel bounds the resume phase label.
func MetricResumePhaseLabel(value string) string {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if _, known := metricResumePhaseVocabulary[lowered]; !known {
		return "other"
	}
	return lowered
}

// RecordResumeReanchor counts ONE re-fetched page whose contents had moved
// past the index a resume cursor recorded against it, so the walk restarted at
// the top of that page.
//
// This is the deploy-verification signal for CHAOS-4177: the same event used
// to surface as a checkpoint conflict that cost the unit one of five attempts.
// After the fix, a deploy should show re-anchors here and no conflicts. A
// steadily rising count on one provider/dataset means pages are shifting
// faster than the walk consumes them, which is the argument for moving that
// route to a stable-identity cursor.
func (m *Metrics) RecordResumeReanchor(provider, dataset, phase string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resumeReanchor[metricProvider(provider)+":"+MetricDatasetLabel(dataset)+
		":"+MetricResumePhaseLabel(phase)]++
}

// RecordUnitTerminalWithRows counts a provider unit that was terminalized
// while it already held committed rows. That combination is the signature
// CHAOS-4130 ran on undetected for days: a unit destroyed mid-stream is
// throwing away cursor position for work it had already paid for, and no
// healthy route does it. Any non-zero rate here deserves an operator.
// metricIncidentEntitlementSeamVocabulary is the closed set of execution
// seams at which a canonical-incident entitlement re-check can refuse a unit:
// before provider fetch, and at the ClickHouse write boundary.
var metricIncidentEntitlementSeamVocabulary = map[string]struct{}{
	"collect": {}, "write": {},
}

// MetricIncidentEntitlementSeamLabel bounds the entitlement seam label.
func MetricIncidentEntitlementSeamLabel(value string) string {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if _, known := metricIncidentEntitlementSeamVocabulary[lowered]; !known {
		return "other"
	}
	return lowered
}

// RecordIncidentEntitlementRefused counts ONE unit refused by the
// execution-time canonical-incident entitlement re-check, by bounded provider,
// dataset and seam. One series carries every gated provider (Jira incidents
// and the PagerDuty datasets) -- the provider label is what distinguishes
// them, so a second provider must never mint a second series (CHAOS-4219).
func (m *Metrics) RecordIncidentEntitlementRefused(provider, dataset, seam string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incidentEntitlementRefused[metricProvider(provider)+":"+MetricDatasetLabel(dataset)+
		":"+MetricIncidentEntitlementSeamLabel(seam)]++
}

func (m *Metrics) RecordUnitTerminalWithRows(provider, dataset string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unitTerminalWithRows[metricProvider(provider)+":"+MetricDatasetLabel(dataset)]++
}

// RecordAllArtifactsUnreadable counts ONE provider unit that failed because
// every cicd/tests artifact it observed was unreadable (CHAOS-4185). The
// condition terminalizes on its first attempt (deterministicTerminalCategory
// maps its sentinel to a durable category), so this fires exactly once per
// affected unit rather than once per retry.
func (m *Metrics) RecordAllArtifactsUnreadable(provider, dataset string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.allArtifactsUnreadable[metricProvider(provider)+":"+MetricDatasetLabel(dataset)]++
}

// writeProviderDatasetCounter renders one provider:dataset keyed counter
// family in stable key order.
func writeProviderDatasetCounter(
	writer io.Writer, name, help string, values map[string]uint64,
) error {
	if _, err := fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s counter\n", name, help, name); err != nil {
		return err
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts := strings.SplitN(key, ":", 2)
		if _, err := fmt.Fprintf(
			writer, "%s{provider=%q,dataset=%q} %d\n", name, parts[0], parts[1], values[key],
		); err != nil {
			return err
		}
	}
	return nil
}

// writeProviderDatasetComponentCounter is the four-label twin of
// writeProviderDatasetCounter. Every key is built from bounded vocabularies,
// so SplitN into exactly four parts is total.
func writeProviderDatasetComponentCounter(
	writer io.Writer, name, help string, values map[string]uint64,
) error {
	if _, err := fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s counter\n", name, help, name); err != nil {
		return err
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts := strings.SplitN(key, ":", 4)
		if len(parts) != 4 {
			continue
		}
		if _, err := fmt.Fprintf(
			writer, "%s{provider=%q,dataset=%q,component=%q,cause=%q} %d\n",
			name, parts[0], parts[1], parts[2], parts[3], values[key],
		); err != nil {
			return err
		}
	}
	return nil
}

// writeProviderDatasetReasonCounter is the three-label twin of
// writeProviderDatasetCounter. Every key is built from bounded vocabularies,
// so SplitN into exactly three parts is total.
func writeProviderDatasetReasonCounter(
	writer io.Writer, name, help, third string, values map[string]uint64,
) error {
	if _, err := fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s counter\n", name, help, name); err != nil {
		return err
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts := strings.SplitN(key, ":", 3)
		if len(parts) != 3 {
			continue
		}
		if _, err := fmt.Fprintf(
			writer, "%s{provider=%q,dataset=%q,%s=%q} %d\n",
			name, parts[0], parts[1], third, parts[2], values[key],
		); err != nil {
			return err
		}
	}
	return nil
}

func (m *Metrics) WritePrometheus(writer io.Writer) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := io.WriteString(writer,
		"# HELP dev_health_provider_requests_total Provider HTTP requests by bounded provider and error class.\n"+
			"# TYPE dev_health_provider_requests_total counter\n"); err != nil {
		return err
	}
	requestKeys := make([]string, 0, len(m.requests))
	for key := range m.requests {
		requestKeys = append(requestKeys, key)
	}
	sort.Strings(requestKeys)
	for _, key := range requestKeys {
		value := m.requests[key]
		parts := strings.SplitN(key, ":", 2)
		if _, err := fmt.Fprintf(writer, "dev_health_provider_requests_total{provider=%q,class=%q} %d\n", parts[0], parts[1], value); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(writer,
		"# HELP dev_health_provider_budget_denied_total Provider cost-budget denials by bounded provider.\n"+
			"# TYPE dev_health_provider_budget_denied_total counter\n"); err != nil {
		return err
	}
	providers := make([]string, 0, len(m.budgetDenied))
	for provider := range m.budgetDenied {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	for _, provider := range providers {
		value := m.budgetDenied[provider]
		if _, err := fmt.Fprintf(writer, "dev_health_provider_budget_denied_total{provider=%q} %d\n", provider, value); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(writer,
		"# HELP dev_health_provider_budget_release_errors_total Provider cost-budget reservation release failures by bounded provider.\n"+
			"# TYPE dev_health_provider_budget_release_errors_total counter\n"); err != nil {
		return err
	}
	providers = providers[:0]
	for provider := range m.budgetReleaseErrors {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	for _, provider := range providers {
		value := m.budgetReleaseErrors[provider]
		if _, err := fmt.Fprintf(writer, "dev_health_provider_budget_release_errors_total{provider=%q} %d\n", provider, value); err != nil {
			return err
		}
	}
	if err := writeProviderDatasetCounter(
		writer, "dev_health_provider_inventory_page_cap_total",
		"Provider inventory phases that stopped at their cumulative page budget, by bounded provider and dataset.",
		m.inventoryPageCap,
	); err != nil {
		return err
	}
	if err := writeProviderDatasetComponentCounter(
		writer, "dev_health_provider_per_run_truncation_total",
		"Workflow runs committed with only the first cap-worth of their items, by bounded provider, dataset, and component.",
		m.perRunTruncation,
	); err != nil {
		return err
	}
	if err := writeProviderDatasetReasonCounter(
		writer, "dev_health_provider_prepared_snapshot_discarded_total",
		"Prepared route snapshots discarded as unusable, by bounded provider, dataset, and reason.",
		"reason", m.snapshotDiscarded,
	); err != nil {
		return err
	}
	if err := writeProviderDatasetReasonCounter(
		writer, "dev_health_provider_artifact_skipped_total",
		"Provider artifacts skipped as unreadable while the rest of the inventory continued, by bounded provider, dataset, and reason.",
		"reason", m.artifactSkipped,
	); err != nil {
		return err
	}
	if err := writeProviderDatasetReasonCounter(
		writer, "dev_health_provider_resume_reanchor_total",
		"Re-fetched provider pages whose contents moved past the resume cursor's index, by bounded provider, dataset, and phase.",
		"phase", m.resumeReanchor,
	); err != nil {
		return err
	}
	if err := writeProviderDatasetCounter(
		writer, "dev_health_provider_unit_terminal_with_rows_total",
		"Provider units terminalized while holding committed rows, by bounded provider and dataset.",
		m.unitTerminalWithRows,
	); err != nil {
		return err
	}
	if err := writeProviderDatasetReasonCounter(
		writer, "dev_health_provider_incident_entitlement_refused_total",
		"Provider units refused by the execution-time canonical-incident entitlement re-check, by bounded provider, dataset, and seam.",
		"seam", m.incidentEntitlementRefused,
	); err != nil {
		return err
	}
	return writeProviderDatasetCounter(
		writer, "dev_health_provider_all_artifacts_unreadable_total",
		"Provider units failed because every cicd/tests artifact observed was unreadable, by bounded provider and dataset.",
		m.allArtifactsUnreadable,
	)
}
