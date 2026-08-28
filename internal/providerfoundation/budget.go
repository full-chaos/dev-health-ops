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
	mu                             sync.Mutex
	requests                       map[string]uint64
	budgetDenied                   map[string]uint64
	budgetReleaseErrors            map[string]uint64
	inventoryPageCap               map[string]uint64
	perRunTruncation               map[string]uint64
	artifactSkipped                map[string]uint64
	snapshotDiscarded              map[string]uint64
	resumeReanchor                 map[string]uint64
	unitTerminalWithRows           map[string]uint64
	incidentEntitlementRefused     map[string]uint64
	allArtifactsUnreadable         map[string]uint64
	workItemTeamAttributionWritten map[string]uint64
	teamAttributionMembershipLayer map[string]uint64
	projectsV2DegradedSnapshots    map[string]uint64
	unitClaimed                    map[string]uint64
	unitFailed                     map[string]uint64
	cicdPartialSuccess             map[string]uint64
}

func NewMetrics() *Metrics {
	return &Metrics{
		requests:                       map[string]uint64{},
		budgetDenied:                   map[string]uint64{},
		budgetReleaseErrors:            map[string]uint64{},
		inventoryPageCap:               map[string]uint64{},
		perRunTruncation:               map[string]uint64{},
		artifactSkipped:                map[string]uint64{},
		snapshotDiscarded:              map[string]uint64{},
		resumeReanchor:                 map[string]uint64{},
		unitTerminalWithRows:           map[string]uint64{},
		incidentEntitlementRefused:     map[string]uint64{},
		allArtifactsUnreadable:         map[string]uint64{},
		workItemTeamAttributionWritten: map[string]uint64{},
		teamAttributionMembershipLayer: map[string]uint64{},
		projectsV2DegradedSnapshots:    map[string]uint64{},
		unitClaimed:                    map[string]uint64{},
		unitFailed:                     map[string]uint64{},
		cicdPartialSuccess:             map[string]uint64{},
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
	// artifact_oversized (CHAOS-4315): a github tests/cicd artifact whose
	// download exceeded the route's size cap, skipped and counted like the
	// two reasons above instead of failing the unit closed.
	"artifact_oversized": {},
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
// A skipped artifact is different again: this series counts every skip
// regardless of whether that skip's cause advances or withholds the
// watermark (report_member's per-cause split lives in
// githubTestsWatermarkAdvancingPairs, providersync/github_tests_reports.go
// -- as of CHAOS-4394 all three whole-artifact causes advance; the doc
// comment there is the up-to-date statement of which pairs do). Folding this
// into a cause on the truncation series would still be wrong: that series is
// about RUNS truncated by a per-run cap, a different unit of measure than
// artifacts skipped, so an operator could no longer read either series
// cleanly (CHAOS-4177).
func (m *Metrics) RecordArtifactSkipped(provider, dataset, reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.artifactSkipped[metricProvider(provider)+":"+MetricDatasetLabel(dataset)+
		":"+MetricArtifactSkipReasonLabel(reason)]++
}

// metricCicdPartialSuccessReasonVocabulary bounds the reason label on
// RecordCicdPartialSuccess to the report_member causes that can advance the
// watermark (see githubTestsWatermarkAdvancingPairs), plus "mixed" for a unit
// whose incomplete observations span more than one distinct cause. Anything
// else -- including an as-yet-unregistered cause -- collapses to "other"
// rather than opening the label.
var metricCicdPartialSuccessReasonVocabulary = map[string]struct{}{
	"artifact_oversized":   {},
	"artifact_unavailable": {},
	"unreadable_archive":   {},
	"per_run_cap":          {},
	"mixed":                {},
}

// MetricCicdPartialSuccessReasonLabel bounds the RecordCicdPartialSuccess
// reason label the same way MetricArtifactSkipReasonLabel bounds its own.
func MetricCicdPartialSuccessReasonLabel(value string) string {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if _, known := metricCicdPartialSuccessReasonVocabulary[lowered]; !known {
		return "other"
	}
	return lowered
}

// RecordCicdPartialSuccess counts ONE completed cicd/tests unit that
// advanced its watermark despite carrying non-empty incomplete evidence
// (CHAOS-4394): a real partial success, not a stall, because every
// contributing cause is on githubTestsWatermarkAdvancingPairs and its
// artifacts are durably recorded (GitHubTestsSkippedArtifact) for a targeted
// backfill. This is a UNIT-level signal, distinct from RecordArtifactSkipped
// (which counts per-artifact) and RecordPerRunTruncation (which counts
// per-run).
//
// repo is deliberately NOT a label here, unlike the ticket's original ask --
// codex review round 1 caught that a synced repository, unlike provider or
// dataset, is not drawn from a fixed, small vocabulary this process can
// enumerate up front: a long-lived worker accumulates one series per
// repository ever added AND renamed over its lifetime, with nothing to ever
// evict a stale one, which is exactly the unbounded-map-growth failure
// MetricDatasetLabel/metricProvider/MetricArtifactSkipReasonLabel's ALLOWLIST
// bounding exists to prevent (see MetricDatasetLabel's doc comment). This
// matches the existing house rule for run id / artifact id -- provider-
// supplied and unbounded, so it belongs in the caller's structured log line,
// never in a durable metric label (see RecordPerRunTruncation and
// recordGitHubTestsSkippedArtifact). The caller logs repo alongside this
// call for per-repo triage; this counter answers "how much, by reason", not
// "which repo".
func (m *Metrics) RecordCicdPartialSuccess(reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cicdPartialSuccess[MetricCicdPartialSuccessReasonLabel(reason)]++
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

// RecordUnitClaimed counts one provider unit successfully claimed for
// execution, by bounded provider and dataset (CHAOS-4078). This is the
// planned-work half of the planned/failed pair the CHAOS-4125 incident found
// missing: pr-reviews/pr-comments/tests planned zero units for 36+ hours with
// no counter anywhere that would have shown a flat line instead of the
// "0 successes" durable-row-only signal an operator had to query for by
// hand. Every claim corresponds to exactly one persisted, previously-planned
// sync_run_units row; this does not double count retries as new plans.
func (m *Metrics) RecordUnitClaimed(provider, dataset string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unitClaimed[metricProvider(provider)+":"+MetricDatasetLabel(dataset)]++
}

// metricUnitFailureReasonVocabulary is the closed set of durable
// sync_run_units failure categories (CHAOS-4078). This is exactly the set of
// literal category strings internal/jobs/providerunit/providerunit.go's four
// handler.Repository.Fail(...) call sites can pass -- the only production
// caller of PostgresRepository.Fail for provider-sync units --
// cross-referenced against every exported *Category constant in that
// package: deterministicTerminalCategory's five ErrIs branches plus its
// ErrorAuthentication/ErrorNotFound switch, exhaustedFailureCategory's two
// return values, and the route-reconciliation and rate-limit-episode-
// exhaustion call sites. An unrecognized category collapses to "other"
// rather than opening the label dimension to a hostile or buggy producer.
//
// Two callers still bypass PostgresRepository.Fail with direct SQL
// (internal/syncdispatchruntime/dispatch_denial.go's "dispatch_denied",
// internal/syncreconciler/unreclaimable_sweep.go's "feature_disabled" /
// "terminal_river_delivery" reconciliation sweep) and so never reach this
// counter regardless of vocabulary -- tracked as a follow-up, not silently
// included here as if they were covered.
var metricUnitFailureReasonVocabulary = map[string]struct{}{
	"provider_dataset_unavailable":    {},
	"pagination_incomplete":           {},
	"github_tests_artifact_oversized": {},
	"all_artifacts_unreadable":        {},
	"feature_disabled":                {},
	"auth":                            {},
	"not_found":                       {},
	"repository_identity_ambiguous":   {},
	"effect_recovery_ambiguous":       {},
	"route_reconciliation_required":   {},
	"rate_limit":                      {},
	"provider_unit_exhausted":         {},
	"github_files_inventory_failed":   {},
}

// MetricUnitFailureReasonLabel bounds a unit failure category label.
func MetricUnitFailureReasonLabel(value string) string {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if _, known := metricUnitFailureReasonVocabulary[lowered]; !known {
		return "other"
	}
	return lowered
}

// RecordUnitFailed counts one provider unit that terminalized as FAILED, by
// bounded provider, dataset, and reason (CHAOS-4078). This is the counter
// CHAOS-4125's own forensics comment asked for: a dataset stuck at 100%
// failure with zero successes is exactly the "zero-success-per-dataset over
// N hours" shape CHAOS-4124 wants alertable, and prior to this the only way
// to see it was a hand-run SQL query against sync_run_units.error.
func (m *Metrics) RecordUnitFailed(provider, dataset, reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unitFailed[metricProvider(provider)+":"+MetricDatasetLabel(dataset)+
		":"+MetricUnitFailureReasonLabel(reason)]++
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

// metricWorkItemTeamAttributionSourceVocabulary is the closed set of
// CHAOS-4244 written-source labels for work_item_team_attributions. This is
// deliberately a COARSER vocabulary than the ClickHouse `source` enum
// (native_team/issue_project/project_ownership/repo_ownership/
// assignee_membership/linked_issue/author_membership/manual_fallback/
// unassigned): "author" and "assignee" are now separate stored sources
// (chris's 2026-08-24 precedence ruling gave the author its own rank 6,
// below linked_issue) rather than an evidence-prefix split of one shared
// rank, which is the dimension this series exists to make visible --
// chris's <=2% target and the reporter-membership rescue question both
// hinge on that split, not on the stored rank alone. native_team and
// manual_fallback collapse to "other" like every other bounded vocabulary
// in this file.
var metricWorkItemTeamAttributionSourceVocabulary = map[string]struct{}{
	"author": {}, "assignee": {}, "linked_issue": {}, "project": {},
	"repo": {}, "unassigned": {},
	// bot_author and ambiguous_membership are the two precision conditions
	// chris named for shipping author attribution (2026-08-23/24): a bot/App
	// author is excluded outright, and a reporter whose own membership
	// resolves to 2+ teams contributes nothing. Both are still "unassigned"
	// outcomes at the ClickHouse row level, but a distinct series label here
	// is what makes the residual READABLE -- the whole point of CHAOS-4150's
	// standing "make the misses loud" order.
	"bot_author": {}, "ambiguous_membership": {},
}

// MetricWorkItemTeamAttributionSourceLabel bounds the written-source label.
func MetricWorkItemTeamAttributionSourceLabel(value string) string {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if _, known := metricWorkItemTeamAttributionSourceVocabulary[lowered]; !known {
		return "other"
	}
	return lowered
}

// RecordWorkItemTeamAttributionWritten counts ONE PRIMARY
// work_item_team_attributions row written, by bounded provider and written
// source (CHAOS-4244). source="unassigned" is the residual chris's <=2%
// target measures against -- this is the live series twin of the durable
// per-run tally in providersync's workItemDerivationObservations (which
// survives as the worker_job_runs record; this is the operator-facing
// scrape).
func (m *Metrics) RecordWorkItemTeamAttributionWritten(provider, source string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workItemTeamAttributionWritten[metricProvider(provider)+":"+MetricWorkItemTeamAttributionSourceLabel(source)]++
}

// metricTeamAttributionMembershipLayerVocabulary bounds the layer label to a
// closed set, mirroring every other bounded vocabulary in this file.
var metricTeamAttributionMembershipLayerVocabulary = map[string]struct{}{
	"admin_override": {}, "provider_fallback": {},
}

// MetricTeamAttributionMembershipLayerLabel bounds the membership-layer
// telemetry label (chris/team-lead, 2026-08-26: "admin is an override, not
// a default -- it's the sync config mapping, but admin can override it in
// the panel"). An unrecognized value collapses to "other" rather than
// minting an unbounded series.
func MetricTeamAttributionMembershipLayerLabel(value string) string {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if _, known := metricTeamAttributionMembershipLayerVocabulary[lowered]; !known {
		return "other"
	}
	return lowered
}

// RecordTeamAttributionMembershipLayer counts ONE winning
// assignee_membership/author_membership resolution by which layer resolved
// it -- admin_override (identities.team_ids ∪ teams.manual_members) or
// provider_fallback (team_memberships ∪ teams.members). Called from
// WriteGitHubWorkItemEffect, the actual metrics-capable write boundary --
// NOT from resolveMembership/resolve(), which stay pure. See
// githubWorkItemTeamAttributionRow.Priority's doc comment for why the
// signal has to be carried that far instead of derived closer to
// resolve().
func (m *Metrics) RecordTeamAttributionMembershipLayer(layer string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.teamAttributionMembershipLayer[MetricTeamAttributionMembershipLayerLabel(layer)]++
}

// MetricProjectsV2DegradedReasonLabel bounds the provider response classes
// emitted by the GitHub Projects V2 collector. The vocabulary is closed so a
// provider payload cannot mint unbounded Prometheus series.
func MetricProjectsV2DegradedReasonLabel(value string) string {
	lowered := strings.ToLower(strings.TrimSpace(value))
	switch lowered {
	case "null_organization", "null_project", "structural_degradation", "unidentified_item":
		return lowered
	default:
		return "other"
	}
}

// RecordProjectsV2DegradedSnapshot counts one GitHub Projects V2 response that
// could not establish an authoritative board snapshot. The collector keeps
// other rows but withholds the family watermark until a complete response.
func (m *Metrics) RecordProjectsV2DegradedSnapshot(reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.projectsV2DegradedSnapshots[MetricProjectsV2DegradedReasonLabel(reason)]++
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
	if err := writeProviderDatasetCounter(
		writer, "dev_health_provider_all_artifacts_unreadable_total",
		"Provider units failed because every cicd/tests artifact observed was unreadable, by bounded provider and dataset.",
		m.allArtifactsUnreadable,
	); err != nil {
		return err
	}
	if err := writeProviderDatasetCounter(
		writer, "dev_health_provider_unit_claimed_total",
		"Provider units successfully claimed for execution, by bounded provider and dataset (CHAOS-4078).",
		m.unitClaimed,
	); err != nil {
		return err
	}
	if err := writeProviderDatasetReasonCounter(
		writer, "dev_health_provider_unit_failed_total",
		"Provider units terminalized FAILED, by bounded provider, dataset, and reason (CHAOS-4078).",
		"reason", m.unitFailed,
	); err != nil {
		return err
	}
	if err := writeProviderLabeledCounter(
		writer, "dev_health_work_item_team_attributions_written_total",
		"work_item_team_attributions rows written, by bounded provider and written source (CHAOS-4244).",
		"source", m.workItemTeamAttributionWritten,
	); err != nil {
		return err
	}
	if err := writeLabeledCounter(
		writer, "dev_health_team_attribution_membership_layer_total",
		"assignee_membership/author_membership resolutions by which layer resolved them (CHAOS-4321).",
		"layer", m.teamAttributionMembershipLayer,
	); err != nil {
		return err
	}
	if _, err := io.WriteString(writer,
		"# HELP dev_health_providersync_projects_v2_degraded_snapshots_total GitHub Projects V2 responses that were structurally degraded, by reason.\n"+"# TYPE dev_health_providersync_projects_v2_degraded_snapshots_total counter\n"); err != nil {
		return err
	}
	reasons := make([]string, 0, len(m.projectsV2DegradedSnapshots))
	for reason := range m.projectsV2DegradedSnapshots {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	for _, reason := range reasons {
		if _, err := fmt.Fprintf(writer,
			"dev_health_providersync_projects_v2_degraded_snapshots_total{reason=%q} %d\n",
			reason, m.projectsV2DegradedSnapshots[reason]); err != nil {
			return err
		}
	}
	if err := writeLabeledCounter(
		writer, "dev_health_cicd_partial_success_total",
		"cicd/tests units that advanced their watermark despite non-empty incomplete evidence, by the report_member/per-run cause that made it partial (CHAOS-4394). Not labeled by repo -- see RecordCicdPartialSuccess's doc comment; find the repo in the structured log line the caller emits alongside this counter.",
		"reason", m.cicdPartialSuccess,
	); err != nil {
		return err
	}
	return nil
}

// writeProviderLabeledCounter is the two-label twin of
// writeProviderDatasetCounter, for series that pair provider with a bounded
// label OTHER than dataset (e.g. "source"). Every key is built from bounded
// vocabularies, so SplitN into exactly two parts is total.
// writeLabeledCounter emits a single-label counter series -- unlike
// writeProviderLabeledCounter, keys are the label value directly, with no
// "provider:" composite-key splitting (this metric is not provider-scoped;
// Python's dev_health_team_attribution_membership_layer_total mirrors this
// exactly, one label, no provider dimension).
func writeLabeledCounter(
	writer io.Writer, name, help, labelName string, values map[string]uint64,
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
		if _, err := fmt.Fprintf(
			writer, "%s{%s=%q} %d\n", name, labelName, key, values[key],
		); err != nil {
			return err
		}
	}
	return nil
}

func writeProviderLabeledCounter(
	writer io.Writer, name, help, labelName string, values map[string]uint64,
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
		if len(parts) != 2 {
			continue
		}
		if _, err := fmt.Fprintf(
			writer, "%s{provider=%q,%s=%q} %d\n",
			name, parts[0], labelName, parts[1], values[key],
		); err != nil {
			return err
		}
	}
	return nil
}
