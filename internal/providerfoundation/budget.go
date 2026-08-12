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

// SyncBudgetKey exactly mirrors Python BudgetGuard's advisory-lock key, so
// that WHEN Go dispatch acquires it in a claim transaction before evaluating
// active units, Go admission will serialize with a concurrent Python
// admission.
//
// NOT YET WIRED, stated in the future tense on purpose. Neither SyncBudgetKey
// nor PostgresBudgetLocker below has a single caller outside this package's
// tests -- no Go dispatch path acquires this lock today, because Go performs
// no unit-dispatch admission at all. The previous present-tense wording read
// as a live serialization guarantee and was cited as evidence of one during
// the CHAOS-3465 mirror review. The key derivation is correct and worth
// keeping ready; it is not in force.
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
	mu                  sync.Mutex
	requests            map[string]uint64
	budgetDenied        map[string]uint64
	budgetReleaseErrors map[string]uint64
}

func NewMetrics() *Metrics {
	return &Metrics{
		requests:            map[string]uint64{},
		budgetDenied:        map[string]uint64{},
		budgetReleaseErrors: map[string]uint64{},
	}
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
	return nil
}
