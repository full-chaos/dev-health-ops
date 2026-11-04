package authapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// refundCounters is an in-memory counter store that also takes a charge back
// (Refund, the optional seam of the login limiter) and keeps the order of
// what it saw, so a cell can read what was charged, refunded and when.
type refundCounters struct {
	mu        sync.Mutex
	counts    map[string]int
	incs      map[string]int
	refunds   map[string]int
	events    []string
	refundErr error
	failInc   string
	noRefund  bool
}

func newRefundCounters() *refundCounters {
	return &refundCounters{counts: map[string]int{}, incs: map[string]int{}, refunds: map[string]int{}}
}

func (s *refundCounters) Increment(_ context.Context, hit httpapi.Hit) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if hit.Limit.ID == s.failInc {
		return 0, pgDown
	}
	key := hit.Limit.ID + "|" + hit.Key
	s.counts[key]++
	s.incs[hit.Limit.ID]++
	s.events = append(s.events, "inc:"+hit.Limit.ID)
	return int64(s.counts[key]), nil
}

func (s *refundCounters) Peek(_ context.Context, hit httpapi.Hit) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(s.counts[hit.Limit.ID+"|"+hit.Key]), nil
}

func (*refundCounters) Backend() string { return "memory" }

// Refund is the seam: a cancelled context fails it, as a real store call.
func (s *refundCounters) Refund(ctx context.Context, hit httpapi.Hit) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		s.events = append(s.events, "refund-failed:"+hit.Limit.ID)
		return err
	}
	if s.refundErr != nil {
		s.events = append(s.events, "refund-failed:"+hit.Limit.ID)
		return s.refundErr
	}
	key := hit.Limit.ID + "|" + hit.Key
	if s.counts[key] > 0 {
		s.counts[key]--
	}
	s.refunds[hit.Limit.ID]++
	s.events = append(s.events, "refund:"+hit.Limit.ID)
	return nil
}

func (s *refundCounters) note(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

// held is the sum of the counters of one limit id.
func (s *refundCounters) held(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for key, n := range s.counts {
		if strings.HasPrefix(key, id+"|") {
			total += n
		}
	}
	return total
}

func (s *refundCounters) refunded(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refunds[id]
}

// norefundStore is a store with no Refund method at all.
type norefundStore struct{ inner *refundCounters }

func (s norefundStore) Increment(ctx context.Context, hit httpapi.Hit) (int64, error) {
	return s.inner.Increment(ctx, hit)
}
func (s norefundStore) Peek(ctx context.Context, hit httpapi.Hit) (int64, error) {
	return s.inner.Peek(ctx, hit)
}
func (norefundStore) Backend() string { return "memory" }

func refundFixture(t *testing.T) (*compatFixture, *refundCounters) {
	t.Helper()
	f := newCompatFixture(t)
	store := newRefundCounters()
	f.counters = store
	f.buildCompat()
	return f, store
}

func wrongLogin(f *compatFixture) *httptest.ResponseRecorder {
	return f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong password", ""), nil)
}

func goodLogin(f *compatFixture) *httptest.ResponseRecorder {
	return f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil)
}

// The stall repro shape: ten tries that fail before the password is compared,
// then the store is back. None of them is spent, so the next login is answered.
func TestLoginDBDownBeforeCompareRefundsAndTheNextLoginIsNot429(t *testing.T) {
	cases := []struct {
		name string
		down func(*compatFixture)
		up   func(*compatFixture)
	}{
		{"user lookup", func(f *compatFixture) { f.loginUsers.err = pgDown }, func(f *compatFixture) { f.loginUsers.err = nil }},
		{"lockout check", func(f *compatFixture) { f.attempts.checkErr = pgDown }, func(f *compatFixture) { f.attempts.checkErr = nil }},
		{"lookup of a non-dependency failure", func(f *compatFixture) { f.loginUsers.err = errors.New("unexpected row shape") }, func(f *compatFixture) { f.loginUsers.err = nil }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, store := refundFixture(t)
			c.down(f)
			for i := 0; i < loginAccountLimit.Count; i++ {
				if r := goodLogin(f); r.Code != http.StatusServiceUnavailable && r.Code != http.StatusInternalServerError {
					t.Fatalf("try %d = %d, want 503 or 500", i+1, r.Code)
				}
			}
			c.up(f)
			if r := goodLogin(f); r.Code != http.StatusOK {
				t.Fatalf("login after the outage = %d, want 200: %s", r.Code, r.Body)
			}
			if n := store.held(loginAccountLimit.ID); n != 1 {
				t.Fatalf("account counter holds %d, want 1 (the success only)", n)
			}
			if n := store.held(loginIPLimit.ID); n != 1 {
				t.Fatalf("address counter holds %d, want 1 (the success only)", n)
			}
		})
	}
}

func TestLoginPreflightFailureRefundsBothCharges(t *testing.T) {
	f, store := refundFixture(t)
	f = onEdDSA(t, f, unreadableKeySigner(t))
	for i := 0; i < 3; i++ {
		expectGoAPIUnavailable(t, goodLogin(f))
	}
	if a, ip := store.held(loginAccountLimit.ID), store.held(loginIPLimit.ID); a != 0 || ip != 0 {
		t.Fatalf("counters hold account=%d address=%d after refused tries, want 0 and 0", a, ip)
	}
}

// Acceptance (2), serial: a wrong password is a 401 and stays charged.
func TestLoginWrongPasswordStaysCharged(t *testing.T) {
	f, store := refundFixture(t)
	for i := 0; i < loginAccountLimit.Count; i++ {
		if r := wrongLogin(f); r.Code != http.StatusUnauthorized {
			t.Fatalf("try %d = %d, want 401", i+1, r.Code)
		}
	}
	assertRateLimited(t, "11th wrong password", wrongLogin(f))
	if n := store.refunded(loginAccountLimit.ID) + store.refunded(loginIPLimit.ID); n != 0 {
		t.Fatalf("%d refunds for 401s, want 0", n)
	}
}

func TestLoginSuccessStaysCharged(t *testing.T) {
	f, store := refundFixture(t)
	if r := goodLogin(f); r.Code != http.StatusOK {
		t.Fatalf("login = %d: %s", r.Code, r.Body)
	}
	if a, ip := store.held(loginAccountLimit.ID), store.held(loginIPLimit.ID); a != 1 || ip != 1 {
		t.Fatalf("counters hold account=%d address=%d after a success, want 1 and 1", a, ip)
	}
}

// A refusal after the compare (the failed attempt cannot be recorded) keeps its
// charge: a guess was checked.
func TestLoginAfterCompareFailureStaysCharged(t *testing.T) {
	f, store := refundFixture(t)
	f.attempts.recordErr = pgDown
	if r := wrongLogin(f); r.Code != http.StatusServiceUnavailable {
		t.Fatalf("login = %d, want 503", r.Code)
	}
	if a, ip := store.held(loginAccountLimit.ID), store.held(loginIPLimit.ID); a != 1 || ip != 1 {
		t.Fatalf("counters hold account=%d address=%d, want 1 and 1", a, ip)
	}
	if n := store.refunded(loginAccountLimit.ID) + store.refunded(loginIPLimit.ID); n != 0 {
		t.Fatalf("%d refunds after the compare, want 0", n)
	}
}

func TestLoginAuditFailureAfterRefusalStaysCharged(t *testing.T) {
	f, store := refundFixture(t)
	f.appendErr = pgDown
	if r := wrongLogin(f); r.Code != http.StatusServiceUnavailable {
		t.Fatalf("login = %d, want 503", r.Code)
	}
	if a := store.held(loginAccountLimit.ID); a != 1 {
		t.Fatalf("account counter holds %d, want 1", a)
	}
}

func TestLoginLockedStaysCharged(t *testing.T) {
	f, store := refundFixture(t)
	f.attempts.locked, f.attempts.remaining = true, time.Minute
	if r := goodLogin(f); r.Code != http.StatusTooManyRequests {
		t.Fatalf("locked login = %d, want 429", r.Code)
	}
	if a := store.held(loginAccountLimit.ID); a != 1 {
		t.Fatalf("account counter holds %d, want 1", a)
	}
}

// A refused hit is never given back: after the budget is spent, a failing
// try is a 429 and the counter keeps counting it.
func TestLoginRefusedHitIsNotRefunded(t *testing.T) {
	f, store := refundFixture(t)
	for i := 0; i < loginAccountLimit.Count; i++ {
		wrongLogin(f)
	}
	f.loginUsers.err = pgDown
	assertRateLimited(t, "11th", goodLogin(f))
	if n := store.refunded(loginAccountLimit.ID); n != 0 {
		t.Fatalf("%d refunds for a 429, want 0", n)
	}
	if a := store.held(loginAccountLimit.ID); a != loginAccountLimit.Count+1 {
		t.Fatalf("account counter holds %d, want %d", a, loginAccountLimit.Count+1)
	}
}

// A refund that fails keeps the charge: the answer is the same 503, and one
// WARN-level line names the limit and the error class, never the address or
// the e-mail.
func TestLoginRefundFailureKeepsTheChargeAndLogsClassOnly(t *testing.T) {
	f, store := refundFixture(t)
	store.refundErr = errors.New("valkey refused for sentinel-person@example.com 203.0.113.9")
	f.loginUsers.err = pgDown
	r := goodLogin(f)
	expectDown(t, "login", r, r.Body.String())
	if a := store.held(loginAccountLimit.ID); a != 1 {
		t.Fatalf("account counter holds %d, want 1 (the charge is kept)", a)
	}
	logs := f.logs.String()
	if !strings.Contains(logs, loginAccountLimit.ID) {
		t.Fatalf("no log line names the limit: %s", logs)
	}
	for _, leak := range []string{compatEmail, "sentinel", "203.0.113.9", "192.0.2.1"} {
		if strings.Contains(logs, leak) {
			t.Fatalf("log line leaks %q: %s", leak, logs)
		}
	}
	if !strings.Contains(logs, `"level":"WARN"`) {
		t.Fatalf("no WARN line for the failed refund: %s", logs)
	}
}

// A store without a refund seam leaves the charge, with no panic.
func TestLoginOnAStoreWithoutRefundKeepsTheCharge(t *testing.T) {
	f := newCompatFixture(t)
	inner := newRefundCounters()
	f.counters = norefundStore{inner: inner}
	f.buildCompat()
	f.loginUsers.err = pgDown
	r := goodLogin(f)
	expectDown(t, "login", r, r.Body.String())
	if a := inner.held(loginAccountLimit.ID); a != 1 {
		t.Fatalf("account counter holds %d, want 1", a)
	}
}

// The refund lands before the answer is written: a client that retries on the
// 503 at once must not race it.
func TestLoginRefundIsDoneBeforeTheAnswerIsWritten(t *testing.T) {
	f, store := refundFixture(t)
	f.loginUsers.err = pgDown
	request := httptest.NewRequest(http.MethodPost, PatternCompatLogin, strings.NewReader(loginBody(compatEmail, compatPassword, "")))
	writer := &noteWriter{ResponseRecorder: httptest.NewRecorder(), store: store}
	f.server.ServeHTTP(writer, request)
	if writer.Code != http.StatusServiceUnavailable {
		t.Fatalf("login = %d", writer.Code)
	}
	at, refunds := -1, 0
	for i, event := range store.events {
		if event == "answer" && at < 0 {
			at = i
		}
		if strings.HasPrefix(event, "refund:") {
			refunds++
			if at >= 0 {
				t.Fatalf("a refund came after the answer: %v", store.events)
			}
		}
	}
	if at < 0 || refunds != 2 {
		t.Fatalf("events %v: want 2 refunds before the answer", store.events)
	}
}

type noteWriter struct {
	*httptest.ResponseRecorder
	store *refundCounters
	noted bool
}

func (w *noteWriter) note() {
	if !w.noted {
		w.noted = true
		w.store.note("answer")
	}
}
func (w *noteWriter) WriteHeader(code int) { w.note(); w.ResponseRecorder.WriteHeader(code) }
func (w *noteWriter) Write(b []byte) (int, error) {
	w.note()
	return w.ResponseRecorder.Write(b)
}

// A client that goes away, or a deadline that runs out, in the user lookup
// must not stop the refund (it runs on its own context).
func TestLoginRefundSurvivesACancelledOrExpiredRequest(t *testing.T) {
	for _, c := range []struct {
		name string
		fail error
		want int
	}{
		{"client cancel", context.Canceled, http.StatusInternalServerError},
		{"deadline", context.DeadlineExceeded, http.StatusServiceUnavailable},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, store := refundFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.loginUsers.beforeErr = cancel
			f.loginUsers.err = fmt.Errorf("lookup: %w", c.fail)
			request := httptest.NewRequest(http.MethodPost, PatternCompatLogin, strings.NewReader(loginBody(compatEmail, compatPassword, ""))).WithContext(ctx)
			response := httptest.NewRecorder()
			f.server.ServeHTTP(response, request)
			if response.Code != c.want {
				t.Fatalf("login = %d, want %d", response.Code, c.want)
			}
			if a, ip := store.held(loginAccountLimit.ID), store.held(loginIPLimit.ID); a != 0 || ip != 0 {
				t.Fatalf("counters hold account=%d address=%d, want 0 and 0: %v", a, ip, store.events)
			}
		})
	}
}

// The account limiter's store fails after the address charge was taken: that
// charge goes back too.
func TestLoginAccountStoreErrorRefundsTheAddressCharge(t *testing.T) {
	f, store := refundFixture(t)
	store.failInc = loginAccountLimit.ID
	if r := goodLogin(f); r.Code != http.StatusServiceUnavailable {
		t.Fatalf("login = %d, want 503", r.Code)
	}
	if ip := store.held(loginIPLimit.ID); ip != 0 {
		t.Fatalf("address counter holds %d, want 0", ip)
	}
}

// Acceptance (2), parallel: forty wrong-password logins arrive together and are
// held inside the user lookup until every refused one has been answered. Ten
// are admitted and answer 401, thirty answer 429, and the budget stays spent.
func TestLoginParallelBurstKeepsTheBoundForWrongPasswords(t *testing.T) {
	f, store := refundFixture(t)
	const total = 40
	arrived := make(chan struct{}, total)
	release := make(chan struct{})
	f.loginUsers.hold = func() { arrived <- struct{}{}; <-release }
	codes := make(chan int, total)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes <- wrongLogin(f).Code
		}()
	}
	limited := 0
	deadline := time.After(30 * time.Second)
	for admitted := 0; admitted < loginAccountLimit.Count || limited < total-loginAccountLimit.Count; {
		select {
		case <-arrived:
			admitted++
		case code := <-codes:
			if code != http.StatusTooManyRequests {
				t.Fatalf("an answer before the release = %d, want 429", code)
			}
			limited++
		case <-deadline:
			t.Fatal("the burst did not settle")
		}
	}
	close(release)
	wg.Wait()
	close(codes)
	unauthorized := 0
	for code := range codes {
		if code != http.StatusUnauthorized {
			t.Fatalf("an admitted login answered %d, want 401", code)
		}
		unauthorized++
	}
	if unauthorized != loginAccountLimit.Count {
		t.Fatalf("%d logins answered 401, want %d", unauthorized, loginAccountLimit.Count)
	}
	assertRateLimited(t, "login after the burst", wrongLogin(f))
	if n := store.refunded(loginAccountLimit.ID); n != 0 {
		t.Fatalf("%d refunds in a burst of 401s, want 0", n)
	}
}
