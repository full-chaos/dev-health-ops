package authruntime

import (
	"net/http"
	"testing"
	"time"
)

func scripted(statuses ...int) (func() int, *int) {
	calls := 0
	return func() int {
		s := statuses[min(calls, len(statuses)-1)]
		calls++
		return s
	}, &calls
}

func TestFirstLoginAnswerWaitsOutRetryable503(t *testing.T) {
	login, calls := scripted(503, 503, 401)
	slept := time.Duration(0)
	got := firstLoginAnswer(login, time.Minute, time.Second, func(d time.Duration) { slept += d })
	if got != http.StatusUnauthorized || *calls != 3 || slept != 2*time.Second {
		t.Fatalf("got status %d after %d calls, %s slept; want 401 after retries (3 calls, 2s)", got, *calls, slept)
	}
}

func TestFirstLoginAnswerStopsAtGuard(t *testing.T) {
	login, calls := scripted(503)
	got := firstLoginAnswer(login, 3*time.Second, time.Second, func(time.Duration) {})
	if got != http.StatusServiceUnavailable || *calls != 4 {
		t.Fatalf("got status %d after %d calls; want 503 after 4 calls (guard applied)", got, *calls)
	}
}

func TestFirstLoginAnswerPassesOtherStatusAtOnce(t *testing.T) {
	login, calls := scripted(500, 401)
	got := firstLoginAnswer(login, time.Minute, time.Second, func(time.Duration) { t.Fatal("slept on a non-503") })
	if got != http.StatusInternalServerError || *calls != 1 {
		t.Fatalf("got status %d after %d calls; want 500 after 1 call (only 503 is retried)", got, *calls)
	}
}
