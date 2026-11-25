package restcore

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"
)

type answerTransport struct {
	answers []int
	calls   int
}

func (a *answerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	status := 599
	if a.calls < len(a.answers) {
		status = a.answers[a.calls]
	}
	a.calls++
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{}`)), Request: request}, nil
}

func waitCore(wait time.Duration, transport *answerTransport, sleeps *[]time.Duration) Core {
	return Core{Provider: "gitlab", HTTP: &http.Client{Transport: transport},
		RetryAfter: func(Response) time.Duration { return wait },
		Sleep:      func(_ context.Context, d time.Duration) error { *sleeps = append(*sleeps, d); return nil }}
}

func TestAWaitPastTheHonourCeilingIsNotSleptAndAnswersAtOnce(t *testing.T) {
	for _, wait := range []time.Duration{6 * time.Second, 24 * time.Hour, time.Duration(math.MaxInt64)} {
		var sleeps []time.Duration
		transport := &answerTransport{answers: []int{429, 429, 429, 429, 429}}
		_, err := waitCore(wait, transport, &sleeps).Get(context.Background(), "http://provider.test/x", "list")
		var typed *Error
		if !errors.As(err, &typed) || typed.Class != "RateLimitException" {
			t.Fatalf("wait %s: err = %v, want a RateLimitException", wait, err)
		}
		if len(sleeps) != 0 {
			t.Fatalf("wait %s: WANT 0 sleeps, got %v", wait, sleeps)
		}
		if transport.calls != 1 {
			t.Fatalf("wait %s: WANT 1 request, got %d", wait, transport.calls)
		}
	}
}

func TestAWaitUnderTheHonourCeilingIsSlept(t *testing.T) {
	for _, wait := range []time.Duration{2 * time.Second, 5 * time.Second} {
		var sleeps []time.Duration
		transport := &answerTransport{answers: []int{429, 200}}
		if _, err := waitCore(wait, transport, &sleeps).Get(context.Background(), "http://provider.test/x", "list"); err != nil {
			t.Fatalf("wait %s: %v", wait, err)
		}
		if len(sleeps) != 1 || sleeps[0] != wait {
			t.Fatalf("wait %s: WANT one sleep of %s, got %v", wait, wait, sleeps)
		}
	}
}

func TestAWaitThatDoesNotLeaveRoomForAnotherRequestIsNotSlept(t *testing.T) {
	var sleeps []time.Duration
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	transport := &answerTransport{answers: []int{429, 200}}
	_, err := waitCore(4*time.Second, transport, &sleeps).Get(ctx, "http://provider.test/x", "list")
	var typed *Error
	if !errors.As(err, &typed) || typed.Class != "RateLimitException" {
		t.Fatalf("err = %v, want a RateLimitException", err)
	}
	if len(sleeps) != 0 {
		t.Fatalf("WANT 0 sleeps with 3 s left on the context, got %v", sleeps)
	}
}

func TestAWaitThatFitsTheRemainingBudgetWithTheMarginIsSlept(t *testing.T) {
	var sleeps []time.Duration
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	transport := &answerTransport{answers: []int{429, 200}}
	if _, err := waitCore(4*time.Second, transport, &sleeps).Get(ctx, "http://provider.test/x", "list"); err != nil {
		t.Fatal(err)
	}
	if len(sleeps) != 1 || sleeps[0] != 4*time.Second {
		t.Fatalf("WANT one sleep of 4s with 60 s left, got %v", sleeps)
	}
}

func TestAWaitWithNoProviderAnswerKeepsTheBackoffWithoutACeiling(t *testing.T) {
	var sleeps []time.Duration
	transport := &answerTransport{answers: []int{429, 429, 429, 429, 200}}
	if _, err := waitCore(0, transport, &sleeps).Get(context.Background(), "http://provider.test/x", "list"); err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
	if len(sleeps) != len(want) {
		t.Fatalf("sleeps %v, want %v", sleeps, want)
	}
	for i := range want {
		if sleeps[i] != want[i] {
			t.Fatalf("sleeps %v, want %v", sleeps, want)
		}
	}
}
