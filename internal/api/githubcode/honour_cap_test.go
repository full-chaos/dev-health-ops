package githubcode

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func honourClient(transport *scriptedTransport, sleeps *[]time.Duration) Client {
	return Client{Token: "tok", HTTP: &http.Client{Transport: transport},
		Sleep: func(_ context.Context, wait time.Duration) error { *sleeps = append(*sleeps, wait); return nil }}
}

func TestAGitHubWaitPastTheHonourCeilingIsNotSleptAndAnswersAtOnce(t *testing.T) {
	farReset := strconv.FormatInt(time.Now().Add(24*time.Hour).Unix(), 10)
	for _, test := range []struct {
		name    string
		code    int
		headers [][2]string
	}{
		{"429 Retry-After a day", 429, [][2]string{{"Retry-After", "86400"}}},
		{"429 Retry-After 1e30", 429, [][2]string{{"Retry-After", "1e30"}}},
		{"429 Retry-After +Inf", 429, [][2]string{{"Retry-After", "+Inf"}}},
		{"429 far x-ratelimit-reset", 429, [][2]string{{"x-ratelimit-reset", farReset}}},
		{"403 Retry-After a day", 403, [][2]string{{"Retry-After", "86400"}}},
		{"403 Retry-After 1e30", 403, [][2]string{{"Retry-After", "1e30"}}},
		{"403 Retry-After +Inf", 403, [][2]string{{"Retry-After", "+Inf"}}},
		{"403 far x-ratelimit-reset", 403, [][2]string{{"x-ratelimit-remaining", "0"}, {"x-ratelimit-reset", farReset}}},
		{"429 Retry-After 6", 429, [][2]string{{"Retry-After", "6"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &scriptedTransport{responses: repeat(5, status(test.code, `{}`, test.headers...))}
			var sleeps []time.Duration
			_, err := honourClient(transport, &sleeps).ListRepositories(context.Background(), ListOptions{})
			var typed *Error
			if !errors.As(err, &typed) || typed.Class != "RateLimitException" {
				t.Fatalf("err = %v, want a RateLimitException", err)
			}
			if len(sleeps) != 0 {
				t.Fatalf("WANT 0 sleeps, got %v", sleeps)
			}
			if len(transport.seen) != 1 {
				t.Fatalf("WANT 1 request, got %d", len(transport.seen))
			}
		})
	}
}

func TestAGitHubWaitUnderTheHonourCeilingIsSlept(t *testing.T) {
	for _, raw := range []string{"2", "5"} {
		wait, _ := time.ParseDuration(raw + "s")
		transport := &scriptedTransport{responses: []scripted{
			status(429, `{}`, [2]string{"Retry-After", raw}),
			ok(`[]`),
		}}
		var sleeps []time.Duration
		if _, err := honourClient(transport, &sleeps).ListRepositories(context.Background(), ListOptions{}); err != nil {
			t.Fatalf("Retry-After %s: %v", raw, err)
		}
		if len(sleeps) != 1 || sleeps[0] != wait {
			t.Fatalf("Retry-After %s: WANT one sleep of %s, got %v", raw, wait, sleeps)
		}
	}
}

func TestAGitHubWaitThatDoesNotFitTheContextIsNotSlept(t *testing.T) {
	transport := &scriptedTransport{responses: []scripted{
		status(429, `{}`, [2]string{"Retry-After", "4"}),
		ok(`[]`),
	}}
	var sleeps []time.Duration
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := honourClient(transport, &sleeps).ListRepositories(ctx, ListOptions{})
	var typed *Error
	if !errors.As(err, &typed) || typed.Class != "RateLimitException" {
		t.Fatalf("err = %v, want a RateLimitException", err)
	}
	if len(sleeps) != 0 {
		t.Fatalf("WANT 0 sleeps with 3 s left, got %v", sleeps)
	}
}
