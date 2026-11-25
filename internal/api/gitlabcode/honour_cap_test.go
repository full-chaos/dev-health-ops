package gitlabcode

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/restcore"
)

func honourClient(transport *scriptedTransport, sleeps *[]time.Duration) Client {
	return Client{BaseURL: "http://gitlab.test", Token: "tok", HTTP: &http.Client{Transport: transport},
		Sleep: func(_ context.Context, wait time.Duration) error { *sleeps = append(*sleeps, wait); return nil }}
}

func TestAGitLabWaitPastTheHonourCeilingIsNotSleptAndAnswersAtOnce(t *testing.T) {
	farReset := strconv.FormatInt(time.Now().Add(24*time.Hour).Unix(), 10)
	for _, test := range []struct {
		name    string
		code    int
		headers [][2]string
	}{
		{"429 Retry-After a day", 429, [][2]string{{"Retry-After", "86400"}}},
		{"429 far RateLimit-Reset", 429, [][2]string{{"RateLimit-Reset", farReset}}},
		{"403 Retry-After a day", 403, [][2]string{{"Retry-After", "86400"}}},
		{"403 far RateLimit-Reset", 403, [][2]string{{"RateLimit-Remaining", "0"}, {"RateLimit-Reset", farReset}}},
		{"429 Retry-After 6", 429, [][2]string{{"Retry-After", "6"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &scriptedTransport{responses: repeat(5, status(test.code, `{}`, test.headers...))}
			var sleeps []time.Duration
			_, err := honourClient(transport, &sleeps).ListGroupProjects(context.Background(), "grp")
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

func TestAGitLabWaitUnderTheHonourCeilingIsSlept(t *testing.T) {
	for _, raw := range []string{"2", "5"} {
		wait, _ := time.ParseDuration(raw + "s")
		transport := &scriptedTransport{responses: []scripted{
			status(429, `{}`, [2]string{"Retry-After", raw}),
			ok(`[{"id": 1, "name": "api", "path_with_namespace": "grp/api"}]`),
		}}
		var sleeps []time.Duration
		if _, err := honourClient(transport, &sleeps).ListGroupProjects(context.Background(), "grp"); err != nil {
			t.Fatalf("Retry-After %s: %v", raw, err)
		}
		if len(sleeps) != 1 || sleeps[0] != wait {
			t.Fatalf("Retry-After %s: WANT one sleep of %s, got %v", raw, wait, sleeps)
		}
	}
}

func TestAGitLabWaitThatDoesNotFitTheContextIsNotSlept(t *testing.T) {
	transport := &scriptedTransport{responses: []scripted{
		status(429, `{}`, [2]string{"Retry-After", "4"}),
		ok(`[{"id": 1, "name": "api", "path_with_namespace": "grp/api"}]`),
	}}
	var sleeps []time.Duration
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := honourClient(transport, &sleeps).ListGroupProjects(ctx, "grp")
	var typed *restcore.Error
	if !errors.As(err, &typed) || typed.Class != "RateLimitException" {
		t.Fatalf("err = %v, want a RateLimitException", err)
	}
	if len(sleeps) != 0 {
		t.Fatalf("WANT 0 sleeps with 3 s left, got %v", sleeps)
	}
}
