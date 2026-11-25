package admin

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

type scriptedPagerDuty struct {
	answers []scriptedAnswer
	calls   int
}

type scriptedAnswer struct {
	status  int
	headers http.Header
}

func (s *scriptedPagerDuty) Do(request *http.Request) (*http.Response, error) {
	answer := scriptedAnswer{status: 599, headers: http.Header{}}
	if s.calls < len(s.answers) {
		answer = s.answers[s.calls]
	}
	s.calls++
	return &http.Response{StatusCode: answer.status, Header: answer.headers.Clone(), Request: request, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
}

func repeatAnswer(count int, answer scriptedAnswer) []scriptedAnswer {
	out := make([]scriptedAnswer, count)
	for index := range out {
		out[index] = answer
	}
	return out
}

var pagerDutyTarget = "https://api.pagerduty.example/services"

func pagerDutyAuthForTest() pagerDutyRequestAuth {
	return pagerDutyRequestAuth{header: "Token token=synthetic"}
}

// A provider wait the core will not sleep (past the ceiling, or past what the request context leaves) ends the call at
// once with the terminal failure of the response that carried it (CHAOS-14621). The context bound keeps a sleeping
// core from running for the wait itself; no duration is asserted.
func TestAPagerDutyWaitPastTheHonourCeilingIsAnsweredAtOnce(t *testing.T) {
	far := strconv.FormatInt(time.Now().Add(24*time.Hour).Unix(), 10)
	for _, row := range []struct {
		name       string
		status     int
		headers    http.Header
		wantStatus int
	}{
		{"429 Retry-After a day", 429, http.Header{"Retry-After": {"86400"}}, 429},
		{"429 far Ratelimit-Reset", 429, http.Header{"Ratelimit-Reset": {far}}, 429},
		{"429 Retry-After 1e30", 429, http.Header{"Retry-After": {"1e30"}}, 429},
		{"503 Retry-After a day", 503, http.Header{"Retry-After": {"86400"}}, 502},
		{"503 far Ratelimit-Reset", 503, http.Header{"Ratelimit-Reset": {far}}, 502},
		{"503 Retry-After 1e30", 503, http.Header{"Retry-After": {"1e30"}}, 502},
	} {
		t.Run(row.name, func(t *testing.T) {
			doer := &scriptedPagerDuty{answers: repeatAnswer(5, scriptedAnswer{status: row.status, headers: row.headers})}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, failure := (&handlers{}).pagerDutyGET(ctx, doer, pagerDutyTarget, pagerDutyAuthForTest())
			if failure == nil || failure.err != nil || failure.status != row.wantStatus {
				t.Fatalf("failure = %+v, WANT the terminal answer %d with no internal error", failure, row.wantStatus)
			}
			if doer.calls != 1 {
				t.Fatalf("%d requests, WANT 1", doer.calls)
			}
		})
	}
}

func TestAPagerDutyWaitThatDoesNotFitTheContextIsAnsweredAtOnce(t *testing.T) {
	doer := &scriptedPagerDuty{answers: []scriptedAnswer{
		{status: 429, headers: http.Header{"Retry-After": {"4"}}},
		{status: 200, headers: http.Header{}},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, failure := (&handlers{}).pagerDutyGET(ctx, doer, pagerDutyTarget, pagerDutyAuthForTest())
	if failure == nil || failure.err != nil || failure.status != http.StatusTooManyRequests {
		t.Fatalf("failure = %+v, WANT the terminal 429 with 3 s left on the context and a 4 s wait", failure)
	}
	if doer.calls != 1 {
		t.Fatalf("%d requests, WANT 1", doer.calls)
	}
}

func TestAPagerDutyWaitUnderTheCeilingIsSleptAndThePageIsRead(t *testing.T) {
	doer := &scriptedPagerDuty{answers: []scriptedAnswer{
		{status: 429, headers: http.Header{"Retry-After": {"2"}}},
		{status: 200, headers: http.Header{}},
	}}
	body, failure := (&handlers{}).pagerDutyGET(context.Background(), doer, pagerDutyTarget, pagerDutyAuthForTest())
	if failure != nil || string(body) != `{}` || doer.calls != 2 {
		t.Fatalf("body %q failure %+v requests %d, want the second answer after the 2 s wait", body, failure, doer.calls)
	}
}

func TestAPagerDutyAnswerWithNoWaitKeepsTheBackoff(t *testing.T) {
	doer := &scriptedPagerDuty{answers: []scriptedAnswer{
		{status: 503, headers: http.Header{}},
		{status: 200, headers: http.Header{}},
	}}
	body, failure := (&handlers{}).pagerDutyGET(context.Background(), doer, pagerDutyTarget, pagerDutyAuthForTest())
	if failure != nil || string(body) != `{}` || doer.calls != 2 {
		t.Fatalf("body %q failure %+v requests %d, want the second answer after the backoff", body, failure, doer.calls)
	}
}
