package admin

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func recordingHandlers(sleeps *[]time.Duration) *handlers {
	return &handlers{pagerDutySleep: func(_ context.Context, d time.Duration) error {
		*sleeps = append(*sleeps, d)
		return nil
	}}
}

func TestAPagerDutyWaitOverTheCeilingRecordsNoSleep(t *testing.T) {
	for _, header := range []string{"6", "86400", "1e30", "+Inf"} {
		for _, status := range []int{429, 503} {
			doer := &scriptedPagerDuty{answers: repeatAnswer(5, scriptedAnswer{status: status, headers: http.Header{"Retry-After": {header}}})}
			var sleeps []time.Duration
			_, failure := recordingHandlers(&sleeps).pagerDutyGET(context.Background(), doer, pagerDutyTarget, pagerDutyAuthForTest())
			if failure == nil || failure.err != nil {
				t.Fatalf("%d Retry-After %s: failure = %+v", status, header, failure)
			}
			if len(sleeps) != 0 || doer.calls != 1 {
				t.Fatalf("%d Retry-After %s: sleeps %v, requests %d, WANT none and 1", status, header, sleeps, doer.calls)
			}
		}
	}
}

func TestAPagerDutyRefusedWaitOn429KeepsItsRetryAfterHeader(t *testing.T) {
	doer := &scriptedPagerDuty{answers: []scriptedAnswer{{status: 429, headers: http.Header{"Retry-After": {"86400"}}}}}
	var sleeps []time.Duration
	_, failure := recordingHandlers(&sleeps).pagerDutyGET(context.Background(), doer, pagerDutyTarget, pagerDutyAuthForTest())
	if failure == nil || failure.status != http.StatusTooManyRequests || failure.header.Get("Retry-After") != "86400" {
		t.Fatalf("failure = %+v, WANT 429 carrying Retry-After 86400", failure)
	}
}

func TestAPagerDutyWaitAtOrUnderTheCeilingIsSleptOnce(t *testing.T) {
	for _, seconds := range []string{"2", "5"} {
		wait, _ := time.ParseDuration(seconds + "s")
		doer := &scriptedPagerDuty{answers: []scriptedAnswer{
			{status: 429, headers: http.Header{"Retry-After": {seconds}}},
			{status: 200, headers: http.Header{}},
		}}
		var sleeps []time.Duration
		body, failure := recordingHandlers(&sleeps).pagerDutyGET(context.Background(), doer, pagerDutyTarget, pagerDutyAuthForTest())
		if failure != nil || string(body) != `{}` || len(sleeps) != 1 || sleeps[0] != wait {
			t.Fatalf("Retry-After %s: body %q failure %+v sleeps %v, WANT one sleep of %s", seconds, body, failure, sleeps, wait)
		}
	}
}

func TestAPagerDutyWaitThatLeavesTheMarginIsSleptAndOneThatDoesNotIsNot(t *testing.T) {
	for _, row := range []struct {
		left      time.Duration
		wantSleep bool
	}{{time.Minute, true}, {3 * time.Second, false}, {34 * time.Second, false}} {
		doer := &scriptedPagerDuty{answers: []scriptedAnswer{
			{status: 429, headers: http.Header{"Retry-After": {"4"}}},
			{status: 200, headers: http.Header{}},
		}}
		var sleeps []time.Duration
		ctx, cancel := context.WithTimeout(context.Background(), row.left)
		_, failure := recordingHandlers(&sleeps).pagerDutyGET(ctx, doer, pagerDutyTarget, pagerDutyAuthForTest())
		cancel()
		if row.wantSleep {
			if failure != nil || len(sleeps) != 1 || sleeps[0] != 4*time.Second {
				t.Fatalf("%s left: failure %+v sleeps %v, WANT one sleep of 4s", row.left, failure, sleeps)
			}
			continue
		}
		if failure == nil || failure.status != http.StatusTooManyRequests || len(sleeps) != 0 || doer.calls != 1 {
			t.Fatalf("%s left: failure %+v sleeps %v requests %d, WANT the 429 at once with no sleep", row.left, failure, sleeps, doer.calls)
		}
	}
}

func TestAPagerDutyAnswerWithNoWaitSleepsTheBackoffWithoutACeiling(t *testing.T) {
	doer := &scriptedPagerDuty{answers: []scriptedAnswer{
		{status: 503, headers: http.Header{}}, {status: 503, headers: http.Header{}},
		{status: 503, headers: http.Header{}}, {status: 503, headers: http.Header{}},
		{status: 200, headers: http.Header{}},
	}}
	var sleeps []time.Duration
	if _, failure := recordingHandlers(&sleeps).pagerDutyGET(context.Background(), doer, pagerDutyTarget, pagerDutyAuthForTest()); failure != nil {
		t.Fatalf("failure = %+v", failure)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
	if len(sleeps) != len(want) {
		t.Fatalf("sleeps %v, want %v", sleeps, want)
	}
	for index := range want {
		if sleeps[index] != want[index] {
			t.Fatalf("sleeps %v, want %v", sleeps, want)
		}
	}
}
