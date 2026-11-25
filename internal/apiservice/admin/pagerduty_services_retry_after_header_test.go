package admin

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// The Retry-After a refused 429 hands to the client is a whole number of seconds that never overflows, on every
// architecture: a value past int64 (or +Inf) is the largest int64 (CHAOS-14621).
func TestARefusedPagerDutyWaitSaturatesItsRetryAfterHeader(t *testing.T) {
	for _, row := range []struct{ name, retryAfter, want string }{
		{"a day", "86400", "86400"},
		{"fractional", "86400.2", "86401"},
		{"past int64", "1e30", "9223372036854775807"},
		{"+Inf", "+Inf", "9223372036854775807"},
		{"2^63", "9223372036854775808", "9223372036854775807"},
	} {
		t.Run(row.name, func(t *testing.T) {
			doer := &scriptedPagerDuty{answers: []scriptedAnswer{{status: 429, headers: http.Header{"Retry-After": {row.retryAfter}}}}}
			h := &handlers{pagerDutySleep: func(context.Context, time.Duration) error { return nil }}
			_, failure := h.pagerDutyGET(context.Background(), doer, pagerDutyTarget, pagerDutyAuthForTest())
			if failure == nil || failure.status != http.StatusTooManyRequests || failure.header.Get("Retry-After") != row.want {
				t.Fatalf("failure = %+v, WANT 429 carrying Retry-After %s", failure, row.want)
			}
			if doer.calls != 1 {
				t.Fatalf("%d requests, WANT 1", doer.calls)
			}
		})
	}
}
