package gitlabcode

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/restcore"
)

func TestRetryAfterReadsTheRateLimitResetInEpochSeconds(t *testing.T) {
	t.Parallel()
	reset := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
	got := retryAfter(restcore.Response{Status: 429, Header: http.Header{"Ratelimit-Reset": {reset}}})
	if got < 59*time.Minute || got > time.Hour+2*time.Second {
		t.Fatalf("RateLimit-Reset %s (epoch seconds, now+1h) gave %s, want about 1h", reset, got)
	}
	if got := retryAfter(restcore.Response{Status: 429, Header: http.Header{}}); got != 0 {
		t.Fatalf("no reset header gave %s, want 0", got)
	}
	both := http.Header{"Retry-After": {"7"}, "Ratelimit-Reset": {reset}}
	if got := retryAfter(restcore.Response{Status: 429, Header: both}); got != 7*time.Second {
		t.Fatalf("Retry-After 7 with a reset gave %s, want 7s (Retry-After wins)", got)
	}
}

func TestRetryAfterIgnoresAnOverflowingReset(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"1767225605000", "+Inf", "NaN", "1e400", "-5"} {
		if got := retryAfter(restcore.Response{Status: 429, Header: http.Header{"Ratelimit-Reset": {raw}}}); got != 0 {
			t.Fatalf("RateLimit-Reset %q gave %s, want 0", raw, got)
		}
	}
}

func TestRetryAfterHonoursAResetMoreThanADayAhead(t *testing.T) {
	t.Parallel()
	reset := strconv.FormatInt(time.Now().Add(48*time.Hour).Unix(), 10)
	got := retryAfter(restcore.Response{Status: 429, Header: http.Header{"Ratelimit-Reset": {reset}}})
	if got < 48*time.Hour-2*time.Second || got > 48*time.Hour+2*time.Second {
		t.Fatalf("RateLimit-Reset %s (epoch seconds, now+48h) gave %s, want about 48h, as main", reset, got)
	}
}
