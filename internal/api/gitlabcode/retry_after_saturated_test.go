package gitlabcode

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/restcore"
)

// A Retry-After the duration cannot hold saturates (ParseRetryAfter), is past the honour ceiling, and so is answered at
// once on every CPU: no sleep, one request, the typed rate-limit error carrying the saturated wait.
func TestAGitLabSaturatedRetryAfterIsNotSleptAndCarriesTheMaxDuration(t *testing.T) {
	for _, code := range []int{429, 403} {
		for _, raw := range []string{"1e30", "+Inf", "9223372037"} {
			t.Run(raw+"/"+strconv.Itoa(code), func(t *testing.T) {
				transport := &scriptedTransport{responses: repeat(5, status(code, `{}`, [2]string{"Retry-After", raw}))}
				var sleeps []time.Duration
				_, err := honourClient(transport, &sleeps).ListGroupProjects(context.Background(), "grp")
				var typed *restcore.Error
				if !errors.As(err, &typed) || typed.Class != "RateLimitException" {
					t.Fatalf("status %d Retry-After %q: err = %v, want a RateLimitException", code, raw, err)
				}
				if typed.RetryAfter != time.Duration(math.MaxInt64) {
					t.Fatalf("status %d Retry-After %q: RetryAfter = %d ns, want %d ns", code, raw, int64(typed.RetryAfter), int64(math.MaxInt64))
				}
				if len(sleeps) != 0 {
					t.Fatalf("status %d Retry-After %q: WANT 0 sleeps, got %v", code, raw, sleeps)
				}
				if len(transport.seen) != 1 {
					t.Fatalf("status %d Retry-After %q: WANT 1 request, got %d", code, raw, len(transport.seen))
				}
			})
		}
	}
}
