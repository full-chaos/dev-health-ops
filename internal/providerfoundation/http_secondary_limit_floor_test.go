package providerfoundation

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// penaltyCaptureGate records the penalty the client arms the shared gate with
// and cancels the request context at once, so the test never sleeps the retry wait.
type penaltyCaptureGate struct {
	penalty atomic.Int64
	cancel  context.CancelFunc
}

func (*penaltyCaptureGate) Wait(context.Context) (time.Duration, error) { return 0, nil }

func (g *penaltyCaptureGate) Penalize(_ context.Context, penalty time.Duration) error {
	g.penalty.Store(int64(penalty))
	g.cancel()
	return nil
}

// Per the GitHub REST rate-limit docs: wait at least one minute before retrying
// when no retry-after header and remaining != 0 (secondary limit). Repro: the
// client waits only its generic 1-30 s backoff and arms the gate with the same.
func TestSecondaryLimitWithoutRetryAfterWaitsAtLeastOneMinute(t *testing.T) {
	t.Parallel()
	body := `{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again."}`
	for _, status := range []int{http.StatusForbidden, http.StatusTooManyRequests} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
				if requests.Add(1) == 1 {
					return testHTTPResponse(request, status, http.Header{"X-Ratelimit-Remaining": {"42"}}, body), nil
				}
				return testHTTPResponse(request, http.StatusOK, http.Header{}, `{}`), nil
			}), DefaultRetryPolicy())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			gate := &penaltyCaptureGate{cancel: cancel}
			client.Gate = gate

			_, _ = client.Do(ctx, http.MethodGet, "/items", nil)

			got := time.Duration(gate.penalty.Load())
			if got < time.Minute {
				t.Fatalf("status %d secondary limit, no Retry-After, remaining 42: gate penalty = %s, want >= 1m", status, got)
			}
		})
	}
}
