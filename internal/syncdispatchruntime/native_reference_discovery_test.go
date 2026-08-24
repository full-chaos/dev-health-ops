package syncdispatchruntime

import (
	"errors"
	"fmt"
	"testing"
)

// TestIsRetryableDiscoveryErrorTreatsBridgeFailuresAsRetryable pins the
// codex-flagged gap (CHAOS-4175): a failure of the populate bridge call
// itself -- a connection refused, a 503, a deadline -- is a transport hop
// Python's original in-process populate step never had, so it never
// matches the ported message-substring markers. Before this fix such a
// failure was misclassified as permanent, terminalizing the whole sync run
// after one transient bridge blip instead of retrying with backoff.
func TestIsRetryableDiscoveryErrorTreatsBridgeFailuresAsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"bare ErrBridgeRequest", ErrBridgeRequest, true},
		{"connection refused wrapped as ErrBridgeRequest", fmt.Errorf("%w: dial tcp 127.0.0.1:8000: connect: connection refused", ErrBridgeRequest), true},
		{"non-2xx status wrapped as ErrBridgeRequest", fmt.Errorf("%w: status=503", ErrBridgeRequest), true},
		{"bare ErrInvalidBridge", ErrInvalidBridge, true},
		{"substring marker still matches", errors.New("request failed: rate limited"), true},
		{"unrelated error is not retryable", errors.New("boom"), false},
		{"nil is not retryable", nil, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := isRetryableDiscoveryError(testCase.err); got != testCase.want {
				t.Fatalf("isRetryableDiscoveryError(%v)=%v want=%v", testCase.err, got, testCase.want)
			}
		})
	}
}
