package syncdispatchruntime

import (
	"errors"
	"fmt"
	"strings"
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

// TestDiscoveryErrorDetailSanitizesCredentialShapedText pins the
// codex-flagged gap (CHAOS-4575 review, P1): discoverErr can wrap a raw
// transport failure whose message embeds the request that failed --
// bridge.go's `do()` propagates the underlying net/http error verbatim,
// which can carry an Authorization header, a bearer token, or URL userinfo.
// discoveryErrorDetail persists into a durable, queryable result column, so
// it must run through the same sanitizer finalize_sync_run's error path
// already uses, not store discoverErr.Error() unchanged.
func TestDiscoveryErrorDetailSanitizesCredentialShapedText(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantRedact bool
	}{
		{
			name:       "bearer token",
			err:        errors.New("sync dispatch bridge request failed: Authorization: Bearer sk-abcdef0123456789"),
			wantRedact: true,
		},
		{
			name:       "url userinfo",
			err:        errors.New("dial tcp: connect to https://user:hunterpassword2@internal.example.com/reference-discovery"),
			wantRedact: true,
		},
		{
			name:       "ordinary status text is left alone",
			err:        fmt.Errorf("%w: status=500", ErrBridgeRequest),
			wantRedact: false,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			detail := discoveryErrorDetail(testCase.err)
			if testCase.wantRedact {
				if !strings.Contains(detail, "[REDACTED]") {
					t.Fatalf("discoveryErrorDetail(%v)=%q want a [REDACTED] marker", testCase.err, detail)
				}
				if strings.Contains(detail, "hunterpassword2") || strings.Contains(detail, "sk-abcdef0123456789") {
					t.Fatalf("discoveryErrorDetail(%v)=%q leaked the original secret", testCase.err, detail)
				}
			} else if strings.Contains(detail, "[REDACTED]") {
				t.Fatalf("discoveryErrorDetail(%v)=%q unexpectedly redacted", testCase.err, detail)
			}
		})
	}
}
