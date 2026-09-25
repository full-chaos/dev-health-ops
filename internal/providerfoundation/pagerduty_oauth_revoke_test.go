package providerfoundation

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type revokeDoerFunc func(*http.Request) (*http.Response, error)

func (f revokeDoerFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

// raise_for_status: a revoke is a failure on anything but a 2xx, a redirect
// included (ClassifyHTTP alone answers nil for a 3xx).
func TestRevokePagerDutyOAuthTokenRefusesAnyNon2xxAnswer(t *testing.T) {
	for status, wantErr := range map[int]bool{200: false, 204: false, 199: true, 301: true, 302: true, 307: true, 308: true, 400: true, 500: true} {
		doer := revokeDoerFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
		})
		err := RevokePagerDutyOAuthToken(context.Background(), doer, PagerDutyRevokeConfig{ClientID: "id"}, "tok")
		if (err != nil) != wantErr {
			t.Errorf("status %d: err = %v, want error %v", status, err, wantErr)
		}
	}
}
