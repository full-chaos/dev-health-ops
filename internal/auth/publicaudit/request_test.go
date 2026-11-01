package publicaudit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// request_metadata is go-api's: ip_address, user_agent, request_id, each only when set, in that order, with Python
// spacing; request_id is the forwarded correlation id, else auth-service's own request id; nothing bound is nil.
func TestRequestMetadataIsGoAPIs(t *testing.T) {
	own := uuid.MustParse("6f2b1c1e-6a0e-4c43-9e0f-0b6c7a1d2e3f")
	full := clientaddr.WithUserAgent(clientaddr.With(context.Background(), netip.MustParseAddr("203.0.113.7")), "venue-oracle/1")
	for name, c := range map[string]struct {
		ctx  context.Context
		want string
	}{
		"nothing bound":    {context.Background(), ""},
		"forwarded id":     {WithForwardedRequestID(requestid.With(full, own), "venue-oracle-req-1"), `{"ip_address": "203.0.113.7", "user_agent": "venue-oracle/1", "request_id": "venue-oracle-req-1"}`},
		"own id":           {requestid.With(full, own), `{"ip_address": "203.0.113.7", "user_agent": "venue-oracle/1", "request_id": "6f2b1c1e-6a0e-4c43-9e0f-0b6c7a1d2e3f"}`},
		"no id":            {full, `{"ip_address": "203.0.113.7", "user_agent": "venue-oracle/1"}`},
		"address only":     {clientaddr.With(context.Background(), netip.MustParseAddr("2001:db8::1")), `{"ip_address": "2001:db8::1"}`},
		"id with no other": {WithForwardedRequestID(context.Background(), "r-1"), `{"request_id": "r-1"}`},
	} {
		got, err := RequestMetadata(c.ctx)
		if err != nil || string(got) != c.want {
			t.Errorf("%s: %q, %v; want %q", name, got, err, c.want)
		}
	}
}

// The middleware binds a non-empty X-Request-Id (go-api's own rule) and nothing for an absent one.
func TestMiddlewareBindsTheForwardedRequestID(t *testing.T) {
	for header, want := range map[string]string{"venue-oracle-req-1": "venue-oracle-req-1", "": ""} {
		var got string
		handler := Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = ForwardedRequestID(r.Context()) }))
		request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
		if header != "" {
			request.Header.Set(ForwardedRequestIDHeader, header)
		}
		handler.ServeHTTP(httptest.NewRecorder(), request)
		if got != want {
			t.Errorf("X-Request-Id %q bound %q, want %q", header, got, want)
		}
	}
}
