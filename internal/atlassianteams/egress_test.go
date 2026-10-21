package atlassianteams

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/egressguard"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

// The Jira site is a stored URL: the tenant_info lookup and the gateway reads dial only what the egress guard allows,
// and a refused site is an error, never the subdomain fallback.
func TestAStoredJiraSiteThatIsNotPublicIsRefused(t *testing.T) {
	t.Cleanup(egressguard.SetAllowForTest())
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer server.Close()
	tenant, err := url.Parse(strings.Replace(server.URL, "127.0.0.1", "localhost", 1))
	if err != nil {
		t.Fatal(err)
	}
	for name, doer := range map[string]*http.Client{
		"default doer":        nil,
		"httpguard.NewClient": httpguard.NewClient(5 * time.Second),
	} {
		var supplied interface {
			Do(*http.Request) (*http.Response, error)
		}
		if doer != nil {
			supplied = doer
		}
		id, err := ResolveCloudID(context.Background(), supplied, tenant)
		if !errors.Is(err, egressguard.ErrRefused) || id != "" {
			t.Fatalf("%s: ResolveCloudID = (%q, %v), want the egress refusal and no fallback id", name, id, err)
		}
	}
	response, err := GatewayHTTPClient(5 * time.Second).Get(server.URL + "/gateway/api/graphql")
	if err == nil {
		response.Body.Close()
	}
	if !errors.Is(err, egressguard.ErrRefused) {
		t.Fatalf("gateway GET = %v, want the egress refusal", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("the site received %d requests", hits.Load())
	}
}

func TestCompletePagesOnlyIsATransportWrapper(t *testing.T) {
	wrapped, ok := CompletePagesOnly(nil).(httpguard.TransportWrapper)
	if !ok || wrapped.UnwrapTransport() != httpguard.SharedTransport() {
		t.Fatal("CompletePagesOnly must expose its inner transport to the egress guard")
	}
	if _, ok := egressguard.Wrap(CompletePagesOnly(nil)).(completePages); !ok {
		t.Fatal("guarding CompletePagesOnly must keep the page check")
	}
}
