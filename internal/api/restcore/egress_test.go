package restcore

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/egressguard"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

// A stored GitHub or GitLab base URL that is not public is refused at connect by the REST core (the repository listing
// of the credential and sync admin routes), whatever client the caller supplied, and is not retried.
func TestCoreGetRefusesANonPublicStoredURL(t *testing.T) {
	t.Cleanup(egressguard.SetAllowForTest())
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer server.Close()
	for name, supplied := range map[string]*http.Client{
		"default client":      nil,
		"httpguard.NewClient": httpguard.NewClient(5 * time.Second),
		"no transport":        {Timeout: 5 * time.Second},
	} {
		var sleeps atomic.Int32
		core := Core{Provider: "gitlab", HTTP: supplied, Headers: map[string]string{"PRIVATE-TOKEN": "secret-token"}, MaxRetries: 3,
			Sleep: func(context.Context, time.Duration) error { sleeps.Add(1); return nil }}
		_, err := core.Get(context.Background(), strings.Replace(server.URL, "127.0.0.1", "localhost", 1)+"/api/v4/projects", "list_projects")
		// The core keeps no cause (TestAnErrorOfTheCoreHasNoCauseToUnwrap): the class names the refusal.
		var typed *Error
		if !errors.As(err, &typed) || typed.Class != "TransportError" || !strings.HasSuffix(typed.Message, ": egress_refused") || strings.Contains(typed.Message, "localhost") {
			t.Fatalf("%s: Get = %v, want the egress_refused transport error without the host", name, err)
		}
		if sleeps.Load() != 0 {
			t.Fatalf("%s: %d retry waits, want none", name, sleeps.Load())
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("the server received %d requests", hits.Load())
	}
}
