package authprovision_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// Both cores (the session credential and the workload token) answer a redirect without following it: the second origin
// sees no request, and the call fails as unavailable.
func TestTheCoresNeverFollowARedirectToAnotherOrigin(t *testing.T) {
	t.Run("session", func(t *testing.T) {
		probe := redirectprobe.New(t)
		base := mustParseURL(t, probe.Base.URL)
		client, err := authprovision.New(base, goldenToken, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Deprovision(context.Background(), uuid.New(), uuid.New()); err == nil {
			t.Fatal("a redirect answer was taken for success")
		}
		probe.Assert(t)
	})
	t.Run("workload", func(t *testing.T) {
		probe := redirectprobe.New(t)
		exchange := newGoldenServer(t)
		exchange.exchange = issuedAnswer(mintWorkloadToken(t))
		client, err := authprovision.NewWorkload(mustParseURL(t, probe.Base.URL), newWorkloadClient(t, exchange), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Deprovision(context.Background(), uuid.New(), uuid.New()); err == nil {
			t.Fatal("a redirect answer was taken for success")
		}
		probe.Assert(t)
	})
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
