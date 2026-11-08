package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// C5. GET /buildinfo maps to org.read, which carries no platform_admin flag. The route verifies the internal envelope
// and never asks the decision point, so a superuser's envelope gets the legacy 200 with every query-api cell on and a
// PDP that refuses everything. A change that makes /buildinfo ask the PDP turns this red: org.read then needs its
// legacy cell and the flag.
func TestBuildInfoNeverAsksThePDP(t *testing.T) {
	quietStdLog(t)
	verifier, priv := iaVerifier(t)
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: ecOrg.String(), Role: "viewer", IsSuperuser: true, IsSuperuserVerified: true})
	srv := pdptest.New(t)
	srv.Deny("no_membership")
	serve := func(mode string) (int, string) {
		cfg := pdp.Config{Mode: mode, Switch: authzenforce.Parse(authzenforce.QueryAPI, strings.Join(authzenforce.Namespaces(), ",")),
			Meter: sdkmetric.NewMeterProvider().Meter("test"), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		client, err := pep.New(pep.Config{BaseURL: srv.URL, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: pdp.ClientService, Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		cfg.Decider = client
		gate := pdp.New(cfg)
		mux := http.NewServeMux()
		mux.HandleFunc("/buildinfo", newBuildInfoHandler(verifier, nil, nil))
		req := httptest.NewRequest(http.MethodGet, "/buildinfo", nil)
		req.Header.Set("Authorization", "Bearer "+envelope)
		rec := httptest.NewRecorder()
		planeHandler(mux, gate).ServeHTTP(rec, req)
		gate.Wait()
		return rec.Code, strings.TrimSpace(rec.Body.String())
	}
	legacyStatus, legacyBody := serve(pdp.ModeOff)
	enforcedStatus, enforcedBody := serve(pdp.ModeEnforce)
	if legacyStatus != http.StatusOK {
		t.Fatalf("legacy: %d %s, want 200", legacyStatus, legacyBody)
	}
	if enforcedStatus != legacyStatus || enforcedBody != legacyBody {
		t.Errorf("enforced %d %s, legacy %d %s; want the legacy answer", enforcedStatus, enforcedBody, legacyStatus, legacyBody)
	}
	if got := srv.Requests(); len(got) != 0 {
		t.Errorf("/buildinfo asked the PDP: %+v", got)
	}
}
