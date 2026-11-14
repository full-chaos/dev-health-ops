package server

import (
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// A proof-write allowlist read that cannot be made decided nothing about the org: it answers go-api's unavailable
// form (503), never the 403 that says the org is not on the list. No write runs either way.
func TestProofWriteAnswersUnavailableWhenTheAllowlistReadFails(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	verifier, _ := iaVerifier(t)
	executed := 0
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"m": true})
	mux.Register("m", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { executed++ }))
	handler := newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(pwMutation): "m"}, verifier, nil, nil, digest.KindMutation, newProofOrgAllowed(pool))

	rec := pwPost(handler, pwMutation, "org-1")
	got := faultAnswer{rec.Code, rec.Body.String(), rec.Header()}
	if problem := unavailableProblem(got); problem != "" {
		t.Fatalf("allowlist read failed: %s", problem)
	}
	if executed != 0 {
		t.Fatalf("resolver ran %d time(s), want 0", executed)
	}
}
