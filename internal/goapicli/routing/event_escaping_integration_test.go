//go:build integration

package routing

// CHAOS-7174: the structured one-line events of enable and disable
// print -recorded-by. A value carrying spaces and `key=value` text (control
// characters are refused earlier, by requireProvenance) must appear QUOTED, as
// one field, not as extra fields of the event. Each test plants such a value
// and reads the real event off stderr.

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

const eventPlant = "lane operation=forged mode_after=primary"

func assertQuotedPlant(t *testing.T, stderrOut, event string) {
	t.Helper()
	line := ""
	for _, l := range strings.Split(stderrOut, "\n") {
		if strings.HasPrefix(l, event+" ") {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("no %s event on stderr:\n%s", event, stderrOut)
	}
	if !strings.HasSuffix(line, `recorded_by="`+eventPlant+`"`) {
		t.Fatalf("recorded_by must be quoted as ONE field, got:\n%s", line)
	}
	if strings.Contains(strings.TrimSuffix(line, `"`+eventPlant+`"`), "forged") {
		t.Fatalf("the plant leaked outside its quotes:\n%s", line)
	}
}

func TestEnableEventQuotesRecordedBy(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := queryAPIFor(t)
	_, stderrOut, err := captureVerb(t, classEnableArgs(server, dsn, "-recorded-by", eventPlant)...)
	if err != nil {
		t.Fatal(err)
	}
	assertQuotedPlant(t, stderrOut, "go_api_routing.enabled")
}

func TestDisableEventQuotesRecordedBy(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	insertClassDecision(t, pool, verbTestClassOperation, "canary", verbTestBuild)
	_, stderrOut, err := captureVerb(t, "disable", "-postgres-uri", dsn,
		"-operations", verbTestClassOperation, "-mode", "python", "-apply",
		"-recorded-by", eventPlant, "-review-evidence", "why")
	if err != nil {
		t.Fatal(err)
	}
	assertQuotedPlant(t, stderrOut, "go_api_routing.disabled")
}

func classEnableArgs(server *httptest.Server, dsn string, extra ...string) []string {
	return append([]string{
		"enable", "-operations", mcpclass.Operation("analytics"), "-mode", "canary",
		"-registry-url", server.URL + "/registry", "-buildinfo-url", server.URL + "/buildinfo",
		"-postgres-uri", dsn, "-recorded-by", "lane-routing-verbs", "-review-evidence", "the enable fixture",
	}, extra...)
}

func classRowModeOf(t *testing.T, pool *pgxpool.Pool, op string) string {
	t.Helper()
	var mode string
	if err := pool.QueryRow(context.Background(), `SELECT mode FROM go_api_class_decision WHERE operation = $1`, op).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	return mode
}

// seedClassRoot seeds the analytics root as a shadow decision: the row `enable` then lights, with no receipt.
func seedClassRoot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	op := mcpclass.Operation("analytics")
	if _, err := goapiproof.Seed(context.Background(), pool, goapiproof.SeedRequest{
		SchemaDigest: localSchemaDigest(), RunningBuild: verbTestBuild, Operations: []string{op},
		DocumentDigest: mcpclass.Digests(), RecordedBy: "test", ReviewEvidence: "class row", PrincipalID: verbTestPrincipalID,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return op
}
