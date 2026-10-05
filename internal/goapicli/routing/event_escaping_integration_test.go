//go:build integration

package routing

// CHAOS-7174: the structured one-line events of enable, disable and repoint
// print -recorded-by. A value carrying spaces and `key=value` text (control
// characters are refused earlier, by requireProvenance) must appear QUOTED, as
// one field, not as extra fields of the event. Each test plants such a value
// and reads the real event off stderr.

import (
	"strings"
	"testing"
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
	pool, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	seedClassRootWithExcludedShape(t, pool)
	server := queryAPIFor(t)
	_, stderrOut, err := captureVerb(t, classEnableArgs(server, dsn, "-allow-excluded", allowExcludedOp, "-recorded-by", eventPlant)...)
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

func TestRepointEventQuotesRecordedBy(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := queryAPIFor(t)
	// A decision that names a build that is NOT the running one, so repoint writes.
	insertClassDecision(t, pool, verbTestClassOperation, "canary", "0000000000000000000000000000000000000001")
	_, stderrOut, err := captureVerb(t, "repoint",
		"-registry-url", server.URL+"/registry", "-buildinfo-url", server.URL+"/buildinfo",
		"-postgres-uri", dsn,
		"-recorded-by", eventPlant, "-review-evidence", "why")
	if err != nil {
		t.Fatal(err)
	}
	assertQuotedPlant(t, stderrOut, "go_api_routing.repointed")
}
