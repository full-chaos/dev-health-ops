//go:build integration

package routing

// CHAOS-7174: the structured one-line events of enable, disable and repoint
// print -recorded-by. A value carrying spaces and `key=value` text (control
// characters are refused earlier, by requireProvenance) must appear QUOTED, as
// one field, not as extra fields of the event. Each test plants such a value
// and reads the real event off stderr.

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
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
	digest := "9999999999999999999999999999999999999999999999999999999999999999"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})
	_, stderrOut, err := captureVerb(t, enableArgs(server, dsn, catalogPath, "-recorded-by", eventPlant)...)
	if err != nil {
		t.Fatal(err)
	}
	assertQuotedPlant(t, stderrOut, "go_api_routing.enabled")
}

func TestDisableEventQuotesRecordedBy(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	digest := "9999999999999999999999999999999999999999999999999999999999999999"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	insertRowAtOperation(t, pool, localSchemaDigest(), digest, verbTestOperation, "canary", verbTestBuild)
	_, stderrOut, err := captureVerb(t, "disable", "-postgres-uri", dsn, "-catalog", catalogPath,
		"-operations", verbTestOperation, "-mode", "python", "-apply",
		"-recorded-by", eventPlant, "-review-evidence", "why")
	if err != nil {
		t.Fatal(err)
	}
	assertQuotedPlant(t, stderrOut, "go_api_routing.disabled")
}

func TestRepointEventQuotesRecordedBy(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	digest := "9999999999999999999999999999999999999999999999999999999999999999"
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})
	// A row that names a build that is NOT the running one, so repoint writes.
	insertRowAtOperation(t, pool, localSchemaDigest(), digest, verbTestOperation, "canary", "0000000000000000000000000000000000000001")
	_, stderrOut, err := captureVerb(t, "repoint",
		"-registry-url", server.URL+"/registry", "-buildinfo-url", server.URL+"/buildinfo",
		"-postgres-uri", dsn,
		"-recorded-by", eventPlant, "-review-evidence", "why")
	if err != nil {
		t.Fatal(err)
	}
	assertQuotedPlant(t, stderrOut, "go_api_routing.repointed")
}

func insertRowAtOperation(t *testing.T, pool *pgxpool.Pool, schema, doc, op, mode, build string) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, schema, doc, op, build); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO go_api_routing_state
		(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1,$2,$3,$4,'go',$5,100,'pre-existing decision','operator')`, schema, doc, op, build, mode); err != nil {
		t.Fatal(err)
	}
}
