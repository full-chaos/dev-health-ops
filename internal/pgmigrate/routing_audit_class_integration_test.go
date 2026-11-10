//go:build integration

package pgmigrate_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// routingAuditRow is one go_api_routing_audits insert with every NOT NULL column set; principal nil is SQL NULL.
func routingAuditRow(class string, principal *string) (string, []any) {
	return `INSERT INTO go_api_routing_audits (correlation_id, action, credential_class, principal_id, recorded_by,
		review_evidence, schema_digest, document_digest, selected_operation, candidate_build_after, mode_after)
		VALUES (gen_random_uuid(), 'enable', $1, $2, 'cell', 'routing audit class cell', 'sha256:s', 'sha256:d', 'mcp:hotspots', 'b', 'canary')`,
		[]any{class, principal}
}

// routingAuditWidening is the chain index of the revision that redefines the routing audit class CHECK.
func routingAuditWidening(t *testing.T, chain []pgmigrate.ChainFile) int {
	t.Helper()
	widening := -1
	for i, file := range chain {
		if strings.Contains(file.SQL, "ck_go_api_routing_audits_credential_class") {
			widening = i
		}
	}
	if widening < 1 {
		t.Fatal("no chain revision above the first redefines ck_go_api_routing_audits_credential_class")
	}
	return widening
}

// routingAuditChecks is pg_get_constraintdef of the two routing audit CHECKs, by name.
func routingAuditChecks(t *testing.T, conn *pgx.Conn) map[string]string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `SELECT con.conname, pg_get_constraintdef(con.oid) FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid WHERE c.relname = 'go_api_routing_audits'
		AND con.conname IN ('ck_go_api_routing_audits_credential_class', 'ck_go_api_routing_audits_principal_pairing')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			t.Fatal(err)
		}
		out[name] = definition
	}
	if len(out) != 2 {
		t.Fatalf("routing audit CHECKs %v, want both", out)
	}
	return out
}

func routingAuditCount(t *testing.T, conn *pgx.Conn) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM go_api_routing_audits").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// writeEarlierRows writes, under the schema before the widening, an envelope row naming its subject and an
// operator_direct row naming none.
func writeEarlierRows(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	subject := "prn_subject"
	for _, row := range []struct {
		class     string
		principal *string
	}{{"effective_principal_envelope", &subject}, {"operator_direct", nil}} {
		query, args := routingAuditRow(row.class, row.principal)
		if _, err := conn.Exec(context.Background(), query, args...); err != nil {
			t.Fatalf("the earlier schema refused a %s row: %v", row.class, err)
		}
	}
}

// An audit row written under the schema before the widening stays valid after it; a user_access_token row must name
// its subject; the down file refuses, naming the row count and changing nothing, while a user_access_token row exists.
func TestTheRoutingAuditWideningKeepsEveryEarlierRowValid(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	widening := routingAuditWidening(t, chain)
	d := newDownInstance(t)
	uri := d.at(t, widening)
	conn := connect(t, uri)
	ctx := context.Background()
	writeEarlierRows(t, conn)
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("upgrade over the earlier rows: %v", err)
	}
	for _, check := range []string{"ck_go_api_routing_audits_credential_class", "ck_go_api_routing_audits_principal_pairing"} {
		if _, err := conn.Exec(ctx, "ALTER TABLE go_api_routing_audits VALIDATE CONSTRAINT "+check); err != nil {
			t.Fatalf("an earlier row fails %s after the widening: %v", check, err)
		}
	}
	if n := routingAuditCount(t, conn); n != 2 {
		t.Fatalf("earlier rows after the upgrade: %d, want 2 untouched", n)
	}
	subject := "prn_subject"
	for _, row := range []struct {
		name      string
		class     string
		principal *string
		admitted  bool
	}{
		{"user access token naming its subject", "user_access_token", &subject, true},
		{"user access token naming none", "user_access_token", nil, false},
		{"operator_direct naming a subject", "operator_direct", &subject, false},
		{"a class outside the set", "edge_token", &subject, false},
	} {
		query, args := routingAuditRow(row.class, row.principal)
		_, err := conn.Exec(ctx, query, args...)
		if (err == nil) != row.admitted {
			t.Errorf("%s: insert error %v, want admitted=%v", row.name, err, row.admitted)
		}
	}
	before := schemaShape(t, uri)
	code, _, stderr := goDowngrade(t, uri, chain[widening-1].Revision)
	if code != cli.ExitFailure || !strings.Contains(stderr, "downgrade_failed") || !strings.Contains(stderr, "down "+chain[widening].Revision) ||
		!strings.Contains(stderr, "holds 1 user_access_token row(s)") {
		t.Fatalf("exit %d stderr %s: the down file must refuse, naming the row count, while a user_access_token row exists", code, stderr)
	}
	if got := schemaShape(t, uri); got != before {
		t.Fatalf("the refused downgrade changed the schema:\n%s", shapeDiff(before, got))
	}
	if n := routingAuditCount(t, conn); n != 3 {
		t.Fatalf("audit rows after the refused downgrade: %d, want the 3 rows untouched", n)
	}
}

// With no user_access_token row the down file restores the two CHECKs exactly as the revision before the widening
// defines them, and the earlier rows stay.
func TestTheRoutingAuditWideningDownRestoresTheEarlierChecks(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	widening := routingAuditWidening(t, chain)
	d := newDownInstance(t)
	want := routingAuditChecks(t, connect(t, d.at(t, widening)))
	uri := d.at(t, widening)
	conn := connect(t, uri)
	writeEarlierRows(t, conn)
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(context.Background(), conn, baseline, chain); err != nil {
		t.Fatal(err)
	}
	if got := routingAuditChecks(t, conn); got["ck_go_api_routing_audits_credential_class"] == want["ck_go_api_routing_audits_credential_class"] {
		t.Fatalf("the widening left the class CHECK as it was: %v", got)
	}
	code, _, stderr := goDowngrade(t, uri, chain[widening-1].Revision)
	if code != cli.ExitOK {
		t.Fatalf("downgrade with no user_access_token row: exit %d stderr %s", code, stderr)
	}
	if got := routingAuditChecks(t, conn); got["ck_go_api_routing_audits_credential_class"] != want["ck_go_api_routing_audits_credential_class"] ||
		got["ck_go_api_routing_audits_principal_pairing"] != want["ck_go_api_routing_audits_principal_pairing"] {
		t.Fatalf("after the downgrade the CHECKs are %v, want the earlier %v", got, want)
	}
	if n := routingAuditCount(t, conn); n != 2 {
		t.Fatalf("earlier rows after the downgrade: %d, want 2", n)
	}
}
