//go:build integration

package pgmigrate_test

import (
	"context"
	"strings"
	"testing"

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

// An audit row written under the schema before the widening (an envelope row naming its subject, an operator_direct
// row naming none) stays valid after it; a user_access_token row must name its subject; the down file refuses while a
// user_access_token row exists, since the table is append-only.
func TestTheRoutingAuditWideningKeepsEveryEarlierRowValid(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	head := len(chain)
	widening := -1
	for i, file := range chain {
		if strings.Contains(file.SQL, "ck_go_api_routing_audits_credential_class") {
			widening = i
		}
	}
	if widening < 0 {
		t.Fatal("no chain revision redefines ck_go_api_routing_audits_credential_class")
	}
	d := newDownInstance(t)
	uri := d.at(t, widening)
	conn := connect(t, uri)
	ctx := context.Background()
	subject := "prn_subject"
	for _, row := range []struct {
		class     string
		principal *string
	}{{"effective_principal_envelope", &subject}, {"operator_direct", nil}} {
		query, args := routingAuditRow(row.class, row.principal)
		if _, err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("the earlier schema refused a %s row: %v", row.class, err)
		}
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain[:head]); err != nil {
		t.Fatalf("upgrade over the earlier rows: %v", err)
	}
	for _, check := range []string{"ck_go_api_routing_audits_credential_class", "ck_go_api_routing_audits_principal_pairing"} {
		if _, err := conn.Exec(ctx, "ALTER TABLE go_api_routing_audits VALIDATE CONSTRAINT "+check); err != nil {
			t.Fatalf("an earlier row fails %s after the widening: %v", check, err)
		}
	}
	var earlier int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM go_api_routing_audits").Scan(&earlier); err != nil || earlier != 2 {
		t.Fatalf("earlier rows after the upgrade: %d (%v), want 2 untouched", earlier, err)
	}
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
	code, _, stderr := goDowngrade(t, uri, chain[widening-1].Revision)
	if code != cli.ExitFailure || !strings.Contains(stderr, "downgrade_failed") || !strings.Contains(stderr, "down "+chain[widening].Revision) {
		t.Fatalf("exit %d stderr %s: the down file must refuse while a user_access_token row exists", code, stderr)
	}
	var rows int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM go_api_routing_audits").Scan(&rows); err != nil || rows != 3 {
		t.Fatalf("audit rows after the refused downgrade: %d (%v), want the 3 rows untouched", rows, err)
	}
}
