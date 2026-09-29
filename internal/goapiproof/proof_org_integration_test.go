//go:build integration

package goapiproof

// CHAOS-7096: AddProofOrg/RemoveProofOrg/ListProofOrgs against a real
// Postgres, schema hand-copied from alembic 0145 -- the same
// hand-copied-DDL-for-Go-test-speed pattern routing_audit_integration_
// test.go's routingauditschema.DDL already uses for go_api_routing_audits,
// rather than running the real Python migration chain from a Go test.

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const proofOrgDDL = `
CREATE TABLE go_api_proof_orgs (
	org_id TEXT PRIMARY KEY,
	added_by TEXT NOT NULL CHECK (char_length(added_by) BETWEEN 1 AND 128),
	reason TEXT NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 2000),
	added_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE go_api_proof_org_audits (
	id BIGSERIAL PRIMARY KEY,
	org_id TEXT NOT NULL,
	action TEXT NOT NULL CHECK (action IN ('add', 'remove')),
	credential_class TEXT NOT NULL CHECK (credential_class IN ('operator_direct')),
	recorded_by TEXT NOT NULL CHECK (char_length(recorded_by) BETWEEN 1 AND 128),
	reason TEXT NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 2000),
	recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_go_api_proof_org_audits_org_recorded ON go_api_proof_org_audits (org_id, recorded_at DESC);
`

func startProofOrgPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := startRegistryPostgres(t)
	if _, err := pool.Exec(context.Background(), proofOrgDDL); err != nil {
		t.Fatalf("create proof-org schema: %v", err)
	}
	return pool
}

func proofOrgAudits(t *testing.T, pool *pgxpool.Pool) []struct {
	orgID, action, credentialClass, recordedBy, reason string
} {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT org_id, action, credential_class, recorded_by, reason FROM go_api_proof_org_audits ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []struct {
		orgID, action, credentialClass, recordedBy, reason string
	}
	for rows.Next() {
		var r struct {
			orgID, action, credentialClass, recordedBy, reason string
		}
		if err := rows.Scan(&r.orgID, &r.action, &r.credentialClass, &r.recordedBy, &r.reason); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func TestAddProofOrgRefusesAnIncompleteRequestBeforeAnyWrite(t *testing.T) {
	pool := startProofOrgPostgres(t)
	for _, req := range []ProofOrgRequest{
		{OrgID: "", RecordedBy: "chris", ReviewEvidence: "why"},
		{OrgID: "org-1", RecordedBy: "", ReviewEvidence: "why"},
		{OrgID: "org-1", RecordedBy: "chris", ReviewEvidence: ""},
	} {
		if err := AddProofOrg(context.Background(), pool, req); err == nil {
			t.Fatalf("%+v: got nil, want ErrProofOrgRequestIncomplete", req)
		}
	}
	rows, err := ListProofOrgs(context.Background(), pool)
	if err != nil || len(rows) != 0 {
		t.Fatalf("rows=%v err=%v, want zero rows -- nothing should have been written", rows, err)
	}
	if audits := proofOrgAudits(t, pool); len(audits) != 0 {
		t.Fatalf("audits=%v, want zero -- an incomplete request must not reach the audit table either", audits)
	}
}

func TestAddProofOrgWritesTheLiveRowAndItsAuditRowTogether(t *testing.T) {
	pool := startProofOrgPostgres(t)
	if err := AddProofOrg(context.Background(), pool, ProofOrgRequest{
		OrgID: "org-1", RecordedBy: "chris", ReviewEvidence: "bootstrapping CHAOS-6098",
	}); err != nil {
		t.Fatal(err)
	}

	rows, err := ListProofOrgs(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].OrgID != "org-1" || rows[0].AddedBy != "chris" || rows[0].Reason != "bootstrapping CHAOS-6098" {
		t.Fatalf("got %+v, want one org-1 row with the actor/reason given", rows)
	}
	if rows[0].AddedAt.After(time.Now()) || rows[0].AddedAt.IsZero() {
		t.Fatalf("added_at = %v, want a real, past timestamp", rows[0].AddedAt)
	}

	audits := proofOrgAudits(t, pool)
	if len(audits) != 1 || audits[0].orgID != "org-1" || audits[0].action != "add" ||
		audits[0].credentialClass != "operator_direct" || audits[0].recordedBy != "chris" || audits[0].reason != "bootstrapping CHAOS-6098" {
		t.Fatalf("audit rows = %+v, want exactly one add/operator_direct/chris row", audits)
	}
}

func TestAddProofOrgTwiceUpsertsTheLiveRowButAuditsEachCall(t *testing.T) {
	pool := startProofOrgPostgres(t)
	ctx := context.Background()
	if err := AddProofOrg(ctx, pool, ProofOrgRequest{OrgID: "org-1", RecordedBy: "chris", ReviewEvidence: "first"}); err != nil {
		t.Fatal(err)
	}
	if err := AddProofOrg(ctx, pool, ProofOrgRequest{OrgID: "org-1", RecordedBy: "someone-else", ReviewEvidence: "second"}); err != nil {
		t.Fatal(err)
	}

	rows, err := ListProofOrgs(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].AddedBy != "someone-else" || rows[0].Reason != "second" {
		t.Fatalf("got %+v, want ONE row reflecting the SECOND call (upsert, not a duplicate)", rows)
	}
	if audits := proofOrgAudits(t, pool); len(audits) != 2 {
		t.Fatalf("audits=%v, want 2 -- each call is its own durable record even though the live row is one", audits)
	}
}

func TestRemoveProofOrgReportsWhetherARowActuallyExistedAndAuditsEitherWay(t *testing.T) {
	pool := startProofOrgPostgres(t)
	ctx := context.Background()

	removed, err := RemoveProofOrg(ctx, pool, ProofOrgRequest{OrgID: "never-added", RecordedBy: "chris", ReviewEvidence: "cleanup"})
	if err != nil {
		t.Fatal(err)
	}
	if removed {
		t.Fatal("removed=true for an org that was never added")
	}

	if err := AddProofOrg(ctx, pool, ProofOrgRequest{OrgID: "org-1", RecordedBy: "chris", ReviewEvidence: "add"}); err != nil {
		t.Fatal(err)
	}
	removed, err = RemoveProofOrg(ctx, pool, ProofOrgRequest{OrgID: "org-1", RecordedBy: "chris", ReviewEvidence: "revoke"})
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("removed=false for an org that WAS on the allowlist")
	}
	if rows, err := ListProofOrgs(ctx, pool); err != nil || len(rows) != 0 {
		t.Fatalf("rows=%v err=%v, want empty after removal", rows, err)
	}

	// Three calls were made above, in order: remove(never-added) [no live
	// row, still audited], add(org-1), remove(org-1) -- every call gets
	// its own audit row regardless of whether the live table changed.
	audits := proofOrgAudits(t, pool)
	if len(audits) != 3 {
		t.Fatalf("audits=%v (%d rows), want 3: remove(never-added), add(org-1), remove(org-1)", audits, len(audits))
	}
}
