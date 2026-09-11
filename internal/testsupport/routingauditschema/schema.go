// Package routingauditschema creates the go_api_routing_audits Postgres
// fixture table that internal/goapiproof's integration tests and
// cmd/go-api-routing's end-to-end verb tests share.
//
// ONE copy, for the reason registryschema exists: the alternative was a
// second hand-kept DDL in the cmd package, drifting from this one with
// nothing to notice.
package routingauditschema

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DDL mirrors alembic 0130, which creates go_api_routing_audits.
//
// The constraints are NOT trimmed to "what the test needs", for the same
// reason registryschema.DDL's are not: the action and credential-class
// CHECKs are the DATABASE-level reason a routing write cannot be recorded
// under a verb or a credential class nobody authorized, and the pairing
// CHECK is the reason "a verified credential named this subject" cannot
// be confused with "no credential was presented". A test against a
// relaxed schema would pass while the real one rejected every write.
//
// The drift check for this mirror lives in PYTHON
// (tests/test_0130_go_api_routing_audits_migration.py), deliberately, for
// the reason registryschema.DDL's own comment gives: reading the alembic
// files FROM a Go test makes them inputs to the Go workflow, and go.yml's
// path filters do not cover src/dev_health_ops/alembic/versions -- so a
// PR changing only a migration would satisfy go-quality vacuously.
const DDL = `
CREATE TABLE go_api_routing_audits (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	correlation_id UUID NOT NULL,
	action TEXT NOT NULL,
	credential_class TEXT NOT NULL,
	principal_id TEXT,
	recorded_by TEXT NOT NULL,
	review_evidence TEXT NOT NULL,
	schema_digest TEXT NOT NULL,
	document_digest TEXT NOT NULL,
	selected_operation TEXT NOT NULL,
	candidate_build_before TEXT,
	candidate_build_after TEXT NOT NULL,
	mode_before TEXT,
	mode_after TEXT NOT NULL,
	recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	CONSTRAINT ck_go_api_routing_audits_action
		CHECK (action IN ('enable', 'disable', 'repoint')),
	CONSTRAINT ck_go_api_routing_audits_credential_class
		CHECK (credential_class IN ('effective_principal_envelope', 'operator_direct')),
	CONSTRAINT ck_go_api_routing_audits_principal_pairing
		CHECK ((credential_class = 'effective_principal_envelope') = (principal_id IS NOT NULL)),
	CONSTRAINT ck_go_api_routing_audits_mode_before
		CHECK (mode_before IS NULL OR mode_before IN ('python', 'shadow', 'canary', 'primary', 'disabled')),
	CONSTRAINT ck_go_api_routing_audits_mode_after
		CHECK (mode_after IN ('python', 'shadow', 'canary', 'primary', 'disabled')),
	CONSTRAINT ck_go_api_routing_audits_review_evidence_bounded
		CHECK (char_length(review_evidence) BETWEEN 1 AND 2000),
	CONSTRAINT ck_go_api_routing_audits_recorded_by_bounded
		CHECK (char_length(recorded_by) BETWEEN 1 AND 128)
);
CREATE INDEX ix_go_api_routing_audits_correlation ON go_api_routing_audits (correlation_id);
CREATE INDEX ix_go_api_routing_audits_operation_recorded
	ON go_api_routing_audits (schema_digest, selected_operation, recorded_at DESC);
`

// Create builds the audit table in a database that already has the
// registry schema (registryschema.Create). Not merged into that DDL: the
// two tables are pinned by two separate Python drift-checks against two
// separate migrations, and merging them would make an audit-only PR touch
// a table the registry drift-check has no reason to know about.
func Create(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, DDL); err != nil {
		return fmt.Errorf("routingauditschema: create audit schema: %w", err)
	}
	return nil
}
