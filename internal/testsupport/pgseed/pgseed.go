// Package pgseed inserts rows into the REAL (migrated) Postgres schema for Go
// integration tests, with every NOT NULL column and foreign key the real tables
// carry filled with a valid default. It exists so a test seeds an organization,
// a feature flag or a license in one call instead of hand-writing an INSERT
// whose column list drifts from the migrations (CHAOS-6769, Trap #412); pair it
// with pgschema.Apply. Each helper takes the identifying and behaviour-bearing
// fields and fails the test on any error.
package pgseed

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func exec(ctx context.Context, t testing.TB, pool *pgxpool.Pool, what, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("pgseed %s: %v", what, err)
	}
}

// Org inserts an organization with the given uuid id and tier.
func Org(ctx context.Context, t testing.TB, pool *pgxpool.Pool, id, tier string) {
	t.Helper()
	exec(ctx, t, pool, "organization", `
INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1::uuid, 'org-' || $1::text, 'org ' || $1::text, $2, true, now(), now())`, id, tier)
}

// FeatureFlag inserts a feature flag row.
func FeatureFlag(ctx context.Context, t testing.TB, pool *pgxpool.Pool, id, key, minTier string, enabled bool) {
	t.Helper()
	exec(ctx, t, pool, "feature flag "+key, `
INSERT INTO feature_flags (id, key, name, min_tier, is_enabled, created_at, updated_at)
VALUES ($1::uuid, $2, $2, $3, $4, now(), now())`, id, key, minTier, enabled)
}

// OrgOverride inserts an org_feature_overrides row for an existing org and feature.
func OrgOverride(ctx context.Context, t testing.TB, pool *pgxpool.Pool, orgID, featureID string, enabled bool) {
	t.Helper()
	exec(ctx, t, pool, "org feature override", `
INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, created_at, updated_at)
VALUES (gen_random_uuid(), $1::uuid, $2::uuid, $3, now(), now())`, orgID, featureID, enabled)
}

// OrgLicense inserts an org_licenses row; featuresOverrideJSON is the raw JSON text of
// features_override.
func OrgLicense(ctx context.Context, t testing.TB, pool *pgxpool.Pool, orgID, tier, featuresOverrideJSON string) {
	t.Helper()
	exec(ctx, t, pool, "org license", `
INSERT INTO org_licenses (id, org_id, tier, features_override, created_at, updated_at)
VALUES (gen_random_uuid(), $1::uuid, $2, $3::json, now(), now())`, orgID, tier, featuresOverrideJSON)
}

// Setting inserts a settings row for orgID (text: settings.org_id is not a uuid column).
func Setting(ctx context.Context, t testing.TB, pool *pgxpool.Pool, orgID, category, key, value string, encrypted bool) {
	t.Helper()
	exec(ctx, t, pool, "setting "+category+"/"+key, `
INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, now(), now())`, orgID, category, key, value, encrypted)
}

// Integration inserts an active integration for orgID.
func Integration(ctx context.Context, t testing.TB, pool *pgxpool.Pool, id, orgID, provider string) {
	t.Helper()
	exec(ctx, t, pool, "integration", `
INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, $3, 'integration-' || $1::text, '{}'::json, true, now(), now())`, id, orgID, provider)
}

// IntegrationSource inserts an enabled repository source under an existing integration.
func IntegrationSource(ctx context.Context, t testing.TB, pool *pgxpool.Pool, id, orgID, integrationID, provider, externalID string) {
	t.Helper()
	exec(ctx, t, pool, "integration source", `
INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name,
	metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, $4, 'repository', $5, $5, $5, '{}'::json, true, now(), now())`,
		id, orgID, integrationID, provider, externalID)
}

// Credential inserts an active integration credential (no secret payload) for orgID.
func Credential(ctx context.Context, t testing.TB, pool *pgxpool.Pool, id, orgID, provider string) {
	t.Helper()
	exec(ctx, t, pool, "integration credential", `
INSERT INTO integration_credentials (id, org_id, provider, name, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, $3, 'credential-' || $1::text, true, now(), now())`, id, orgID, provider)
}

// RoutingState upserts the go_api_routing_state row for one (schema digest, document digest,
// operation) triple in the given mode, registering the candidate build the row's foreign key needs.
func RoutingState(ctx context.Context, t testing.TB, pool *pgxpool.Pool, schemaDigest, documentDigest, operation, mode string) {
	t.Helper()
	exec(ctx, t, pool, "go_api candidate build", `
INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
VALUES ($1, $2, $3, 'test-build') ON CONFLICT DO NOTHING`, schemaDigest, documentDigest, operation)
	exec(ctx, t, pool, "go_api routing state", `
INSERT INTO go_api_routing_state (schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode)
VALUES ($1, $2, $3, 'test-build', 'python', $4)
ON CONFLICT (schema_digest, document_digest, selected_operation) DO UPDATE SET mode = $4`,
		schemaDigest, documentDigest, operation, mode)
}

// SetFeatureFlag makes key exist with exactly this floor and enabled state, replacing the row the
// migrations already register for it (they register the shipped flags). Use it where a test needs a
// specific state of a shipped flag; use FeatureFlag for a flag the migrations do not register.
func SetFeatureFlag(ctx context.Context, t testing.TB, pool *pgxpool.Pool, id, key, minTier string, enabled bool) {
	t.Helper()
	exec(ctx, t, pool, "reset feature flag "+key, `DELETE FROM feature_flags WHERE key = $1`, key)
	FeatureFlag(ctx, t, pool, id, key, minTier, enabled)
}

// User inserts an active local user (email derived from the id).
func User(ctx context.Context, t testing.TB, pool *pgxpool.Pool, id string) {
	t.Helper()
	exec(ctx, t, pool, "user", `
INSERT INTO users (id, email, created_at, updated_at) VALUES ($1::uuid, $1::text || '@example.test', now(), now())`, id)
}

// DevConversation inserts an Ask Dev conversation (retention 30 days, empty scope) for an
// existing organization and user.
func DevConversation(ctx context.Context, t testing.TB, pool *pgxpool.Pool, id, orgID, userID string) {
	t.Helper()
	exec(ctx, t, pool, "dev conversation", `
INSERT INTO dev_conversations (id, org_id, user_id, current_scope, retention_days)
VALUES ($1::uuid, $2::uuid, $3::uuid, '{}'::jsonb, 30)`, id, orgID, userID)
}

// WorkerJobOutbox inserts a worker_job_outbox row with every NOT NULL column and check constraint
// satisfied for the given status (pending, claimed, delivered or dead): a claimed row carries its
// claim, a delivered row its River job id and delivery time.
func WorkerJobOutbox(ctx context.Context, t testing.TB, pool *pgxpool.Pool, dedupeKey, jobKind, status string) {
	t.Helper()
	exec(ctx, t, pool, "worker job outbox "+status, `
INSERT INTO worker_job_outbox (id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority,
	max_attempts, scheduled_at, status, claim_token, claimed_at, claim_expires_at, attempt_count, next_attempt_at,
	river_job_id, delivered_at, created_at, updated_at)
VALUES (gen_random_uuid(), $1::text, $2::text, 1, '{}'::json, 'sha256:' || repeat('0', 64), 'default', 2, 5, now(), $3::text,
	CASE WHEN $3::text = 'claimed' THEN gen_random_uuid() END,
	CASE WHEN $3::text = 'claimed' THEN now() END,
	CASE WHEN $3::text = 'claimed' THEN now() + interval '1 hour' END,
	0, now(),
	CASE WHEN $3::text = 'delivered' THEN abs(hashtextextended($1::text, 0)) END,
	CASE WHEN $3::text = 'delivered' THEN now() END,
	now(), now())`, dedupeKey, jobKind, status)
}
