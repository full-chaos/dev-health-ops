//go:build integration

package riverstore_test

import (
	"context"
	"testing"
	"time"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestPostureManifestLockstepAppliedStampAndComparisonBothDirections is
// CHAOS-5437's proof of the whole loop: (1) a base-branch-shaped migrate run
// (PostureManifestDigest unset) applies NO tracking at all, proving the gap
// this ticket closes is real; (2) go-worker-migrate (ApplyPinnedMigrations)
// stamps an applied digest; (3) postgres.CheckPostureManifestLockstep reads
// it back correctly in BOTH directions -- current, and superseded.
//
// It deliberately does not touch postgres.DomainPosture()/QueuePosture()/
// CoordinatorPosture() at all: PostureManifestDigest values here are synthetic
// literals, because the mechanism under test (the table, the upsert, the
// two-query comparison) does not depend on what a digest MEANS, only on how
// migrate writes it and how the check reads it back.
func TestPostureManifestLockstepAppliedStampAndComparisonBothDirections(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, instance)

	domainRole, err := containers.RoleName("posture_lockstep_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("posture_lockstep_queue", instance)
	if err != nil {
		t.Fatal(err)
	}

	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	defer containers.DropRole(adminPool, domainRole, t.Logf)
	defer containers.DropRole(adminPool, queueRole, t.Logf)
	createRuntimeRoles(t, ctx, adminPool, domainRole, queueRole)

	// Minimal fixture: only the tables ApplyPinnedMigrations' domain grant
	// block guards with to_regclass, so this test's SCHEMA is self-contained
	// rather than reusing the large fixture in migrate_integration_test.go
	// (a mismatch there is that file's own venue-coverage concern, not this
	// one's).
	for _, statement := range []string{
		"CREATE TABLE public.alembic_version (version_num varchar(32) PRIMARY KEY)",
	} {
		if _, err := adminPool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	// --- RED PROOF: every caller before CHAOS-5437 (every integration test
	// in this package, and go-worker-migrate before this ticket) builds
	// MigrationOptions with PostureManifestDigest left at its zero value.
	// Prove that path applies NO tracking at all -- not "tracks it but
	// compares wrong", genuinely nothing -- so a lockstep check against this
	// database structurally cannot detect ANY binary as stale, no matter
	// which digest it declares. This is the exact gap CHAOS-5437 closes: the
	// 2026-09-07 incident's stale binaries had no mechanism to consult here,
	// not merely a mechanism that answered incorrectly.
	baseBranchOptions := riverstore.MigrationOptions{Schema: "river", DomainRole: domainRole, QueueRole: queueRole}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, baseBranchOptions); err != nil {
		t.Fatalf("ApplyPinnedMigrations() (no PostureManifestDigest, base-branch shape) error = %v", err)
	}
	var trackingTableExists bool
	if err := adminPool.QueryRow(ctx, "SELECT to_regclass('public.worker_posture_manifest_applied') IS NOT NULL").Scan(&trackingTableExists); err != nil {
		t.Fatal(err)
	}
	if trackingTableExists {
		t.Fatal("worker_posture_manifest_applied exists after a migrate run with no PostureManifestDigest -- the base-branch/opt-out path must apply no tracking at all")
	}
	if result, err := postgresstore.CheckPostureManifestLockstep(ctx, adminPool, "any-digest-at-all"); err != nil || !result.Lockstep {
		t.Fatalf("CheckPostureManifestLockstep() with no tracking table = (%+v, %v), want Lockstep=true, err=nil (nothing to compare against, not a false refusal)", result, err)
	}

	options := riverstore.MigrationOptions{
		Schema:                 "river",
		DomainRole:             domainRole,
		QueueRole:              queueRole,
		PostureManifestDigest:  "digest-v1",
		PostureManifestBuildID: "build-v1",
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, options); err != nil {
		t.Fatalf("ApplyPinnedMigrations() (first digest) error = %v", err)
	}

	// A binary declaring the CURRENT digest is lockstep.
	result, err := postgresstore.CheckPostureManifestLockstep(ctx, adminPool, "digest-v1")
	if err != nil || !result.Lockstep {
		t.Fatalf("CheckPostureManifestLockstep(digest-v1) = (%+v, %v), want Lockstep=true, err=nil", result, err)
	}

	// A binary declaring a digest that was NEVER applied (ahead of migrate,
	// or from before manifest tracking existed) is not refused -- there is no
	// history proving it is the stale one.
	result, err = postgresstore.CheckPostureManifestLockstep(ctx, adminPool, "digest-never-applied")
	if err != nil || !result.Lockstep {
		t.Fatalf("CheckPostureManifestLockstep(never-applied) = (%+v, %v), want Lockstep=true, err=nil", result, err)
	}

	// Re-run migrate with a NEWER manifest -- this is #2361/165eed2e91 in the
	// real incident. The upsert keys on manifest_digest, so this ADDS a row
	// rather than overwriting the first.
	options.PostureManifestDigest = "digest-v2"
	options.PostureManifestBuildID = "build-v2"
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, options); err != nil {
		t.Fatalf("ApplyPinnedMigrations() (second digest) error = %v", err)
	}

	// A binary still declaring "digest-v1" is now the incident's exact
	// scenario: its own manifest WAS applied, but something newer superseded
	// it. This is the refusal the incident's binaries had nothing to trigger.
	result, err = postgresstore.CheckPostureManifestLockstep(ctx, adminPool, "digest-v1")
	if result.Lockstep || err == nil {
		t.Fatalf("CheckPostureManifestLockstep(digest-v1) after digest-v2 applied = (%+v, %v), want Lockstep=false, ErrPostureManifestStale", result, err)
	}
	if result.AppliedDigest != "digest-v2" {
		t.Fatalf("stale result names AppliedDigest = %q, want %q", result.AppliedDigest, "digest-v2")
	}

	// A binary declaring the NEW current digest is lockstep again.
	result, err = postgresstore.CheckPostureManifestLockstep(ctx, adminPool, "digest-v2")
	if err != nil || !result.Lockstep {
		t.Fatalf("CheckPostureManifestLockstep(digest-v2) = (%+v, %v), want Lockstep=true, err=nil", result, err)
	}

	// The domain role can read the table it needs (it holds nothing else in
	// this minimal fixture, so this also proves the migration's own GRANT
	// for the table -- not just its CREATE TABLE -- actually landed).
	domainPool := openPool(t, ctx, roleURI(t, instance.URI, domainRole, domainPassword, mustDatabaseName(t, instance.URI)))
	defer domainPool.Close()
	if _, err := postgresstore.CheckPostureManifestLockstep(ctx, domainPool, "digest-v2"); err != nil {
		t.Fatalf("CheckPostureManifestLockstep() over the domain role's own pool: %v", err)
	}
}

func mustDatabaseName(t *testing.T, uri string) string {
	t.Helper()
	name, err := containers.DatabaseName(uri)
	if err != nil {
		t.Fatal(err)
	}
	return name
}
