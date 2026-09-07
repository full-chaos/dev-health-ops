package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostureManifestAppliedTable is the Go-migrate-owned public-schema table
// go-worker-migrate stamps every run with the exact posture manifest it just
// applied (CHAOS-5437). It is deliberately NOT an Alembic table: nothing
// outside go-worker-migrate ever creates or writes it, so its DDL lives in
// ApplyPinnedMigrations itself (internal/storage/river/migrate.go), the same
// place River's own pinned schema and the runtime GRANT statements are
// applied, in the SAME transaction -- either the whole migration lands or
// none of it does.
const PostureManifestAppliedTable = "worker_posture_manifest_applied"

// ErrPostureManifestStale is returned by CheckPostureManifestLockstep when a
// posture manifest newer than the one compiled into the calling binary has
// been applied by go-worker-migrate. This is CHAOS-5437's guard against the
// 2026-09-07 incident: go-worker-heavy, go-worker-ops, go-scheduler,
// go-reconciler and three stream runners ran images built before #2361/
// 165eed2e91 widened the domain-posture manifest, and sat `not_ready` for
// hours (streams: 9h) with no crash loop and no alert once go-worker-migrate
// re-ran from a current image.
var ErrPostureManifestStale = errors.New("postgres: a newer posture manifest has been applied than this binary declares")

// PostureManifestLockstepResult names both digests and the applied_at
// timestamp of whatever go-worker-migrate most recently applied. Every field
// is safe to log: BinaryDigest/AppliedDigest are sha256 hex digests of a
// checked-in Go table (internal/storage/postgres/domain_authorization.go's
// domainPosture/queuePosture/coordinatorPosture), never a DSN, host, or
// credential, and AppliedAt is a plain timestamp.
type PostureManifestLockstepResult struct {
	BinaryDigest  string
	AppliedDigest string
	AppliedAt     time.Time
	// Lockstep is true when the applied manifest is no newer than this
	// binary's own -- either they match, or this binary's own manifest was
	// never recorded as applied at all (it may simply be ahead of the last
	// migrate run; the existing domain_postgres/queue_postgres/
	// coordinator_postgres checks already refuse readiness on a missing
	// grant in that case, so this check does not need to duplicate it).
	Lockstep bool
}

// PostureManifestDigest returns a stable content hash over every runtime
// role's declared posture manifest (DomainPosture, QueuePosture,
// CoordinatorPosture, combined and in that fixed order). It is the posture
// manifest's OWN build identity, independent of the calling binary's
// version/commit metadata: go-worker-migrate stamps this exact value into
// PostureManifestAppliedTable on every run, and every go-* runtime binary
// recomputes the identical value from its own compiled-in posture at
// startup, so CheckPostureManifestLockstep can compare "what I declare" to
// "what was last applied" without either side re-deriving the other's table
// list. Deterministic across processes and across runs of the same binary:
// domainPosture/queuePosture/coordinatorPosture build fixed-order slice
// literals, never a map, so there is nothing here for iteration order to
// perturb.
func PostureManifestDigest() string {
	return postureManifestDigest(DomainPosture(), QueuePosture(), CoordinatorPosture())
}

func postureManifestDigest(postures ...RolePosture) string {
	hash := sha256.New()
	for _, posture := range postures {
		writePostureDigestInput(hash, posture)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// writePostureDigestInput serializes one RolePosture deterministically.
// Every field is delimited (never concatenated bare) so that, for example,
// TableName "ab" ColumnName "c" cannot hash identically to TableName "a"
// ColumnName "bc" -- table/column/sequence identifiers are validated
// elsewhere (validRuntimeIdentifier) to a closed lowercase/underscore/digit
// set that can never itself contain "|" or "\n", so the delimiters below are
// never ambiguous with real content.
func writePostureDigestInput(hash io.Writer, posture RolePosture) {
	for _, table := range posture.RequiredTables {
		fmt.Fprintf(hash, "table|%s|%t|%t|%t\n", table.TableName, table.AllowInsert, table.AllowUpdate, table.AllowDelete)
	}
	fmt.Fprint(hash, "--\n")
	for _, column := range posture.ColumnScoped {
		fmt.Fprintf(hash, "column|%s|%s|%s\n", column.TableName, column.ColumnName, column.Privilege)
	}
	fmt.Fprint(hash, "--\n")
	for _, sequence := range posture.RequiredSequences {
		fmt.Fprintf(hash, "sequence|%s\n", sequence)
	}
	fmt.Fprint(hash, "==\n")
}

// CheckPostureManifestLockstep asks whether the posture manifest
// go-worker-migrate most recently applied is no newer than binaryDigest (the
// caller's own PostureManifestDigest()). It is deliberately independent of
// wall-clock/build-time comparison -- comparing this binary's own build
// timestamp against the database's applied_at would need trusted, synced
// clocks across the build host and the database server, which this
// deployment does not guarantee. Instead it reasons purely from content
// history in PostureManifestAppliedTable:
//
//   - No row has ever been applied, or the table itself does not exist yet
//     (a database ahead of THIS PR's own migrate, or one that predates
//     CHAOS-5437 entirely): nothing to compare against. Lockstep=true; this
//     is a bootstrap/transitional state, not proof of staleness.
//   - The most recently applied digest equals binaryDigest: this binary IS
//     the current posture. Lockstep=true.
//   - binaryDigest was applied before, but a DIFFERENT, more recent digest is
//     now the latest: something newer shipped and migrated since this binary
//     was built. Lockstep=false, ErrPostureManifestStale.
//   - binaryDigest has never been applied at all, but the latest applied
//     digest differs: this binary may simply be ahead of the last migrate
//     run (its own new grants are not live yet -- domain_postgres's own
//     missing-grant check already refuses readiness for that case). There is
//     no proof this binary is the STALE one, so it is not refused here.
//     Lockstep=true.
func CheckPostureManifestLockstep(ctx context.Context, pool *pgxpool.Pool, binaryDigest string) (PostureManifestLockstepResult, error) {
	if pool == nil || binaryDigest == "" {
		return PostureManifestLockstepResult{}, ErrUnavailable
	}

	var tableExists bool
	if err := pool.QueryRow(
		ctx, `SELECT to_regclass('public.`+PostureManifestAppliedTable+`') IS NOT NULL`,
	).Scan(&tableExists); err != nil {
		return PostureManifestLockstepResult{}, ErrUnavailable
	}
	if !tableExists {
		return PostureManifestLockstepResult{BinaryDigest: binaryDigest, Lockstep: true}, nil
	}

	// codex review round 1 (P2, fixed): the latest-row lookup and the
	// own-row lookup used to be two independent queries, so a migrate run
	// could commit BETWEEN them -- the latest digest read by the first query
	// could be superseded by the time the second query ran, producing a
	// transient false-stale refusal on a binary that was actually current
	// the instant it was checked. One query, one round trip, one snapshot:
	// no window for another transaction to land in between. The secondary
	// `manifest_digest` sort key makes the "latest" row deterministic even
	// on the (practically unreachable, applied_at has microsecond
	// resolution) chance two rows share the same applied_at.
	const latestAndOwnQuery = `
WITH latest AS (
	SELECT manifest_digest, applied_at
	FROM public.` + PostureManifestAppliedTable + `
	ORDER BY applied_at DESC, manifest_digest DESC
	LIMIT 1
)
SELECT
	latest.manifest_digest,
	latest.applied_at,
	EXISTS (SELECT 1 FROM public.` + PostureManifestAppliedTable + ` WHERE manifest_digest = $1) AS own_applied
FROM latest`
	var latestDigest string
	var latestAppliedAt time.Time
	var ownApplied bool
	err := pool.QueryRow(ctx, latestAndOwnQuery, binaryDigest).Scan(&latestDigest, &latestAppliedAt, &ownApplied)
	if errors.Is(err, pgx.ErrNoRows) {
		return PostureManifestLockstepResult{BinaryDigest: binaryDigest, Lockstep: true}, nil
	}
	if err != nil {
		return PostureManifestLockstepResult{}, ErrUnavailable
	}
	if latestDigest == binaryDigest {
		return PostureManifestLockstepResult{
			BinaryDigest: binaryDigest, AppliedDigest: latestDigest, AppliedAt: latestAppliedAt, Lockstep: true,
		}, nil
	}
	if !ownApplied {
		return PostureManifestLockstepResult{
			BinaryDigest: binaryDigest, AppliedDigest: latestDigest, AppliedAt: latestAppliedAt, Lockstep: true,
		}, nil
	}
	return PostureManifestLockstepResult{
		BinaryDigest: binaryDigest, AppliedDigest: latestDigest, AppliedAt: latestAppliedAt, Lockstep: false,
	}, ErrPostureManifestStale
}
