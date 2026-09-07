// Package postureguard is the one place every go-* runtime binary's
// posture_manifest_lockstep readiness check and its Prometheus gauge live
// (CHAOS-5437). It exists so the refusal message, the gauge name, and the
// digest-comparison wiring are written once and reused by
// cmd/dev-health-worker, cmd/dev-health-scheduler, cmd/dev-health-reconciler
// and cmd/dev-health-stream-runner, rather than copy-pasted four times and
// drifting the way domainReady/queueReady/riverSchemaReady already have
// across those same four binaries.
package postureguard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// Checker is one binary's own narrow capability method for this check --
// e.g. (*postgresWorkerDatabase).PostureManifestLockstep -- rather than a
// raw *pgxpool.Pool, matching every other workerDatabase/schedulerDatabase/
// reconcilerDatabase capability in this repo (DomainTxOpener,
// GitHubProjectsV2Configured, ...): the database-facing struct owns the
// pool, callers get a typed method.
type Checker func(ctx context.Context, binaryDigest string) (postgres.PostureManifestLockstepResult, error)

// Guard wraps one binary's posture-manifest lockstep check: a bounded
// health.CheckFunc-shaped Ready method, plus a health.MetricsSource-shaped
// Gauge recording the last outcome as dev_health_worker_posture_manifest_mismatch.
// service is a label, not a DSN/host/credential -- it is always one of this
// repo's own compile-time service names (dev-health-worker,
// dev-health-scheduler, dev-health-reconciler, dev-health-stream-runner).
type Guard struct {
	service string
	check   Checker
	digest  string

	mu       sync.RWMutex
	mismatch bool
}

// New builds a Guard for one binary. check is normally the concrete
// database struct's own PostureManifestLockstep method value; digest is
// normally postgres.PostureManifestDigest() -- a parameter rather than a
// direct call so tests can inject a synthetic value without a live
// database.
func New(service string, check Checker, digest string) *Guard {
	return &Guard{service: service, check: check, digest: digest}
}

// Ready is a health.CheckFunc: nil when this binary's compiled-in posture
// manifest is no newer than what go-worker-migrate has applied, a wrapped
// ErrPostureManifestStale naming both digests otherwise. It also updates the
// gauge Gauge later scrapes, so the two never disagree about the last
// evaluated outcome.
func (guard *Guard) Ready(ctx context.Context) error {
	if guard == nil || guard.check == nil || guard.digest == "" {
		return errors.New("posture_manifest_lockstep: guard not configured")
	}
	result, err := guard.check(ctx, guard.digest)
	if err != nil && !errors.Is(err, postgres.ErrPostureManifestStale) {
		// The diagnostic query itself failed (unreachable database, etc.) --
		// report unavailable rather than claiming a mismatch we could not
		// actually measure, and leave the gauge at its last known value.
		return fmt.Errorf("posture_manifest_lockstep: %w", err)
	}
	guard.setMismatch(!result.Lockstep)
	if errors.Is(err, postgres.ErrPostureManifestStale) {
		return fmt.Errorf(
			"posture_manifest_lockstep: this image's posture manifest (%s) is older than the manifest "+
				"go-worker-migrate applied at %s (%s) -- rebuild/redeploy this image: %w",
			result.BinaryDigest, result.AppliedAt.UTC().Format("2006-01-02T15:04:05Z"), result.AppliedDigest, err,
		)
	}
	return nil
}

func (guard *Guard) setMismatch(mismatch bool) {
	if guard == nil {
		return
	}
	guard.mu.Lock()
	guard.mismatch = mismatch
	guard.mu.Unlock()
}

// Gauge is the health.MetricsSource half of Guard: it satisfies that
// interface's WritePrometheus(io.Writer) error method directly, so a caller
// registers the SAME *Guard value with both RegisterRequired(name,
// guard.Ready) and RegisterMetrics(name, guard) -- no second type needed.
func (guard *Guard) WritePrometheus(w io.Writer) error {
	guard.mu.RLock()
	mismatch := guard.mismatch
	guard.mu.RUnlock()
	value := 0
	if mismatch {
		value = 1
	}
	if _, err := fmt.Fprintln(w, "# HELP dev_health_worker_posture_manifest_mismatch 1 when this service's compiled-in posture manifest is older than the manifest go-worker-migrate most recently applied (CHAOS-5437)."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE dev_health_worker_posture_manifest_mismatch gauge"); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "dev_health_worker_posture_manifest_mismatch{service=%q} %d\n", guard.service, value)
	return err
}
