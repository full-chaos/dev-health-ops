//go:build integration

package providersync

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// This file authors no DDL. The deployments table and its ReplacingMergeTree
// key come from the actual Python ClickHouse migration chain, including the
// org-key migration that makes the tenant fence meaningful. A local CREATE
// TABLE copy would only prove the copy, not the deployed schema.
func TestGitLabDeploymentsEffectsAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newDeploymentsIntegrationConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	github := GitHubDeploymentsClickHouseEffects{Conn: conn, Lease: lease}
	gitlab := GitLabDeploymentsClickHouseEffects{Conn: conn, Lease: lease}

	t.Run("GitHub retains winning version readback", func(t *testing.T) {
		claim := nativeTestClaim("github", "deployments")
		now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
		current := deploymentIntegrationRow(claim, "github-winning", now)
		previous := current
		previous.LastSynced = now.Add(-time.Hour)
		previous.ReleaseRef = "old"

		if err := github.WriteEffect(ctx, claim, deploymentEffect(t, previous)); err != nil {
			t.Fatal(err)
		}
		assertDeploymentInspection(t, ctx, github, claim, deploymentEffect(t, current), EffectAbsent)
		if err := github.WriteEffect(ctx, claim, deploymentEffect(t, current)); err != nil {
			t.Fatal(err)
		}
		assertDeploymentInspection(t, ctx, github, claim, deploymentEffect(t, current), EffectExact)
	})

	t.Run("GitHub keeps same natural key tenant-fenced", func(t *testing.T) {
		claim := nativeTestClaim("github", "deployments")
		now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
		row := deploymentIntegrationRow(claim, "github-tenant", now)
		foreignClaim := claim
		foreignClaim.OrgID = "org-github-foreign"
		foreignRow := row
		foreignRow.OrgID = foreignClaim.OrgID
		foreignRow.ReleaseRef = "foreign"

		if err := github.WriteEffect(ctx, foreignClaim, deploymentEffect(t, foreignRow)); err != nil {
			t.Fatal(err)
		}
		assertDeploymentInspection(t, ctx, github, claim, deploymentEffect(t, row), EffectAbsent)
		if err := github.WriteEffect(ctx, claim, deploymentEffect(t, row)); err != nil {
			t.Fatal(err)
		}
		assertDeploymentInspection(t, ctx, github, claim, deploymentEffect(t, row), EffectExact)
	})

	t.Run("GitLab exact replay and divergent conflict", func(t *testing.T) {
		claim := nativeTestClaim("gitlab", "deployments")
		now := time.Date(2026, 8, 9, 12, 0, 0, 123000000, time.UTC)
		largePullRequestNumber := uint64(3_000_000_000)
		if uint64(math.MaxInt) < largePullRequestNumber {
			t.Skip("int cannot represent a UInt32 pull request number on this platform")
		}
		row := deploymentIntegrationFullRow(claim, "gitlab-replay", now, int(largePullRequestNumber))
		effect := deploymentEffect(t, row)
		freezeDeploymentMerges(t, ctx, conn)

		assertDeploymentInspection(t, ctx, gitlab, claim, effect, EffectAbsent)
		if err := gitlab.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatal(err)
		}
		assertDeploymentInspection(t, ctx, gitlab, claim, effect, EffectExact)
		if err := gitlab.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("idempotent replay write: %v", err)
		}
		assertDeploymentPhysicalCount(t, ctx, conn, row, 2)
		assertDeploymentInspection(t, ctx, gitlab, claim, effect, EffectExact)

		divergent := row
		divergent.LastSynced = now.Add(time.Minute)
		divergent.ReleaseRef = "v2"
		if err := gitlab.WriteEffect(ctx, claim, deploymentEffect(t, divergent)); err != nil {
			t.Fatalf("divergent writer: %v", err)
		}
		assertDeploymentInspection(t, ctx, gitlab, claim, effect, EffectConflict)
	})

	t.Run("GitLab tenant fence and forged org", func(t *testing.T) {
		claim := nativeTestClaim("gitlab", "deployments")
		now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
		row := deploymentIntegrationRow(claim, "gitlab-tenant", now)
		foreignClaim := claim
		foreignClaim.OrgID = "org-gitlab-foreign"
		foreignRow := row
		foreignRow.OrgID = foreignClaim.OrgID
		foreignRow.ReleaseRef = "foreign"

		if err := gitlab.WriteEffect(ctx, foreignClaim, deploymentEffect(t, foreignRow)); err != nil {
			t.Fatal(err)
		}
		assertDeploymentInspection(t, ctx, gitlab, claim, deploymentEffect(t, row), EffectAbsent)
		if err := gitlab.WriteEffect(ctx, claim, deploymentEffect(t, row)); err != nil {
			t.Fatal(err)
		}
		assertDeploymentInspection(t, ctx, gitlab, claim, deploymentEffect(t, row), EffectExact)

		forged := deploymentIntegrationRow(claim, "gitlab-forged", now)
		forged.OrgID = "org-forged"
		forgedEffect := deploymentEffect(t, forged)
		if err := gitlab.WriteEffect(ctx, claim, forgedEffect); !errors.Is(err, providerfoundation.ErrInvalidScope) {
			t.Fatalf("forged org write error=%v", err)
		}
		inspection, err := gitlab.InspectEffect(ctx, claim, forgedEffect)
		if !errors.Is(err, providerfoundation.ErrInvalidScope) || inspection != EffectConflict {
			t.Fatalf("forged org inspection=%s error=%v", inspection, err)
		}
		assertDeploymentInspection(
			t, ctx, gitlab, claim,
			deploymentEffect(t, deploymentIntegrationRow(claim, "gitlab-forged", now)),
			EffectAbsent,
		)
	})

	t.Run("GitLab lease fences query prepare and send", func(t *testing.T) {
		claim := nativeTestClaim("gitlab", "deployments")
		now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
		effect := deploymentEffect(t, deploymentIntegrationRow(claim, "gitlab-lease", now))

		beforeConn := &deploymentEffectsIntegrationProbeConn{Conn: conn}
		lost := providerfoundation.LeaseGuardFunc(func(context.Context) error {
			return providerfoundation.ErrLeaseLost
		})
		beforeSink := GitLabDeploymentsClickHouseEffects{Conn: beforeConn, Lease: lost}
		if err := beforeSink.WriteEffect(ctx, claim, effect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
			t.Fatalf("lease before prepare error=%v", err)
		}
		inspection, err := beforeSink.InspectEffect(ctx, claim, effect)
		if !errors.Is(err, providerfoundation.ErrLeaseLost) || inspection != EffectConflict {
			t.Fatalf("lease before query inspection=%s error=%v", inspection, err)
		}
		if beforeConn.prepares != 0 || beforeConn.queries != 0 {
			t.Fatalf("lost lease reached ClickHouse: prepares=%d queries=%d", beforeConn.prepares, beforeConn.queries)
		}

		sendConn := &deploymentEffectsIntegrationProbeConn{Conn: conn}
		lostAfterAppend := &deploymentEffectsIntegrationSecondAssertionLosesLease{}
		sendSink := GitLabDeploymentsClickHouseEffects{Conn: sendConn, Lease: lostAfterAppend}
		if err := sendSink.WriteEffect(ctx, claim, effect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
			t.Fatalf("lease before send error=%v", err)
		}
		if lostAfterAppend.calls != 2 || sendConn.prepares != 1 || sendConn.batch == nil ||
			sendConn.batch.appends != 1 || sendConn.batch.sends != 0 || sendConn.batch.aborts != 1 {
			t.Fatalf(
				"lease before send calls=%d prepares=%d batch=%+v",
				lostAfterAppend.calls, sendConn.prepares, sendConn.batch,
			)
		}
		assertDeploymentInspection(t, ctx, gitlab, claim, effect, EffectAbsent)
	})
}

type deploymentEffectsIntegrationSink interface {
	WriteEffect(context.Context, Claim, EffectBatch) error
	InspectEffect(context.Context, Claim, EffectBatch) (EffectInspection, error)
}

func assertDeploymentInspection(
	t *testing.T,
	ctx context.Context,
	sink deploymentEffectsIntegrationSink,
	claim Claim,
	effect EffectBatch,
	want EffectInspection,
) {
	t.Helper()
	inspection, err := sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != want {
		t.Fatalf("inspection=%s error=%v want=%s", inspection, err, want)
	}
}

func newDeploymentsIntegrationConn(t *testing.T) (context.Context, driver.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	// Apply before opening the Go connection so a migration failure is reported
	// at the authority boundary rather than later as a missing-table query.
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ctx, conn
}

func deploymentIntegrationRow(claim Claim, deploymentID string, now time.Time) deploymentRow {
	deployedAt := now.Add(-time.Minute)
	return deploymentRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79",
		DeploymentID: deploymentID, DeployedAt: &deployedAt,
		ReleaseRef: "v1", ReleaseRefConfidence: 1, LastSynced: now,
	}
}

func deploymentIntegrationFullRow(claim Claim, deploymentID string, now time.Time, pullRequestNumber int) deploymentRow {
	now = now.UTC().Truncate(time.Millisecond)
	row := deploymentIntegrationRow(claim, deploymentID, now)
	status := "success"
	environment := "production"
	startedAt := now.Add(-10 * time.Minute)
	finishedAt := now.Add(-5 * time.Minute)
	deployedAt := now.Add(-4 * time.Minute)
	mergedAt := now.Add(-15 * time.Minute)
	row.Status = &status
	row.Environment = &environment
	row.StartedAt = &startedAt
	row.FinishedAt = &finishedAt
	row.DeployedAt = &deployedAt
	row.MergedAt = &mergedAt
	row.PullRequestNumber = &pullRequestNumber
	row.ReleaseRef = "v1.2.3"
	row.ReleaseRefConfidence = 0.875
	row.LastSynced = now
	return row
}

func assertDeploymentPhysicalCount(
	t *testing.T,
	ctx context.Context,
	conn driver.Conn,
	row deploymentRow,
	want uint64,
) {
	t.Helper()
	var got uint64
	if err := conn.QueryRow(
		ctx,
		`SELECT count() FROM deployments WHERE org_id = ? AND repo_id = ? AND deployment_id = ?`,
		row.OrgID,
		row.RepoID,
		row.DeploymentID,
	).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("physical deployment rows=%d want=%d", got, want)
	}
}

func freezeDeploymentMerges(t *testing.T, ctx context.Context, conn driver.Conn) {
	t.Helper()
	// ReplacingMergeTree may merge a replay before the next statement. Freeze
	// only background merges around this assertion so count() proves the two
	// physical inserts while FINAL still proves their one logical row.
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES deployments`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Exec(ctx, `SYSTEM START MERGES deployments`); err != nil {
			t.Errorf("resume deployment merges: %v", err)
		}
	})
}

func deploymentEffect(t *testing.T, rows ...deploymentRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("deployments", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

type deploymentEffectsIntegrationProbeConn struct {
	driver.Conn
	prepares int
	queries  int
	batch    *deploymentEffectsIntegrationProbeBatch
}

func (conn *deploymentEffectsIntegrationProbeConn) PrepareBatch(
	ctx context.Context,
	query string,
	options ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	conn.prepares++
	batch, err := conn.Conn.PrepareBatch(ctx, query, options...)
	if err != nil {
		return nil, err
	}
	conn.batch = &deploymentEffectsIntegrationProbeBatch{Batch: batch}
	return conn.batch, nil
}

func (conn *deploymentEffectsIntegrationProbeConn) Query(
	ctx context.Context,
	query string,
	args ...any,
) (driver.Rows, error) {
	conn.queries++
	return conn.Conn.Query(ctx, query, args...)
}

type deploymentEffectsIntegrationProbeBatch struct {
	driver.Batch
	appends int
	sends   int
	aborts  int
}

func (batch *deploymentEffectsIntegrationProbeBatch) Append(values ...any) error {
	batch.appends++
	return batch.Batch.Append(values...)
}

func (batch *deploymentEffectsIntegrationProbeBatch) Send() error {
	batch.sends++
	return batch.Batch.Send()
}

func (batch *deploymentEffectsIntegrationProbeBatch) Abort() error {
	batch.aborts++
	return batch.Batch.Abort()
}

type deploymentEffectsIntegrationSecondAssertionLosesLease struct{ calls int }

func (guard *deploymentEffectsIntegrationSecondAssertionLosesLease) Assert(context.Context) error {
	guard.calls++
	if guard.calls == 2 {
		return providerfoundation.ErrLeaseLost
	}
	return nil
}
