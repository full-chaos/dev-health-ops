package providersync

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestGitLabDeploymentsEffectsAcceptsLegitimateEmptyEffect(t *testing.T) {
	ctx := context.Background()
	claim := nativeTestClaim("gitlab", "deployments")
	empty := deploymentEffectsUnitEffect(t, nil)
	sink := GitLabDeploymentsClickHouseEffects{
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}

	if err := sink.WriteEffect(ctx, claim, empty); err != nil {
		t.Fatalf("empty write error=%v", err)
	}
	inspection, err := sink.InspectEffect(ctx, claim, empty)
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("empty inspection=%s error=%v", inspection, err)
	}

	githubClaim := nativeTestClaim("github", "deployments")
	if err := sink.WriteEffect(ctx, githubClaim, empty); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("GitLab sink accepted GitHub claim: %v", err)
	}
}

func TestGitLabDeploymentsEffectsRejectsProviderDatasetAndDestinationBeforeClickHouse(t *testing.T) {
	ctx := context.Background()
	validClaim := nativeTestClaim("gitlab", "deployments")
	valid := deploymentEffectsUnitEffect(t, []deploymentRow{
		deploymentEffectsUnitRow(validClaim, "scope"),
	})
	wrongProviderClaim := validClaim
	wrongProviderClaim.Provider = "github"
	validLease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	tests := []struct {
		name   string
		claim  Claim
		effect EffectBatch
	}{
		{name: "provider", claim: wrongProviderClaim, effect: valid},
		{name: "dataset", claim: nativeTestClaim("gitlab", "commits"), effect: valid},
		{name: "destination", claim: validClaim, effect: func() EffectBatch {
			wrong := valid
			wrong.Destination = "git_commits"
			return wrong
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := &deploymentEffectsRecordingConn{}
			sink := GitLabDeploymentsClickHouseEffects{Conn: conn, Lease: validLease}
			if err := sink.WriteEffect(ctx, test.claim, test.effect); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("write error=%v", err)
			}
			inspection, err := sink.InspectEffect(ctx, test.claim, test.effect)
			if !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
				t.Fatalf("inspection=%s error=%v", inspection, err)
			}
			if conn.prepares != 0 || conn.queries != 0 {
				t.Fatalf("invalid scope reached ClickHouse: prepares=%d queries=%d", conn.prepares, conn.queries)
			}
		})
	}
}

func TestGitLabDeploymentsEffectsFenceTenantAndLeaseBeforeClickHouse(t *testing.T) {
	ctx := context.Background()
	claim := nativeTestClaim("gitlab", "deployments")
	valid := deploymentEffectsUnitEffect(t, []deploymentRow{
		deploymentEffectsUnitRow(claim, "101"),
	})
	validLease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })

	t.Run("forged org is rejected before ClickHouse", func(t *testing.T) {
		forged := deploymentEffectsUnitRow(claim, "101")
		forged.OrgID = "org-forged"
		forgedEffect := deploymentEffectsUnitEffect(t, []deploymentRow{forged})
		conn := &deploymentEffectsRecordingConn{}
		sink := GitLabDeploymentsClickHouseEffects{Conn: conn, Lease: validLease}

		if err := sink.WriteEffect(ctx, claim, forgedEffect); !errors.Is(err, providerfoundation.ErrInvalidScope) {
			t.Fatalf("forged write error=%v", err)
		}
		inspection, err := sink.InspectEffect(ctx, claim, forgedEffect)
		if !errors.Is(err, providerfoundation.ErrInvalidScope) || inspection != EffectConflict {
			t.Fatalf("forged inspection=%s error=%v", inspection, err)
		}
		if conn.prepares != 0 || conn.queries != 0 {
			t.Fatalf("forged tenant reached ClickHouse: prepares=%d queries=%d", conn.prepares, conn.queries)
		}
	})

	t.Run("lost lease prevents prepare and query", func(t *testing.T) {
		conn := &deploymentEffectsRecordingConn{}
		lost := providerfoundation.LeaseGuardFunc(func(context.Context) error {
			return providerfoundation.ErrLeaseLost
		})
		sink := GitLabDeploymentsClickHouseEffects{Conn: conn, Lease: lost}

		if err := sink.WriteEffect(ctx, claim, valid); !errors.Is(err, providerfoundation.ErrLeaseLost) {
			t.Fatalf("pre-prepare lease error=%v", err)
		}
		inspection, err := sink.InspectEffect(ctx, claim, valid)
		if !errors.Is(err, providerfoundation.ErrLeaseLost) || inspection != EffectConflict {
			t.Fatalf("pre-query inspection=%s error=%v", inspection, err)
		}
		if conn.prepares != 0 || conn.queries != 0 {
			t.Fatalf("lost lease reached ClickHouse: prepares=%d queries=%d", conn.prepares, conn.queries)
		}
	})

	t.Run("second lease assertion prevents send", func(t *testing.T) {
		conn := &deploymentEffectsRecordingConn{}
		lostAfterAppend := &deploymentEffectsSecondAssertionLosesLease{}
		sink := GitLabDeploymentsClickHouseEffects{Conn: conn, Lease: lostAfterAppend}

		if err := sink.WriteEffect(ctx, claim, valid); !errors.Is(err, providerfoundation.ErrLeaseLost) {
			t.Fatalf("pre-send lease error=%v", err)
		}
		if lostAfterAppend.calls != 2 || conn.prepares != 1 || conn.batch == nil ||
			conn.batch.appends != 1 || conn.batch.sends != 0 || conn.batch.aborts != 1 {
			t.Fatalf(
				"pre-send fence calls=%d prepares=%d batch=%+v",
				lostAfterAppend.calls, conn.prepares, conn.batch,
			)
		}
	})

	t.Run("valid GitLab effect is written", func(t *testing.T) {
		conn := &deploymentEffectsRecordingConn{}
		sink := GitLabDeploymentsClickHouseEffects{Conn: conn, Lease: validLease}

		if err := sink.WriteEffect(ctx, claim, valid); err != nil {
			t.Fatalf("valid GitLab write error=%v", err)
		}
		if conn.prepares != 1 || conn.batch == nil || conn.batch.appends != 1 || conn.batch.sends != 1 {
			t.Fatalf("GitLab write prepares=%d batch=%+v", conn.prepares, conn.batch)
		}
	})
}

func TestGitLabDeploymentsEffectsAppendsNullableUInt32PullRequestNumber(t *testing.T) {
	const largePullRequestNumber = uint64(3_000_000_000)
	if uint64(math.MaxInt) < largePullRequestNumber {
		t.Skip("int cannot represent a UInt32 pull request number on this platform")
	}
	claim := nativeTestClaim("gitlab", "deployments")
	row := deploymentEffectsUnitRow(claim, "large-pr")
	pullRequestNumber := int(largePullRequestNumber)
	row.PullRequestNumber = &pullRequestNumber
	effect := deploymentEffectsUnitEffect(t, []deploymentRow{row})
	conn := &deploymentEffectsRecordingConn{}
	sink := GitLabDeploymentsClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}

	if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
		t.Fatal(err)
	}
	if conn.batch == nil || len(conn.batch.values) != 1 || len(conn.batch.values[0]) != 13 {
		t.Fatalf("append values=%+v", conn.batch)
	}
	got, ok := conn.batch.values[0][8].(*uint32)
	if !ok || got == nil || *got != uint32(largePullRequestNumber) {
		t.Fatalf("pull request append=%T %#v want *uint32(%d)", conn.batch.values[0][8], conn.batch.values[0][8], largePullRequestNumber)
	}
}

func deploymentEffectsUnitRow(claim Claim, deploymentID string) deploymentRow {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	deployedAt := now.Add(-time.Minute)
	return deploymentRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79",
		DeploymentID: deploymentID, DeployedAt: &deployedAt,
		ReleaseRef: "v1", ReleaseRefConfidence: 1, LastSynced: now,
	}
}

func deploymentEffectsUnitEffect(t *testing.T, rows []deploymentRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("deployments", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

type deploymentEffectsRecordingConn struct {
	driver.Conn
	prepares int
	queries  int
	batch    *deploymentEffectsRecordingBatch
}

func (conn *deploymentEffectsRecordingConn) PrepareBatch(
	context.Context, string, ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	conn.prepares++
	if conn.batch == nil {
		conn.batch = &deploymentEffectsRecordingBatch{}
	}
	return conn.batch, nil
}

func (conn *deploymentEffectsRecordingConn) Query(
	context.Context, string, ...any,
) (driver.Rows, error) {
	conn.queries++
	return emptyDeploymentEffectsRows{}, nil
}

type deploymentEffectsRecordingBatch struct {
	driver.Batch
	appends int
	sends   int
	aborts  int
	values  [][]any
}

func (batch *deploymentEffectsRecordingBatch) Append(values ...any) error {
	batch.appends++
	batch.values = append(batch.values, append([]any(nil), values...))
	return nil
}

func (batch *deploymentEffectsRecordingBatch) Send() error {
	batch.sends++
	return nil
}

func (batch *deploymentEffectsRecordingBatch) Abort() error {
	batch.aborts++
	return nil
}

type emptyDeploymentEffectsRows struct{}

func (emptyDeploymentEffectsRows) Next() bool                       { return false }
func (emptyDeploymentEffectsRows) Scan(...any) error                { return nil }
func (emptyDeploymentEffectsRows) ScanStruct(any) error             { return nil }
func (emptyDeploymentEffectsRows) ColumnTypes() []driver.ColumnType { return nil }
func (emptyDeploymentEffectsRows) Totals(...any) error              { return nil }
func (emptyDeploymentEffectsRows) Columns() []string                { return nil }
func (emptyDeploymentEffectsRows) Close() error                     { return nil }
func (emptyDeploymentEffectsRows) Err() error                       { return nil }
func (emptyDeploymentEffectsRows) HasData() bool                    { return false }

type deploymentEffectsSecondAssertionLosesLease struct{ calls int }

func (guard *deploymentEffectsSecondAssertionLosesLease) Assert(context.Context) error {
	guard.calls++
	if guard.calls == 2 {
		return providerfoundation.ErrLeaseLost
	}
	return nil
}
