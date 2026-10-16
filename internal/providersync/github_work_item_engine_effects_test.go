package providersync

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var (
	githubWorkItemEngineEffectAt      = time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC)
	githubWorkItemEngineEffectOlderAt = time.Date(2026, 8, 4, 0, 30, 0, 0, time.UTC)
	githubWorkItemEngineEffectNewerAt = time.Date(2026, 8, 6, 0, 30, 0, 0, time.UTC)
)

func githubWorkItemEngineEffectRepoID() *uuid.UUID {
	value := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	return &value
}

func githubWorkItemEngineEffectIssueRow() githubIssueTypeMetricsDailyRow {
	return githubIssueTypeMetricsDailyRow{
		RepoID: githubWorkItemEngineEffectRepoID(), Day: "2026-08-04",
		Provider: "github", TeamID: "payments", IssueTypeNorm: "bug",
		CreatedCount: 1, CompletedCount: 2, ActiveCount: 3,
		CycleP50Hours: 4, CycleP90Hours: 5, LeadP50Hours: floatPointer(6),
		ComputedAt: githubWorkItemEngineEffectAt, OrgID: "org-acme",
	}
}

func TestCompareGitHubIssueTypeMetricsVersionRequiresOneUnambiguousFullRow(
	t *testing.T,
) {
	tests := []struct {
		name     string
		mutate   func(*githubIssueTypeMetricsDailyRow)
		distinct int
		want     EffectInspection
	}{
		{name: "identical row", distinct: 1, want: EffectExact},
		{name: "absent", distinct: 0, want: EffectAbsent},
		{name: "two distinct groups conflict even when first equals expected", distinct: 2, want: EffectConflict},
		{name: "two distinct groups at the newest generation conflict", distinct: 2, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.ComputedAt = githubWorkItemEngineEffectOlderAt
		}, want: EffectConflict},
		{name: "single older row is absent", distinct: 1, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.ComputedAt = githubWorkItemEngineEffectOlderAt
		}, want: EffectAbsent},
		{name: "single newer row conflicts", distinct: 1, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.ComputedAt = githubWorkItemEngineEffectNewerAt
		}, want: EffectConflict},
		{name: "repo differs", distinct: 1, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.RepoID = nil
		}, want: EffectConflict},
		{name: "created differs", distinct: 1, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.CreatedCount++
		}, want: EffectConflict},
		{name: "completed differs", distinct: 1, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.CompletedCount++
		}, want: EffectConflict},
		{name: "active differs", distinct: 1, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.ActiveCount++
		}, want: EffectConflict},
		{name: "p50 differs", distinct: 1, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.CycleP50Hours++
		}, want: EffectConflict},
		{name: "p90 differs", distinct: 1, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.CycleP90Hours++
		}, want: EffectConflict},
		{name: "lead differs", distinct: 1, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.LeadP50Hours = floatPointer(*row.LeadP50Hours + 1)
		}, want: EffectConflict},
		{name: "tenant differs", distinct: 1, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.OrgID = "org-other"
		}, want: EffectConflict},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			expected := githubWorkItemEngineEffectIssueRow()
			actual := githubWorkItemEngineEffectIssueRow()
			if testCase.mutate != nil {
				testCase.mutate(&actual)
			}
			if got := compareGitHubIssueTypeMetricsVersion(
				expected, actual, testCase.distinct, "org-acme",
			); got != testCase.want {
				t.Fatalf("inspection=%v want=%v", got, testCase.want)
			}
		})
	}
}

func TestGitHubWorkItemEngineLogicalKeysIncludeNullableRepoID(t *testing.T) {
	repoA := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	repoB := uuid.MustParse("22222222-2222-4222-8222-222222222222")

	issueA := githubWorkItemEngineEffectIssueRow()
	issueB := issueA
	issueA.RepoID, issueB.RepoID = &repoA, &repoB
	if !githubWorkItemDerivedUniqueSortingKeys(
		[]githubIssueTypeMetricsDailyRow{issueA, issueB},
		githubIssueTypeMetricsSortingKey,
	) {
		t.Fatal("issue logical identity collapsed different repo_id values")
	}

}

type githubWorkItemEngineRecordingConn struct {
	driver.Conn
	queries []string
}

func (conn *githubWorkItemEngineRecordingConn) Query(
	_ context.Context, query string, _ ...any,
) (driver.Rows, error) {
	conn.queries = append(conn.queries, query)
	return emptyGitHubWorkItemDerivationRows{}, nil
}

func TestGitHubWorkItemEnginePlainMergeTreeReadbacksNeverUseFinal(t *testing.T) {
	ctx := context.Background()
	conn := &githubWorkItemEngineRecordingConn{}
	lease := githubWorkItemCompositionLease()

	issueEffect := githubWorkItemEngineUnitEffect(
		t, githubIssueTypeMetricsDestination,
		[]githubIssueTypeMetricsDailyRow{githubWorkItemEngineEffectIssueRow()},
	)
	for _, testCase := range []struct {
		name        string
		destination string
		adapter     GitHubWorkItemEffectAdapter
		effect      EffectBatch
	}{
		{
			name: "issue type", destination: githubIssueTypeMetricsDestination,
			adapter: GitHubIssueTypeMetricsClickHouseEffects{Conn: conn, Lease: lease},
			effect:  issueEffect,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			identity := GitHubWorkItemEffectIdentity{
				OrgID: "org-acme", Provider: "github", Dataset: "work-items",
				Generation: "gen-1", Destination: testCase.destination,
				RowCount: len(testCase.effect.Rows),
			}
			inspection, err := testCase.adapter.InspectGitHubWorkItemEffect(
				ctx, identity, testCase.effect,
			)
			if err != nil || inspection != EffectAbsent {
				t.Fatalf("inspection=%s err=%v want absent", inspection, err)
			}
		})
	}
	if len(conn.queries) != 1 {
		t.Fatalf("readback queries=%d want=1", len(conn.queries))
	}
	for _, query := range conn.queries {
		if strings.Contains(strings.ToUpper(query), "FINAL") {
			t.Fatalf("plain MergeTree readback must not use FINAL:\n%s", query)
		}
	}
}

func githubWorkItemEngineUnitEffect[T any](
	t *testing.T, destination string, rows []T,
) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues(destination, EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

type githubWorkItemEngineWriteConn struct {
	driver.Conn
	prepares int
	batch    *githubWorkItemEngineWriteBatch
}

func (conn *githubWorkItemEngineWriteConn) PrepareBatch(
	context.Context, string, ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	conn.prepares++
	if conn.batch == nil {
		conn.batch = &githubWorkItemEngineWriteBatch{}
	}
	return conn.batch, nil
}

type githubWorkItemEngineWriteBatch struct {
	driver.Batch
	appends, sends, aborts int
}

func (batch *githubWorkItemEngineWriteBatch) Append(...any) error {
	batch.appends++
	return nil
}

func (batch *githubWorkItemEngineWriteBatch) Send() error {
	batch.sends++
	return nil
}

func (batch *githubWorkItemEngineWriteBatch) Abort() error {
	batch.aborts++
	return nil
}

type githubWorkItemEngineAdapterCase struct {
	name        string
	destination string
	valid       EffectBatch
	forgedOrg   EffectBatch
	adapter     func(driver.Conn, providerfoundation.LeaseGuard) GitHubWorkItemEffectAdapter
}

func githubWorkItemEngineAdapterCases(t *testing.T) []githubWorkItemEngineAdapterCase {
	t.Helper()
	issue := githubWorkItemEngineEffectIssueRow()
	foreignIssue := issue
	foreignIssue.OrgID = "org-other"
	return []githubWorkItemEngineAdapterCase{
		{
			name: "issue type", destination: githubIssueTypeMetricsDestination,
			valid: githubWorkItemEngineUnitEffect(
				t, githubIssueTypeMetricsDestination, []githubIssueTypeMetricsDailyRow{issue},
			),
			forgedOrg: githubWorkItemEngineUnitEffect(
				t, githubIssueTypeMetricsDestination,
				[]githubIssueTypeMetricsDailyRow{foreignIssue},
			),
			adapter: func(conn driver.Conn, lease providerfoundation.LeaseGuard) GitHubWorkItemEffectAdapter {
				return GitHubIssueTypeMetricsClickHouseEffects{Conn: conn, Lease: lease}
			},
		},
	}
}

func githubWorkItemEngineTestIdentity(
	destination string, effect EffectBatch,
) GitHubWorkItemEffectIdentity {
	return GitHubWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "github", Dataset: "work-items",
		Generation: "gen-1", Destination: destination, RowCount: len(effect.Rows),
	}
}

func TestGitHubWorkItemEngineAdaptersFailClosedOnDependencyTenantAndLease(t *testing.T) {
	ctx := context.Background()
	validLease := githubWorkItemCompositionLease()
	for _, testCase := range githubWorkItemEngineAdapterCases(t) {
		t.Run(testCase.name, func(t *testing.T) {
			identity := githubWorkItemEngineTestIdentity(
				testCase.destination, testCase.valid,
			)

			withoutConn := testCase.adapter(nil, validLease)
			if err := withoutConn.WriteGitHubWorkItemEffect(
				ctx, identity, testCase.valid,
			); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("write without conn error=%v", err)
			}
			if _, err := withoutConn.InspectGitHubWorkItemEffect(
				ctx, identity, testCase.valid,
			); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("inspect without conn error=%v", err)
			}

			conn := &githubWorkItemEngineWriteConn{}
			forged := testCase.adapter(conn, validLease)
			if err := forged.WriteGitHubWorkItemEffect(
				ctx, identity, testCase.forgedOrg,
			); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("forged tenant write error=%v", err)
			}
			if _, err := forged.InspectGitHubWorkItemEffect(
				ctx, identity, testCase.forgedOrg,
			); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("forged tenant inspect error=%v", err)
			}
			if conn.prepares != 0 {
				t.Fatalf("forged tenant prepared %d batches", conn.prepares)
			}

			lostBefore := providerfoundation.LeaseGuardFunc(
				func(context.Context) error { return providerfoundation.ErrLeaseLost },
			)
			conn = &githubWorkItemEngineWriteConn{}
			if err := testCase.adapter(conn, lostBefore).WriteGitHubWorkItemEffect(
				ctx, identity, testCase.valid,
			); !errors.Is(err, providerfoundation.ErrLeaseLost) {
				t.Fatalf("pre-write lease error=%v", err)
			}
			if conn.prepares != 0 {
				t.Fatalf("lost lease prepared %d batches before refusal", conn.prepares)
			}

			readConn := &githubWorkItemEngineRecordingConn{}
			inspection, err := testCase.adapter(
				readConn, lostBefore,
			).InspectGitHubWorkItemEffect(ctx, identity, testCase.valid)
			if !errors.Is(err, providerfoundation.ErrLeaseLost) ||
				inspection != EffectConflict {
				t.Fatalf("pre-read lease inspection=%s error=%v", inspection, err)
			}
			if len(readConn.queries) != 0 {
				t.Fatalf("lost read lease executed queries=%v", readConn.queries)
			}

			conn = &githubWorkItemEngineWriteConn{}
			lostAfter := &secondAssertionLosesLease{}
			if err := testCase.adapter(conn, lostAfter).WriteGitHubWorkItemEffect(
				ctx, identity, testCase.valid,
			); !errors.Is(err, providerfoundation.ErrLeaseLost) {
				t.Fatalf("post-append lease error=%v", err)
			}
			if lostAfter.calls != 2 || conn.prepares != 1 || conn.batch == nil ||
				conn.batch.appends != 1 || conn.batch.sends != 0 || conn.batch.aborts != 1 {
				t.Fatalf(
					"post-append fence calls=%d prepares=%d batch=%+v",
					lostAfter.calls, conn.prepares, conn.batch,
				)
			}
		})
	}
}
