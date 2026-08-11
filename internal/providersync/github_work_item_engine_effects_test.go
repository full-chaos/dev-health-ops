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
		CycleP50Hours: 4, CycleP90Hours: 5, LeadP50Hours: 6,
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
		{name: "older divergent row remains ambiguous", distinct: 2, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.ComputedAt = githubWorkItemEngineEffectOlderAt
		}, want: EffectConflict},
		{name: "single older row conflicts without a latest-row contract", distinct: 1, mutate: func(row *githubIssueTypeMetricsDailyRow) {
			row.ComputedAt = githubWorkItemEngineEffectOlderAt
		}, want: EffectConflict},
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
			row.LeadP50Hours++
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

func githubWorkItemEngineEffectClassificationRow() githubInvestmentClassificationDailyRow {
	area, rule := "security", "sec_general"
	return githubInvestmentClassificationDailyRow{
		RepoID: githubWorkItemEngineEffectRepoID(), Day: "2026-08-04",
		ArtifactType: "work_item", ArtifactID: "acme/api#1", Provider: "github",
		InvestmentArea: &area, ProjectStream: "general", Confidence: 1,
		RuleID: &rule, ComputedAt: githubWorkItemEngineEffectAt, OrgID: "org-acme",
	}
}

func TestCompareGitHubInvestmentClassificationVersionRequiresOneUnambiguousFullRow(
	t *testing.T,
) {
	tests := []struct {
		name     string
		mutate   func(*githubInvestmentClassificationDailyRow)
		distinct int
		want     EffectInspection
	}{
		{name: "identical row", distinct: 1, want: EffectExact},
		{name: "absent", distinct: 0, want: EffectAbsent},
		{name: "two distinct groups conflict even when first equals expected", distinct: 2, want: EffectConflict},
		{name: "older divergent row remains ambiguous", distinct: 2, mutate: func(row *githubInvestmentClassificationDailyRow) {
			row.ComputedAt = githubWorkItemEngineEffectOlderAt
		}, want: EffectConflict},
		{name: "single older row conflicts without a latest-row contract", distinct: 1, mutate: func(row *githubInvestmentClassificationDailyRow) {
			row.ComputedAt = githubWorkItemEngineEffectOlderAt
		}, want: EffectConflict},
		{name: "single newer row conflicts", distinct: 1, mutate: func(row *githubInvestmentClassificationDailyRow) {
			row.ComputedAt = githubWorkItemEngineEffectNewerAt
		}, want: EffectConflict},
		{name: "repo differs", distinct: 1, mutate: func(row *githubInvestmentClassificationDailyRow) {
			row.RepoID = nil
		}, want: EffectConflict},
		{name: "confidence differs", distinct: 1, mutate: func(row *githubInvestmentClassificationDailyRow) {
			row.Confidence = 0
		}, want: EffectConflict},
		{name: "rule differs", distinct: 1, mutate: func(row *githubInvestmentClassificationDailyRow) {
			other := "other"
			row.RuleID = &other
		}, want: EffectConflict},
		{name: "tenant differs", distinct: 1, mutate: func(row *githubInvestmentClassificationDailyRow) {
			row.OrgID = "org-other"
		}, want: EffectConflict},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			expected := githubWorkItemEngineEffectClassificationRow()
			actual := githubWorkItemEngineEffectClassificationRow()
			if testCase.mutate != nil {
				testCase.mutate(&actual)
			}
			if got := compareGitHubInvestmentClassificationVersion(
				expected, actual, testCase.distinct, "org-acme",
			); got != testCase.want {
				t.Fatalf("inspection=%v want=%v", got, testCase.want)
			}
		})
	}
}

func githubWorkItemEngineEffectMetricsRow() githubInvestmentMetricsDailyRow {
	area := "security"
	return githubInvestmentMetricsDailyRow{
		RepoID: githubWorkItemEngineEffectRepoID(), Day: "2026-08-04",
		TeamID: "payments", InvestmentArea: &area, ProjectStream: "general",
		DeliveryUnits: 3, WorkItemsCompleted: 2, PRsMerged: 1, ChurnLOC: 4,
		CycleP50Hours: 5, ComputedAt: githubWorkItemEngineEffectAt, OrgID: "org-acme",
	}
}

func TestCompareGitHubInvestmentMetricsVersionRejectsAmbiguousNewestTuple(
	t *testing.T,
) {
	tests := []struct {
		name           string
		mutate         func(*githubInvestmentMetricsDailyRow)
		latestDistinct int
		want           EffectInspection
	}{
		{name: "identical latest tuple", latestDistinct: 1, want: EffectExact},
		{name: "absent", latestDistinct: 0, want: EffectAbsent},
		{name: "equal-time divergence is not arbitrary argMax", latestDistinct: 2, want: EffectConflict},
		{name: "older latest tuple is absent", latestDistinct: 1, mutate: func(row *githubInvestmentMetricsDailyRow) {
			row.ComputedAt = githubWorkItemEngineEffectOlderAt
		}, want: EffectAbsent},
		{name: "newer latest tuple conflicts", latestDistinct: 1, mutate: func(row *githubInvestmentMetricsDailyRow) {
			row.ComputedAt = githubWorkItemEngineEffectNewerAt
		}, want: EffectConflict},
		{name: "repo differs", latestDistinct: 1, mutate: func(row *githubInvestmentMetricsDailyRow) {
			row.RepoID = nil
		}, want: EffectConflict},
		{name: "units differ", latestDistinct: 1, mutate: func(row *githubInvestmentMetricsDailyRow) {
			row.DeliveryUnits++
		}, want: EffectConflict},
		{name: "completed differs", latestDistinct: 1, mutate: func(row *githubInvestmentMetricsDailyRow) {
			row.WorkItemsCompleted++
		}, want: EffectConflict},
		{name: "merged differs", latestDistinct: 1, mutate: func(row *githubInvestmentMetricsDailyRow) {
			row.PRsMerged++
		}, want: EffectConflict},
		{name: "churn differs", latestDistinct: 1, mutate: func(row *githubInvestmentMetricsDailyRow) {
			row.ChurnLOC++
		}, want: EffectConflict},
		{name: "cycle differs", latestDistinct: 1, mutate: func(row *githubInvestmentMetricsDailyRow) {
			row.CycleP50Hours++
		}, want: EffectConflict},
		{name: "tenant differs", latestDistinct: 1, mutate: func(row *githubInvestmentMetricsDailyRow) {
			row.OrgID = "org-other"
		}, want: EffectConflict},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			expected := githubWorkItemEngineEffectMetricsRow()
			actual := githubWorkItemEngineEffectMetricsRow()
			if testCase.mutate != nil {
				testCase.mutate(&actual)
			}
			if got := compareGitHubInvestmentMetricsVersion(
				expected, actual, testCase.latestDistinct, "org-acme",
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

	classificationA := githubWorkItemEngineEffectClassificationRow()
	classificationB := classificationA
	classificationA.RepoID, classificationB.RepoID = nil, &repoB
	if !githubWorkItemDerivedUniqueSortingKeys(
		[]githubInvestmentClassificationDailyRow{classificationA, classificationB},
		githubInvestmentClassificationSortingKey,
	) {
		t.Fatal("classification logical identity collapsed NULL and present repo_id")
	}

	metricA := githubWorkItemEngineEffectMetricsRow()
	metricB := metricA
	metricA.RepoID, metricB.RepoID = &repoA, nil
	if !githubWorkItemDerivedUniqueSortingKeys(
		[]githubInvestmentMetricsDailyRow{metricA, metricB},
		githubInvestmentMetricsSortingKey,
	) {
		t.Fatal("investment metric logical identity collapsed present and NULL repo_id")
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
	classificationEffect := githubWorkItemEngineUnitEffect(
		t, githubInvestmentClassificationsDestination,
		[]githubInvestmentClassificationDailyRow{
			githubWorkItemEngineEffectClassificationRow(),
		},
	)
	metricsEffect := githubWorkItemEngineUnitEffect(
		t, githubInvestmentMetricsDestination,
		[]githubInvestmentMetricsDailyRow{githubWorkItemEngineEffectMetricsRow()},
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
		{
			name: "classification", destination: githubInvestmentClassificationsDestination,
			adapter: GitHubInvestmentClassificationsClickHouseEffects{Conn: conn, Lease: lease},
			effect:  classificationEffect,
		},
		{
			name: "metrics", destination: githubInvestmentMetricsDestination,
			adapter: GitHubInvestmentMetricsClickHouseEffects{Conn: conn, Lease: lease},
			effect:  metricsEffect,
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
	if len(conn.queries) != 3 {
		t.Fatalf("readback queries=%d want=3", len(conn.queries))
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
	classification := githubWorkItemEngineEffectClassificationRow()
	foreignClassification := classification
	foreignClassification.OrgID = "org-other"
	metrics := githubWorkItemEngineEffectMetricsRow()
	foreignMetrics := metrics
	foreignMetrics.OrgID = "org-other"
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
		{
			name: "classification", destination: githubInvestmentClassificationsDestination,
			valid: githubWorkItemEngineUnitEffect(
				t, githubInvestmentClassificationsDestination,
				[]githubInvestmentClassificationDailyRow{classification},
			),
			forgedOrg: githubWorkItemEngineUnitEffect(
				t, githubInvestmentClassificationsDestination,
				[]githubInvestmentClassificationDailyRow{foreignClassification},
			),
			adapter: func(conn driver.Conn, lease providerfoundation.LeaseGuard) GitHubWorkItemEffectAdapter {
				return GitHubInvestmentClassificationsClickHouseEffects{Conn: conn, Lease: lease}
			},
		},
		{
			name: "metrics", destination: githubInvestmentMetricsDestination,
			valid: githubWorkItemEngineUnitEffect(
				t, githubInvestmentMetricsDestination,
				[]githubInvestmentMetricsDailyRow{metrics},
			),
			forgedOrg: githubWorkItemEngineUnitEffect(
				t, githubInvestmentMetricsDestination,
				[]githubInvestmentMetricsDailyRow{foreignMetrics},
			),
			adapter: func(conn driver.Conn, lease providerfoundation.LeaseGuard) GitHubWorkItemEffectAdapter {
				return GitHubInvestmentMetricsClickHouseEffects{Conn: conn, Lease: lease}
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
