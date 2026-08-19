package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestNewGitHubWorkItemEngineDeriverRejectsPartialEngines(t *testing.T) {
	statusMapping := loadRealStatusMapping(t)
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		name       string
		mapping    *StatusMapping
		classifier *InvestmentClassifier
	}{
		{name: "missing status mapping", classifier: classifier},
		{name: "missing investment classifier", mapping: statusMapping},
		{name: "both missing"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			engine, err := NewGitHubWorkItemEngineDeriver(
				testCase.mapping, testCase.classifier,
			)
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want ErrInvalidConfiguration", err)
			}
			if engine != nil {
				t.Fatalf("rejected constructor returned engine=%+v", engine)
			}
		})
	}
}

func TestNewGitHubWorkItemDeriverLoadsBothExplicitConfigsAtomically(t *testing.T) {
	conn := stubWorkItemConn{}
	lease := githubWorkItemCompositionLease()
	statusPath := resolveStatusMappingConfig(t, "real")
	investmentPath := investmentConfigPath(t, "real")

	for _, testCase := range []struct {
		name           string
		conn           driver.Conn
		lease          providerfoundation.LeaseGuard
		statusPath     string
		investmentPath string
	}{
		{name: "missing connection", lease: lease, statusPath: statusPath, investmentPath: investmentPath},
		{name: "missing lease", conn: conn, statusPath: statusPath, investmentPath: investmentPath},
		{name: "blank status path", conn: conn, lease: lease, investmentPath: investmentPath},
		{name: "blank investment path", conn: conn, lease: lease, statusPath: statusPath},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			deriver, err := NewGitHubWorkItemDeriver(
				testCase.conn, testCase.lease,
				testCase.statusPath, testCase.investmentPath,
			)
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want ErrInvalidConfiguration", err)
			}
			if deriver != nil {
				t.Fatalf("rejected constructor returned deriver=%+v", deriver)
			}
		})
	}

	deriver, err := NewGitHubWorkItemDeriver(
		conn, lease, statusPath, investmentPath,
	)
	if err != nil {
		t.Fatal(err)
	}
	source, ok := deriver.Source.(githubWorkItemClickHouseDerivationContextSource)
	if !ok || source.Conn == nil || source.Lease == nil {
		t.Fatalf("constructor installed unreachable source=%T %+v", deriver.Source, source)
	}
	engine, ok := deriver.engine.(*GitHubWorkItemEngineDeriver)
	if !ok || engine.statusMapping == nil || engine.investmentClassifier == nil {
		t.Fatalf("constructor installed partial engine=%T %+v", deriver.engine, engine)
	}
}

func TestNewGitHubWorkItemDeriverPreservesInvestmentLoaderContracts(t *testing.T) {
	conn := stubWorkItemConn{}
	lease := githubWorkItemCompositionLease()
	statusPath := resolveStatusMappingConfig(t, "real")

	// Python intentionally treats a named-but-missing investment file as an
	// empty rule set and uses the legacy fallback. The atomic constructor must
	// not reinterpret that established engine behavior as a path error.
	missingPath := filepath.Join(t.TempDir(), "missing-investment.yaml")
	deriver, err := NewGitHubWorkItemDeriver(
		conn, lease, statusPath, missingPath,
	)
	if err != nil {
		t.Fatal(err)
	}
	engine := deriver.engine.(*GitHubWorkItemEngineDeriver)
	component := ""
	classification, err := engine.investmentClassifier.Classify(InvestmentArtifact{
		Labels: []string{"security"}, Component: &component,
		Title: "security item", Provider: "github",
	})
	if err != nil {
		t.Fatal(err)
	}
	if classification.InvestmentArea == nil ||
		*classification.InvestmentArea != legacyDefaultInvestmentArea ||
		classification.ProjectStream == nil ||
		*classification.ProjectStream != legacyDefaultProjectStream ||
		classification.Confidence != 0 {
		t.Fatalf("missing-file fallback=%+v", classification)
	}

	if built, err := NewGitHubWorkItemDeriver(
		conn, lease, statusPath, investmentConfigPath(t, "raises_priority_null"),
	); err == nil || built != nil {
		t.Fatalf("classifier load failure returned deriver=%+v error=%v", built, err)
	}
	if built, err := NewGitHubWorkItemDeriver(
		conn, lease, filepath.Join(t.TempDir(), "missing-status.yaml"),
		investmentConfigPath(t, "real"),
	); err == nil || built != nil {
		t.Fatalf("status load failure returned deriver=%+v error=%v", built, err)
	}
}

func TestGitHubWorkItemEngineDeriverRejectsCorruptedPartialEngine(t *testing.T) {
	statusMapping := loadRealStatusMapping(t)
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	claim, rows, day, computedAt, derived := githubWorkItemEngineFixture(t)

	for _, testCase := range []struct {
		name   string
		engine *GitHubWorkItemEngineDeriver
	}{
		{name: "missing status mapping", engine: &GitHubWorkItemEngineDeriver{
			investmentClassifier: classifier,
		}},
		{name: "missing investment classifier", engine: &GitHubWorkItemEngineDeriver{
			statusMapping: statusMapping,
		}},
		{name: "both missing", engine: &GitHubWorkItemEngineDeriver{}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := testCase.engine.Derive(
				context.Background(), claim, rows, day, computedAt, derived,
			); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want ErrInvalidConfiguration", err)
			}
		})
	}
}

func TestGitHubWorkItemEngineDeriverUsesBothRealEnginesPerDay(t *testing.T) {
	statusMapping := loadRealStatusMapping(t)
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewGitHubWorkItemEngineDeriver(statusMapping, classifier)
	if err != nil {
		t.Fatal(err)
	}
	claim, rows, day, computedAt, derived := githubWorkItemEngineFixture(t)

	encoded, err := engine.Derive(
		context.Background(), claim, rows, day, computedAt, derived,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 3 {
		t.Fatalf("destinations=%d want=3", len(encoded))
	}

	issueTypes := decodeGitHubWorkItemEngineRows[githubIssueTypeMetricsDailyRow](
		t, encoded[githubIssueTypeMetricsDestination],
	)
	classifications := decodeGitHubWorkItemEngineRows[githubInvestmentClassificationDailyRow](
		t, encoded[githubInvestmentClassificationsDestination],
	)
	metrics := decodeGitHubWorkItemEngineRows[githubInvestmentMetricsDailyRow](
		t, encoded[githubInvestmentMetricsDestination],
	)

	repoID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	wantStamp := computedAt.Truncate(time.Second)
	wantIssueTypes := []githubIssueTypeMetricsDailyRow{
		{
			RepoID: &repoID, Day: "2026-08-04", Provider: "github",
			TeamID: "payments", IssueTypeNorm: "bug",
			CreatedCount: 1, CompletedCount: 2, ActiveCount: 2,
			CycleP50Hours: 9, CycleP90Hours: 9, LeadP50Hours: 0,
			ComputedAt: wantStamp, OrgID: claim.OrgID,
		},
		{
			Day: "2026-08-04", Provider: "github", TeamID: "unassigned",
			IssueTypeNorm: "chore", ActiveCount: 1,
			ComputedAt: wantStamp, OrgID: claim.OrgID,
		},
		{
			Day: "2026-08-04", Provider: "github", TeamID: "unassigned",
			IssueTypeNorm: "issue", ComputedAt: wantStamp, OrgID: claim.OrgID,
		},
	}
	if !reflect.DeepEqual(issueTypes, wantIssueTypes) {
		t.Fatalf("issue types:\n got: %#v\nwant: %#v", issueTypes, wantIssueTypes)
	}

	security, general, secRule := "security", "general", "sec_general"
	product, feature, featureRule := "product", "feature", "prod_feat"
	wantClassifications := []githubInvestmentClassificationDailyRow{
		{
			RepoID: &repoID, Day: "2026-08-04", ArtifactType: "work_item",
			ArtifactID: "acme/api#1", Provider: "github",
			InvestmentArea: &security, ProjectStream: general,
			Confidence: 1, RuleID: &secRule, ComputedAt: wantStamp, OrgID: claim.OrgID,
		},
		{
			RepoID: &repoID, Day: "2026-08-04", ArtifactType: "work_item",
			ArtifactID: "acme/api#2", Provider: "github",
			InvestmentArea: &security, ProjectStream: general,
			Confidence: 1, RuleID: &secRule, ComputedAt: wantStamp, OrgID: claim.OrgID,
		},
		{
			Day: "2026-08-04", ArtifactType: "work_item",
			ArtifactID: "acme/api#3", Provider: "github",
			InvestmentArea: &product, ProjectStream: feature,
			Confidence: 1, RuleID: &featureRule, ComputedAt: wantStamp, OrgID: claim.OrgID,
		},
	}
	if !reflect.DeepEqual(classifications, wantClassifications) {
		t.Fatalf("classifications:\n got: %#v\nwant: %#v", classifications, wantClassifications)
	}

	wantMetrics := []githubInvestmentMetricsDailyRow{
		{
			RepoID: &repoID, Day: "2026-08-04", TeamID: "payments",
			InvestmentArea: &security, ProjectStream: general,
			DeliveryUnits: 4, WorkItemsCompleted: 2,
			CycleP50Hours: 9, ComputedAt: wantStamp, OrgID: claim.OrgID,
		},
	}
	if !reflect.DeepEqual(metrics, wantMetrics) {
		t.Fatalf("investment metrics:\n got: %#v\nwant: %#v", metrics, wantMetrics)
	}
}

func TestGitHubWorkItemDeriverPropagatesReachableClassifierError(t *testing.T) {
	statusMapping := loadRealStatusMapping(t)
	// This config is valid at construction time. Its first rule has an explicit
	// null match block, so the mirrored Python AttributeError is raised only
	// when a real work item reaches Classify.
	classifier, err := NewInvestmentClassifier(
		investmentConfigPath(t, "raises_match_null"),
	)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewGitHubWorkItemEngineDeriver(statusMapping, classifier)
	if err != nil {
		t.Fatal(err)
	}
	claim, rows, _, computedAt, _ := githubWorkItemEngineFixture(t)
	deriver := GitHubWorkItemDeriver{
		Source: &fakeGitHubWorkItemDerivationContextSource{},
		engine: engine,
	}

	derived, err := deriver.Derive(context.Background(), claim, rows, computedAt)
	if derived != nil {
		t.Fatalf("classifier failure returned partial derived rows=%v", derived)
	}
	var configError *InvestmentConfigError
	if !errors.As(err, &configError) || configError.PythonException != "AttributeError" {
		t.Fatalf("error=%T %v want reachable mirrored AttributeError", err, err)
	}
}

func TestGitHubWorkItemEngineDeriverSuppliesPythonWorkItemArtifactShape(t *testing.T) {
	statusMapping := loadRealStatusMapping(t)
	claim, rows, day, computedAt, derived := githubWorkItemEngineFixture(t)
	rows.WorkItems = rows.WorkItems[:1]

	t.Run("component is present and empty", func(t *testing.T) {
		classifier, err := NewInvestmentClassifier(
			investmentConfigPath(t, "bare_component"),
		)
		if err != nil {
			t.Fatal(err)
		}
		engine, err := NewGitHubWorkItemEngineDeriver(statusMapping, classifier)
		if err != nil {
			t.Fatal(err)
		}
		componentRows := rows
		componentRows.WorkItems = append([]githubWorkItemRow(nil), rows.WorkItems...)
		componentRows.WorkItems[0].Labels = []string{"barecomponent"}
		encoded, err := engine.Derive(
			context.Background(), claim, componentRows, day, computedAt, derived,
		)
		if err != nil {
			t.Fatal(err)
		}
		classifications := decodeGitHubWorkItemEngineRows[githubInvestmentClassificationDailyRow](
			t, encoded[githubInvestmentClassificationsDestination],
		)
		if len(classifications) != 1 || classifications[0].InvestmentArea == nil ||
			*classifications[0].InvestmentArea != "bare_component_area" ||
			classifications[0].RuleID == nil ||
			*classifications[0].RuleID != "bare_component" {
			t.Fatalf("empty component did not reach bare-string matcher: %+v", classifications)
		}
	})

	t.Run("paths are absent", func(t *testing.T) {
		classifier, err := NewInvestmentClassifier(
			investmentConfigPath(t, "path_prefix_null"),
		)
		if err != nil {
			t.Fatal(err)
		}
		engine, err := NewGitHubWorkItemEngineDeriver(statusMapping, classifier)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := engine.Derive(
			context.Background(), claim, rows, day, computedAt, derived,
		)
		if err != nil {
			t.Fatalf("path-prefix rule must stay inert without artifact paths: %v", err)
		}
		classifications := decodeGitHubWorkItemEngineRows[githubInvestmentClassificationDailyRow](
			t, encoded[githubInvestmentClassificationsDestination],
		)
		if len(classifications) != 1 || classifications[0].InvestmentArea == nil ||
			*classifications[0].InvestmentArea != legacyDefaultInvestmentArea ||
			classifications[0].ProjectStream != legacyDefaultProjectStream ||
			classifications[0].Confidence != 0 ||
			classifications[0].RuleID == nil ||
			*classifications[0].RuleID != legacyFallbackRuleID {
			t.Fatalf("pathless artifact did not take legacy fallback: %+v", classifications)
		}
	})
}

func TestGitHubWorkItemEngineClosedAtDoesNotTerminateWithoutCompletedAt(t *testing.T) {
	statusMapping := loadRealStatusMapping(t)
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewGitHubWorkItemEngineDeriver(statusMapping, classifier)
	if err != nil {
		t.Fatal(err)
	}
	claim, fixtureRows, day, computedAt, derived := githubWorkItemEngineFixture(t)
	item := fixtureRows.WorkItems[2]
	closedAt := day.Add(-time.Hour)
	item.ClosedAt, item.CompletedAt = &closedAt, nil
	encoded, err := engine.Derive(
		context.Background(), claim,
		githubWorkItemRows{WorkItems: []githubWorkItemRow{item}},
		day, computedAt, derived,
	)
	if err != nil {
		t.Fatal(err)
	}
	issueTypes := decodeGitHubWorkItemEngineRows[githubIssueTypeMetricsDailyRow](
		t, encoded[githubIssueTypeMetricsDestination],
	)
	classifications := decodeGitHubWorkItemEngineRows[githubInvestmentClassificationDailyRow](
		t, encoded[githubInvestmentClassificationsDestination],
	)
	metrics := decodeGitHubWorkItemEngineRows[githubInvestmentMetricsDailyRow](
		t, encoded[githubInvestmentMetricsDestination],
	)
	if len(issueTypes) != 1 || issueTypes[0].ActiveCount != 1 ||
		issueTypes[0].CompletedCount != 0 || len(classifications) != 1 ||
		len(metrics) != 0 {
		t.Fatalf(
			"closed-only item issue=%+v classifications=%+v metrics=%+v",
			issueTypes, classifications, metrics,
		)
	}
}

func TestGitHubWorkItemEngineClosedAtInsideDayDoesNotCreateCompletionMetric(t *testing.T) {
	statusMapping := loadRealStatusMapping(t)
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewGitHubWorkItemEngineDeriver(statusMapping, classifier)
	if err != nil {
		t.Fatal(err)
	}
	claim, fixtureRows, day, computedAt, derived := githubWorkItemEngineFixture(t)
	item := fixtureRows.WorkItems[2]
	closedAt := day.Add(10 * time.Hour)
	item.ClosedAt, item.CompletedAt = &closedAt, nil
	encoded, err := engine.Derive(
		context.Background(), claim,
		githubWorkItemRows{WorkItems: []githubWorkItemRow{item}},
		day, computedAt, derived,
	)
	if err != nil {
		t.Fatal(err)
	}
	metrics := decodeGitHubWorkItemEngineRows[githubInvestmentMetricsDailyRow](
		t, encoded[githubInvestmentMetricsDestination],
	)
	if len(metrics) != 0 {
		t.Fatalf("closed-only in-day item emitted completion metrics=%+v", metrics)
	}
}

func TestGitHubWorkItemEngineUnassignedInvestmentMetricUsesEmptyTeam(t *testing.T) {
	statusMapping := loadRealStatusMapping(t)
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewGitHubWorkItemEngineDeriver(statusMapping, classifier)
	if err != nil {
		t.Fatal(err)
	}
	claim, fixtureRows, day, computedAt, derived := githubWorkItemEngineFixture(t)
	item := fixtureRows.WorkItems[2]
	startedAt, completedAt := day.Add(2*time.Hour), day.Add(10*time.Hour)
	item.StartedAt, item.CompletedAt = &startedAt, &completedAt
	encoded, err := engine.Derive(
		context.Background(), claim,
		githubWorkItemRows{WorkItems: []githubWorkItemRow{item}},
		day, computedAt, derived,
	)
	if err != nil {
		t.Fatal(err)
	}
	metrics := decodeGitHubWorkItemEngineRows[githubInvestmentMetricsDailyRow](
		t, encoded[githubInvestmentMetricsDestination],
	)
	if len(metrics) != 1 || metrics[0].TeamID != "" {
		t.Fatalf("unassigned metric rows=%+v want one row with empty team_id", metrics)
	}
}

func githubWorkItemEngineFixture(
	t *testing.T,
) (Claim, githubWorkItemRows, time.Time, time.Time, githubWorkItemDerivationContext) {
	t.Helper()
	claim := githubWorkItemOracleClaim()
	claim.OrgID = "org-acme"
	day := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 8, 5, 0, 30, 17, 987654321, time.UTC)
	repoID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	stringValue := func(value string) *string { return &value }
	floatValue := func(value float64) *float64 { return &value }
	timeValue := func(value time.Time) *time.Time { return &value }
	item := func(id string) githubWorkItemRow {
		return githubWorkItemRow{
			WorkItemID: id, Provider: "github", Title: id, Type: "issue",
			Status: "todo", ProjectID: stringValue("acme/api"),
			CreatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			UpdatedAt: day, OrgID: claim.OrgID,
		}
	}
	first := item("acme/api#1")
	first.RepoID = &repoID
	first.Type = "pull_request"
	first.Labels = []string{"bug", "security"}
	first.CreatedAt = time.Date(2026, 8, 4, 1, 0, 0, 0, time.UTC)
	first.StartedAt = timeValue(time.Date(2026, 8, 4, 2, 0, 0, 0, time.UTC))
	first.CompletedAt = timeValue(time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC))
	first.StoryPoints = floatValue(3.7)

	second := item("acme/api#2")
	second.RepoID = &repoID
	second.Labels = []string{"security", "bug"}
	second.StartedAt = timeValue(time.Date(2026, 8, 4, 5, 0, 0, 0, time.UTC))
	second.CompletedAt = timeValue(time.Date(2026, 8, 4, 14, 0, 0, 0, time.UTC))
	second.StoryPoints = floatValue(0)

	third := item("acme/api#3")
	third.ProjectID = stringValue("acme/other")
	third.Labels = []string{"feature", "chore"}

	future := item("acme/api#4")
	future.ProjectID = stringValue("acme/other")
	future.CreatedAt = time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)

	prior := item("acme/api#5")
	prior.ProjectID = stringValue("acme/other")
	prior.Labels = []string{"maintenance"}
	prior.CompletedAt = timeValue(time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))

	repoIDString := repoID.String()
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{
		Repos: []githubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "payments", TeamName: "Payments",
			RepoID: &repoIDString, RepoFullName: "acme/api", IsPrimary: 1,
			Specificity: 100, Priority: 10,
			UpdatedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		}},
	})
	return claim, githubWorkItemRows{WorkItems: []githubWorkItemRow{
		first, second, third, future, prior,
	}}, day, computedAt, derived
}

func decodeGitHubWorkItemEngineRows[T any](
	t *testing.T, rows []json.RawMessage,
) []T {
	t.Helper()
	result := make([]T, 0, len(rows))
	for _, raw := range rows {
		var row T
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	return result
}
