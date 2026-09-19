package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/types"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/tools/go/packages"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// preparedRouteRowTypes are the Go types each enrolled destination's sinks
// decode their rows into, one per sink when providers' sinks differ.
var preparedRouteRowTypes = map[string][]any{
	"deployments":                             {deploymentRow{}},
	"git_pull_requests":                       {pullRequestRow{}},
	"git_pull_request_reviews":                {pullRequestReviewRow{}},
	"ai_attribution":                          {githubAIAttributionRow{}},
	"estimate_coverage_metrics_daily":         {githubEstimateCoverageMetricsDailyRow{}},
	"investment_classifications_daily":        {githubInvestmentClassificationDailyRow{}},
	"investment_metrics_daily":                {githubInvestmentMetricsDailyRow{}},
	"issue_type_metrics_daily":                {githubIssueTypeMetricsDailyRow{}},
	"sprints":                                 {githubSprintRow{}},
	"work_item_cycle_times":                   {githubWorkItemCycleTimePersistenceRow{}},
	"work_item_dependencies":                  {githubWorkItemDependencyRow{}},
	"work_item_interactions":                  {githubWorkItemInteractionRow{}},
	"work_item_metrics_daily":                 {githubWorkItemMetricsDailyRow{}},
	"work_item_reopen_events":                 {githubWorkItemReopenRow{}},
	"work_item_state_durations_daily":         {githubWorkItemStateDurationDailyRow{}},
	"work_item_team_attributions":             {githubWorkItemTeamAttributionRow{}},
	"work_item_transitions":                   {githubWorkItemTransitionRow{}},
	"work_item_user_metrics_daily":            {githubWorkItemUserMetricsDailyRow{}},
	"project_membership_transitions":          {projectmembership.Row{}},
	"projects":                                {projectmembership.CatalogRow{}},
	"work_items":                              {githubWorkItemRow{}},
	"git_commit_stats":                        {commitStatsRow{}},
	"git_commits":                             {gitCommitRow{}},
	"git_files":                               {gitFileRow{}},
	"repos":                                   {repositoryRow{}},
	"security_alerts":                         {securityAlertRow{}, gitLabSecurityAlertRow{}},
	"feature_flag":                            {launchDarklyFlagRow{}},
	"feature_flag_event":                      {launchDarklyEventRow{}},
	"feature_flag_link":                       {launchDarklyLinkRow{}},
	"work_graph_edges":                        {launchDarklyEdgeRow{}},
	"operational_services":                    {gitLabOperationalServiceRow{}},
	"operational_service_repository_mappings": {gitLabServiceRepositoryMappingRow{}},
	"operational_incidents":                   {jiraIncidentRow{}},
}

// preparedRouteDroppedKeys are the row keys the projection drops: keys a
// route builds that no sink writes or reads. A change here is a change to what
// a prepared route persists.
var preparedRouteDroppedKeys = map[string][]string{
	"work_item_transitions": {"provider"},
	"work_items":            {"description", "due_at", "priority_raw", "service_class"},
}

func jsonTagNames(typ reflect.Type) []string {
	var names []string
	for index := range typ.NumField() {
		field := typ.Field(index)
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			names = append(names, jsonTagNames(field.Type)...)
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "-" || !field.IsExported() {
			continue
		}
		if name == "" {
			name = field.Name
		}
		names = append(names, name)
	}
	return names
}

func enrolledPreparedRouteDestinations(t *testing.T) []string {
	t.Helper()
	seen := map[string]bool{}
	for _, provider := range []string{"github", "gitlab", "jira", "linear", "launchdarkly", "pagerduty"} {
		for _, dataset := range []string{"work-items", "deployments", "prs", "commits", "commit-stats", "files", "repo-metadata", "security", "feature-flags", "incidents"} {
			destinations, ok := preparedManifestRouteDestinations(provider, dataset)
			if !ok {
				continue
			}
			for _, destination := range destinations {
				seen[destination] = true
			}
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

func TestEveryEnrolledDestinationProjectsOntoItsSinkInsert(t *testing.T) {
	destinations := enrolledPreparedRouteDestinations(t)
	if len(destinations) == 0 {
		t.Fatal("no enrolled destinations")
	}
	for _, destination := range destinations {
		keys, ok := preparedRouteColumns(destination)
		if !ok {
			t.Fatalf("enrolled destination %q has no INSERT to project onto", destination)
		}
		keys = maps.Clone(keys)
		for key := range preparedRouteSinkReadKeys[destination] {
			keys[key] = true
		}
		rowTypes := preparedRouteRowTypes[destination]
		if len(rowTypes) == 0 {
			t.Fatalf("enrolled destination %q has no pinned row type", destination)
		}
		for _, rowType := range rowTypes {
			var dropped []string
			for _, name := range jsonTagNames(reflect.TypeOf(rowType)) {
				if !keys[name] {
					dropped = append(dropped, name)
				}
			}
			sort.Strings(dropped)
			if strings.Join(dropped, ",") != strings.Join(preparedRouteDroppedKeys[destination], ",") {
				t.Errorf("%s %T: projection drops %v, pinned %v", destination, rowType, dropped, preparedRouteDroppedKeys[destination])
			}
		}
	}
}

func TestSinkReadKeysAreRouteSignalsNotText(t *testing.T) {
	allowed := map[string]map[string]reflect.Kind{
		"deployments":                 {"lifecycle_lookup_failed": reflect.Bool, "pull_request_lookup_failed": reflect.Bool},
		"git_pull_requests":           {"reviews_lookup_failed": reflect.Bool},
		"work_item_team_attributions": {"ownership_reason": reflect.String, "priority": reflect.Int},
	}
	kinds := map[sinkReadKind]reflect.Kind{sinkReadBool: reflect.Bool, sinkReadInt: reflect.Int, sinkReadEnum: reflect.String}
	if len(preparedRouteSinkReadKeys) != len(allowed) {
		t.Fatalf("sink-read destinations=%d want %d", len(preparedRouteSinkReadKeys), len(allowed))
	}
	if len(preparedRouteSinkReadKeys) == 0 {
		t.Fatal("no sink-read keys")
	}
	for destination, keys := range preparedRouteSinkReadKeys {
		if len(preparedRouteRowTypes[destination]) != 1 {
			t.Fatalf("%s: sink-read keys need exactly one pinned row type", destination)
		}
		rowType := reflect.TypeOf(preparedRouteRowTypes[destination][0])
		if len(keys) == 0 {
			t.Fatalf("%s: empty sink-read list", destination)
		}
		for key, signal := range keys {
			want, ok := allowed[destination][key]
			if !ok || kinds[signal.kind] != want {
				t.Fatalf("%s: sink-read key %q kind %v is not pinned as %s", destination, key, signal.kind, want)
			}
			if (signal.kind == sinkReadEnum) != (len(signal.values) > 0) {
				t.Fatalf("%s: sink-read key %q: a string signal needs a closed value set, and only a string signal has one", destination, key)
			}
			found := false
			for index := range rowType.NumField() {
				field := rowType.Field(index)
				if name, _, _ := strings.Cut(field.Tag.Get("json"), ","); name == key {
					found = field.Type.Kind() == want
				}
			}
			if !found {
				t.Fatalf("%s: sink-read key %q is not a %s field of %s", destination, key, want, rowType)
			}
		}
	}
}

// TestProjectionStatementsAreTheSinksOwnInserts pins that each destination's
// mapped statement is the one its own sink writes with: some function passes
// the statement to a call AND decodes that destination's pinned row type.
func TestProjectionStatementsAreTheSinksOwnInserts(t *testing.T) {
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  "../..",
	}, "./internal/providersync")
	if err != nil || packages.PrintErrors(loaded) > 0 {
		t.Fatalf("load: %v", err)
	}
	mapped := map[string]string{}
	type writer struct{ arguments, indexes, calls map[string]bool }
	var writers []writer
	indexesByFunction := map[string]map[string]bool{}
	for _, pkg := range loaded {
		for _, file := range pkg.Syntax {
			for _, declaration := range file.Decls {
				switch typed := declaration.(type) {
				case *ast.GenDecl:
					for _, spec := range typed.Specs {
						value, ok := spec.(*ast.ValueSpec)
						if !ok || len(value.Names) != 1 || value.Names[0].Name != "preparedRouteInsertStatements" || len(value.Values) != 1 {
							continue
						}
						literal, ok := value.Values[0].(*ast.CompositeLit)
						if !ok {
							continue
						}
						for _, element := range literal.Elts {
							if pair, ok := element.(*ast.KeyValueExpr); ok {
								mapped[strings.Trim(types.ExprString(pair.Key), `"`)] = types.ExprString(pair.Value)
							}
						}
					}
				case *ast.FuncDecl:
					if typed.Body == nil {
						continue
					}
					found := writer{arguments: map[string]bool{}, indexes: map[string]bool{}, calls: map[string]bool{}}
					// A writer handed its decoded rows as a parameter decodes them
					// in its caller: a typed []Row parameter counts as the decode.
					for _, field := range typed.Type.Params.List {
						found.indexes[strings.TrimPrefix(types.ExprString(field.Type), "[]")] = true
					}
					ast.Inspect(typed.Body, func(node ast.Node) bool {
						switch inner := node.(type) {
						case *ast.CallExpr:
							if name, ok := inner.Fun.(*ast.Ident); ok {
								found.calls[name.Name] = true
							}
							for _, argument := range inner.Args {
								found.arguments[types.ExprString(argument)] = true
							}
						case *ast.IndexExpr:
							found.indexes[types.ExprString(inner.Index)] = true
						}
						return true
					})
					writers = append(writers, found)
					if typed.Recv == nil {
						indexesByFunction[typed.Name.Name] = found.indexes
					}
				}
			}
		}
	}
	if len(mapped) != len(preparedRouteInsertStatements) {
		t.Fatalf("mapped statements=%d want %d", len(mapped), len(preparedRouteInsertStatements))
	}
	if len(mapped) == 0 {
		t.Fatal("no mapped statements")
	}
	// A writer decodes a row type itself or through a package function it
	// calls (one hop: a decode helper).
	decodes := func(candidate writer, rowName string) bool {
		if candidate.indexes[rowName] {
			return true
		}
		for called := range candidate.calls {
			if indexesByFunction[called][rowName] {
				return true
			}
		}
		return false
	}
	for destination, statement := range mapped {
		for _, row := range preparedRouteRowTypes[destination] {
			rowType := reflect.TypeOf(row)
			rowName := rowType.Name()
			if strings.HasSuffix(rowType.PkgPath(), "/projectmembership") {
				rowName = "projectmembership." + rowName
			}
			owned := false
			for _, candidate := range writers {
				owned = owned || (candidate.arguments[statement] && decodes(candidate, rowName))
			}
			if !owned {
				t.Errorf("%s: no function writes with %s and decodes %s", destination, statement, rowName)
			}
		}
	}
}

func TestProjectionFollowsTheInsertColumnList(t *testing.T) {
	keys, ok := preparedRouteColumns("work_items")
	if !ok || keys["description"] || !keys["title"] || !keys["org_id"] {
		t.Fatalf("work_items keys=%v", keys)
	}
	widened := strings.Replace(preparedRouteInsertStatements["work_items"], "source_id)", "source_id, description)", 1)
	if keys := projectionKeys(widened); !keys["description"] {
		t.Fatal("a column added to the INSERT is not kept by the projection")
	}
}

type projectedRowSink struct {
	mu   sync.Mutex
	rows map[string][]json.RawMessage
}

func (sink *projectedRowSink) WriteEffect(_ context.Context, _ Claim, batch EffectBatch) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.rows == nil {
		sink.rows = map[string][]json.RawMessage{}
	}
	sink.rows[batch.Destination] = append(sink.rows[batch.Destination], batch.Rows...)
	return nil
}

func (sink *projectedRowSink) InspectEffect(context.Context, Claim, EffectBatch) (EffectInspection, error) {
	return EffectAbsent, nil
}

func deploymentsBatchWithRouteText(t *testing.T, claim Claim) CompleteRouteBatch {
	t.Helper()
	row := map[string]any{}
	encoded, err := json.Marshal(deploymentEffectsUnitRow(claim, "901"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &row); err != nil {
		t.Fatal(err)
	}
	row["lifecycle_lookup_failed"] = true
	row["description"] = "provider-text-not-written"
	effect, err := effectBatchFromValues("deployments", EffectReadbackRequired, []map[string]any{row})
	if err != nil {
		t.Fatal(err)
	}
	batch := preparedDeploymentsBatch(t, claim)
	batch.Effects = []EffectBatch{effect}
	return batch
}

func TestPreparedRouteCommitsAndSnapshotsOnlyTheProjection(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedWorkItemsSession(t, now, "github", "deployments")
	batch := deploymentsBatchWithRouteText(t, claim)
	ledger := &memoryEffectLedger{}
	sink := &projectedRowSink{}
	if _, err := preparedGitHubExecutor(now, &staticCompleteRouteHandler{batch: batch}, ledger, sink).
		Execute(context.Background(), session, preparedDeploymentsDescriptor(t)); err != nil {
		t.Fatal(err)
	}
	written := sink.rows["deployments"]
	if len(written) != 1 || strings.Contains(string(written[0]), "provider-text-not-written") ||
		!strings.Contains(string(written[0]), `"lifecycle_lookup_failed":true`) {
		t.Fatalf("sink rows=%s, want the projection with the sink-read signal", written)
	}
	if len(ledger.preparedSnapshot) == 0 || strings.Contains(string(ledger.preparedSnapshot), "provider-text-not-written") {
		t.Fatalf("snapshot=%s, want a snapshot without the dropped key", ledger.preparedSnapshot)
	}
	projected, err := projectPreparedRouteEffects(batch.Effects)
	if err != nil {
		t.Fatal(err)
	}
	if !effectDigestsMatchLedger(projected, ledger.state) || effectDigestsMatchLedger(batch.Effects, ledger.state) {
		t.Fatal("ledger digests are not the projected rows' digests")
	}
}

func TestSnapshotRefusesAnUnprojectedRow(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, _ := preparedWorkItemsSession(t, now, "github", "deployments")
	batch := deploymentsBatchWithRouteText(t, claim)
	if _, _, err := encodePreparedRouteManifest(claim, batch, ShadowComparison{Match: true}, now); !errors.Is(err, ErrEffectRecoveryUnsafe) {
		t.Fatalf("err=%v, want a refusal of a row the sink does not write", err)
	}
	projected, err := projectPreparedRouteEffects(batch.Effects)
	if err != nil {
		t.Fatal(err)
	}
	batch.Effects = projected
	if _, _, err := encodePreparedRouteManifest(claim, batch, ShadowComparison{Match: true}, now); err != nil {
		t.Fatalf("projected batch err=%v", err)
	}
}

func TestProjectionKeepsMembershipRejections(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	batch := preparedGitHubWorkItemsFixture(t, claim)
	rejection := json.RawMessage(`{"work_item_id":"acme/api#1","provider":"github","source":"assignee_membership","reason":"repo_not_owned"}`)
	for index := range batch.Effects {
		if batch.Effects[index].Destination == githubTeamAttributionsDestination {
			batch.Effects[index].MembershipRejections = []json.RawMessage{rejection}
		}
	}
	projected, err := projectPreparedRouteEffects(batch.Effects)
	if err != nil {
		t.Fatal(err)
	}
	kept := 0
	for _, effect := range projected {
		kept += len(effect.MembershipRejections)
	}
	if kept != 1 {
		t.Fatalf("membership rejections kept=%d want 1", kept)
	}
}

func TestTeamAttributionOwnershipReasonsAreTheCascadeLabels(t *testing.T) {
	want := map[string]bool{"": true, teamattribution.GithubWorkItemDerivationOwnershipCheckedLabel(""): true}
	for _, reason := range []string{
		teamattribution.MembershipOwnershipReasonNotOwned, teamattribution.MembershipOwnershipReasonUnknown,
		teamattribution.MembershipOwnershipReasonTeamNullCarrying,
	} {
		want[teamattribution.GithubWorkItemDerivationOwnershipCheckedLabel(reason)] = true
	}
	if !maps.Equal(teamAttributionOwnershipReasons, want) {
		t.Fatalf("ownership reasons=%v want %v", teamAttributionOwnershipReasons, want)
	}
}

func TestProjectionRefusesASignalOutsideItsKindOrSet(t *testing.T) {
	log := captureSlog(t)
	for _, cell := range []struct {
		destination string
		row         map[string]any
		admitted    bool
	}{
		{"work_item_team_attributions", map[string]any{"ownership_reason": "owned", "priority": 10}, true},
		{"work_item_team_attributions", map[string]any{"ownership_reason": "", "priority": 0}, true},
		{"work_item_team_attributions", map[string]any{"ownership_reason": "free text from anywhere"}, false},
		{"work_item_team_attributions", map[string]any{"ownership_reason": 3}, false},
		{"work_item_team_attributions", map[string]any{"priority": "10"}, false},
		{"work_item_team_attributions", map[string]any{"priority": 1.5}, false},
		{"deployments", map[string]any{"lifecycle_lookup_failed": true}, true},
		{"deployments", map[string]any{"lifecycle_lookup_failed": "provider text"}, false},
		{"git_pull_requests", map[string]any{"reviews_lookup_failed": map[string]any{"x": 1}}, false},
		{"deployments", map[string]any{"lifecycle_lookup_failed": nil}, false},
		{"work_item_team_attributions", map[string]any{"priority": nil}, false},
		{"work_item_team_attributions", map[string]any{"ownership_reason": nil}, false},
	} {
		effect, err := effectBatchFromValues(cell.destination, EffectReadbackRequired, []map[string]any{cell.row})
		if err != nil {
			t.Fatal(err)
		}
		_, err = projectPreparedRouteEffects([]EffectBatch{effect})
		if (err == nil) != cell.admitted {
			t.Errorf("%s %v err=%v admitted=%v", cell.destination, cell.row, err, cell.admitted)
		}
		if !cell.admitted && !errors.Is(err, ErrEffectRecoveryUnsafe) {
			t.Errorf("%s %v err=%v, want ErrEffectRecoveryUnsafe", cell.destination, cell.row, err)
		}
	}
	for _, want := range []string{"provider_sync.prepared_projection_signal_refused", "destination=work_item_team_attributions", "key=ownership_reason"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log lacks %q: %s", want, log.String())
		}
	}
	if strings.Contains(log.String(), "free text from anywhere") || strings.Contains(log.String(), "provider text") {
		t.Fatalf("log carries a refused value: %s", log.String())
	}
}

// TestProjectionKeepsOneAdmittedValueForADuplicatedKey covers a row whose
// signal key appears twice: the projection keeps one key whose value it
// admitted, the value the sink decodes, and never stores the other.
func TestProjectionKeepsOneAdmittedValueForADuplicatedKey(t *testing.T) {
	for _, cell := range []struct {
		row      string
		admitted bool
	}{
		{`{"deployment_id":"1","lifecycle_lookup_failed":"provider text","lifecycle_lookup_failed":true}`, true},
		{`{"deployment_id":"1","lifecycle_lookup_failed":true,"lifecycle_lookup_failed":null}`, false},
	} {
		effect, err := BuildEffectBatch("deployments", EffectReadbackRequired, []json.RawMessage{json.RawMessage(cell.row)})
		if err != nil {
			t.Fatal(err)
		}
		projected, err := projectPreparedRouteEffects([]EffectBatch{effect})
		if (err == nil) != cell.admitted {
			t.Fatalf("row %s err=%v admitted=%v", cell.row, err, cell.admitted)
		}
		if err != nil {
			continue
		}
		stored := string(projected[0].Rows[0])
		if strings.Count(stored, "lifecycle_lookup_failed") != 1 || strings.Contains(stored, "provider text") {
			t.Fatalf("stored row %s", stored)
		}
		rows, err := decodeEffectRows[deploymentRow](projected[0])
		if err != nil || !rows[0].LifecycleLookupFailed {
			t.Fatalf("sink decodes %+v err=%v", rows, err)
		}
	}
}

func TestProjectionRefusesADestinationWithNoInsert(t *testing.T) {
	effect, err := effectBatchFromValues("github_blame_path_progress", EffectReadbackRequired, []map[string]string{{"path": "a"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projectPreparedRouteEffects([]EffectBatch{effect}); !errors.Is(err, ErrEffectRecoveryUnsafe) {
		t.Fatalf("err=%v", err)
	}
}

// TestLedgerWithoutSnapshotFinishesOnTheRowsItWasWrittenWith covers a ledger
// written without a snapshot: by an earlier binary (unprojected digests) or by
// a fallback commit of this one (projected digests). Each finishes on its own
// form, and only the unprojected one says so.
func TestLedgerWithoutSnapshotFinishesOnTheRowsItWasWrittenWith(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	for _, cell := range []struct {
		name        string
		unprojected bool
	}{{"earlier binary", true}, {"fallback commit", false}} {
		t.Run(cell.name, func(t *testing.T) {
			log := captureSlog(t)
			claim, session := preparedWorkItemsSession(t, now, "github", "deployments")
			batch := deploymentsBatchWithRouteText(t, claim)
			effects := batch.Effects
			if !cell.unprojected {
				projected, err := projectPreparedRouteEffects(batch.Effects)
				if err != nil {
					t.Fatal(err)
				}
				effects = projected
			}
			state, err := NewEffectLedgerState(claim, effects, now)
			if err != nil {
				t.Fatal(err)
			}
			ledger := &memoryEffectLedger{state: state}
			sink := &projectedRowSink{}
			if _, err := preparedGitHubExecutor(now, &staticCompleteRouteHandler{batch: batch}, ledger, sink).
				Execute(context.Background(), session, preparedDeploymentsDescriptor(t)); err != nil {
				t.Fatal(err)
			}
			wrote := strings.Contains(string(sink.rows["deployments"][0]), "provider-text-not-written")
			warned := strings.Contains(log.String(), "provider_sync.prepared_projection_ledger_unprojected")
			if wrote != cell.unprojected || warned != cell.unprojected {
				t.Fatalf("unprojected rows written=%v warned=%v, want %v", wrote, warned, cell.unprojected)
			}
		})
	}
}
