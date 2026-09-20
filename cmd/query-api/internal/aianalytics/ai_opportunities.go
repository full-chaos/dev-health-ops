package aianalytics

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	minPRs                  = 10
	repetitiveClusterMinPRs = 5
	maxOpportunityLimit     = 100
	testGenGapThreshold     = 0.50
	depUpdateMinPRs         = 5
	migrationMinPRs         = 5
	docDriftMinCodeCommits  = 20
	flakyMinCases           = 50
	flakyRateThreshold      = 0.05
)

// opportunityKinds maps a kind to its stored spelling (the text its stable id
// is hashed from) and its GraphQL enum value.
var opportunityKinds = map[model.AIOpportunityKind]string{
	model.AIOpportunityKindRepetitiveChange:     "repetitive_change",
	model.AIOpportunityKindHighReviewLoad:       "high_review_load",
	model.AIOpportunityKindHighRework:           "high_rework",
	model.AIOpportunityKindSlowCycle:            "slow_cycle",
	model.AIOpportunityKindUncoveredTestArea:    "uncovered_test_area",
	model.AIOpportunityKindTestGeneration:       "test_gen",
	model.AIOpportunityKindDependencyUpdates:    "dep_updates",
	model.AIOpportunityKindMechanicalMigrations: "migrations",
	model.AIOpportunityKindDocumentationDrift:   "doc_drift",
	model.AIOpportunityKindFlakyTestTriage:      "flaky_triage",
}

// impactRow is one deduplicated rollup row the metric rules read.
type impactRow struct {
	RepoID     string
	TeamID     string
	Bucket     string
	PrsTotal   int64
	ReviewsPer *float64
	CycleHours *float64
	ReworkPrs  int64
	TestGapPrs int64
}

// bucketAgg accumulates one bucket group's inputs.
type bucketAgg struct {
	prsTotal        int64
	reviewsWeighted float64
	reviewsWeight   int64
	cycleWeighted   float64
	cycleWeight     int64
	reworkPrs       int64
	testGapPrs      int64
}

func (a *bucketAgg) add(r impactRow) {
	a.prsTotal = a.prsTotal + r.PrsTotal
	if r.ReviewsPer != nil && r.PrsTotal > 0 {
		a.reviewsWeighted = a.reviewsWeighted + float64((*r.ReviewsPer)*float64(r.PrsTotal))
		a.reviewsWeight = a.reviewsWeight + r.PrsTotal
	}
	if r.CycleHours != nil && r.PrsTotal > 0 {
		a.cycleWeighted = a.cycleWeighted + float64((*r.CycleHours)*float64(r.PrsTotal))
		a.cycleWeight = a.cycleWeight + r.PrsTotal
	}
	a.reworkPrs = a.reworkPrs + r.ReworkPrs
	a.testGapPrs = a.testGapPrs + r.TestGapPrs
}

func (a *bucketAgg) reviewsPerPr() *float64 {
	return ratioOrNil(a.reviewsWeighted, float64(a.reviewsWeight))
}
func (a *bucketAgg) cycleHours() *float64 { return ratioOrNil(a.cycleWeighted, float64(a.cycleWeight)) }
func (a *bucketAgg) reworkRate() *float64 {
	return ratioOrNil(float64(a.reworkPrs), float64(a.prsTotal))
}
func (a *bucketAgg) testGapRate() *float64 { return intRatioOrNil(a.testGapPrs, a.prsTotal) }

type repoAgg struct {
	repoID string
	teamID *string
	ai     bucketAgg
	human  bucketAgg
}

// opportunity is one detected opportunity before projection.
type opportunity struct {
	kind   model.AIOpportunityKind
	repoID string
	teamID *string
	title  string
	why    string
	score  float64
	refs   []string
}

func newOpportunity(kind model.AIOpportunityKind, repoID string, teamID *string, title, why string, score float64, refs []string) opportunity {
	return opportunity{kind: kind, repoID: repoID, teamID: teamID, title: title, why: why, score: clamp01(score), refs: refs}
}

// metricOpportunities groups rollup rows by (repository, team) in first-seen
// order and applies the metric rules to each group.
func metricOpportunities(rows []impactRow) []opportunity {
	groups := map[string]*repoAgg{}
	var order []string
	for _, r := range rows {
		repo, ok := canonicalRepo(r.RepoID)
		if !ok {
			continue
		}
		r.RepoID = repo
		key := r.RepoID + "\x00" + r.TeamID
		g, ok := groups[key]
		if !ok {
			var team *string
			if r.TeamID != "" {
				t := r.TeamID
				team = &t
			}
			g = &repoAgg{repoID: r.RepoID, teamID: team}
			groups[key] = g
			order = append(order, key)
		}
		switch {
		case aiBuckets[r.Bucket]:
			g.ai.add(r)
		case r.Bucket == bucketHuman:
			g.human.add(r)
		}
	}
	out := []opportunity{}
	for _, key := range order {
		out = append(out, metricRules(groups[key])...)
	}
	return out
}

func metricRules(a *repoAgg) []opportunity {
	var out []opportunity
	humanGap := a.human.testGapRate()
	if a.human.prsTotal >= minPRs && humanGap != nil && *humanGap >= testGenGapThreshold {
		out = append(out, newOpportunity(model.AIOpportunityKindTestGeneration, a.repoID, a.teamID,
			"Test generation candidate in "+a.repoID,
			"Human-authored PRs had a "+pct(*humanGap, 0)+" test gap rate across "+strconv.FormatInt(a.human.prsTotal, 10)+" PRs over the last 30 days.",
			scoreDelta(*humanGap, testGenGapThreshold),
			[]string{"ai_impact_metrics_daily:test_gap_rate:" + a.repoID}))
	}
	if a.ai.prsTotal < minPRs {
		return out
	}
	aiReviews, humanReviews := a.ai.reviewsPerPr(), a.human.reviewsPerPr()
	if aiReviews != nil && humanReviews != nil && *humanReviews > 0 && *aiReviews >= float64(*humanReviews*1.5) {
		ratio := *aiReviews / *humanReviews
		out = append(out, newOpportunity(model.AIOpportunityKindHighReviewLoad, a.repoID, a.teamID,
			"High AI review load in "+a.repoID,
			"AI-assisted PRs averaged "+fixed(*aiReviews, 1)+" reviews vs "+fixed(*humanReviews, 1)+" for human PRs over the last 30 days.",
			scoreRatio(ratio, 1.5),
			[]string{"ai_impact_metrics_daily:reviews_per_pr:" + a.repoID}))
	}
	aiRework, humanRework := a.ai.reworkRate(), a.human.reworkRate()
	if aiRework != nil && humanRework != nil && *aiRework >= 0.25 && *aiRework-*humanRework >= 0.10 {
		out = append(out, newOpportunity(model.AIOpportunityKindHighRework, a.repoID, a.teamID,
			"High AI rework in "+a.repoID,
			"AI-assisted PRs had a "+pct(*aiRework, 0)+" rework rate vs "+pct(*humanRework, 0)+" for human PRs over the last 30 days.",
			scoreDelta(*aiRework-*humanRework, 0.10),
			[]string{"ai_impact_metrics_daily:rework_rate:" + a.repoID}))
	}
	aiCycle, humanCycle := a.ai.cycleHours(), a.human.cycleHours()
	if aiCycle != nil && humanCycle != nil && *humanCycle > 0 && *aiCycle >= float64(*humanCycle*1.25) {
		ratio := *aiCycle / *humanCycle
		out = append(out, newOpportunity(model.AIOpportunityKindSlowCycle, a.repoID, a.teamID,
			"Slow AI cycle time in "+a.repoID,
			"AI-assisted PRs averaged "+fixed(*aiCycle, 1)+" cycle hours vs "+fixed(*humanCycle, 1)+" for human PRs over the last 30 days.",
			scoreRatio(ratio, 1.25),
			[]string{"ai_impact_metrics_daily:cycle_time_avg_hours:" + a.repoID}))
	}
	aiGap := a.ai.testGapRate()
	if aiGap != nil && *aiGap >= 0.50 {
		out = append(out, newOpportunity(model.AIOpportunityKindUncoveredTestArea, a.repoID, a.teamID,
			"Uncovered AI test area in "+a.repoID,
			"AI-assisted PRs had a "+pct(*aiGap, 0)+" test gap rate over the last 30 days.",
			scoreDelta(*aiGap, 0.50),
			[]string{"ai_impact_metrics_daily:test_gap_rate:" + a.repoID}))
	}
	return out
}

// project renders an opportunity as its response object.
func (o opportunity) project() model.AIOpportunity {
	drill := []model.AIWorkGraphDrilldownRef{}
	for _, ref := range o.refs {
		parts := strings.Split(ref, ":")
		if len(parts) != 3 || parts[0] != "git_pull_requests" {
			continue
		}
		drill = append(drill, model.AIWorkGraphDrilldownRef{RootType: "pr", RootID: parts[1] + "#" + parts[2], Label: "PR " + parts[2]})
	}
	team := ""
	if o.teamID != nil {
		team = *o.teamID
	}
	repo := o.repoID
	return model.AIOpportunity{
		OpportunityID:       stableOpportunityID(opportunityKinds[o.kind], o.repoID, team),
		Kind:                o.kind,
		RepoID:              &repo,
		TeamID:              o.teamID,
		Title:               o.title,
		Rationale:           o.why,
		Score:               o.score,
		EvidenceRefs:        o.refs,
		WorkGraphDrilldowns: drill,
	}
}

// ---- readers --------------------------------------------------------------

func opportunityBindings(orgID string, repoID string, extra ...clickhouse.Binding) []clickhouse.Binding {
	b := []clickhouse.Binding{{Name: "org_id", Value: orgID}}
	if repoID != "" {
		b = append(b, clickhouse.Binding{Name: "repo_id", Value: repoID})
	}
	return append(b, extra...)
}

func loadImpactRowsForOpportunities(ctx context.Context, client QueryClient, orgID, repoID, teamID string) ([]impactRow, error) {
	filters := []string{"day >= today() - 30"}
	bindings := opportunityBindings(orgID, repoID)
	if repoID != "" {
		filters = append(filters, "repo_id = {repo_id:UUID}")
	}
	if teamID != "" {
		bindings = append(bindings, clickhouse.Binding{Name: "team_id", Value: teamID})
		filters = append(filters, "team_id = {team_id:String}")
	}
	filters = append(filters, "org_id = {org_id:String}")
	rs, err := client.Query(ctx, `SELECT
    team_id,
    toString(repo_id) AS repo_id_str,
    attribution_bucket,
    argMax(prs_total, metrics.computed_at) AS prs_total,
    argMax(reviews_per_pr, metrics.computed_at) AS reviews_per_pr,
    argMax(cycle_time_avg_hours, metrics.computed_at) AS cycle_time_avg_hours,
    argMax(rework_prs, metrics.computed_at) AS rework_prs,
    argMax(test_gap_prs, metrics.computed_at) AS test_gap_prs
FROM ai_impact_metrics_daily AS metrics
WHERE `+strings.Join(filters, " AND ")+`
GROUP BY org_id, team_id, repo_id, work_type, day, attribution_bucket`, bindings)
	if err != nil {
		return nil, fmt.Errorf("aianalytics: opportunity impact query: %w", err)
	}
	defer rs.Close()
	var out []impactRow
	for rs.Next() {
		var (
			r                    impactRow
			prs, rework, testGap uint32
		)
		if err := rs.Scan(&r.TeamID, &r.RepoID, &r.Bucket, &prs, &r.ReviewsPer, &r.CycleHours, &rework, &testGap); err != nil {
			return nil, fmt.Errorf("aianalytics: opportunity impact scan: %w", err)
		}
		r.PrsTotal, r.ReworkPrs, r.TestGapPrs = int64(prs), int64(rework), int64(testGap)
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: opportunity impact rows: %w", err)
	}
	return out, nil
}

// clusterRow is one grouped row of a cluster read.
type clusterRow struct {
	RepoID string
	Count  int64
	Prefix string
	Refs   []string
}

func loadClusters(ctx context.Context, client QueryClient, statement string, bindings []clickhouse.Binding, withPrefix bool) ([]clusterRow, error) {
	rs, err := client.Query(ctx, statement, bindings)
	if err != nil {
		return nil, fmt.Errorf("aianalytics: opportunity cluster query: %w", err)
	}
	defer rs.Close()
	var out []clusterRow
	for rs.Next() {
		var (
			r clusterRow
			n uint64
		)
		if withPrefix {
			if err := rs.Scan(&r.RepoID, &r.Prefix, &n, &r.Refs); err != nil {
				return nil, fmt.Errorf("aianalytics: opportunity cluster scan: %w", err)
			}
		} else if err := rs.Scan(&r.RepoID, &n, &r.Refs); err != nil {
			return nil, fmt.Errorf("aianalytics: opportunity cluster scan: %w", err)
		}
		r.Count = int64(n)
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: opportunity cluster rows: %w", err)
	}
	return out, nil
}

func firstFive(refs []string) []string {
	if len(refs) > 5 {
		return refs[:5]
	}
	return refs
}

const repetitiveStatement = `SELECT
    toString(pr.repo_id) AS repo_id,
    lower(arrayStringConcat(arraySlice(splitByChar(' ', coalesce(pr.title, '')), 1, 3), ' ')) AS title_prefix,
    count() AS prs_total,
    groupArray(concat('git_pull_requests:', toString(pr.repo_id), ':', toString(pr.number))) AS pr_refs
FROM git_pull_requests AS pr
LEFT JOIN work_graph_issue_pr AS link
    ON link.repo_id = pr.repo_id AND link.pr_number = pr.number
    AND link.org_id = {org_id:String}
INNER JOIN ai_attribution_resolved AS attr
    ON attr.repo_id = pr.repo_id
    AND attr.subject_type = 'pull_request'
    AND (attr.subject_id = toString(pr.number) OR attr.subject_id = link.work_item_id)
    AND toString(attr.org_id) = {org_id:String}
LEFT JOIN work_items AS wi FINAL
    ON wi.repo_id = link.repo_id AND wi.work_item_id = link.work_item_id
    AND wi.org_id = {org_id:String}
WHERE pr.created_at >= now() - INTERVAL 30 DAY@@REPO@@
  AND toString(attr.org_id) = {org_id:String}
  AND pr.org_id = {org_id:String}
  AND attr.kind IN ('ai_assisted', 'agent_created', 'ai_review')
  AND coalesce(pr.title, '') != ''
GROUP BY repo_id, coalesce(pr.author_email, pr.author_name, ''), coalesce(nullIf(wi.type, ''), 'pull_request'), title_prefix
HAVING prs_total >= {cluster_min:UInt32}
ORDER BY prs_total DESC
LIMIT 100`

type titleRule struct {
	kind      model.AIOpportunityKind
	minPRs    int
	titleExpr string
	title     string
	why       string
	metricRef string
}

var titlePatternRules = []titleRule{
	{
		kind:   model.AIOpportunityKindDependencyUpdates,
		minPRs: depUpdateMinPRs,
		titleExpr: "match(lower(coalesce(pr.title, '')), '^(bump|update|upgrade|chore\\\\(deps\\\\)|build\\\\(deps\\\\))')" +
			" AND match(lower(coalesce(pr.title, '')), '(depend|deps|version|package|requirement|lockfile| from .* to )')",
		title:     "Manual dependency updates in %s",
		why:       "%d dependency-update PRs were authored by humans in the last 30 days; dependency bumps are a documented AI automation target.",
		metricRef: "git_pull_requests:dependency_update_prs",
	},
	{
		kind:      model.AIOpportunityKindMechanicalMigrations,
		minPRs:    migrationMinPRs,
		titleExpr: "match(lower(coalesce(pr.title, '')), '(migrat|mass rename|codemod|deprecat.* api|bulk (rename|move))')",
		title:     "Mechanical migration toil in %s",
		why:       "%d migration-style PRs were authored by humans in the last 30 days; mechanical migrations are a documented AI automation target.",
		metricRef: "git_pull_requests:migration_prs",
	},
}

const titleToilStatement = `SELECT
    toString(pr.repo_id) AS repo_id,
    count() AS prs_total,
    groupArray(concat('git_pull_requests:', toString(pr.repo_id), ':', toString(pr.number))) AS pr_refs
FROM git_pull_requests AS pr
LEFT ANTI JOIN ai_attribution_resolved AS attr
    ON attr.repo_id = pr.repo_id
    AND attr.subject_type = 'pull_request'
    AND attr.subject_id = toString(pr.number)
    AND attr.kind IN ('ai_assisted', 'agent_created')
    AND toString(attr.org_id) = {org_id:String}
WHERE pr.created_at >= now() - INTERVAL 30 DAY
  AND @@TITLE@@
  AND lower(coalesce(pr.author_name, '')) NOT LIKE '%bot%'
  AND lower(coalesce(pr.author_name, '')) NOT LIKE '%renovate%'@@REPO@@
  AND pr.org_id = {org_id:String}
GROUP BY repo_id
HAVING prs_total >= {min_prs:UInt32}
ORDER BY prs_total DESC
LIMIT 100`

const docFileExpr = "(file_path LIKE '%.md' OR file_path LIKE '%.rst' OR file_path LIKE '%.adoc'" +
	" OR file_path LIKE 'docs/%' OR file_path LIKE '%/docs/%')"

const docDriftStatement = `SELECT
    toString(c.repo_id) AS repo_id,
    uniqExactIf(c.hash, NOT ` + docFileExpr + `) AS code_commits,
    countIf(` + docFileExpr + `) AS doc_changes
FROM git_commits AS c
INNER JOIN git_commit_stats AS s
    ON s.repo_id = c.repo_id
    AND s.commit_hash = c.hash
    AND s.org_id = c.org_id
WHERE c.committer_when >= now() - INTERVAL 30 DAY@@REPO@@
  AND c.org_id = {org_id:String}
GROUP BY repo_id
HAVING code_commits >= {min_commits:UInt32} AND doc_changes = 0
ORDER BY code_commits DESC
LIMIT 100`

const flakyStatement = `SELECT
    toString(repo_id) AS repo_id,
    sum(total_cases) AS cases_total,
    sum(flake_rate * total_cases) / sum(total_cases) AS weighted_flake_rate
FROM (
    SELECT *
    FROM testops_test_metrics_daily
    WHERE org_id = {org_id:String}
    ORDER BY computed_at DESC
    LIMIT 1 BY org_id, repo_id, day
) AS testops_test_metrics_daily
WHERE day >= today() - 30@@REPO@@
  AND org_id = {org_id:String}
GROUP BY repo_id
HAVING cases_total >= {min_cases:UInt64}
   AND weighted_flake_rate >= {flake_threshold:Float64}
ORDER BY weighted_flake_rate DESC
LIMIT 100`

// withRepo splices the optional repository predicate into a statement.
func withRepo(statement, predicate string) string {
	return strings.ReplaceAll(statement, "@@REPO@@", predicate)
}

func repoFilter(repoID, column string) string {
	if repoID == "" {
		return ""
	}
	return "\n  AND " + column + " = {repo_id:UUID}"
}

// boundedOpportunityLimit is the response size: absent or non-positive means
// the default of 25, and no request exceeds 100.
func boundedOpportunityLimit(limit int) int {
	if limit <= 0 {
		return 25
	}
	if limit > maxOpportunityLimit {
		return maxOpportunityLimit
	}
	return limit
}

// AiOpportunities answers aiOpportunities.
func AiOpportunities(ctx context.Context, client QueryClient, orgID string, in *model.AIScopeInput, limit int) (*model.AIOpportunitiesResult, error) {
	if client == nil {
		return nil, fmt.Errorf("aianalytics: clickhouse client is required")
	}
	sc, err := normalizeScope(ctx, client, orgID, in)
	if err != nil {
		return nil, err
	}
	if sc.unresolved {
		return &model.AIOpportunitiesResult{OrgID: orgID, Recommendations: []model.AIOpportunity{}, DetectorReady: true}, nil
	}
	bounded := boundedOpportunityLimit(limit)

	impact, err := loadImpactRowsForOpportunities(ctx, client, orgID, sc.repoID, sc.teamID)
	if err != nil {
		return nil, err
	}
	opps := metricOpportunities(impact)
	if sc.teamID == "" {
		more, err := workflowOpportunities(ctx, client, orgID, sc.repoID)
		if err != nil {
			return nil, err
		}
		opps = append(opps, more...)
	}
	sort.SliceStable(opps, func(i, j int) bool { return opps[i].score > opps[j].score })
	if len(opps) > bounded {
		opps = opps[:bounded]
	}
	recs := make([]model.AIOpportunity, 0, len(opps))
	for _, o := range opps {
		recs = append(recs, o.project())
	}
	return &model.AIOpportunitiesResult{OrgID: orgID, Recommendations: recs, DetectorReady: true}, nil
}

// workflowOpportunities runs the repetitive-change, title-pattern,
// documentation-drift and flaky-test rules, none of which carries a team scope.
func workflowOpportunities(ctx context.Context, client QueryClient, orgID, repoID string) ([]opportunity, error) {
	var out []opportunity

	rep, err := loadClusters(ctx, client, withRepo(repetitiveStatement, repoFilter(repoID, "pr.repo_id")),
		opportunityBindings(orgID, repoID, clickhouse.Binding{Name: "cluster_min", Value: uint32(repetitiveClusterMinPRs)}), true)
	if err != nil {
		return nil, err
	}
	for _, r := range rep {
		refs := firstFive(r.Refs)
		if len(refs) == 0 {
			continue
		}
		prefix := r.Prefix
		if prefix == "" {
			prefix = "similar"
		}
		repo, ok := canonicalRepo(r.RepoID)
		if !ok {
			continue
		}
		out = append(out, newOpportunity(model.AIOpportunityKindRepetitiveChange, repo, nil,
			"Repetitive AI change pattern in "+repo,
			strconv.FormatInt(r.Count, 10)+" AI-assisted PRs shared the title prefix '"+prefix+"' with the same author/work type over the last 30 days.",
			scoreDelta(float64(r.Count)/float64(repetitiveClusterMinPRs), 1.0), refs))
	}

	for _, rule := range titlePatternRules {
		st := strings.Replace(withRepo(titleToilStatement, repoFilter(repoID, "pr.repo_id")), "@@TITLE@@", rule.titleExpr, 1)
		rows, err := loadClusters(ctx, client, st,
			opportunityBindings(orgID, repoID, clickhouse.Binding{Name: "min_prs", Value: uint32(rule.minPRs)}), false)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			repo, ok := canonicalRepo(r.RepoID)
			if !ok {
				continue
			}
			refs := firstFive(r.Refs)
			if len(refs) == 0 {
				continue
			}
			refs = append(append([]string(nil), refs...), rule.metricRef+":"+repo)
			out = append(out, newOpportunity(rule.kind, repo, nil,
				fmt.Sprintf(rule.title, repo), fmt.Sprintf(rule.why, r.Count),
				scoreDelta(float64(r.Count)/float64(rule.minPRs), 1.0), refs))
		}
	}

	drift, err := loadDocDrift(ctx, client, orgID, repoID)
	if err != nil {
		return nil, err
	}
	out = append(out, drift...)
	flaky, err := loadFlaky(ctx, client, orgID, repoID)
	if err != nil {
		return nil, err
	}
	return append(out, flaky...), nil
}

// canonicalRepo re-renders a repository id the way a UUID constructor does.
func canonicalRepo(raw string) (string, bool) {
	u, err := pythonparity.ParseUUID(raw)
	if err != nil {
		return "", false
	}
	return u.String(), true
}

func loadDocDrift(ctx context.Context, client QueryClient, orgID, repoID string) ([]opportunity, error) {
	rs, err := client.Query(ctx, withRepo(docDriftStatement, repoFilter(repoID, "c.repo_id")),
		opportunityBindings(orgID, repoID, clickhouse.Binding{Name: "min_commits", Value: uint32(docDriftMinCodeCommits)}))
	if err != nil {
		return nil, fmt.Errorf("aianalytics: documentation drift query: %w", err)
	}
	defer rs.Close()
	var out []opportunity
	for rs.Next() {
		var (
			id            string
			commits, docs uint64
		)
		if err := rs.Scan(&id, &commits, &docs); err != nil {
			return nil, fmt.Errorf("aianalytics: documentation drift scan: %w", err)
		}
		repo, ok := canonicalRepo(id)
		if !ok {
			continue
		}
		out = append(out, newOpportunity(model.AIOpportunityKindDocumentationDrift, repo, nil,
			"Documentation drift in "+repo,
			strconv.FormatUint(commits, 10)+" code commits landed in the last 30 days with zero documentation-file changes.",
			scoreDelta(float64(commits)/float64(docDriftMinCodeCommits), 1.0),
			[]string{"git_commit_stats:doc_changes:" + repo}))
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: documentation drift rows: %w", err)
	}
	return out, nil
}

func loadFlaky(ctx context.Context, client QueryClient, orgID, repoID string) ([]opportunity, error) {
	rs, err := client.Query(ctx, withRepo(flakyStatement, repoFilter(repoID, "testops_test_metrics_daily.repo_id")),
		opportunityBindings(orgID, repoID,
			clickhouse.Binding{Name: "min_cases", Value: uint64(flakyMinCases)},
			clickhouse.Binding{Name: "flake_threshold", Value: strconv.FormatFloat(flakyRateThreshold, 'g', -1, 64)}))
	if err != nil {
		return nil, fmt.Errorf("aianalytics: flaky test query: %w", err)
	}
	defer rs.Close()
	var out []opportunity
	for rs.Next() {
		var (
			id    string
			cases uint64
			rate  float64
		)
		if err := rs.Scan(&id, &cases, &rate); err != nil {
			return nil, fmt.Errorf("aianalytics: flaky test scan: %w", err)
		}
		repo, ok := canonicalRepo(id)
		if !ok {
			continue
		}
		out = append(out, newOpportunity(model.AIOpportunityKindFlakyTestTriage, repo, nil,
			"Flaky test triage candidate in "+repo,
			"Test cases flaked at "+pct(rate, 1)+" across "+strconv.FormatUint(cases, 10)+" executions over the last 30 days.",
			scoreDelta(rate, flakyRateThreshold),
			[]string{"testops_test_metrics_daily:flake_rate:" + repo}))
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: flaky test rows: %w", err)
	}
	return out, nil
}
