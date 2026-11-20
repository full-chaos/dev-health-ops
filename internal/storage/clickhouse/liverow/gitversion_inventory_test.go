package liverow

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// git_pull_requests and git_commits are ReplacingMergeTree(last_synced): until a merge a re-synced row is two rows, so a
// raw read counts it twice or reads its old text. This inventory pins WHERE those tables are named, not HOW they are
// read: every top-level declaration of non-test code under internal/ with a string literal that holds the whole token
// git_pull_requests or git_commits is listed in gitTableSites with its count of such literals and a class. A new
// declaration, a changed count or a stale key fails, so a new read is classified and given a cell before it lands.
//
// The walk is go/ast only: a string literal is unquoted and split into identifier tokens ([A-Za-z0-9_] runs); no SQL is
// parsed. NOT pinned: whether a site dedups, or dedups correctly (the cells named in the classes pin that for the
// sites read deduped); a FINAL removed from a listed site (count and key do not change); a table name built at run time
// (a fmt.Sprintf argument, a variable, a constant of another package, storedversion's Kept.Base, quadrant's
// dedupFrom); a site moved inside one declaration; SQL outside internal/ and in _test.go files.
var gitTableSites = map[string]gitTableSite{
	"apiservice/admin/retention.go:retentionResourceTypes":                                                                {1, "NAMES", "a retention resource type name; no read"},
	"goapiproof/operations.go:operationSpecs":                                                                             {1, "NAMES", "Reason text of a proof spec; no query of this process"},
	"goapiproof/restcorpus.go:pullRequestDrilldownDefects":                                                                {6, "NAMES", "Reason texts of the reference plane's declared defects; no query of this process"},
	"goapiproof/restcorpus.go:flamePRIDBoundParity":                                                                       {2, "NAMES", "Reason texts of the reference plane; no query of this process"},
	"goapiproof/restcorpus.go:heatmapDedupParity":                                                                         {3, "NAMES", "Reason texts of the reference plane; no query of this process"},
	"jobs/investment/chquery/entities.go:Reader.FetchPullRequests":                                                        {4, "DEDUPED-CELL", "FINAL; cell TestFetchPullRequestsReadsTheCurrentVersionOfAResyncedPullRequest (+3 error texts)"},
	"jobs/investment/chquery/entities.go:Reader.FetchCommits":                                                             {4, "SAFE", "raw; the materializer maps commits by id and a commit's content is fixed by its hash (+3 error texts)"},
	"jobs/metrics/daily/ai_governance_native_clickhouse.go:governanceArtifactsSQL":                                        {1, "DEDUPED", "FINAL"},
	"jobs/metrics/daily/ai_impact_incident_team_clickhouse.go:LoadAIImpactIncidentTriggers":                               {3, "SAFE", "the author read is FINAL; the two attribution arms are raw: first row per (repo, number) wins in Go and the PR set comes from the FINAL reads"},
	"jobs/metrics/daily/ai_impact_incident_team_clickhouse.go:LoadAIImpactRepoMergedPRTeams":                              {1, "DEDUPED", "FINAL"},
	"jobs/metrics/daily/ai_impact_native_clickhouse.go:LoadAIImpactPullRequests":                                          {1, "DEDUPED", "FINAL"},
	"jobs/metrics/daily/ai_impact_native_clickhouse.go:LoadAIImpactAttributions":                                          {1, "SAFE", "raw; first row per (repo, number) wins in Go, and compute reads a label only for a PR of the FINAL read LoadAIImpactPullRequests"},
	"jobs/metrics/daily/ai_impact_native_clickhouse.go:LoadAIImpactPRCommitLinkage":                                       {1, "SAFE", "raw join of git_commits; the consumers dedup by commit hash (aiimpact testChangesByPR any, followupCommitsByPR map by hash)"},
	"jobs/metrics/daily/ai_workflow_native_clickhouse.go:LoadAIWorkflowPullRequests":                                      {1, "DEDUPED", "FINAL"},
	"jobs/metrics/daily/clickhouse.go:notDiscoveredPullRequests,notDiscoveredCommits,notDiscoveredWorkItems":              {2, "NAMES", "source labels; no read"},
	"jobs/metrics/daily/clickhouse.go:notDiscoveredRepositoriesSQL":                                                       {1, "SAFE", "raw; GROUP BY repo_id: a set of repository ids"},
	"jobs/metrics/daily/compoundingrisk/clickhouse.go:reviewWindowQuery":                                                  {1, "DEDUPED", "FINAL"},
	"jobs/metrics/daily/repouser/clickhouse.go:commitStatsQuery":                                                          {1, "DEDUPED-CELL", "FINAL on git_commits and git_commit_stats; cells TestCommitStatsLoaderReadsAResyncedCommitOnce, TestCommitStatsLoaderReadsAResyncedStatRowOnce"},
	"jobs/metrics/daily/repouser/clickhouse.go:pullRequestsQuery":                                                         {1, "DEDUPED", "FINAL; cell TestPullRequestLoaderReadsCurrentVersionOfResyncedPullRequest"},
	"jobs/metrics/daily/reviewedges/clickhouse.go:pullRequestsQuery":                                                      {1, "DEDUPED", "FINAL; floor final_floor_test.go"},
	"jobs/metrics/daily/wellbeing_native_clickhouse.go:LoadWellbeingCommits":                                              {1, "DEDUPED", "FINAL"},
	"jobs/metrics/daily/work_item_engine_native_clickhouse.go:LoadWorkItemLinkedPullRequests":                             {1, "DEDUPED", "FINAL"},
	"jobs/report/delivery_pooled.go:pullRequestSizesSource":                                                               {1, "RULED", "raw legacy arm for days written before the writer read FINAL; ruled in CHAOS-14168, pinned delivery_pooled_test.go"},
	"jobs/workgraph/issuecommitedges/clickhouse.go:Loader.Load":                                                           {1, "NAMES", "error text"},
	"jobs/workgraph/issuecommitedges/clickhouse.go:Loader.loadCommits":                                                    {1, "SAFE", "raw; edges dedup by edge id and a commit message is fixed by its hash"},
	"jobs/workgraph/issuepredges/clickhouse.go:fastPathQueryBase":                                                         {1, "DEDUPED", "FINAL"},
	"jobs/workgraph/issuepredges/clickhouse.go:Loader.LoadTextParseInputs":                                                {1, "NAMES", "error text"},
	"jobs/workgraph/issuepredges/clickhouse.go:Loader.loadPullRequests":                                                   {1, "DEDUPED-CELL", "FINAL; cell TestTextParseReadsTheCurrentTextOfAResyncedPullRequest"},
	"jobs/workgraph/issuepredges/clickhouse.go:Loader.LoadHeuristicInputs":                                                {1, "NAMES", "error text"},
	"jobs/workgraph/issuepredges/clickhouse.go:Loader.loadHeuristicPullRequests":                                          {1, "SAFE", "raw (repo, number, created_at): created_at is the same in every version and the nearest pick keeps the first of a tie"},
	"jobs/workgraph/issueprlinks/clickhouse.go:Loader.Load":                                                               {1, "NAMES", "error text"},
	"jobs/workgraph/issueprlinks/clickhouse.go:Loader.loadPullRequests":                                                   {1, "DEDUPED", "FINAL"},
	"jobs/workgraph/prcommit/clickhouse.go:Loader.Load":                                                                   {2, "NAMES", "error texts"},
	"jobs/workgraph/prcommit/clickhouse.go:Loader.loadPullRequests":                                                       {1, "SAFE", "raw; a set of known pull request numbers"},
	"jobs/workgraph/prcommit/clickhouse.go:Loader.loadCommits":                                                            {1, "SAFE", "raw; links dedup by (org, repo, pr, hash) and a commit message is fixed by its hash"},
	"jobs/workgraph/prcommit/clickhouse.go:fastPathQueryBase":                                                             {1, "DEDUPED", "FINAL"},
	"localgit/stored_version.go:repoInsert,commitInsert,pullRequestInsert":                                                {2, "NAMES", "INSERT statements"},
	"localgit/stored_version.go:commitContract":                                                                           {1, "NAMES", "stored version contract table name"},
	"localgit/stored_version.go:pullRequestContract":                                                                      {1, "NAMES", "stored version contract table name"},
	"localgit/stored_version.go:StoredVersionSpecs":                                                                       {2, "NAMES", "writer spec keys"},
	"localgit/stored_version.go:localKept":                                                                                {1, "NAMES", "kept group key"},
	"providersync/catalog_evidence_start.go:readCatalogRepoEvidence":                                                      {1, "DEDUPED", "FINAL"},
	"providersync/execution_registry.go:Descriptor":                                                                       {2, "NAMES", "route destination names"},
	"providersync/execution_registry.go:githubPRSocialRouteDestinations":                                                  {1, "NAMES", "route destination names"},
	"providersync/github_commits_effects_clickhouse.go:writeGitCommitsEffect":                                             {1, "NAMES", "effect destination name"},
	"providersync/github_commits_effects_clickhouse.go:inspectGitCommitsEffect":                                           {1, "NAMES", "effect destination name"},
	"providersync/github_commits_effects_clickhouse.go:inspectGitCommit":                                                  {1, "DEDUPED", "FINAL (readback)"},
	"providersync/github_commits_route.go:GitHubCommitsRouteHandler.Collect":                                              {1, "NAMES", "effect destination name"},
	"providersync/github_pr_reviews_effects_clickhouse.go:GitHubPullRequestSocialClickHouseEffects.WriteEffect":           {1, "NAMES", "effect destination name"},
	"providersync/github_pr_reviews_effects_clickhouse.go:GitHubPullRequestSocialClickHouseEffects.InspectEffect":         {1, "NAMES", "effect destination name"},
	"providersync/github_pr_reviews_route.go:GitHubPullRequestSocialRouteHandler.Collect":                                 {2, "NAMES", "effect destination names"},
	"providersync/github_prs_effects_clickhouse.go:GitHubPullRequestClickHouseEffects.writePullRequestEffect":             {1, "NAMES", "effect destination name"},
	"providersync/github_prs_effects_clickhouse.go:GitHubPullRequestClickHouseEffects.inspectPullRequestEffect":           {1, "NAMES", "effect destination name"},
	"providersync/github_prs_effects_clickhouse.go:GitHubPullRequestClickHouseEffects.scanWinningPullRequestVersion":      {1, "DEDUPED", "FINAL (readback)"},
	"providersync/github_prs_route.go:GitHubPullRequestRouteHandler.Collect":                                              {1, "NAMES", "effect destination name"},
	"providersync/gitlab_commits_route.go:GitLabCommitsRouteHandler.Collect":                                              {1, "NAMES", "effect destination name"},
	"providersync/gitlab_prs_route.go:GitLabPullRequestRouteHandler.Collect":                                              {1, "NAMES", "effect destination name"},
	"providersync/prepared_route_projection.go:preparedRouteInsertStatements":                                             {2, "NAMES", "insert statement keys"},
	"providersync/prepared_route_projection.go:preparedRouteSinkReadKeys":                                                 {1, "NAMES", "sink key"},
	"providersync/stored_version.go:pullRequestInsert":                                                                    {1, "NAMES", "INSERT statement"},
	"providersync/stored_version.go:pullRequestContract":                                                                  {1, "NAMES", "stored version contract table name"},
	"providersync/stored_version.go:setPullRequestKept":                                                                   {1, "NAMES", "error text"},
	"providersync/stored_version.go:StoredVersionSpecs":                                                                   {2, "NAMES", "writer spec keys"},
	"providersync/stored_version.go:gitCommitsInsert":                                                                     {1, "NAMES", "INSERT statement"},
	"providersync/stored_version.go:gitCommitsContract":                                                                   {1, "NAMES", "stored version contract table name"},
	"providersync/team_repo_ownership_derivation_clickhouse.go:loadTeamRepoOwnershipIssuePRLinks":                         {1, "DEDUPED", "FINAL"},
	"queryapi/aianalytics/ai_opportunities.go:opportunity.project":                                                        {1, "NAMES", "evidence ref prefix"},
	"queryapi/aianalytics/ai_opportunities.go:repetitiveStatement":                                                        {1, "DEDUPED-CELL", "FINAL on git_pull_requests and work_graph_issue_pr, uniqExact(pr.number); cells TestRealClickHouse_AIOpportunitiesCountAResyncedPullRequestOnce, TestRealClickHouse_AIRepetitiveClusterCountsAPullRequestOnce"},
	"queryapi/aianalytics/ai_opportunities.go:titlePatternRules":                                                          {2, "NAMES", "metric ref names"},
	"queryapi/aianalytics/ai_opportunities.go:titleToilStatement":                                                         {1, "DEDUPED-CELL", "FINAL; cell TestRealClickHouse_AIOpportunitiesCountAResyncedPullRequestOnce"},
	"queryapi/aianalytics/ai_opportunities.go:docDriftStatement":                                                          {1, "SAFE", "raw; uniqExactIf on the hash and doc_changes tested = 0 only; green in TestRealClickHouse_AIOpportunitiesCountAResyncedPullRequestOnce"},
	"queryapi/aianalytics/loaders.go:dedupedPRs":                                                                          {2, "DEDUPED", "argMax(.., last_synced) GROUP BY the key; the second literal is the candidate key set"},
	"queryapi/aianalytics/loaders_b.go:attributedPRsStatement":                                                            {1, "DEDUPED-CELL", "FINAL on the pull requests and the link, one row per (repo, number) before the page; cells TestRealClickHouse_AIAttributedPRsPageHoldsDistinctPullRequests, TestRealClickHouse_AIAttributedPRsPageCountsPullRequests"},
	"queryapi/aianalytics/names.go:loadPRTitles":                                                                          {1, "DEDUPED", "argMax GROUP BY the key (latestrow)"},
	"queryapi/analytics/filtertranslation.go:validateDeveloperEmails":                                                     {1, "NAMES", "error text"},
	"queryapi/analytics/investment.go:workUnitAuthorsSource":                                                              {2, "DEDUPED", "argMax(tuple(..), last_synced) GROUP BY the key"},
	"queryapi/busfactor/busfactor.go:loadEvidence":                                                                        {1, "DEDUPED", "FINAL"},
	"queryapi/datahealth/identity.go:observedIdentitiesSQL":                                                               {1, "DEDUPED-CELL", "FINAL; cell TestIdentityMappingCountsAResyncedCommitOnce"},
	"queryapi/drilldown/prs.go:fetchPullRequestsQuery":                                                                    {1, "DEDUPED", "FINAL"},
	"queryapi/flame/clickhouse.go:fetchPullRequestQuery":                                                                  {1, "DEDUPED", "FINAL"},
	"queryapi/graph/generated.go:sources":                                                                                 {1, "NAMES", "schema description text"},
	"queryapi/heatmap/queries.go:fetchReviewWaitDensity":                                                                  {1, "DEDUPED", "FINAL"},
	"queryapi/heatmap/queries.go:fetchReviewWaitEvidence":                                                                 {1, "DEDUPED", "FINAL"},
	"queryapi/heatmap/queries.go:fetchRepoTouchpoints":                                                                    {2, "DEDUPED", "FINAL"},
	"queryapi/heatmap/queries.go:fetchIndividualActiveHours":                                                              {1, "DEDUPED", "FINAL"},
	"queryapi/heatmap/queries.go:fetchIndividualActiveEvidence":                                                           {1, "DEDUPED", "FINAL"},
	"queryapi/investmentexplain/quotetitles.go:Reader.fetchPullRequestTitles":                                             {1, "DEDUPED", "FINAL"},
	"queryapi/people/drilldownprs.go:fetchPersonPullRequestsQuery":                                                        {1, "DEDUPED", "FINAL"},
	"queryapi/quadrant/quadrant.go:RepoMetrics":                                                                           {1, "DEDUPED", "metric spec table; dedupFrom wraps it in FINAL"},
	"queryapi/quadrant/quadrant.go:replacingMergeTreeTables":                                                              {1, "NAMES", "the FINAL table list of dedupFrom"},
	"queryapi/reviewedges/people.go:pullRequestAuthorNamesSQL":                                                            {1, "DEDUPED", "FINAL"},
	"queryapi/workgraph/displaynames.go:resolvePRDisplayNames":                                                            {1, "DEDUPED", "FINAL"},
	"queryapi/workgraph/pr.go:PRCoreRowExists":                                                                            {1, "SAFE", "raw existence read, LIMIT 1: a version never makes a row stop existing"},
	"queryapi/workgraph/pr.go:FetchPRCoreRow":                                                                             {1, "DEDUPED", "FINAL"},
	"queryapi/workgraph/pr.go:ResolveCommits":                                                                             {1, "DEDUPED", "FINAL"},
	"storedversion/repair.go:PullRequestKept,DeploymentKept":                                                              {2, "NAMES", "kept group base table and INSERT; the repair reads FINAL of the base"},
	"streamhandlers/external_clickhouse.go:externalInsertQuery":                                                           {2, "NAMES", "INSERT statements"},
	"streamhandlers/external_clickhouse.go:externalRecordValues":                                                          {8, "NAMES", "column texts of decode errors"},
	"streamhandlers/external_stored_version.go:externalPullRequestContract,externalReviewContract,externalCommitContract": {2, "NAMES", "stored version contract table names"},
	"streamhandlers/external_stored_version.go:externalUnstoredFields":                                                    {1, "NAMES", "reason text"},
	"streamhandlers/stored_version.go:StoredVersionSpecs":                                                                 {2, "NAMES", "writer spec keys"},
	"syncdispatchruntime/touched_days_clickhouse.go:recordTouchedGitSQL":                                                  {1, "SAFE", "raw; GROUP BY day, repo_id; it reads the versions synced since a time on purpose, an old version's day is one more day to recompute"},
}

type gitTableSite struct {
	count int
	class string // DEDUPED-CELL, DEDUPED, SAFE, RULED or NAMES
	why   string
}

var gitTableClasses = map[string]bool{"DEDUPED-CELL": true, "DEDUPED": true, "SAFE": true, "RULED": true, "NAMES": true}

func gitTableTokens(literal string) bool {
	for _, token := range strings.FieldsFunc(literal, func(r rune) bool {
		return !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9')
	}) {
		if token == "git_pull_requests" || token == "git_commits" {
			return true
		}
	}
	return false
}

func gitTableDeclName(decl ast.Decl) string {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if d.Recv != nil && len(d.Recv.List) == 1 {
			typ := d.Recv.List[0].Type
			if star, ok := typ.(*ast.StarExpr); ok {
				typ = star.X
			}
			if index, ok := typ.(*ast.IndexExpr); ok {
				typ = index.X
			}
			if ident, ok := typ.(*ast.Ident); ok {
				return ident.Name + "." + d.Name.Name
			}
		}
		return d.Name.Name
	case *ast.GenDecl:
		var names []string
		for _, spec := range d.Specs {
			switch s := spec.(type) {
			case *ast.ValueSpec:
				for _, n := range s.Names {
					names = append(names, n.Name)
				}
			case *ast.TypeSpec:
				names = append(names, s.Name.Name)
			}
		}
		return strings.Join(names, ",")
	}
	return "?"
}

func gitTableInventory(t *testing.T, internalDir string) map[string]int {
	t.Helper()
	found := map[string]int{}
	err := filepath.WalkDir(internalDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(internalDir, path)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			n := 0
			ast.Inspect(decl, func(node ast.Node) bool {
				lit, ok := node.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatalf("%s: unquote %s: %v", rel, lit.Value, err)
				}
				if gitTableTokens(value) {
					n++
				}
				return true
			})
			if n > 0 {
				found[filepath.ToSlash(rel)+":"+gitTableDeclName(decl)] += n
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func TestGitTableTokensMatchWholeTokensOnly(t *testing.T) {
	for literal, want := range map[string]bool{
		"FROM git_pull_requests AS pr":       true,
		"git_commits":                        true,
		"`git_commits`.hash":                 true,
		"git_pull_requests:repo:1":           true,
		"FROM git_pull_request_kept":         false,
		"git_pull_request_reviews":           false,
		"git_commit_stats":                   false,
		"git_commits_x":                      false,
		"xgit_commits":                       false,
		"the git pull requests of the table": false,
	} {
		if got := gitTableTokens(literal); got != want {
			t.Errorf("gitTableTokens(%q) = %v, want %v", literal, got, want)
		}
	}
}

func TestEveryGitTableSiteIsClassified(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	found := gitTableInventory(t, filepath.Join(root, "internal"))
	var problems []string
	for key, n := range found {
		site, ok := gitTableSites[key]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("%s: %d literal(s) name git_pull_requests or git_commits and the site is not classified", key, n))
		case site.count != n:
			problems = append(problems, fmt.Sprintf("%s: %d literal(s), listed %d", key, n, site.count))
		}
	}
	for key, site := range gitTableSites {
		if _, ok := found[key]; !ok {
			problems = append(problems, key+": listed, no literal names the tables any more (stale key)")
		}
		if !gitTableClasses[site.class] || strings.TrimSpace(site.why) == "" {
			problems = append(problems, key+": a class of "+fmt.Sprint(sortedGitTableClasses())+" and a reason are required")
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		t.Error(p)
	}
}

func sortedGitTableClasses() []string {
	out := make([]string, 0, len(gitTableClasses))
	for c := range gitTableClasses {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}
