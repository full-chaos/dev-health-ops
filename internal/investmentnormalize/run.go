package investmentnormalize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
)

// Conn is the ClickHouse connection the script needs.
type Conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	Exec(ctx context.Context, query string, args ...any) error
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// ErrUnmappedKeys is returned by Apply when the window holds a legacy key the mapping table does not hold.
var ErrUnmappedKeys = errors.New("investmentnormalize: the window holds legacy keys the mapping table does not hold; nothing was written")

// Options select what to read. From is inclusive, To exclusive, both whole days (UTC).
type Options struct {
	Org   string
	From  time.Time
	To    time.Time
	RunID string
	Now   func() time.Time
}

// Report is what a run counted: counts only, never a title or an id of an item. Every item read is counted
// in exactly ONE of: NotCompleted, AlreadyCovered, NoRepoReachable, Unmapped, Unmatched, Unchanged, Rows.
type Report struct {
	Items           int            // distinct work items with a classification in the window
	NotCompleted    int            // no work item row, or not completed on the classified day: not placeable in time
	AlreadyCovered  int            // named in the evidence of a canonical unit: already described, left out
	NoRepoReachable int            // no repository reachable through the entity tree (the reasons are below)
	NoRepoReasons   map[string]int // why, by reason
	Unmatched       int            // product/general items no estimate rule matched: stay out (no data)
	Fallback        int            // product/general items considered (the estimated population)
	Unmapped        map[string]int // legacy keys the table does not hold, by "area/stream"
	Unchanged       int            // the identical row is already stored (same id, input hash, mapping version): not rewritten
	ByTheme         map[string]int // rows that would be (were) written, by theme
	BySubcategory   map[string]int
	ByRule          map[string]int // "map:<area>/<stream>" or the estimate rule name
	ByTier          map[string]int // by the strongest link tier ("own_repository" for a pull request item)
	Rows            int            // rows that would be (were) written
	RepoEffortRows  int            // work_unit_repo_effort rows that would be (were) written
}

func newReport() Report {
	return Report{
		NoRepoReasons: map[string]int{}, Unmapped: map[string]int{}, ByTheme: map[string]int{},
		BySubcategory: map[string]int{}, ByRule: map[string]int{}, ByTier: map[string]int{},
	}
}

// share renders n of total as a percentage ("n/a" when there is nothing to divide).
func share(n, total int) string {
	if total == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(n)/float64(total))
}

// UnmappedTotal is the number of items with an unmapped key.
func (r Report) UnmappedTotal() int {
	n := 0
	for _, c := range r.Unmapped {
		n += c
	}
	return n
}

// Accounted is the sum of the disjoint buckets; it equals Items when every item was decided.
func (r Report) Accounted() int {
	return r.NotCompleted + r.AlreadyCovered + r.NoRepoReachable + r.UnmappedTotal() + r.Unmatched + r.Unchanged + r.Rows
}

// Lines renders the report as the dry-run table (stable order).
func (r Report) Lines() []string {
	out := []string{
		fmt.Sprintf("items read: %d", r.Items),
		fmt.Sprintf("skipped, not completed on the classified day: %d", r.NotCompleted),
		fmt.Sprintf("skipped: covered by a canonical unit: %d", r.AlreadyCovered),
		fmt.Sprintf("skipped, no repository reachable: %d", r.NoRepoReachable),
		fmt.Sprintf("no repository reachable: %s: %d", reasonNoLink, r.NoRepoReasons[reasonNoLink]),
		fmt.Sprintf("no repository reachable: %s: %d", reasonNoChurn, r.NoRepoReasons[reasonNoChurn]),
		fmt.Sprintf("no repository reachable: %s: %d", reasonNoOwnRepository, r.NoRepoReasons[reasonNoOwnRepository]),
		fmt.Sprintf("unmatched (product/general, no estimate rule): %d", r.Unmatched),
		fmt.Sprintf("unmatched share of product/general: %s", share(r.Unmatched, r.Fallback)),
		fmt.Sprintf("unmapped keys (items): %d", r.UnmappedTotal()),
		fmt.Sprintf("already stored, not rewritten: %d", r.Unchanged),
		fmt.Sprintf("rows (normalized): %d", r.Rows),
		fmt.Sprintf("repository effort rows: %d", r.RepoEffortRows),
		fmt.Sprintf("product/general items (estimated): %d", r.Fallback),
	}
	for _, section := range []struct {
		title string
		m     map[string]int
	}{
		{"by theme", r.ByTheme}, {"by subcategory", r.BySubcategory},
		{"by rule", r.ByRule}, {"by tier", r.ByTier}, {"unmapped", r.Unmapped},
	} {
		keys := make([]string, 0, len(section.m))
		for k := range section.m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			out = append(out, fmt.Sprintf("%s: %s = %d", section.title, k, section.m[k]))
		}
	}
	return out
}

// The reasons a repository is not reachable (CHAOS-9227).
const (
	reasonNoOwnRepository = "pull request item without its own repository"
	reasonNoLink          = "no link"
	reasonNoChurn         = "all links churn 0"
)

// Tiers of the repository of an item.
const tierOwnRepository = "own_repository"

type sourceRow struct {
	artifactID  string
	provider    string
	area        string
	stream      string
	legacyRule  string
	title       string
	description *string
	labels      []string
	createdAt   time.Time
	completedAt time.Time
	points      *float64
	itemType    string
	ownRepo     *uuid.UUID
	covered     bool
}

// isChangeRequest says whether a work item is a pull request / merge request: such an item keeps its own
// repository. Every other item (an issue, a task, a story) reaches repositories only through its linked
// pull requests (work_graph_issue_pr).
func isChangeRequest(itemType string) bool {
	switch strings.ToLower(itemType) {
	case "pr", "pull_request", "merge_request", "mr":
		return true
	}
	return false
}

// itemsQuery joins every classified work item of the window to its work item (by the item id: an issue has no
// repository of its own to join on) and says whether a canonical unit already names it in its evidence. The
// coverage rule is the readers' own (CHAOS-9206 purpose read): the issues and prs of a unit's structural
// evidence, a pull request reference turned into the item id form (ghpr:/gh: for GitHub, gitlab:...! for GitLab).
const itemsQuery = `
WITH
repo_names AS (
    SELECT toString(id) AS repo_uuid, argMax(repo, last_synced) AS repo,
           if(uniqExact(provider) = 1, argMax(provider, last_synced), '') AS provider
    FROM repos WHERE org_id = {org:String} GROUP BY id
),
canonical AS (
    SELECT work_unit_id, argMax(categorization_status, computed_at) AS st,
           argMax(structural_evidence_json, computed_at) AS ev
    FROM work_unit_investments WHERE org_id = {org:String}
    GROUP BY work_unit_id HAVING st != '` + Status + `'
),
covered_items AS (
    SELECT DISTINCT item FROM (
        SELECT arrayJoin(multiIf(
            NOT match(ref, '^[0-9a-fA-F-]{36}#pr[0-9]+$'), [ref],
            rn.repo = '' OR rn.provider = '', [''],
            rn.provider = 'gitlab', [concat('gitlab:', rn.repo, '!', splitByString('#pr', ref)[2])],
            [concat('ghpr:', rn.repo, '#', splitByString('#pr', ref)[2]), concat('gh:', rn.repo, '#', splitByString('#pr', ref)[2])]
        )) AS item
        FROM canonical AS u
        ARRAY JOIN arrayDistinct(arrayConcat(JSONExtract(ev, 'issues', 'Array(String)'), JSONExtract(ev, 'prs', 'Array(String)'))) AS ref
        LEFT JOIN repo_names AS rn ON rn.repo_uuid = splitByString('#pr', ref)[1]
    ) WHERE item != ''
)
SELECT c.artifact_id, c.provider, c.investment_area, c.project_stream, c.rule_id,
       w.title, w.description, w.labels, w.created_at, w.completed_at, w.story_points, w.item_type, w.own_repo,
       c.artifact_id IN (SELECT item FROM covered_items) AS covered
FROM (
    SELECT artifact_id, provider, day,
           argMax(investment_area, computed_at) AS investment_area,
           argMax(project_stream, computed_at) AS project_stream,
           argMax(rule_id, computed_at) AS rule_id
    FROM investment_classifications_daily
    WHERE org_id = {org:String} AND artifact_type = 'work_item'
      AND day >= toDate({from:String}) AND day < toDate({to:String})
    GROUP BY artifact_id, provider, day
) AS c
INNER JOIN (
    SELECT provider, work_item_id,
           argMax(title, last_synced) AS title, argMax(description, last_synced) AS description,
           argMax(labels, last_synced) AS labels, argMax(created_at, last_synced) AS created_at,
           argMax(completed_at, last_synced) AS completed_at, argMax(story_points, last_synced) AS story_points,
           argMax(type, last_synced) AS item_type, argMax(repo_id, last_synced) AS own_repo
    FROM work_items WHERE org_id = {org:String}
    GROUP BY provider, work_item_id
) AS w ON w.work_item_id = c.artifact_id AND w.provider = c.provider
WHERE w.completed_at IS NOT NULL AND toDate(w.completed_at) = c.day
ORDER BY c.day, c.artifact_id
SETTINGS max_execution_time = 600`

// classifiedQuery counts the work ITEMS the window reads: the classifier writes one row per active item per
// day, so the denominator groups by item, not by item and day (the denominator of the report: every item is
// counted in exactly one bucket).
const classifiedQuery = `
SELECT count()
FROM (
    SELECT artifact_id, provider
    FROM investment_classifications_daily
    WHERE org_id = {org:String} AND artifact_type = 'work_item'
      AND day >= toDate({from:String}) AND day < toDate({to:String})
    GROUP BY artifact_id, provider
)
SETTINGS max_execution_time = 600`

// linksQuery reads the issue to pull request links of the organization, with the churn of the pull request
// (additions + deletions, the weight the canonical units split a repository effort by: PR churn). The tier
// is the provenance of the link: native > explicit_text > heuristic; all three count as links.
const linksQuery = `
SELECT l.work_item_id, l.repo_id, l.pr_number, l.provenance, ifNull(p.churn, 0) AS churn
FROM (
    SELECT work_item_id, repo_id, pr_number, argMax(provenance, version_rank) AS provenance
    FROM work_graph_issue_pr WHERE org_id = {org:String}
    GROUP BY work_item_id, repo_id, pr_number
) AS l
LEFT JOIN (
    SELECT repo_id, number, argMax(toFloat64(ifNull(additions, 0)) + toFloat64(ifNull(deletions, 0)), last_synced) AS churn
    FROM git_pull_requests WHERE org_id = {org:String}
    GROUP BY repo_id, number
) AS p ON p.repo_id = l.repo_id AND p.number = l.pr_number
ORDER BY l.work_item_id, l.repo_id, l.pr_number
SETTINGS max_execution_time = 600`

// storedQuery reads the normalized rows an organization already holds: id, input hash, mapping version.
const storedQuery = `
SELECT work_unit_id, categorization_input_hash, categorization_model_version
FROM work_unit_investments FINAL
WHERE org_id = {org:String} AND categorization_status = '` + Status + `'
SETTINGS max_execution_time = 600`

func args(o Options) []any {
	return []any{
		clickhouse.Named("org", o.Org),
		clickhouse.Named("from", o.From.Format("2006-01-02")),
		clickhouse.Named("to", o.To.Format("2006-01-02")),
	}
}

// issueLink is one link of an issue to a pull request.
type issueLink struct {
	repo   uuid.UUID
	number uint32
	tier   string
	churn  float64
}

var tierRank = map[string]int{"native": 3, "explicit_text": 2, "heuristic": 1}

func readLinks(ctx context.Context, conn Conn, o Options) (map[string][]issueLink, error) {
	rows, err := conn.Query(ctx, linksQuery, clickhouse.Named("org", o.Org))
	if err != nil {
		return nil, fmt.Errorf("read issue to pull request links: %w", err)
	}
	defer rows.Close()
	out := map[string][]issueLink{}
	for rows.Next() {
		var id string
		var l issueLink
		if err := rows.Scan(&id, &l.repo, &l.number, &l.tier, &l.churn); err != nil {
			return nil, fmt.Errorf("scan issue to pull request link: %w", err)
		}
		out[id] = append(out[id], l)
	}
	return out, rows.Err()
}

type storedRow struct{ hash, version string }

func readStored(ctx context.Context, conn Conn, o Options) (map[string]storedRow, error) {
	rows, err := conn.Query(ctx, storedQuery, clickhouse.Named("org", o.Org))
	if err != nil {
		return nil, fmt.Errorf("read stored normalized rows: %w", err)
	}
	defer rows.Close()
	out := map[string]storedRow{}
	for rows.Next() {
		var id string
		var r storedRow
		if err := rows.Scan(&id, &r.hash, &r.version); err != nil {
			return nil, fmt.Errorf("scan stored normalized row: %w", err)
		}
		out[id] = r
	}
	return out, rows.Err()
}

// allocation is one repository of an item with its share of the effort.
type allocation struct {
	repo   uuid.UUID
	weight float64
	source string
}

// allocate decides the repositories of one item through the entity tree. A pull request / merge request item
// keeps its own repository. Any other item reaches repositories only through work_graph_issue_pr rows; the
// effort is split by the churn of the linked pull requests per repository, the rule the canonical units use
// (units.AllocateRepoEffort: pr_churn; the sum of the weights is 1). It returns the strongest link tier,
// or the reason no repository is reachable.
func allocate(s sourceRow, links map[string][]issueLink) (allocs []allocation, tier, reason string) {
	if isChangeRequest(s.itemType) {
		if s.ownRepo == nil || *s.ownRepo == uuid.Nil {
			return nil, "", reasonNoOwnRepository
		}
		return []allocation{{repo: *s.ownRepo, weight: 1, source: tierOwnRepository}}, tierOwnRepository, ""
	}
	linked := links[s.artifactID]
	if len(linked) == 0 {
		return nil, "", reasonNoLink
	}
	churnByRepo := map[uuid.UUID]float64{}
	total := 0.0
	best := ""
	for _, l := range linked {
		if tierRank[l.tier] > tierRank[best] {
			best = l.tier
		}
		if l.churn > 0 {
			churnByRepo[l.repo] += l.churn
			total += l.churn
		}
	}
	if total <= 0 {
		return nil, best, reasonNoChurn
	}
	repos := make([]uuid.UUID, 0, len(churnByRepo))
	for repo := range churnByRepo {
		repos = append(repos, repo)
	}
	sort.Slice(repos, func(i, j int) bool { return repos[i].String() < repos[j].String() })
	for _, repo := range repos {
		allocs = append(allocs, allocation{repo: repo, weight: churnByRepo[repo] / total, source: "pr_churn"})
	}
	return allocs, best, ""
}

// Plan reads the window and decides every row. It writes nothing.
func Plan(ctx context.Context, conn Conn, o Options) ([]chwrite.InvestmentRecord, []chwrite.RepoEffortRecord, Report, error) {
	if strings.TrimSpace(o.Org) == "" {
		return nil, nil, Report{}, errors.New("investmentnormalize: an organization id is required")
	}
	if !o.From.Before(o.To) {
		return nil, nil, Report{}, errors.New("investmentnormalize: --from must be before --to")
	}
	now := time.Now
	if o.Now != nil {
		now = o.Now
	}
	total, err := countRows(ctx, conn, classifiedQuery, args(o)...)
	if err != nil {
		return nil, nil, Report{}, fmt.Errorf("count classified work items: %w", err)
	}
	links, err := readLinks(ctx, conn, o)
	if err != nil {
		return nil, nil, Report{}, err
	}
	stored, err := readStored(ctx, conn, o)
	if err != nil {
		return nil, nil, Report{}, err
	}
	rows, err := conn.Query(ctx, itemsQuery, args(o)...)
	if err != nil {
		return nil, nil, Report{}, fmt.Errorf("read legacy classifications: %w", err)
	}
	defer rows.Close()
	report := newReport()
	report.Items = int(total)
	var records []chwrite.InvestmentRecord
	var efforts []chwrite.RepoEffortRecord
	computedAt := now().UTC()
	joined := 0
	for rows.Next() {
		var s sourceRow
		var completed *time.Time
		if err := rows.Scan(&s.artifactID, &s.provider, &s.area, &s.stream, &s.legacyRule,
			&s.title, &s.description, &s.labels, &s.createdAt, &completed, &s.points, &s.itemType, &s.ownRepo, &s.covered); err != nil {
			return nil, nil, Report{}, fmt.Errorf("scan legacy classification: %w", err)
		}
		if completed == nil {
			continue
		}
		joined++
		s.completedAt = completed.UTC()
		if s.covered {
			report.AlreadyCovered++
			continue
		}
		allocs, tier, reason := allocate(s, links)
		if reason != "" {
			report.NoRepoReachable++
			report.NoRepoReasons[reason]++
			continue
		}
		var subcategory, ruleName string
		switch {
		case IsDefaultFallback(s.area, s.stream):
			report.Fallback++
			sub, name, ok := Estimate(s.title, s.description, s.labels)
			if !ok {
				report.Unmatched++
				continue
			}
			subcategory, ruleName = sub, name
		default:
			sub, ok := MapSpecific(s.area, s.stream)
			if !ok {
				report.Unmapped[s.area+"/"+s.stream]++
				continue
			}
			subcategory, ruleName = sub, "map:"+s.area+"/"+s.stream
		}
		theme := Theme(subcategory)
		rec, repoEfforts := record(o, s, allocs, tier, subcategory, theme, ruleName, computedAt)
		if prior, ok := stored[rec.WorkUnitID]; ok && prior.hash == rec.CategorizationInputHash && strings.Contains(prior.version, "mapping="+MappingVersion) {
			report.Unchanged++
			continue
		}
		report.Rows++
		report.ByTheme[theme]++
		report.BySubcategory[subcategory]++
		report.ByRule[ruleName]++
		report.ByTier[tier]++
		report.RepoEffortRows += len(repoEfforts)
		records = append(records, rec)
		efforts = append(efforts, repoEfforts...)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, Report{}, fmt.Errorf("read legacy classifications: %w", err)
	}
	report.NotCompleted = report.Items - joined
	return records, efforts, report, nil
}

func record(o Options, s sourceRow, allocs []allocation, tier, subcategory, theme, ruleName string, computedAt time.Time) (chwrite.InvestmentRecord, []chwrite.RepoEffortRecord) {
	units := 1.0
	if s.points != nil && *s.points != 0 && !math.IsNaN(*s.points) && !math.IsInf(*s.points, 0) {
		units = *s.points
	}
	units = math.Trunc(units) // the legacy delivery_units (workitemengine.ComputeInvestmentDaily)
	workUnitID := fmt.Sprintf("%s:%s:%s:%s", Status, o.Org, s.provider, s.artifactID)
	// The effort of each repository: the item's effort times its weight, the last one the remainder, so the
	// repositories sum to the item's effort exactly.
	efforts := make([]chwrite.RepoEffortRecord, 0, len(allocs))
	repoList := make([]map[string]any, 0, len(allocs))
	var unitRepo *uuid.UUID
	remaining := units
	for index, a := range allocs {
		value := units * a.weight
		if index == len(allocs)-1 {
			value = remaining
		}
		remaining -= value
		repo := a.repo
		source := "normalized_legacy"
		efforts = append(efforts, chwrite.RepoEffortRecord{
			WorkUnitID: workUnitID, RepoID: &repo, RepoSource: &source, EffortMetric: EffortMetric, EffortValue: value,
			AllocationWeight: a.weight, AllocationSource: a.source, CategorizationRunID: o.RunID, ComputedAt: computedAt,
		})
		repoList = append(repoList, map[string]any{"repo_id": repo.String(), "weight": a.weight, "source": a.source})
	}
	if len(allocs) == 1 {
		repo := allocs[0].repo
		unitRepo = &repo // a unit names ONE repository only when it has exactly one (the canonical rule)
	}
	evidence, _ := json.Marshal(map[string]any{
		"issues": []string{}, "prs": []string{}, "commits": []string{}, "edges": []string{},
		"normalized_legacy": map[string]any{
			"source": "investment_classifications_daily", "investment_area": s.area, "project_stream": s.stream,
			"legacy_rule_id": s.legacyRule, "rule": ruleName, "mapping_version": MappingVersion,
			"repository_tier": tier, "repositories": repoList,
		},
	})
	// The input hash names EVERY input of the row, the OUTCOME of the estimate included: the estimate reads the
	// description and the labels, so hashing only the title would call a re-estimated row "unchanged".
	hashParts := []string{
		o.Org, s.provider, s.artifactID, s.area, s.stream, s.legacyRule, s.title, ruleName, subcategory, theme, tier,
		strconv.FormatFloat(units, 'g', -1, 64), s.createdAt.UTC().Format(time.RFC3339Nano), s.completedAt.UTC().Format(time.RFC3339Nano),
	}
	for _, a := range allocs {
		hashParts = append(hashParts, a.repo.String(), strconv.FormatFloat(a.weight, 'g', -1, 64))
	}
	hash := sha256.Sum256([]byte(strings.Join(hashParts, "\x00")))
	workUnitType, name, provider := "work_item", s.title, s.provider
	quotes := uint32(0)
	return chwrite.InvestmentRecord{
		WorkUnitID:   workUnitID,
		WorkUnitType: &workUnitType, WorkUnitName: &name,
		FromTS: s.createdAt.UTC(), ToTS: s.completedAt, RepoID: unitRepo, Provider: &provider,
		EffortMetric: EffortMetric, EffortValue: units,
		ThemeDistribution: map[string]float64{theme: 1.0}, SubcategoryDistribution: map[string]float64{subcategory: 1.0},
		StructuralEvidenceJSON: string(evidence),
		EvidenceQuality:        QualityScore, EvidenceQualityBand: QualityBand,
		CategorizationStatus: Status, CategorizationErrorsJSON: "[]",
		CategorizationModelVersion: "normalized_legacy;mapping=" + MappingVersion + ";source=investment_classifications_daily",
		CategorizationInputHash:    hex.EncodeToString(hash[:]),
		CategorizationRunID:        o.RunID, ComputedAt: computedAt, EvidenceQuoteCount: &quotes,
	}, efforts
}

// Apply writes the planned rows in batches. It refuses a window that holds an unmapped key.
func Apply(ctx context.Context, conn Conn, o Options, records []chwrite.InvestmentRecord, efforts []chwrite.RepoEffortRecord, report Report) error {
	if report.UnmappedTotal() > 0 {
		return ErrUnmappedKeys
	}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		return err
	}
	const batch = 2000
	// The repository efforts go FIRST and the unit rows LAST: a plan judges "already stored" by the unit row,
	// so a failure between the two writes leaves a unit row absent (the retry writes both again, the efforts
	// idempotently) and never a unit row without its efforts.
	for start := 0; start < len(efforts); start += batch {
		end := start + batch
		if end > len(efforts) {
			end = len(efforts)
		}
		if _, err := writer.WriteRepoEffort(ctx, o.Org, efforts[start:end]); err != nil {
			return err
		}
	}
	for start := 0; start < len(records); start += batch {
		end := start + batch
		if end > len(records) {
			end = len(records)
		}
		if _, err := writer.WriteInvestments(ctx, o.Org, records[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// RollbackCount counts the normalized rows an org holds (all, or one run).
func RollbackCount(ctx context.Context, conn Conn, org, runID string) (uint64, error) {
	query, bind := rollbackWhere(org, runID)
	rows, err := conn.Query(ctx, "SELECT count() FROM work_unit_investments FINAL WHERE "+query, bind...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n uint64
	for rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return 0, err
		}
	}
	return n, rows.Err()
}

// Rollback deletes the normalized rows of an org (all, or one run): only rows with the provenance status.
func Rollback(ctx context.Context, conn Conn, org, runID string) error {
	query, bind := rollbackWhere(org, runID)
	// The repository efforts FIRST, while the unit rows still say which units the run owns: every version of the
	// effort rows of those units (a retry after a failed apply wrote a second version under another run id), and
	// the rows of the run itself (a failed apply leaves efforts with no unit row). Only the provenance's own
	// unit ids: `normalized_legacy:<org>:...`.
	effort := "org_id = {org:String} AND startsWith(work_unit_id, '" + Status + ":')"
	effortBind := []any{clickhouse.Named("org", org)}
	if runID != "" {
		effort += " AND (categorization_run_id = {run:String} OR work_unit_id IN (SELECT work_unit_id FROM work_unit_investments WHERE " + query + "))"
		effortBind = append(effortBind, clickhouse.Named("run", runID))
	}
	if err := conn.Exec(ctx, "ALTER TABLE work_unit_repo_effort DELETE WHERE "+effort+" SETTINGS mutations_sync = 2", effortBind...); err != nil {
		return err
	}
	return conn.Exec(ctx, "ALTER TABLE work_unit_investments DELETE WHERE "+query+" SETTINGS mutations_sync = 2", bind...)
}

func rollbackWhere(org, runID string) (string, []any) {
	query := "org_id = {org:String} AND categorization_status = '" + Status + "'"
	bind := []any{clickhouse.Named("org", org)}
	if runID != "" {
		query += " AND categorization_run_id = {run:String}"
		bind = append(bind, clickhouse.Named("run", runID))
	}
	return query, bind
}

func countRows(ctx context.Context, conn Conn, query string, bind ...any) (uint64, error) {
	rows, err := conn.Query(ctx, query, bind...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n uint64
	for rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return 0, err
		}
	}
	return n, rows.Err()
}
