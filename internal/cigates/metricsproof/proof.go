// Package metricsproof is the executed-proof readback for the metrics.daily families.
//
// Earlier checks asserted that a job ran, not that it produced rows for the org it was supposed to compute for. This
// readback asks ClickHouse: rows must exist for the seeded org with computed_at at or after the run start, and every
// repo id those rows carry must be a repo ClickHouse knows for that org. CI's live backend e2e gate calls it through
// `cigate metrics-proof`.
//
// Exit codes: 0 every requested family produced valid rows; 1 at least one family has zero rows or wrote a repo id
// ClickHouse repos does not have (the dead or mismatched id shape); 2 usage error.
package metricsproof

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Conn is the part of a ClickHouse connection the readback uses.
type Conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// Opener connects to ClickHouse from a DSN and returns the connection with its close function.
type Opener func(ctx context.Context, uri string) (Conn, func() error, error)

// FamilyRowCount is the row count and newest computed_at one readback group holds.
type FamilyRowCount struct {
	Rows             uint64
	LatestComputedAt time.Time
}

// IDSet is a set of ids.
type IDSet map[string]struct{}

// One representative table per family, keyed (repo_id, day) with a computed_at column. internal/jobs/metrics/daily/
// families.json lists every table each compute call writes; checking the first is checking the call executed.
var RepoDayFamilies = map[string]string{
	"cicd":                 "cicd_metrics_daily",
	"deploy":               "deploy_metrics_daily",
	"work_item_issue_type": "issue_type_metrics_daily",
	"work_item_investment": "work_item_completion_daily",
	"incident":             "incident_metrics_daily",
	"testops_pipeline":     "testops_pipeline_metrics_daily",
	"testops_test":         "testops_test_metrics_daily",
	"testops_coverage":     "testops_coverage_metrics_daily",
	"repo_user_commit":     "repo_metrics_daily",
	"dora":                 "dora_metrics_daily",
	"complexity":           "repo_complexity_daily",
	"file_hotspots":        "file_metrics_daily",
	"file_risk_hotspots":   "file_hotspot_daily",
	"review_edges":         "review_edges_daily",
}

// Team-keyed families: keyed (team_id, day) and scoped by org_id only. "unassigned" is a valid team_id, so there is no
// live-id oracle to cross-check against.
var TeamDayFamilies = map[string]string{
	"team_wellbeing":      "team_metrics_daily",
	"team_cognitive_load": "team_cognitive_load_daily",
	"team_complexity":     "team_complexity_daily",
	"work_item_state":     "work_item_state_durations_daily",
	"recommendations":     "recommendations_daily",
}

// Scope-keyed families: compounding_risk_daily is keyed (org_id, scope, scope_id, day, computed_at) with no repo_id.
// Repo-scope rows carry the repo id in scope_id (a String); the readback pins scope = 'repo' so team-scope rows from
// the finalize family cannot stand in as proof the partition path ran.
var ScopeIDRepoFamilies = map[string]string{
	"compounding_risk": "compounding_risk_daily",
}

// compounding_risk_team writes the same table at scope = 'team'; scope_id is a team id, so no live-id cross-check.
var ScopeIDTeamFamilies = map[string]string{
	"compounding_risk_team": "compounding_risk_daily",
}

// The benchmarking tables are keyed (metric_name, scope_type, scope_key, period_end): no repo_id, team_id or day.
// Repo-scope rows carry the repo id in scope_key.
var ScopeKeyFamilies = map[string]string{
	"benchmarking": "testops_metric_baselines",
}

// ic_finalize's first write target mixes two id spaces in repo_id: a git-backed identity keeps its real repo id, a
// work-item-only identity gets a deterministic per-identity id (SynthesizedRepoID in
// internal/jobs/metrics/daily/icfinalize). Value is (table, identity column).
var SynthesizedRepoIDFamilies = map[string][2]string{
	"ic_finalize": {"user_metrics_daily", "author_email"},
}

// synthesizedRepoNamespace and the payload below must match icfinalize.SynthesizedRepoID exactly; the tests compare
// the two.
var synthesizedRepoNamespace = uuid.MustParse("1b4e28ba-2fa1-11d2-883f-b9a761bde3fb")

// SynthesizedRepoID is the repo id ic_finalize writes for an identity with work-item metrics but no git record.
func SynthesizedRepoID(orgID, identityID string) string {
	return uuid.NewSHA1(synthesizedRepoNamespace, []byte(orgID+"\x1f"+identityID)).String()
}

// Known reports whether the readback has a shape for the family.
func Known(family string) bool {
	_, a := RepoDayFamilies[family]
	_, b := TeamDayFamilies[family]
	_, c := ScopeIDRepoFamilies[family]
	_, d := ScopeIDTeamFamilies[family]
	_, e := ScopeKeyFamilies[family]
	_, f := SynthesizedRepoIDFamilies[family]
	return a || b || c || d || e || f
}

// allFamilies is every family the readback accepts, shape by shape, each shape sorted.
func allFamilies() []string {
	var out []string
	for _, m := range []map[string]string{RepoDayFamilies, TeamDayFamilies, ScopeIDRepoFamilies, ScopeIDTeamFamilies, ScopeKeyFamilies} {
		out = append(out, sortedMapKeys(m)...)
	}
	var synth []string
	for k := range SynthesizedRepoIDFamilies {
		synth = append(synth, k)
	}
	sort.Strings(synth)
	return append(out, synth...)
}

func sortedMapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func dateTime64Literal(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000000") }

func sortedIDs(s IDSet) []string {
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedCountKeys(m map[string]FamilyRowCount) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func totalRows(m map[string]FamilyRowCount) uint64 {
	var n uint64
	for _, v := range m {
		n += v.Rows
	}
	return n
}

func idSet(ids []string) IDSet {
	out := IDSet{}
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

func difference(a, b IDSet) IDSet {
	out := IDSet{}
	for k := range a {
		if _, ok := b[k]; !ok {
			out[k] = struct{}{}
		}
	}
	return out
}

func queryStrings(ctx context.Context, c Conn, query string, args ...any) ([]string, error) {
	rows, err := c.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func queryCounts(ctx context.Context, c Conn, query string, args ...any) (map[string]FamilyRowCount, error) {
	rows, err := c.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]FamilyRowCount{}
	for rows.Next() {
		var (
			key    string
			n      uint64
			latest time.Time
		)
		if err := rows.Scan(&key, &n, &latest); err != nil {
			return nil, err
		}
		out[key] = FamilyRowCount{Rows: n, LatestComputedAt: latest}
	}
	return out, rows.Err()
}

// LiveRepoIDs returns the org's real ClickHouse repo ids. It is the same query as
// internal/jobs/metrics/daily/clickhouse.go ClickHouseRepositoryDiscoverer.RepositoryIDs, the oracle the dead-id defect
// bypassed: validating against a re-derivation of "the real repos" would move the same id-space bug into the check.
func LiveRepoIDs(ctx context.Context, c Conn, orgID string) (IDSet, error) {
	ids, err := queryStrings(ctx, c, `
        SELECT id FROM (
          SELECT id, argMax(tuple(repo, settings, provider), last_synced) AS latest
          FROM repos
          WHERE org_id = {org_id:String}
          GROUP BY org_id, id
        )
        ORDER BY id
        `, clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, err
	}
	return idSet(ids), nil
}

// FamilyReadback returns the rows the table holds for this org's repos since runStart. The query binds org_id as well
// as the repo id set: a repo id set alone lets a foreign or blank-org row for the target org's own repo satisfy it.
func FamilyReadback(ctx context.Context, c Conn, table, orgID string, repoIDs IDSet, runStart time.Time) (map[string]FamilyRowCount, error) {
	if len(repoIDs) == 0 {
		return map[string]FamilyRowCount{}, nil
	}
	return queryCounts(ctx, c, fmt.Sprintf(`
        SELECT repo_id, count() AS n, max(computed_at) AS latest
        FROM %s
        WHERE org_id = {org_id:String}
          AND repo_id IN {repo_ids:Array(UUID)}
          AND computed_at >= {run_start:DateTime64(6)}
        GROUP BY repo_id
        `, table),
		clickhouse.Named("org_id", orgID), clickhouse.Named("repo_ids", sortedIDs(repoIDs)),
		clickhouse.Named("run_start", dateTime64Literal(runStart)))
}

// UnscopedRepoIDs returns the repo ids the table carries for this run regardless of org. It detects the dead-id
// failure mode directly: rows written with a repo id ClickHouse repos does not have.
func UnscopedRepoIDs(ctx context.Context, c Conn, table string, runStart time.Time) (IDSet, error) {
	ids, err := queryStrings(ctx, c,
		fmt.Sprintf("SELECT DISTINCT repo_id FROM %s WHERE computed_at >= {run_start:DateTime64(6)}", table),
		clickhouse.Named("run_start", dateTime64Literal(runStart)))
	if err != nil {
		return nil, err
	}
	return idSet(ids), nil
}

// TeamReadback is FamilyReadback's counterpart for team-keyed tables, scoped by org_id directly.
func TeamReadback(ctx context.Context, c Conn, table, orgID string, runStart time.Time) (map[string]FamilyRowCount, error) {
	return queryCounts(ctx, c, fmt.Sprintf(`
        SELECT team_id, count() AS n, max(computed_at) AS latest
        FROM %s
        WHERE org_id = {org_id:String}
          AND computed_at >= {run_start:DateTime64(6)}
        GROUP BY team_id
        `, table),
		clickhouse.Named("org_id", orgID), clickhouse.Named("run_start", dateTime64Literal(runStart)))
}

// ScopeKeyRepoReadback reads scope_type/scope_key tables. scope_type = 'repo' is pinned so team- and global-scope rows
// cannot stand in as proof the repo path ran, and scope_key is bound as Array(String) because it is a String column.
func ScopeKeyRepoReadback(ctx context.Context, c Conn, table, orgID string, repoIDs IDSet, runStart time.Time) (map[string]FamilyRowCount, error) {
	if len(repoIDs) == 0 {
		return map[string]FamilyRowCount{}, nil
	}
	return queryCounts(ctx, c, fmt.Sprintf(`
        SELECT scope_key, count() AS n, max(computed_at) AS latest
        FROM %s
        WHERE org_id = {org_id:String}
          AND scope_type = 'repo'
          AND scope_key IN {repo_ids:Array(String)}
          AND computed_at >= {run_start:DateTime64(6)}
        GROUP BY scope_key
        `, table),
		clickhouse.Named("org_id", orgID), clickhouse.Named("repo_ids", sortedIDs(repoIDs)),
		clickhouse.Named("run_start", dateTime64Literal(runStart)))
}

// ScopeIDRepoReadback reads scope/scope_id tables. scope = 'repo' is pinned (the table also holds team-scope rows
// written by the finalize family) and scope_id is a String, so the repo id set is bound as Array(String). The
// cross-check against the live repo ids keeps the dead-id oracle for this shape.
func ScopeIDRepoReadback(ctx context.Context, c Conn, table, orgID string, repoIDs IDSet, runStart time.Time) (map[string]FamilyRowCount, error) {
	if len(repoIDs) == 0 {
		return map[string]FamilyRowCount{}, nil
	}
	return queryCounts(ctx, c, fmt.Sprintf(`
        SELECT scope_id, count() AS n, max(computed_at) AS latest
        FROM %s
        WHERE org_id = {org_id:String}
          AND scope = 'repo'
          AND scope_id IN {repo_ids:Array(String)}
          AND computed_at >= {run_start:DateTime64(6)}
        GROUP BY scope_id
        `, table),
		clickhouse.Named("org_id", orgID), clickhouse.Named("repo_ids", sortedIDs(repoIDs)),
		clickhouse.Named("run_start", dateTime64Literal(runStart)))
}

// ScopeIDTeamReadback reads the same table at scope = 'team', grouped by scope_id (a team id), with no live-id check.
func ScopeIDTeamReadback(ctx context.Context, c Conn, table, orgID string, runStart time.Time) (map[string]FamilyRowCount, error) {
	return queryCounts(ctx, c, fmt.Sprintf(`
        SELECT scope_id, count() AS n, max(computed_at) AS latest
        FROM %s
        WHERE org_id = {org_id:String}
          AND scope = 'team'
          AND computed_at >= {run_start:DateTime64(6)}
        GROUP BY scope_id
        `, table),
		clickhouse.Named("org_id", orgID), clickhouse.Named("run_start", dateTime64Literal(runStart)))
}

// UnscopedScopeIDs is UnscopedRepoIDs' counterpart for scope/scope_id tables.
func UnscopedScopeIDs(ctx context.Context, c Conn, table string, runStart time.Time) (IDSet, error) {
	ids, err := queryStrings(ctx, c,
		fmt.Sprintf("SELECT DISTINCT scope_id FROM %s WHERE scope = 'repo' AND computed_at >= {run_start:DateTime64(6)}", table),
		clickhouse.Named("run_start", dateTime64Literal(runStart)))
	if err != nil {
		return nil, err
	}
	return idSet(ids), nil
}

// SynthesizedRepoReadback is FamilyReadback for a table whose repo_id is valid when it is either a live repo or the
// synthesized id for that row's own identity. It returns the total row count and the stray repo ids.
func SynthesizedRepoReadback(ctx context.Context, c Conn, table, identityColumn, orgID string, repoIDs IDSet, runStart time.Time) (uint64, IDSet, error) {
	rows, err := c.Query(ctx, fmt.Sprintf(`
        SELECT repo_id, %s, count() AS n
        FROM %s
        WHERE org_id = {org_id:String}
          AND computed_at >= {run_start:DateTime64(6)}
        GROUP BY repo_id, %s
        `, identityColumn, table, identityColumn),
		clickhouse.Named("org_id", orgID), clickhouse.Named("run_start", dateTime64Literal(runStart)))
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	var total uint64
	stray := IDSet{}
	for rows.Next() {
		var (
			repoID, identity string
			n                uint64
		)
		if err := rows.Scan(&repoID, &identity, &n); err != nil {
			return 0, nil, err
		}
		total += n
		if _, ok := repoIDs[repoID]; ok {
			continue
		}
		if repoID == SynthesizedRepoID(orgID, identity) {
			continue
		}
		stray[repoID] = struct{}{}
	}
	return total, stray, rows.Err()
}

type repoEntry struct {
	Table             string   `json:"table"`
	OrgID             string   `json:"org_id"`
	RowsWritten       uint64   `json:"rows_written"`
	ReposWithRows     []string `json:"repos_with_rows"`
	RepoIDsOutsideOrg []string `json:"repo_ids_outside_org"`
}

type teamEntry struct {
	Table         string   `json:"table"`
	OrgID         string   `json:"org_id"`
	RowsWritten   uint64   `json:"rows_written"`
	TeamsWithRows []string `json:"teams_with_rows"`
}

type scopeKeyEntry struct {
	Table         string   `json:"table"`
	OrgID         string   `json:"org_id"`
	RowsWritten   uint64   `json:"rows_written"`
	ReposWithRows []string `json:"repos_with_rows"`
}

type synthEntry struct {
	Table             string   `json:"table"`
	OrgID             string   `json:"org_id"`
	RowsWritten       uint64   `json:"rows_written"`
	RepoIDsOutsideOrg []string `json:"repo_ids_outside_org"`
}

type summaryItem struct {
	family string
	value  any
}

type summary []summaryItem

func (s summary) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, item := range s {
		if i > 0 {
			b.WriteByte(',')
		}
		key, err := json.Marshal(item.family)
		if err != nil {
			return nil, err
		}
		val, err := json.Marshal(item.value)
		if err != nil {
			return nil, err
		}
		b.Write(key)
		b.WriteByte(':')
		b.Write(val)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

type options struct {
	uri, orgID, runStartRaw, summaryJSON string
	families                             []string
}

type usageError string

func (e usageError) Error() string { return string(e) }

func parseArgs(args []string, getenv func(string) string) (options, time.Time, error) {
	opts := options{uri: getenv("CLICKHOUSE_URI")}
	valid := map[string]bool{}
	all := allFamilies()
	for _, f := range all {
		valid[f] = true
	}
	var familiesSet bool
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name, inline, hasInline := strings.Cut(arg, "=")
		value := func() (string, error) {
			if hasInline {
				return inline, nil
			}
			if i+1 >= len(args) {
				return "", usageError(fmt.Sprintf("argument %s: expected one argument", name))
			}
			i++
			return args[i], nil
		}
		var err error
		switch name {
		case "--clickhouse-uri":
			opts.uri, err = value()
		case "--org-id":
			opts.orgID, err = value()
		case "--run-start":
			opts.runStartRaw, err = value()
		case "--summary-json":
			opts.summaryJSON, err = value()
		case "--families":
			familiesSet = true
			opts.families = nil
			if hasInline {
				opts.families = append(opts.families, inline)
			}
			for i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
				i++
				opts.families = append(opts.families, args[i])
			}
			if len(opts.families) == 0 {
				err = usageError("argument --families: expected at least one argument")
			}
		default:
			err = usageError(fmt.Sprintf("unrecognized arguments: %s", arg))
		}
		if err != nil {
			return opts, time.Time{}, err
		}
	}
	if opts.orgID == "" {
		return opts, time.Time{}, usageError("the following arguments are required: --org-id")
	}
	if opts.runStartRaw == "" {
		return opts, time.Time{}, usageError("the following arguments are required: --run-start")
	}
	if familiesSet {
		for _, f := range opts.families {
			if !valid[f] {
				return opts, time.Time{}, usageError(fmt.Sprintf("argument --families: invalid choice: %q (choose from %s)", f, strings.Join(all, ", ")))
			}
		}
	} else {
		opts.families = all
	}
	runStart, err := parseRunStart(opts.runStartRaw)
	if err != nil {
		return opts, time.Time{}, usageError(fmt.Sprintf("argument --run-start: invalid ISO8601 timestamp: %q", opts.runStartRaw))
	}
	if opts.uri == "" {
		return opts, time.Time{}, usageError("--clickhouse-uri or CLICKHOUSE_URI is required")
	}
	return opts, runStart, nil
}

// parseRunStart reads the ISO8601 forms the callers emit; a stamp without an offset is UTC.
func parseRunStart(s string) (time.Time, error) {
	for _, layout := range []string{
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02",
	} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unparsable timestamp %q", s)
}

func zeroRows(family, table, what, runStartRaw, orgID string) string {
	return fmt.Sprintf("%s (%s): zero_rows_with_source_data -- no %s with computed_at >= %s for %s.", family, table, what, runStartRaw, orgID)
}

// Run is `cigate metrics-proof`. The DSN reaches it by CLICKHOUSE_URI so a credential never has to sit in argv.
func Run(args []string, getenv func(string) string, open Opener, stdout, stderr io.Writer) int {
	opts, runStart, err := parseArgs(args, getenv)
	if err != nil {
		fmt.Fprintf(stderr, "usage: cigate metrics-proof --org-id ID --run-start TS [--clickhouse-uri URI] [--families F...] [--summary-json PATH]\ncigate metrics-proof: error: %s\n", err)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn, closeConn, err := open(ctx, opts.uri)
	if err != nil {
		fmt.Fprintf(stderr, "metrics-proof: connect: %s\n", redactURI(err.Error(), opts.uri))
		return 1
	}
	defer closeConn()
	rc, err := run(ctx, conn, opts, runStart, stdout, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "metrics-proof: %s\n", redactURI(err.Error(), opts.uri))
		return 1
	}
	return rc
}

func redactURI(msg, uri string) string {
	if uri == "" {
		return msg
	}
	return strings.ReplaceAll(msg, uri, "<clickhouse-uri>")
}

func run(ctx context.Context, c Conn, opts options, runStart time.Time, stdout, stderr io.Writer) (int, error) {
	repoIDs, err := LiveRepoIDs(ctx, c, opts.orgID)
	if err != nil {
		return 0, err
	}
	if len(repoIDs) == 0 {
		fmt.Fprintf(stderr, "FAILED: org %s has no rows in ClickHouse repos. Nothing was actually synced for this org -- the readback below would be checking against an empty oracle, which is the same false-clean shape as the zero-repo partitions reporting succeeded defect.\n", opts.orgID)
		return 1, nil
	}
	var out summary
	var failures []string
	for _, family := range opts.families {
		if spec, ok := SynthesizedRepoIDFamilies[family]; ok {
			table, identityColumn := spec[0], spec[1]
			total, stray, err := SynthesizedRepoReadback(ctx, c, table, identityColumn, opts.orgID, repoIDs, runStart)
			if err != nil {
				return 0, err
			}
			out = append(out, summaryItem{family, synthEntry{table, opts.orgID, total, sortedIDs(stray)}})
			if total == 0 {
				failures = append(failures, zeroRows(family, table, "row", opts.runStartRaw, "org "+opts.orgID))
			}
			if len(stray) > 0 {
				failures = append(failures, fmt.Sprintf("%s (%s): wrote repo_id(s) %s that are neither a live repo for this org nor the expected synthesized id for their own identity -- the exact CHAOS-4263 dead-id shape.", family, table, pyList(sortedIDs(stray))))
			}
			continue
		}
		if table, ok := TeamDayFamilies[family]; ok {
			rows, err := TeamReadback(ctx, c, table, opts.orgID, runStart)
			if err != nil {
				return 0, err
			}
			total := totalRows(rows)
			out = append(out, summaryItem{family, teamEntry{table, opts.orgID, total, sortedCountKeys(rows)}})
			if total == 0 {
				failures = append(failures, zeroRows(family, table, "row", opts.runStartRaw, "org "+opts.orgID))
			}
			continue
		}
		if table, ok := ScopeKeyFamilies[family]; ok {
			rows, err := ScopeKeyRepoReadback(ctx, c, table, opts.orgID, repoIDs, runStart)
			if err != nil {
				return 0, err
			}
			total := totalRows(rows)
			out = append(out, summaryItem{family, scopeKeyEntry{table, opts.orgID, total, sortedCountKeys(rows)}})
			if total == 0 {
				failures = append(failures, fmt.Sprintf("%s (%s): zero_rows_with_source_data -- no scope_type='repo' row with computed_at >= %s for any of org %s's %d live repo(s).", family, table, opts.runStartRaw, opts.orgID, len(repoIDs)))
			}
			continue
		}
		if table, ok := ScopeIDRepoFamilies[family]; ok {
			rows, err := ScopeIDRepoReadback(ctx, c, table, opts.orgID, repoIDs, runStart)
			if err != nil {
				return 0, err
			}
			total := totalRows(rows)
			all, err := UnscopedScopeIDs(ctx, c, table, runStart)
			if err != nil {
				return 0, err
			}
			stray := difference(all, repoIDs)
			out = append(out, summaryItem{family, repoEntry{table, opts.orgID, total, sortedCountKeys(rows), sortedIDs(stray)}})
			if total == 0 {
				failures = append(failures, fmt.Sprintf("%s (%s): zero_rows_with_source_data -- no scope='repo' row with computed_at >= %s for any of org %s's %d live repo(s).", family, table, opts.runStartRaw, opts.orgID, len(repoIDs)))
			}
			if len(stray) > 0 {
				failures = append(failures, fmt.Sprintf("%s (%s): wrote scope_id(s) %s that are not in ClickHouse repos for any org at all -- the exact CHAOS-4263 dead-id shape.", family, table, pyList(sortedIDs(stray))))
			}
			continue
		}
		if table, ok := ScopeIDTeamFamilies[family]; ok {
			rows, err := ScopeIDTeamReadback(ctx, c, table, opts.orgID, runStart)
			if err != nil {
				return 0, err
			}
			total := totalRows(rows)
			out = append(out, summaryItem{family, teamEntry{table, opts.orgID, total, sortedCountKeys(rows)}})
			if total == 0 {
				failures = append(failures, fmt.Sprintf("%s (%s): zero_rows_with_source_data -- no scope='team' row with computed_at >= %s for org %s.", family, table, opts.runStartRaw, opts.orgID))
			}
			continue
		}
		table := RepoDayFamilies[family]
		rows, err := FamilyReadback(ctx, c, table, opts.orgID, repoIDs, runStart)
		if err != nil {
			return 0, err
		}
		total := totalRows(rows)
		all, err := UnscopedRepoIDs(ctx, c, table, runStart)
		if err != nil {
			return 0, err
		}
		stray := difference(all, repoIDs)
		out = append(out, summaryItem{family, repoEntry{table, opts.orgID, total, sortedCountKeys(rows), sortedIDs(stray)}})
		if total == 0 {
			failures = append(failures, fmt.Sprintf("%s (%s): zero_rows_with_source_data -- no row with computed_at >= %s for any of org %s's %d live repo(s).", family, table, opts.runStartRaw, opts.orgID, len(repoIDs)))
		}
		if len(stray) > 0 {
			failures = append(failures, fmt.Sprintf("%s (%s): wrote repo_id(s) %s that are not in ClickHouse repos for any org at all -- the exact CHAOS-4263 dead-id shape.", family, table, pyList(sortedIDs(stray))))
		}
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		return 0, err
	}
	if _, err := stdout.Write(buf.Bytes()); err != nil {
		return 0, err
	}
	if opts.summaryJSON != "" {
		if err := os.WriteFile(opts.summaryJSON, bytes.TrimSuffix(buf.Bytes(), []byte("\n")), 0o644); err != nil {
			return 0, err
		}
	}
	if len(failures) > 0 {
		fmt.Fprintln(stderr, "\nFAILED:")
		for _, line := range failures {
			fmt.Fprintf(stderr, "  - %s\n", line)
		}
		return 1, nil
	}
	fmt.Fprintf(stdout, "\nOK: %d family(ies) produced rows for org %s, all within its %d live repo id(s).\n", len(opts.families), opts.orgID, len(repoIDs))
	return 0, nil
}

// pyList renders ids as `['a', 'b']`, the form the failure lines have always used.
func pyList(ids []string) string {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = "'" + id + "'"
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// OpenClickHouse connects over the native protocol from a clickhouse:// DSN.
func OpenClickHouse(_ context.Context, uri string) (Conn, func() error, error) {
	opts, err := clickhouse.ParseDSN(uri)
	if err != nil {
		return nil, nil, fmt.Errorf("parse DSN: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, nil, err
	}
	return conn, conn.Close, nil
}
