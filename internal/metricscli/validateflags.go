// Package metricscli is the `metrics` group of dho: read-only diagnostics over
// the analytics database. Its first verb, `metrics validate-flags`, is the
// feature-flag pipeline health check `dev-hops metrics validate-flags` ran: a
// fixed set of queries against ClickHouse, each reduced to a status and a line
// of text, printed as one report.
//
// The text is the Python report's, byte for byte (the numbers are formatted as
// Python formats them), because operators read it and scripts grep it.
package metricscli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// Command is the `metrics` group.
func Command() cli.Command {
	return cli.Command{
		Name:    "metrics",
		Summary: "read-only diagnostics over the analytics database",
		Kind:    cli.Group,
		Children: []cli.Command{{
			Name:    "validate-flags",
			Summary: "run the feature-flag pipeline validation checks and print the report",
			Kind:    cli.Verb,
			Run:     runValidateFlags,
		}},
	}
}

// Status is the outcome of one check.
type Status string

// The four statuses, in the order the summary line lists them.
const (
	StatusOK       Status = "ok"
	StatusWarn     Status = "warn"
	StatusCritical Status = "critical"
	StatusSkip     Status = "skip"
)

var statusOrder = []Status{StatusOK, StatusWarn, StatusCritical, StatusSkip}

// Field is one key=value of a detail row; Value is already text (Python's str()
// of the value).
type Field struct{ Key, Value string }

// Check is the result of one check.
type Check struct {
	Name    string
	Status  Status
	Message string
	Detail  [][]Field
}

// Report is the result of every check for one org.
type Report struct {
	OrgID  string
	Checks []Check
}

// HasCritical reports whether any check is critical.
func (report Report) HasCritical() bool {
	for _, check := range report.Checks {
		if check.Status == StatusCritical {
			return true
		}
	}
	return false
}

// HasWarnings reports whether any check is a warning.
func (report Report) HasWarnings() bool {
	for _, check := range report.Checks {
		if check.Status == StatusWarn {
			return true
		}
	}
	return false
}

// SummaryLine is "Validation report for org='…': N ok, N warn, …" (only the
// non-zero counts).
func (report Report) SummaryLine() string {
	counts := map[Status]int{}
	for _, check := range report.Checks {
		counts[check.Status]++
	}
	var parts []string
	for _, status := range statusOrder {
		if counts[status] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[status], status))
		}
	}
	return fmt.Sprintf("Validation report for org=%s: %s", pythonparity.StrRepr(report.OrgID), strings.Join(parts, ", "))
}

// Format is the report text (without the trailing newline print adds).
func Format(report Report) string {
	icons := map[Status]string{StatusOK: "✓", StatusWarn: "⚠", StatusCritical: "✗", StatusSkip: "—"}
	lines := []string{
		fmt.Sprintf("Feature Flag Pipeline Validation — org=%s", pythonparity.StrRepr(report.OrgID)),
		strings.Repeat("=", 60),
	}
	for _, check := range report.Checks {
		lines = append(lines, fmt.Sprintf("  [%s] %s: %s", icons[check.Status], check.Name, check.Message))
		if len(check.Detail) > 0 && (check.Status == StatusWarn || check.Status == StatusCritical) {
			for index, row := range check.Detail {
				if index >= 5 {
					break
				}
				parts := make([]string, len(row))
				for position, field := range row {
					parts[position] = field.Key + "=" + field.Value
				}
				lines = append(lines, "      "+strings.Join(parts, ", "))
			}
		}
	}
	lines = append(lines, "", report.SummaryLine())
	switch {
	case report.HasCritical():
		lines = append(lines, "RESULT: CRITICAL — pipeline health checks failed.")
	case report.HasWarnings():
		lines = append(lines, "RESULT: WARNING — review flagged items.")
	default:
		lines = append(lines, "RESULT: OK — all checks passed.")
	}
	return strings.Join(lines, "\n")
}

// pyFixed is format(value, ".<precision>f").
func pyFixed(value float64, precision int) string {
	text, err := pythonparity.FormatFixed(value, precision)
	if err != nil {
		return strconv.FormatFloat(value, 'f', precision, 64)
	}
	return text
}

// pyPercent is format(value, ".<precision>%"): the value times 100, formatted
// fixed, then a percent sign.
func pyPercent(value float64, precision int) string { return pyFixed(value*100, precision) + "%" }

func pyFloat(value float64) string { return pythonparity.Repr(value) }
func pyInt(value uint64) string    { return strconv.FormatUint(value, 10) }

// query runs one statement with server-side parameters and returns its rows
// through scan, one call per row.
func query(ctx context.Context, conn driver.Conn, statement string, scan func(rows driver.Rows) error, parameters ...any) error {
	rows, err := conn.Query(ctx, statement, parameters...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

func params(orgID string, lookback *uint32) []any {
	out := []any{clickhouse.Named("org_id", orgID)}
	if lookback != nil {
		out = append(out, clickhouse.Named("lookback", *lookback))
	}
	return out
}

// ratioStatus maps a ratio to critical below criticalBelow, warn below
// warnBelow, else ok.
func ratioStatus(ratio, criticalBelow, warnBelow float64) Status {
	switch {
	case ratio < criticalBelow:
		return StatusCritical
	case ratio < warnBelow:
		return StatusWarn
	default:
		return StatusOK
	}
}

func checkCoverage(ctx context.Context, conn driver.Conn, orgID string, lookback uint32) (Check, error) {
	var total, covered uint64
	var found bool
	err := query(ctx, conn, `
        WITH releases AS (
            SELECT DISTINCT release_ref
            FROM deployments
            WHERE release_ref != ''
              AND org_id = {org_id:String}
              AND toDate(coalesce(deployed_at, started_at))
                  >= today() - {lookback:UInt32}
        ),
        covered AS (
            SELECT DISTINCT release_ref
            FROM telemetry_signal_bucket
            WHERE release_ref != ''
              AND org_id = {org_id:String}
              AND toDate(bucket_start) >= today() - {lookback:UInt32}
        )
        SELECT
            count() AS total_releases,
            countIf(c.release_ref != '') AS covered_releases
        FROM releases AS r
        LEFT JOIN covered AS c ON r.release_ref = c.release_ref
    `, func(rows driver.Rows) error {
		found = true
		return rows.Scan(&total, &covered)
	}, params(orgID, &lookback)...)
	if err != nil {
		return Check{}, err
	}
	if !found || total == 0 {
		return Check{Name: "coverage", Status: StatusSkip, Message: "No releases found in lookback window."}, nil
	}
	ratio := float64(covered) / float64(total)
	return Check{
		Name:    "coverage",
		Status:  ratioStatus(ratio, 0.50, 0.70),
		Message: fmt.Sprintf("%d/%d releases have telemetry (%s).", covered, total, pyPercent(ratio, 0)),
		Detail:  [][]Field{{{"total_releases", pyInt(total)}, {"covered_releases", pyInt(covered)}, {"ratio", pyFloat(ratio)}}},
	}, nil
}

// dedupTables are the event tables whose dedupe_key must be unique.
var dedupTables = []struct{ Table, KeyColumn string }{
	{"feature_flag_event", "dedupe_key"},
	{"telemetry_signal_bucket", "dedupe_key"},
}

func checkDedup(ctx context.Context, conn driver.Conn, orgID string) (Check, error) {
	type issue struct {
		table                string
		total, distinct, dup uint64
		ratio                float64
	}
	var issues []issue
	for _, table := range dedupTables {
		var total, distinct uint64
		var found bool
		err := query(ctx, conn, fmt.Sprintf(`
            SELECT
                count() AS total,
                count(DISTINCT %s) AS distinct_keys
            FROM %s
            WHERE org_id = {org_id:String}
        `, table.KeyColumn, table.Table), func(rows driver.Rows) error {
			found = true
			return rows.Scan(&total, &distinct)
		}, params(orgID, nil)...)
		if err != nil {
			return Check{}, err
		}
		if !found {
			continue
		}
		duplicates := total - distinct
		if int64(total)-int64(distinct) > 0 {
			ratio := 0.0
			if total != 0 {
				ratio = float64(duplicates) / float64(total)
			}
			issues = append(issues, issue{table.Table, total, distinct, duplicates, ratio})
		}
	}
	if len(issues) == 0 {
		return Check{Name: "dedup_verification", Status: StatusOK, Message: "No duplicate dedupe_keys found in event tables."}, nil
	}
	worst := issues[0].ratio
	names := make([]string, len(issues))
	detail := make([][]Field, len(issues))
	for index, item := range issues {
		if item.ratio > worst {
			worst = item.ratio
		}
		names[index] = item.table
		detail[index] = []Field{{"table", item.table}, {"total_rows", pyInt(item.total)}, {"distinct_keys", pyInt(item.distinct)}, {"duplicates", pyInt(item.dup)}, {"dup_ratio", pyFloat(item.ratio)}}
	}
	status := StatusWarn
	if worst > 0.05 {
		status = StatusCritical
	}
	return Check{
		Name:    "dedup_verification",
		Status:  status,
		Message: fmt.Sprintf("Duplicate dedupe_keys in: %s (worst ratio: %s).", strings.Join(names, ", "), pyPercent(worst, 2)),
		Detail:  detail,
	}, nil
}

// requiredFlagFields are the feature_flag columns a complete record populates:
// a string column is populated when it is not ” and not NULL. last_synced is a
// DateTime64: the Python check compared it with ” too, which ClickHouse 26.x
// refuses (code 41, "Cannot read DateTime"), so the whole Python command failed
// before printing anything. Here a timestamp is populated when it is not NULL
// (the column is not Nullable, so that is always so: the same answer the check
// meant to give).
var requiredFlagFields = []struct {
	Column   string
	IsString bool
}{
	{"provider", true}, {"flag_key", true}, {"environment", true}, {"flag_type", true}, {"last_synced", false},
}

func checkSchemaCompleteness(ctx context.Context, conn driver.Conn, orgID string) (Check, error) {
	conditions := make([]string, len(requiredFlagFields))
	for index, field := range requiredFlagFields {
		if field.IsString {
			conditions[index] = fmt.Sprintf("%s != '' AND %s IS NOT NULL", field.Column, field.Column)
		} else {
			conditions[index] = fmt.Sprintf("%s IS NOT NULL", field.Column)
		}
	}
	var total, complete uint64
	var found bool
	err := query(ctx, conn, fmt.Sprintf(`
        SELECT
            count() AS total,
            countIf(%s) AS complete
        FROM feature_flag FINAL
        WHERE org_id = {org_id:String}
    `, strings.Join(conditions, " AND ")), func(rows driver.Rows) error {
		found = true
		return rows.Scan(&total, &complete)
	}, params(orgID, nil)...)
	if err != nil {
		return Check{}, err
	}
	if !found || total == 0 {
		return Check{Name: "schema_completeness", Status: StatusSkip, Message: "No feature_flag records found."}, nil
	}
	ratio := float64(complete) / float64(total)
	return Check{
		Name:    "schema_completeness",
		Status:  ratioStatus(ratio, 0.80, 0.95),
		Message: fmt.Sprintf("%d/%d flags have all required fields (%s).", complete, total, pyPercent(ratio, 0)),
		Detail:  [][]Field{{{"total", pyInt(total)}, {"complete", pyInt(complete)}, {"ratio", pyFloat(ratio)}}},
	}, nil
}

func checkDrift(ctx context.Context, conn driver.Conn, orgID string, lookback uint32) (Check, error) {
	type day struct {
		day   time.Time
		count uint64
	}
	var days []day
	err := query(ctx, conn, `
        SELECT
            toDate(bucket_start) AS day,
            count() AS bucket_count
        FROM telemetry_signal_bucket
        WHERE org_id = {org_id:String}
          AND toDate(bucket_start) >= today() - {lookback:UInt32}
        GROUP BY day
        ORDER BY day
    `, func(rows driver.Rows) error {
		var item day
		if err := rows.Scan(&item.day, &item.count); err != nil {
			return err
		}
		days = append(days, item)
		return nil
	}, params(orgID, &lookback)...)
	if err != nil {
		return Check{}, err
	}
	if len(days) < 2 {
		return Check{Name: "drift_detection", Status: StatusSkip, Message: "Not enough daily data for drift detection."}, nil
	}
	var spikes [][]Field
	for index := 1; index < len(days); index++ {
		previous, current := days[index-1].count, days[index].count
		if previous == 0 {
			continue
		}
		ratio := float64(current) / float64(previous)
		if ratio > 2.0 || ratio < 0.5 {
			rounded, err := strconv.ParseFloat(pyFixed(ratio, 2), 64)
			if err != nil {
				return Check{}, err
			}
			spikes = append(spikes, []Field{
				{"day", days[index].day.Format("2006-01-02")}, {"prev_day", days[index-1].day.Format("2006-01-02")},
				{"prev_count", pyInt(previous)}, {"curr_count", pyInt(current)}, {"change_ratio", pyFloat(rounded)},
			})
		}
	}
	if len(spikes) == 0 {
		return Check{Name: "drift_detection", Status: StatusOK, Message: "No >2× day-over-day volume changes detected."}, nil
	}
	return Check{
		Name:    "drift_detection",
		Status:  StatusWarn,
		Message: fmt.Sprintf("%d day(s) with >2× volume change in last %dd.", len(spikes), lookback),
		Detail:  spikes,
	}, nil
}

func checkOrgIsolation(ctx context.Context, conn driver.Conn, orgID string) (Check, error) {
	tables := []string{"feature_flag", "feature_flag_event", "telemetry_signal_bucket", "release_impact_daily"}
	leaks := 0
	for _, table := range tables {
		var others uint64
		var found bool
		err := query(ctx, conn, fmt.Sprintf(`
            SELECT count(DISTINCT org_id) AS org_count
            FROM %s
            WHERE org_id != {org_id:String}
              AND org_id != ''
        `, table), func(rows driver.Rows) error {
			found = true
			return rows.Scan(&others)
		}, params(orgID, nil)...)
		if err != nil {
			return Check{}, err
		}
		if found && others > 0 {
			leaks++
		}
	}
	if leaks == 0 {
		return Check{Name: "org_isolation", Status: StatusOK, Message: "No cross-org data detected (spot check)."}, nil
	}
	// Other orgs existing is normal in a multi-tenant deployment; the check
	// reminds the operator to verify scoped queries, it does not fail.
	return Check{
		Name:    "org_isolation",
		Status:  StatusOK,
		Message: fmt.Sprintf("Multi-tenant data present in %d table(s) — verify scoped queries filter correctly.", leaks),
	}, nil
}

func checkJoinIntegrity(ctx context.Context, conn driver.Conn, orgID string, lookback uint32) (Check, error) {
	var total, matched uint64
	var found bool
	err := query(ctx, conn, `
        WITH impact AS (
            SELECT DISTINCT release_ref, environment
            FROM release_impact_daily
            WHERE org_id = {org_id:String}
              AND day >= today() - {lookback:UInt32}
        ),
        joined AS (
            SELECT i.release_ref, i.environment,
                   d.release_ref AS deploy_ref
            FROM impact AS i
            LEFT JOIN (
                SELECT DISTINCT release_ref, environment
                FROM deployments
                WHERE release_ref != ''
                  AND org_id = {org_id:String}
            ) AS d
            ON i.release_ref = d.release_ref
               AND i.environment = d.environment
        )
        SELECT
            count() AS total,
            countIf(deploy_ref != '') AS matched
        FROM joined
    `, func(rows driver.Rows) error {
		found = true
		return rows.Scan(&total, &matched)
	}, params(orgID, &lookback)...)
	if err != nil {
		return Check{}, err
	}
	if !found || total == 0 {
		return Check{Name: "join_integrity", Status: StatusSkip, Message: "No release_impact_daily rows in lookback window."}, nil
	}
	ratio := float64(matched) / float64(total)
	return Check{
		Name:    "join_integrity",
		Status:  ratioStatus(ratio, 0.50, 0.80),
		Message: fmt.Sprintf("%d/%d impact rows join to deployments (%s).", matched, total, pyPercent(ratio, 0)),
		Detail:  [][]Field{{{"total", pyInt(total)}, {"matched", pyInt(matched)}, {"ratio", pyFloat(ratio)}}},
	}, nil
}

var confidenceBuckets = []struct {
	Low, High float64
	Label     string
}{
	{0.0, 0.2, "very_low"}, {0.2, 0.4, "low"}, {0.4, 0.6, "medium"}, {0.6, 0.8, "high"}, {0.8, 1.01, "very_high"},
}

func checkConfidenceDistribution(ctx context.Context, conn driver.Conn, orgID string, lookback uint32) (Check, error) {
	var scores []float64
	err := query(ctx, conn, `
        SELECT
            release_impact_confidence_score AS score
        FROM release_impact_daily
        WHERE org_id = {org_id:String}
          AND day >= today() - {lookback:UInt32}
          AND release_impact_confidence_score IS NOT NULL
    `, func(rows driver.Rows) error {
		// The column is Float32; the Python client widens it to a double.
		var score float32
		if err := rows.Scan(&score); err != nil {
			return err
		}
		scores = append(scores, float64(score))
		return nil
	}, params(orgID, &lookback)...)
	if err != nil {
		return Check{}, err
	}
	if len(scores) == 0 {
		return Check{Name: "confidence_distribution", Status: StatusSkip, Message: "No confidence scores in lookback window."}, nil
	}
	total := len(scores)
	histogram := make([][]Field, len(confidenceBuckets))
	var veryLowPct float64
	for index, bucket := range confidenceBuckets {
		count := 0
		for _, score := range scores {
			if bucket.Low <= score && score < bucket.High {
				count++
			}
		}
		pct := float64(count) / float64(total)
		if index == 0 {
			veryLowPct = pct
		}
		histogram[index] = []Field{
			{"bucket", bucket.Label},
			{"range", fmt.Sprintf("[%s, %s)", pyFixed(bucket.Low, 1), pyFixed(bucket.High, 1))},
			{"count", strconv.Itoa(count)}, {"pct", pyFloat(pct)},
		}
	}
	status, message := StatusOK, ""
	if veryLowPct > 0.50 {
		status = StatusWarn
		message = fmt.Sprintf("%s of confidence scores are very low (<0.2) — check coverage and sample sizes.", pyPercent(veryLowPct, 0))
	} else {
		message = fmt.Sprintf("%d scores, avg=%s.", total, pyFixed(pythonparity.Sum(scores)/float64(total), 2))
	}
	return Check{Name: "confidence_distribution", Status: status, Message: message, Detail: histogram}, nil
}

// Validate runs every check for the org, in the order the report lists them.
func Validate(ctx context.Context, conn driver.Conn, orgID string, lookbackDays uint32) (Report, error) {
	drift := lookbackDays
	if drift > 14 {
		drift = 14
	}
	report := Report{OrgID: orgID}
	steps := []func() (Check, error){
		func() (Check, error) { return checkCoverage(ctx, conn, orgID, lookbackDays) },
		func() (Check, error) { return checkDedup(ctx, conn, orgID) },
		func() (Check, error) { return checkSchemaCompleteness(ctx, conn, orgID) },
		func() (Check, error) { return checkDrift(ctx, conn, orgID, drift) },
		func() (Check, error) { return checkOrgIsolation(ctx, conn, orgID) },
		func() (Check, error) { return checkJoinIntegrity(ctx, conn, orgID, lookbackDays) },
		func() (Check, error) { return checkConfidenceDistribution(ctx, conn, orgID, lookbackDays) },
	}
	for _, step := range steps {
		check, err := step()
		if err != nil {
			return report, err
		}
		report.Checks = append(report.Checks, check)
	}
	return report, nil
}

const usage = `Usage: dho metrics validate-flags [--lookback <days>] [--org <uuid>]

Runs the feature-flag pipeline validation checks against ClickHouse and prints
the report. Read-only. Exits 1 when any check is critical, or when a check
cannot run.

Flags:
  --lookback   number of days to inspect (default 30)
  --org        the organization to validate (default: the ORG_ID environment
               variable, else the empty organization)

Environment:
  CLICKHOUSE_URI (or _FILE, or the DEV_HEALTH_CH_* component form)   ClickHouse DSN, native protocol
`

func runValidateFlags(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho metrics validate-flags", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, usage) }
	lookback := flags.Int("lookback", 30, "number of days to inspect")
	org := flags.String("org", "", "the organization to validate")
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted")
		return cli.ExitUsage
	}
	if *lookback < 0 {
		fmt.Fprintln(env.Stderr, "argument error: --lookback must not be negative")
		return cli.ExitUsage
	}
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}
	// Python takes the organization verbatim: an explicit --org (even an empty
	// one, which scopes the checks to the rows with no organization) wins over
	// ORG_ID, and neither value is trimmed.
	orgID := *org
	given := false
	flags.Visit(func(set *flag.Flag) { given = given || set.Name == "org" })
	if !given {
		orgID, _ = env.Lookup("ORG_ID")
	}

	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	dsn, configured, err := config.ResolveDSN(env.Lookup, "CLICKHOUSE_URI", config.ClickHouseSpec)
	if err != nil {
		logger.Error("flag validation failed", "error", err.Error())
		return cli.ExitFailure
	}
	if !configured {
		logger.Error("flag validation failed", "error", "ClickHouse URI is required (set CLICKHOUSE_URI).")
		return cli.ExitFailure
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	chConfig := chstorage.DefaultConfig(dsn.Reveal())
	chConfig.MaxOpenConns, chConfig.MaxIdleConns = 1, 1
	chConfig.ReadTimeout = 2 * time.Minute
	conn, err := chstorage.Open(ctx, chConfig)
	if err != nil {
		logger.Error("flag validation failed", "error", boundary.Redact(err).Error())
		return cli.ExitFailure
	}
	defer conn.Close()

	report, err := Validate(ctx, conn, orgID, uint32(*lookback))
	if err != nil {
		logger.Error("flag validation failed", "error", boundary.Redact(err).Error())
		return cli.ExitFailure
	}
	logger.Info(report.SummaryLine())
	if _, err := io.WriteString(env.Stdout, Format(report)+"\n"); err != nil {
		return cli.ExitFailure
	}
	if report.HasCritical() {
		return cli.ExitFailure
	}
	return cli.ExitOK
}
