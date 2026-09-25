package fixturescli

import (
	"bytes"
	"compress/gzip"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// The frozen rows of the synthetic sync targets (R24). The generator that made
// them, dev-hops' SyntheticDataGenerator, is a producer slated for deletion
// that only CI used as a test seed: its rows for the parameter sets CI runs
// were written once by the real Python `sync <target> --provider synthetic`
// into a real ClickHouse at the migration head, dumped table by table, and
// committed. `dho fixtures load-synthetic` loads them, with every timestamp
// moved by the time since they were frozen so the window is "as if generated
// now", as the Python run's was.
//
//go:embed testdata/synthetic/*.json.gz
var syntheticFiles embed.FS

// FrozenColumn is one column of a frozen table: its name and its ClickHouse
// type, as system.columns reports it.
type FrozenColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// FrozenTable is the rows one target wrote to one table, in the column order
// of Columns, as ClickHouse's JSONCompactEachRow writes them.
type FrozenTable struct {
	Name    string         `json:"name"`
	Columns []FrozenColumn `json:"columns"`
	Rows    [][]any        `json:"rows"`
}

// FrozenTarget is one target's rows and the instant they were generated at.
type FrozenTarget struct {
	Name     string        `json:"name"`
	FrozenAt string        `json:"frozen_at"`
	Tables   []FrozenTable `json:"tables"`
}

// FrozenSet is the rows of every target for one (organization, repository,
// window) the CI scripts run.
type FrozenSet struct {
	// Producer is the commit whose Python generator wrote the rows.
	Producer string         `json:"producer"`
	OrgID    string         `json:"org_id"`
	RepoName string         `json:"repo_name"`
	Days     int            `json:"days"`
	Targets  []FrozenTarget `json:"targets"`
}

// SyntheticSetFile is the embedded file of a frozen set.
func SyntheticSetFile(orgID, repoName string, days int) string {
	return fmt.Sprintf("testdata/synthetic/%s_%s_%dd.json.gz", orgID, strings.NewReplacer("/", "__").Replace(repoName), days)
}

// LoadFrozenSet reads the frozen set for the parameters, or names the sets
// that exist. The rows carry ids derived from the organization and the
// repository, so a set is only valid for the exact pair it was frozen for.
func LoadFrozenSet(orgID, repoName string, days int) (FrozenSet, error) {
	raw, err := syntheticFiles.ReadFile(SyntheticSetFile(orgID, repoName, days))
	if err != nil {
		return FrozenSet{}, fmt.Errorf("no frozen synthetic rows for organization %s, repository %s, %d day(s); the frozen sets are: %s",
			orgID, repoName, days, strings.Join(FrozenSetNames(), "; "))
	}
	return decodeFrozenSet(raw)
}

func decodeFrozenSet(raw []byte) (FrozenSet, error) {
	reader, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return FrozenSet{}, err
	}
	defer reader.Close()
	var set FrozenSet
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	if err := decoder.Decode(&set); err != nil {
		return FrozenSet{}, err
	}
	return set, nil
}

// FrozenSetNames lists the embedded sets as "org / repository / N days".
func FrozenSetNames() []string {
	entries, err := syntheticFiles.ReadDir("testdata/synthetic")
	if err != nil {
		return nil
	}
	var names []string
	for _, entry := range entries {
		raw, err := syntheticFiles.ReadFile("testdata/synthetic/" + entry.Name())
		if err != nil {
			continue
		}
		set, err := decodeFrozenSet(raw)
		if err != nil {
			continue
		}
		names = append(names, fmt.Sprintf("%s / %s / %d days", set.OrgID, set.RepoName, set.Days))
	}
	sort.Strings(names)
	return names
}

// Target returns the frozen rows of one target.
func (set FrozenSet) Target(name string) (FrozenTarget, bool) {
	for _, target := range set.Targets {
		if target.Name == name {
			return target, true
		}
	}
	return FrozenTarget{}, false
}

var dateTime64Type = regexp.MustCompile(`^(?:Nullable\()?DateTime64\((\d+), 'UTC'\)\)?$`)

// timePrecision reports the fractional digits of a UTC DateTime64 column type
// (Nullable or not) and whether the type is one. Every timestamp the synthetic
// targets write is one; a DateTime64 in another zone is not shifted here and
// the freezer refuses it.
func timePrecision(columnType string) (int, bool) {
	match := dateTime64Type.FindStringSubmatch(columnType)
	if match == nil {
		return 0, false
	}
	precision, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, false
	}
	return precision, true
}

// ShiftTimes returns the table's rows with every DateTime64 value moved by
// delta. A frozen value is "2026-09-25 00:14:58.880" (UTC); NULL stays NULL.
func (table FrozenTable) ShiftTimes(delta time.Duration) ([][]any, error) {
	shifted := make([][]any, len(table.Rows))
	for index, row := range table.Rows {
		if len(row) != len(table.Columns) {
			return nil, fmt.Errorf("table %s row %d has %d value(s) for %d column(s)", table.Name, index, len(row), len(table.Columns))
		}
		out := make([]any, len(row))
		copy(out, row)
		for column, definition := range table.Columns {
			precision, isTime := timePrecision(definition.Type)
			if !isTime || out[column] == nil {
				continue
			}
			text, ok := out[column].(string)
			if !ok {
				return nil, fmt.Errorf("table %s row %d column %s: %v is not a timestamp string", table.Name, index, definition.Name, out[column])
			}
			parsed, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", text, time.UTC)
			if err != nil {
				return nil, fmt.Errorf("table %s row %d column %s: %w", table.Name, index, definition.Name, err)
			}
			layout := "2006-01-02 15:04:05"
			if precision > 0 {
				layout += "." + strings.Repeat("0", precision)
			}
			out[column] = parsed.Add(delta).UTC().Format(layout)
		}
		shifted[index] = out
	}
	return shifted, nil
}

// Structure is the column list format() reads the rows with.
func (table FrozenTable) Structure() string {
	parts := make([]string, len(table.Columns))
	for index, column := range table.Columns {
		parts[index] = "`" + column.Name + "` " + column.Type
	}
	return strings.Join(parts, ", ")
}

func (table FrozenTable) columnList() string {
	names := make([]string, len(table.Columns))
	for index, column := range table.Columns {
		names[index] = "`" + column.Name + "`"
	}
	return strings.Join(names, ", ")
}

// insertChunk is the number of rows one statement carries, so a table of tens
// of thousands of rows never sends a multi-megabyte parameter.
const insertChunk = 2000

// Load inserts the target's frozen rows into conn's database, table by table
// in frozen order, with every timestamp moved by delta. It returns the number
// of rows inserted per table.
func (target FrozenTarget) Load(ctx context.Context, conn driver.Conn, delta time.Duration) (map[string]int, error) {
	counts := map[string]int{}
	// The rows travel inside the statement text; the default query size limit
	// (256 KiB) is below one chunk of the largest table.
	insertContext := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"max_query_size": maxQuerySize}))
	for _, table := range target.Tables {
		rows, err := table.ShiftTimes(delta)
		if err != nil {
			return counts, err
		}
		statement := "INSERT INTO `" + table.Name + "` (" + table.columnList() + ") SELECT " + table.columnList() +
			" FROM format(JSONCompactEachRow, ?, ?)"
		for start := 0; start < len(rows); start += insertChunk {
			end := min(start+insertChunk, len(rows))
			var lines bytes.Buffer
			encoder := json.NewEncoder(&lines)
			for _, row := range rows[start:end] {
				if err := encoder.Encode(row); err != nil {
					return counts, fmt.Errorf("table %s: %w", table.Name, err)
				}
			}
			if err := conn.Exec(insertContext, statement, table.Structure(), lines.String()); err != nil {
				return counts, fmt.Errorf("insert into %s: %w", table.Name, err)
			}
			counts[table.Name] += end - start
		}
	}
	return counts, nil
}

// maxQuerySize is the query size the insert statements are allowed: a chunk of
// insertChunk rows of the widest table.
const maxQuerySize = 64 << 20

// LoadSynthetic loads the frozen rows of one target for the parameters, shifted
// to now, and returns the rows inserted per table.
func LoadSynthetic(ctx context.Context, conn driver.Conn, orgID, repoName string, days int, targetName string, now time.Time) (map[string]int, error) {
	if !validTarget(targetName) {
		return nil, fmt.Errorf("target %q: only valid for %s", targetName, strings.Join(Targets, ", "))
	}
	set, err := LoadFrozenSet(orgID, repoName, days)
	if err != nil {
		return nil, err
	}
	target, ok := set.Target(targetName)
	if !ok {
		return nil, fmt.Errorf("the frozen set for %s / %s / %d days has no target %s", orgID, repoName, days, targetName)
	}
	frozenAt, err := time.Parse(time.RFC3339Nano, target.FrozenAt)
	if err != nil {
		return nil, fmt.Errorf("frozen_at of target %s: %w", targetName, err)
	}
	return target.Load(ctx, conn, now.Sub(frozenAt))
}

const loadUsage = `Usage: dho fixtures load-synthetic --target <cicd|deployments|incidents|tests> --repo-name <owner/repo> --org <uuid> --backfill <days>

Loads the frozen rows of one synthetic target into ClickHouse, with every
timestamp moved so the window ends now. The rows were written once by the
Python synthetic generator for the parameter sets CI runs (see
LoadFrozenSet); a set that was not frozen is refused, naming the ones that
were. It replaces "dev-hops sync <target> --provider synthetic". Run
"dho fixtures finalize-synthetic-sync" afterwards, once every target is loaded.

Flags:
  --target      the synthetic target (required)
  --repo-name   the synthetic repo name (required)
  --org         the organization id (default: the ORG_ID environment variable)
  --backfill    window in days (default 1)

Environment:
  CLICKHOUSE_URI (or _FILE, or the DEV_HEALTH_CH_* component form)   ClickHouse DSN, native protocol
`

func runLoadSynthetic(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho fixtures load-synthetic", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, loadUsage) }
	target := flags.String("target", "", "the synthetic target")
	repoName := flags.String("repo-name", "", "the synthetic repo name")
	org := flags.String("org", "", "the organization id")
	backfill := flags.Int("backfill", 1, "window in days")
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
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}
	if !validTarget(*target) {
		fmt.Fprintf(env.Stderr, "argument error: --target must be one of %s\n", strings.Join(Targets, ", "))
		return cli.ExitUsage
	}
	if strings.TrimSpace(*repoName) == "" {
		fmt.Fprintln(env.Stderr, "argument error: --repo-name is required")
		return cli.ExitUsage
	}
	if *backfill < 1 {
		fmt.Fprintln(env.Stderr, "argument error: --backfill must be at least 1")
		return cli.ExitUsage
	}
	orgID := strings.TrimSpace(*org)
	if orgID == "" {
		orgID, _ = env.Lookup("ORG_ID")
		orgID = strings.TrimSpace(orgID)
	}
	if orgID == "" {
		fmt.Fprintln(env.Stderr, "argument error: an organization is required: --org or the ORG_ID environment variable")
		return cli.ExitUsage
	}
	if _, err := LoadFrozenSet(orgID, *repoName, *backfill); err != nil {
		return writeError(env.Stderr, cli.ExitRefused, "no_frozen_rows", err.Error())
	}

	dsn, configured, err := config.ResolveDSN(env.Lookup, "CLICKHOUSE_URI", config.ClickHouseSpec)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "configuration_error", err.Error())
	}
	if !configured {
		return writeError(env.Stderr, cli.ExitFailure, "configuration_error", "CLICKHOUSE_URI is required")
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	chConfig := chstorage.DefaultConfig(dsn.Reveal())
	chConfig.MaxOpenConns, chConfig.MaxIdleConns = 1, 1
	chConfig.ReadTimeout = 2 * time.Minute
	conn, err := chstorage.Open(ctx, chConfig)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "clickhouse_unavailable", boundary.Redact(err).Error())
	}
	defer conn.Close()

	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	started := time.Now()
	counts, err := LoadSynthetic(ctx, conn, orgID, *repoName, *backfill, *target, time.Now().UTC())
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "load_failed", boundary.Redact(err).Error())
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	summary := make([]string, 0, len(names))
	for _, name := range names {
		summary = append(summary, fmt.Sprintf("%s=%d", name, counts[name]))
	}
	logger.Info("synthetic rows loaded", "target", *target, "rows", strings.Join(summary, ","), "duration_ms", time.Since(started).Milliseconds())
	if err := json.NewEncoder(env.Stdout).Encode(map[string]any{"target": *target, "rows": counts}); err != nil {
		fmt.Fprintln(env.Stderr, "could not write the result")
		return cli.ExitFailure
	}
	return cli.ExitOK
}
