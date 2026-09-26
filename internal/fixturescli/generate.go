package fixturescli

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
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

// The frozen worlds of `fixtures generate` (CHAOS-6468, option a).
//
// The Python generator (dev-hops fixtures generate, SyntheticDataGenerator) is not repeatable even
// against itself: the same seed writes the same tables with the same row counts, but its ids are
// uuid4 and its timestamps are anchored on the run's clock, so no two runs are byte-identical and a
// byte-compatible port has nothing to be compatible with. What CI and the acceptance stack need is a
// seeded, coherent world in ClickHouse whose window ends now. So the parameter sets they run were
// written once by the real Python verb into a real ClickHouse at the migration head, dumped table by
// table, and committed, digest-pinned; `dho fixtures generate` loads the world that matches the
// requested parameters, moved by whole days so its last generated day is today and with the
// organization replaced by the caller's, and refuses a parameter set that was not frozen, naming the
// ones that were. The Python verb is not in the chain any more.
//
// Two dumped tables are populated by a materialized view when the rows of the table it reads are
// inserted; they are marked Derived and not inserted, so the view fills them exactly as it did when
// the world was written.
//
//go:embed testdata/generate/*.json.gz
var worldFiles embed.FS

// frozenWorldDigests pins the frozen worlds by content: the sha256 of the exact embedded bytes.
// LoadFrozenWorld checks it on every load, not only in CI, because the parameters alone (RepoName,
// Days, Seed, ...) name a world but say nothing about whether its rows are the ones the digest was
// taken over -- a corrupted, truncated or hand-edited file with the same parameters would otherwise
// load silently. A file changes only by re-running TestFreezeGenerateWorlds against the live Python
// producer, and then its digest here is updated in the same commit.
var frozenWorldDigests = map[string]string{
	"testdata/generate/synthetic_acme__live-e2e_r1_14d_c6_p24_t10_s20260219_mg.json.gz": "f97e2e36842a69783189a82d27549d0f873d6e0a4e95764dc042300bfc1d6029",
}

// GenerateParams are the parameters of one frozen `fixtures generate` run: the flags that change
// what the Python verb writes. A seed is required, because an unseeded run is not repeatable.
type GenerateParams struct {
	Provider      string `json:"provider"`
	RepoName      string `json:"repo_name"`
	RepoCount     int    `json:"repo_count"`
	Days          int    `json:"days"`
	CommitsPerDay int    `json:"commits_per_day"`
	PRCount       int    `json:"pr_count"`
	TeamCount     int    `json:"team_count"`
	Seed          int64  `json:"seed"`
	WithMetrics   bool   `json:"with_metrics"`
	WithWorkGraph bool   `json:"with_work_graph"`
}

// String names a parameter set as the flags that select it.
func (p GenerateParams) String() string {
	out := fmt.Sprintf("--provider %s --repo-name %s --repo-count %d --days %d --commits-per-day %d --pr-count %d --team-count %d --seed %d",
		p.Provider, p.RepoName, p.RepoCount, p.Days, p.CommitsPerDay, p.PRCount, p.TeamCount, p.Seed)
	if p.WithMetrics {
		out += " --with-metrics"
	}
	if p.WithWorkGraph {
		out += " --with-work-graph"
	}
	return out
}

// WorldFile is the embedded file of a frozen world.
func WorldFile(p GenerateParams) string {
	slug := strings.NewReplacer("/", "__").Replace(p.RepoName)
	flags := ""
	if p.WithMetrics {
		flags += "m"
	}
	if p.WithWorkGraph {
		flags += "g"
	}
	if flags == "" {
		flags = "raw"
	}
	return fmt.Sprintf("testdata/generate/%s_%s_r%d_%dd_c%d_p%d_t%d_s%d_%s.json.gz", p.Provider, slug, p.RepoCount, p.Days, p.CommitsPerDay, p.PRCount, p.TeamCount, p.Seed, flags)
}

// WorldTable is one frozen table. Derived tables are dumped so the loaded world can be compared
// with the capture in full, and are not inserted: a materialized view fills them.
type WorldTable struct {
	FrozenTable
	Derived bool `json:"derived,omitempty"`
}

// FrozenWorld is everything the real Python `fixtures generate` wrote for one parameter set, the
// organization it wrote it for and the instant it ran.
type FrozenWorld struct {
	// Producer is the commit whose Python generator wrote the rows.
	Producer string         `json:"producer"`
	FrozenAt string         `json:"frozen_at"`
	OrgID    string         `json:"org_id"`
	Params   GenerateParams `json:"params"`
	Tables   []WorldTable   `json:"tables"`
}

func decodeWorld(raw []byte) (FrozenWorld, error) {
	reader, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return FrozenWorld{}, err
	}
	defer reader.Close()
	var world FrozenWorld
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	if err := decoder.Decode(&world); err != nil {
		return FrozenWorld{}, err
	}
	return world, nil
}

// WorldNames lists the embedded worlds as the flags that select them.
func WorldNames() []string {
	entries, err := worldFiles.ReadDir("testdata/generate")
	if err != nil {
		return nil
	}
	var names []string
	for _, entry := range entries {
		raw, err := worldFiles.ReadFile("testdata/generate/" + entry.Name())
		if err != nil {
			continue
		}
		if world, err := decodeWorld(raw); err == nil {
			names = append(names, world.Params.String())
		}
	}
	sort.Strings(names)
	return names
}

// LoadFrozenWorld reads the frozen world for the parameters, or names the worlds that exist.
func LoadFrozenWorld(p GenerateParams) (FrozenWorld, error) {
	refuse := fmt.Errorf("no frozen world for %s; the frozen worlds are:\n  %s", p, strings.Join(WorldNames(), "\n  "))
	file := WorldFile(p)
	raw, err := worldFiles.ReadFile(file)
	if err != nil {
		return FrozenWorld{}, refuse
	}
	sum := sha256.Sum256(raw)
	if digest, pinned := frozenWorldDigests[file]; !pinned || hex.EncodeToString(sum[:]) != digest {
		return FrozenWorld{}, fmt.Errorf("%s does not match its pinned digest: the frozen world is not the one it was proved against", file)
	}
	world, err := decodeWorld(raw)
	if err != nil {
		return FrozenWorld{}, err
	}
	// The file name only found the world: what was read must be what was asked for.
	if world.Params != p {
		return FrozenWorld{}, refuse
	}
	return world, nil
}

// columnBase strips the wrappers that do not change how a value is written.
func columnBase(columnType string) string {
	for {
		switch {
		case strings.HasPrefix(columnType, "Nullable(") && strings.HasSuffix(columnType, ")"):
			columnType = columnType[len("Nullable(") : len(columnType)-1]
		case strings.HasPrefix(columnType, "LowCardinality(") && strings.HasSuffix(columnType, ")"):
			columnType = columnType[len("LowCardinality(") : len(columnType)-1]
		default:
			return columnType
		}
	}
}

var dateTime64Column = regexp.MustCompile(`^DateTime64\((\d+)(?:, '(UTC)')?\)$`)

// shiftKind says how a column's type moves in time: 0 not at all, else the layout of its values.
// The freezer runs the server in UTC and refuses a zone other than UTC, so a column without a zone is
// UTC too.
func shiftLayout(columnType string) (layout string, ok bool, err error) {
	base := columnBase(columnType)
	switch {
	case base == "Date" || base == "Date32":
		return "2006-01-02", true, nil
	case base == "DateTime" || base == "DateTime('UTC')":
		return "2006-01-02 15:04:05", true, nil
	case strings.HasPrefix(base, "DateTime64"):
		match := dateTime64Column.FindStringSubmatch(base)
		if match == nil {
			return "", false, fmt.Errorf("unsupported column type %s: a time zone other than UTC is not shifted", columnType)
		}
		precision, convErr := strconv.Atoi(match[1])
		if convErr != nil {
			return "", false, convErr
		}
		layout = "2006-01-02 15:04:05"
		if precision > 0 {
			layout += "." + strings.Repeat("0", precision)
		}
		return layout, true, nil
	case strings.HasPrefix(base, "DateTime("):
		return "", false, fmt.Errorf("unsupported column type %s: a time zone other than UTC is not shifted", columnType)
	}
	return "", false, nil
}

func holdsOrg(columnType string) bool {
	base := columnBase(columnType)
	return base == "String" || base == "UUID"
}

// Transform returns the table's rows with every date and timestamp moved by whole days and every
// value equal to fromOrg replaced by toOrg (in string and UUID columns). NULL stays NULL.
func (table WorldTable) Transform(days int, fromOrg, toOrg string) ([][]any, error) {
	rows := make([][]any, len(table.Rows))
	layouts := make([]string, len(table.Columns))
	shifts := make([]bool, len(table.Columns))
	orgs := make([]bool, len(table.Columns))
	for index, column := range table.Columns {
		layout, ok, err := shiftLayout(column.Type)
		if err != nil {
			return nil, fmt.Errorf("table %s column %s: %w", table.Name, column.Name, err)
		}
		layouts[index], shifts[index], orgs[index] = layout, ok, holdsOrg(column.Type)
	}
	for index, row := range table.Rows {
		if len(row) != len(table.Columns) {
			return nil, fmt.Errorf("table %s row %d has %d value(s) for %d column(s)", table.Name, index, len(row), len(table.Columns))
		}
		out := make([]any, len(row))
		copy(out, row)
		for column := range out {
			if out[column] == nil {
				continue
			}
			switch {
			case shifts[column] && days != 0:
				text, ok := out[column].(string)
				if !ok {
					return nil, fmt.Errorf("table %s row %d column %s: %v is not a time string", table.Name, index, table.Columns[column].Name, out[column])
				}
				parsed, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", text, time.UTC)
				if err != nil {
					parsed, err = time.ParseInLocation("2006-01-02", text, time.UTC)
				}
				if err != nil {
					return nil, fmt.Errorf("table %s row %d column %s: %w", table.Name, index, table.Columns[column].Name, err)
				}
				out[column] = parsed.AddDate(0, 0, days).UTC().Format(layouts[column])
			case orgs[column] && fromOrg != toOrg:
				if text, ok := out[column].(string); ok && text == fromOrg {
					out[column] = toOrg
				}
			}
		}
		rows[index] = out
	}
	return rows, nil
}

// WholeDays is the number of calendar days from the day the world was frozen to the day of now, in
// UTC: the shift that puts the last generated day on today and leaves every time of day alone.
func (world FrozenWorld) WholeDays(now time.Time) (int, error) {
	frozenAt, err := time.Parse(time.RFC3339Nano, world.FrozenAt)
	if err != nil {
		return 0, fmt.Errorf("frozen_at of the world: %w", err)
	}
	from := time.Date(frozenAt.UTC().Year(), frozenAt.UTC().Month(), frozenAt.UTC().Day(), 0, 0, 0, 0, time.UTC)
	to := time.Date(now.UTC().Year(), now.UTC().Month(), now.UTC().Day(), 0, 0, 0, 0, time.UTC)
	return int(to.Sub(from).Hours() / 24), nil
}

// LoadWorld inserts the world's rows into conn's database for org, table by table in frozen order,
// moved so the world ends on the day of now, and returns the rows inserted per table. Every non-derived
// table is confirmed to exist before any row is written, so a table missing later in the frozen order
// (a partially migrated database) refuses the whole load instead of leaving the earlier tables' rows
// behind with no way for the caller to know the world is incomplete.
func LoadWorld(ctx context.Context, conn driver.Conn, world FrozenWorld, org string, now time.Time) (map[string]int, error) {
	days, err := world.WholeDays(now)
	if err != nil {
		return nil, err
	}
	if err := preflightTables(ctx, conn, world); err != nil {
		return nil, err
	}
	counts := map[string]int{}
	// optimize_on_insert=0: the tables are Replacing/Collapsing MergeTrees holding several versions of a
	// row, which the Python verb inserted in separate blocks; merging the rows of one block on insert
	// would drop versions the world holds.
	insertContext := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"max_query_size": maxQuerySize, "optimize_on_insert": 0}))
	for _, table := range world.Tables {
		if table.Derived {
			continue
		}
		rows, err := table.Transform(days, world.OrgID, org)
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

// preflightTables refuses to start a load unless every non-derived table the world expects to write
// already exists, so a table missing later in the frozen order is caught before the first INSERT
// rather than after some earlier tables already hold rows.
func preflightTables(ctx context.Context, conn driver.Conn, world FrozenWorld) error {
	rows, err := conn.Query(ctx, "SELECT name FROM system.tables WHERE database = currentDatabase()")
	if err != nil {
		return err
	}
	existing := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		existing[name] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	var missing []string
	for _, table := range world.Tables {
		if table.Derived || existing[table.Name] {
			continue
		}
		missing = append(missing, table.Name)
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("missing table(s), nothing written: %s", strings.Join(missing, ", "))
	}
	return nil
}

// liveProviders are the providers whose rows in an organization mean it holds connector-synced data
// (LIVE_PROVIDERS of the Python verb).
var liveProviders = []string{"github", "gitlab", "jira", "linear", "bitbucket"}

// syncedProviders is the Python verb's mixed-organization guard: the live providers that already hold
// work items or repositories of the organization. A table that does not exist yet holds nothing.
func syncedProviders(ctx context.Context, conn driver.Conn, org string) ([]string, error) {
	found := map[string]bool{}
	for _, table := range []string{"work_items", "repos"} {
		rows, err := conn.Query(ctx, "SELECT DISTINCT provider FROM "+table+" WHERE org_id = ? AND provider IN ?", org, liveProviders)
		if err != nil {
			var exception *clickhouse.Exception
			if errors.As(err, &exception) && (exception.Code == 60 || exception.Code == 81) {
				continue
			}
			return nil, err
		}
		for rows.Next() {
			var provider string
			if err := rows.Scan(&provider); err != nil {
				rows.Close()
				return nil, err
			}
			found[provider] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	var out []string
	for provider := range found {
		out = append(out, provider)
	}
	sort.Strings(out)
	return out, nil
}

// serverTimezone is the ClickHouse server's default timezone: the zone a Date/DateTime/DateTime64
// column without an explicit one is read and written in. The frozen worlds were captured against a
// UTC server (the freezer refuses otherwise) and Transform's Date/DateTime values carry no zone
// suffix, so loading into a server whose default zone is not UTC would silently reinterpret every
// shifted value at the wrong offset instead of failing.
func serverTimezone(ctx context.Context, conn driver.Conn) (string, error) {
	var zone string
	if err := conn.QueryRow(ctx, "SELECT timezone()").Scan(&zone); err != nil {
		return "", err
	}
	return zone, nil
}

// normalizeSink lets a caller of the Python verb pass what it passed there. Python's client spoke
// HTTP, so its DSN often names the HTTP port (8123, 8443 with TLS) under the clickhouse:// scheme;
// the Go client speaks the native protocol on a clickhouse:// DSN and HTTP on an http(s):// one, so a
// DSN on an HTTP port is read as the HTTP DSN it is.
func normalizeSink(dsn string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	switch {
	case (parsed.Scheme == "clickhouse" || parsed.Scheme == "http") && parsed.Port() == "8123":
		parsed.Scheme = "http"
	case (parsed.Scheme == "clickhouses" || parsed.Scheme == "https") && parsed.Port() == "8443":
		parsed.Scheme = "https"
	default:
		return dsn
	}
	return parsed.String()
}

const generateUsage = `Usage: dho fixtures generate --sink <clickhouse dsn> --seed <n> [flags]

Loads a frozen synthetic world into ClickHouse: the rows the Python "dev-hops fixtures generate"
wrote once for the same parameters, moved so the window ends today and written for the given
organization. The worlds exist for the parameter sets CI and the end-to-end suites run; a set that
was not frozen is refused, naming the ones that were. A seed is required (an unseeded run is not
repeatable). Analytics only: the users, organizations and licenses the Python verb also wrote to
PostgreSQL are not written here, and a PostgreSQL URI in the environment is refused so a missing
account is never silent.

Flags (as "dev-hops fixtures generate"):
  --sink                    ClickHouse DSN (default: the CLICKHOUSE_URI environment variable); a DSN on
                            the HTTP port (8123, or 8443 with TLS) is spoken to over HTTP, as Python did
  --db-type                 must be clickhouse when given
  --org                     organization id (default: the ORG_ID environment variable, else the
                            default demo organization); it must be a UUID
  --repo-name               repository name (default acme/demo-app)
  --repo-count              repositories (default 1)
  --days                    days of data (default 30)
  --commits-per-day         commits per day (default 5)
  --pr-count                pull requests (default 20)
  --team-count              teams (default 10)
  --seed                    seed (required)
  --provider                synthetic (default), github, gitlab or jira
  --with-metrics            also the derived metrics
  --with-work-graph         also the work graph
  --skip-coherence-validation, --allow-mixed-org   as in Python
`

// defaultOrg is the Python verb's default organization: uuid5(6ba7b810-..., "default-org").
const defaultOrg = "99741251-4686-5952-911e-46095bcd8122"

const defaultRepoName = "acme/demo-app"

func runGenerate(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho fixtures generate", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, generateUsage) }
	sink := flags.String("sink", "", "ClickHouse DSN")
	dbType := flags.String("db-type", "", "explicit DB type")
	org := flags.String("org", "", "organization id")
	repoName := flags.String("repo-name", defaultRepoName, "repository name")
	repoCount := flags.Int("repo-count", 1, "repositories")
	days := flags.Int("days", 30, "days of data")
	commits := flags.Int("commits-per-day", 5, "commits per day")
	prs := flags.Int("pr-count", 20, "pull requests")
	teams := flags.Int("team-count", 10, "teams")
	seed := flags.String("seed", "", "seed")
	provider := flags.String("provider", "synthetic", "provider")
	withMetrics := flags.Bool("with-metrics", false, "derived metrics")
	withGraph := flags.Bool("with-work-graph", false, "work graph")
	flags.Bool("skip-coherence-validation", false, "accepted, no effect")
	allowMixed := flags.Bool("allow-mixed-org", false, "write into an organization that holds synced data")
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
	switch *provider {
	case "synthetic", "github", "gitlab", "jira":
	default:
		fmt.Fprintf(env.Stderr, "argument error: --provider must be one of synthetic, github, gitlab, jira\n")
		return cli.ExitUsage
	}
	if *dbType != "" && *dbType != "clickhouse" {
		fmt.Fprintf(env.Stderr, "argument error: --db-type %s: only clickhouse is supported\n", *dbType)
		return cli.ExitUsage
	}
	if strings.TrimSpace(*seed) == "" {
		fmt.Fprintln(env.Stderr, "argument error: --seed is required: an unseeded run is not repeatable, so no world can stand for it")
		return cli.ExitUsage
	}
	seedValue, err := strconv.ParseInt(strings.TrimSpace(*seed), 10, 64)
	if err != nil {
		fmt.Fprintf(env.Stderr, "argument error: --seed %q is not an integer\n", *seed)
		return cli.ExitUsage
	}
	orgID := strings.TrimSpace(*org)
	if orgID == "" {
		orgID, _ = env.Lookup("ORG_ID")
		orgID = strings.TrimSpace(orgID)
	}
	if orgID == "" {
		orgID = defaultOrg
	}
	if !uuidText.MatchString(orgID) {
		fmt.Fprintf(env.Stderr, "argument error: the organization %q is not a UUID\n", orgID)
		return cli.ExitUsage
	}
	params := GenerateParams{
		Provider: *provider, RepoName: *repoName, RepoCount: *repoCount, Days: *days, CommitsPerDay: *commits,
		PRCount: *prs, TeamCount: *teams, Seed: seedValue, WithMetrics: *withMetrics, WithWorkGraph: *withGraph,
	}
	world, err := LoadFrozenWorld(params)
	if err != nil {
		return writeError(env.Stderr, cli.ExitRefused, "no_frozen_world", err.Error())
	}
	for _, name := range []string{"DATABASE_URI", "POSTGRES_URI", "DATABASE_URL"} {
		if value, ok := env.Lookup(name); ok && strings.TrimSpace(value) != "" {
			return writeError(env.Stderr, cli.ExitRefused, "auth_seeding_not_ported",
				name+" is set: the Python verb would also seed the demo users, organization and license into PostgreSQL, which this verb does not yet; unset it to load the analytics rows only")
		}
	}

	dsn := strings.TrimSpace(*sink)
	if dsn == "" {
		resolved, configured, err := config.ResolveDSN(env.Lookup, "CLICKHOUSE_URI", config.ClickHouseSpec)
		if err != nil {
			return writeError(env.Stderr, cli.ExitFailure, "configuration_error", err.Error())
		}
		if !configured {
			return writeError(env.Stderr, cli.ExitFailure, "configuration_error", "--sink or CLICKHOUSE_URI is required")
		}
		dsn = resolved.Reveal()
	}
	dsn = normalizeSink(dsn)
	boundary := secrets.NewBoundary(dsn)
	chConfig := chstorage.DefaultConfig(dsn)
	chConfig.MaxOpenConns, chConfig.MaxIdleConns = 1, 1
	chConfig.ReadTimeout = 5 * time.Minute
	conn, err := chstorage.Open(ctx, chConfig)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "clickhouse_unavailable", boundary.Redact(err).Error())
	}
	defer conn.Close()

	if !*allowMixed {
		providers, err := syncedProviders(ctx, conn, orgID)
		if err != nil {
			return writeError(env.Stderr, cli.ExitFailure, "guard_failed", boundary.Redact(err).Error())
		}
		if len(providers) > 0 {
			return writeError(env.Stderr, cli.ExitRefused, "mixed_org",
				fmt.Sprintf("Org %s already holds synced data from %v. Generating synthetic fixtures into it would pollute Investment/team/repo rollups with demo repos and teams. Use a dedicated demo org, or pass --allow-mixed-org to override.", orgID, providers))
		}
	}

	if zone, err := serverTimezone(ctx, conn); err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "clickhouse_unavailable", boundary.Redact(err).Error())
	} else if zone != "UTC" {
		return writeError(env.Stderr, cli.ExitRefused, "non_utc_server",
			fmt.Sprintf("the ClickHouse server runs in %s: the frozen world's Date/DateTime columns without an explicit zone were captured as UTC, and loading them into a non-UTC server would silently shift every value by the offset", zone))
	}

	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	started := time.Now()
	counts, err := LoadWorld(ctx, conn, world, orgID, time.Now().UTC())
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "load_failed", boundary.Redact(err).Error())
	}
	total := 0
	for _, count := range counts {
		total += count
	}
	logger.Info("frozen world loaded", "org_id", orgID, "tables", len(counts), "rows", total, "duration_ms", time.Since(started).Milliseconds())
	if err := json.NewEncoder(env.Stdout).Encode(map[string]any{"org_id": orgID, "params": params.String(), "rows": counts}); err != nil {
		fmt.Fprintln(env.Stderr, "could not write the result")
		return cli.ExitFailure
	}
	return cli.ExitOK
}

var uuidText = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
