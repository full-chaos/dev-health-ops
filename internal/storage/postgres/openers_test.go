package postgres

import (
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Every place the repository opens a PostgreSQL connection or pool with pgx
// directly is found in the code (not from a list somebody keeps) and must be
// classified: a driver failure text carries the effective login (and, from a
// server that echoes, the password) whatever configuration source supplied them, so
// each opener either builds its redaction boundary from the resolved
// configuration, returns a fixed generic message, is the Open helper that redacts,
// discards the error, is a server that logs, or is test-only. A new opener fails
// this test until it is classified (CHAOS-6665).

type openerKind string

const (
	// kindBoundary: the file builds its boundary with Boundary (this package).
	kindBoundary openerKind = "boundary"
	// kindGeneric: a connect failure becomes a fixed message (RedactedConnectError).
	kindGeneric openerKind = "generic"
	// kindHelper: the Open helper itself, which redacts with WithRedactedCauseAlso.
	kindHelper openerKind = "helper"
	// kindDiscarded: the error is dropped, never printed.
	kindDiscarded openerKind = "discarded"
	// kindServer: a long-running server, its errors go to its own logs.
	kindServer openerKind = "server"
	// kindTest: test support and compatibility probes, not a shipped verb.
	kindTest openerKind = "test"
)

// otherBoundaries is how many DSN-only boundaries (secrets.NewBoundary) a
// `boundary` file may still build, for a database that is not PostgreSQL (a Valkey
// URI): a file that builds one for PostgreSQL too would print the resolved
// credentials, so any other count fails.
var otherBoundaries = map[string]int{
	"internal/fixturescli/fixturescli.go":       1, // the VALKEY_URI boundary
	"internal/fixturescli/product_telemetry.go": 1, // the ClickHouse boundary of the seeding writes
}

var openers = map[string]struct {
	kind openerKind
	why  string
}{
	"internal/admincli/admincli.go":                             {kindBoundary, "admin features seed"},
	"internal/adminops/users.go":                                {kindBoundary, "admin users and orgs: the pool and the error redactor"},
	"internal/auth/usersbackfill/command.go":                    {kindBoundary, "auth users-backfill: the pool, with the pool-open error redacted"},
	"internal/backfillrun/command.go":                           {kindBoundary, "backfill run"},
	"internal/fixturescli/fixturescli.go":                       {kindBoundary, "fixtures finalize-synthetic-sync"},
	"internal/fixturescli/recommendations_backstop.go":          {kindBoundary, "fixtures generate --backstop-database"},
	"internal/fixturescli/product_telemetry.go":                 {kindBoundary, "fixtures product-telemetry organizations lookup"},
	"internal/goapicli/prove/main.go":                           {kindBoundary, "go-api-prove: the boundary run() composes"},
	"internal/goapicli/routing/main.go":                         {kindBoundary, "goapi routing: connectPostgres, the helper of status, enable, disable, repoint and carry"},
	"internal/maintenancecli/maintenance.go":                    {kindBoundary, "maintenance"},
	"internal/pgmigrate/command.go":                             {kindBoundary, "migrate postgres upgrade, status, current"},
	"internal/pgmigrate/downgrade.go":                           {kindBoundary, "migrate postgres downgrade: the connection is opened with pgx.Connect and every error goes through Boundary(dsn).Redact"},
	"internal/pgmigrate/indexbuild.go":                          {kindBoundary, "migrate postgres build-index: the connection is opened with pgx.Connect and every error goes through Boundary(dsn).Redact"},
	"internal/pgmigrate/indexbuild_watch.go":                    {kindDiscarded, "migrate postgres build-index: the blocker watch copies the build connection's already-resolved config; a connect failure is logged as its SQLSTATE only, never its text, and never fails the build"},
	"internal/pgmigrate/preflight.go":                           {kindBoundary, "migrate postgres preflight: the connection is opened with pgx.Connect and every error goes through Boundary(dsn).Redact"},
	"internal/pushlifecyclecli/command.go":                      {kindDiscarded, "push-lifecycle backfill-created: a failed pool open prints a fixed line naming only the variable; the driver error text is dropped"},
	"internal/synccli/jira_stored_credential.go":                {kindBoundary, "sync teams --provider jira: resolves the org's stored jira credential from Postgres, the pool and the error redactor"},
	"tools/record-provider-answers/main.go":                     {kindBoundary, "the one-off recorder of provider answers (CHAOS-9124): resolves an integration credential from Postgres, the pool and the error redactor"},
	"internal/goapicli/restprove/main.go":                       {kindGeneric, "a failed connect is the fixed message"},
	"internal/migrationmatrix/restproven.go":                    {kindGeneric, "a failed connect is the fixed message"},
	"internal/storage/postgres/factory.go":                      {kindHelper, "Open and New: WithRedactedCauseAlso with the resolved credentials"},
	"internal/storage/postgres/first_connect.go":                {kindHelper, "OpenWaiting: WithRedactedCauseAlso with the resolved credentials; ConnectWaiting returns pgx's error unchanged and each caller is scanned as an opener"},
	"internal/synccli/dblookup.go":                              {kindDiscarded, "a connect or query error is swallowed into 'not found'"},
	"internal/providersync/repository_postgres.go":              {kindDiscarded, "the lease renewal pool copies the running work pool's already-resolved config and dials lazily: the constructor's only failure is config validation, which carries no login, and a failed renewal is dropped into ErrLeaseLost, never printed (CHAOS-6889)"},
	"internal/rivermigrate/ownermove.go":                        {kindBoundary, "migrate owner-move: the connection is opened with pgx.Connect and its connect error goes through Boundary(dsn).Redact"},
	"internal/rivermigrate/roles.go":                            {kindDiscarded, "migrate roles' Authenticate closure: the connect error is inspected for SQLSTATE 28P01 only (a wrong password) and its text is never printed or logged; the message names a role label"},
	"internal/cigates/livee2e/ssoseedverb.go":                   {kindTest, "live-e2e test-only seed verb (CHAOS-13636): a pool from the PG* environment of the CI job, in no served binary; deleted when 13636 PR2 lands"},
	"internal/testsupport/authplane/authplane.go":               {kindTest, "venue auth plane: a pool on the Go copy for auth-service's session and identity stores"},
	"internal/testsupport/authplane/authorize.go":               {kindTest, "venue decision point: a pool on the test database for auth-service's authorize routes (decisionstate)"},
	"internal/testsupport/containers/remote.go":                 {kindTest, "test containers"},
	"internal/testsupport/containers/shared.go":                 {kindTest, "test containers: the shared server's capacity check and leak snapshot"},
	"internal/testsupport/containers/template.go":               {kindTest, "test containers: the shared server's template build, fingerprint and finish check"},
	"internal/testsupport/domainroledb/domainroledb.go":         {kindTest, "domain-role venue: the migrated schema and grants, a pool as the domain role"},
	"internal/testsupport/ingestpg/ingestpg.go":                 {kindTest, "external-ingest proof: a pool to a fake PostgreSQL wire server that serves the token and audit statements"},
	"internal/testsupport/pgschema/pgschema.go":                 {kindTest, "test schema helper"},
	"internal/testsupport/pinrig/pinrig.go":                     {kindTest, "pin proof: pools as the runtime and session reader roles"},
	"internal/testsupport/platformroleseam/platformroleseam.go": {kindTest, "platform role seam: the migrated schemas, a pool as the session reader role"},
	"internal/testsupport/superuserwriter/superuserwriter.go":   {kindTest, "superuser-write proof: a pool as a role that may write every public column except users.is_superuser"},
	"internal/testsupport/usertokens/usertokens.go":             {kindTest, "user-token proof: a pool that refuses every connection and counts the attempts"},
	"internal/testsupport/venueoracle/venueoracle.go":           {kindTest, "venue oracle harness"},
	"internal/testsupport/venueoracle/gosubstrate.go":           {kindTest, "venue oracle harness: the Go-built schema of a frozen venue"},
	"internal/testsupport/venueoracle/authsessions.go":          {kindTest, "venue oracle harness: the auth schema and the minted sessions' principal and session rows of the Go copy"},
	"tests/compatibility/river/go/probe.go":                     {kindTest, "compatibility probe"},
	"tests/compatibility/river/nminus1/probe.go":                {kindTest, "compatibility probe"},
}

// repoRoot is the module root, found from this package's directory.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test directory")
		}
		dir = parent
	}
}

// fileCalls counts the selector calls (pkg.Func) a file makes.
func fileCalls(t *testing.T, path string) map[string]int {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	found := map[string]int{}
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
			if pkg, ok := selector.X.(*ast.Ident); ok {
				found[pkg.Name+"."+selector.Sel.Name]++
			} else {
				found["."+selector.Sel.Name]++
			}
		}
		return true
	})
	return found
}

func TestEveryPostgresOpenerIsClassified(t *testing.T) {
	root := repoRoot(t)
	opened := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			switch name {
			case ".git", ".venv", "node_modules", "vendor", ".remember", "worktrees":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		calls := fileCalls(t, path)
		// ConnectWaiting returns pgx's own error unchanged, so a caller is an opener like a pgx.Connect caller.
		for _, opener := range []string{"pgx.Connect", "pgx.ConnectConfig", "pgxpool.New", "pgxpool.NewWithConfig", "pgstorage.ConnectWaiting", "postgres.ConnectWaiting", "postgresstore.ConnectWaiting"} {
			if calls[opener] > 0 {
				relative, _ := filepath.Rel(root, path)
				opened[filepath.ToSlash(relative)] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for path := range opened {
		found = append(found, path)
	}
	sort.Strings(found)
	if len(found) < 15 {
		t.Fatalf("only %d openers found: the scan measures nothing: %v", len(found), found)
	}
	for _, path := range found {
		class, known := openers[path]
		if !known {
			t.Errorf("%s opens PostgreSQL with pgx and is not classified: build its boundary with Boundary(), or classify it here with the reason it needs none", path)
			continue
		}
		calls := fileCalls(t, filepath.Join(root, path))
		switch class.kind {
		case kindBoundary:
			if calls["pgstorage.Boundary"]+calls["postgres.Boundary"] == 0 {
				t.Errorf("%s is classified %q (%s) but does not call the resolved-credential Boundary", path, class.kind, class.why)
			}
			if got := calls["secrets.NewBoundary"]; got != otherBoundaries[path] {
				t.Errorf("%s builds %d DSN-only secrets.NewBoundary boundaries, the table allows %d (only for a database that is not PostgreSQL): a DSN-only boundary leaves the credentials pgx resolved from the environment", path, got, otherBoundaries[path])
			}
		case kindGeneric:
			if calls["secrets.RedactedConnectError"] == 0 {
				t.Errorf("%s is classified %q (%s) but does not return secrets.RedactedConnectError", path, class.kind, class.why)
			}
		case kindHelper:
			if calls["secrets.WithRedactedCauseAlso"] == 0 {
				t.Errorf("%s is classified %q (%s) but does not call secrets.WithRedactedCauseAlso", path, class.kind, class.why)
			}
		}
	}
	for path := range openers {
		if !opened[path] {
			t.Errorf("%s is classified but no longer opens PostgreSQL with pgx: remove it from the table", path)
		}
	}
}

// Every opener that builds its boundary with Boundary is RUN with a connection
// string pgx cannot parse, and no login or password text of it may reach its
// output: pgx's parse error echoes the string with only the password masked, and
// a boundary cannot find the login in a string that does not parse, so Boundary
// fails closed (CHAOS-12733). A new boundary opener fails this census until it has
// a driver here, and a driver whose opener leaks fails by the leak check.

const (
	censusLogin    = "census_login_q7x9"
	censusPassword = "census_password_q7x9"
)

var censusDSNs = map[string]string{
	"uri":       "postgres://" + censusLogin + ":" + censusPassword + "@[::1",
	"key=value": "user=" + censusLogin + " password=" + censusPassword + " host=[::1 sslmode=bogus",
}

type openerRun struct {
	args []string
	env  []string // {DSN} is the malformed connection string
	// reaches is text the output must hold, so the run measures the open and not an earlier refusal.
	reaches string
}

type openerDriver struct {
	binary string // "dho" or "recorder"
	runs   []openerRun
	// drivenBy names a test (file::Test) that runs an opener no binary can reach without a live server.
	drivenBy string
}

const censusClickHouse = "CLICKHOUSE_URI=clickhouse://census:census-password-1@127.0.0.1:1/db"

var openerDrivers = map[string]openerDriver{
	"internal/admincli/admincli.go":                    {binary: "dho", runs: []openerRun{{args: []string{"admin", "features", "seed"}, env: []string{"POSTGRES_URI={DSN}"}}}},
	"internal/adminops/users.go":                       {binary: "dho", runs: []openerRun{{args: []string{"admin", "users", "list"}, env: []string{"POSTGRES_URI={DSN}"}}}},
	"internal/auth/usersbackfill/command.go":           {binary: "dho", runs: []openerRun{{args: []string{"auth", "users-backfill"}, env: []string{"AUTH_DATABASE_URI={DSN}"}}}},
	"internal/backfillrun/command.go":                  {binary: "dho", runs: []openerRun{{args: []string{"backfill", "run", "--config-id", "census"}, env: []string{"POSTGRES_URI={DSN}"}}}},
	"internal/fixturescli/fixturescli.go":              {binary: "dho", runs: []openerRun{{args: []string{"fixtures", "finalize-synthetic-sync", "--target", "cicd", "--repo-name", "r", "--org", "o"}, env: []string{"MIGRATION_DATABASE_URI={DSN}", "DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN=1"}}}},
	"internal/fixturescli/recommendations_backstop.go": {drivenBy: "internal/fixturescli/recommendations_backstop_census_test.go::TestBackstopOpenFailureHoldsNoLoginOfAnUnparsableDSN"},
	"internal/fixturescli/product_telemetry.go":        {binary: "dho", runs: []openerRun{{args: []string{"fixtures", "product-telemetry", "--days", "1", "--sessions-per-day", "1"}, env: []string{"POSTGRES_URI={DSN}", censusClickHouse}}}},
	"internal/goapicli/prove/main.go":                  {drivenBy: "internal/goapicli/prove/credentials_census_test.go::TestOpenPostgresPoolHoldsNoLoginOfAnUnparsableDSN"},
	"internal/goapicli/routing/main.go":                {binary: "dho", runs: []openerRun{{args: []string{"goapi", "routing", "status"}, env: []string{"POSTGRES_URI={DSN}"}, reaches: "could not be parsed as a Postgres DSN"}}},
	"internal/maintenancecli/maintenance.go":           {binary: "dho", runs: []openerRun{{args: []string{"maintenance", "cleanup-tokens"}, env: []string{"POSTGRES_URI={DSN}"}}}},
	"internal/pgmigrate/command.go": {binary: "dho", runs: []openerRun{
		{args: []string{"migrate", "postgres", "status"}, env: []string{"POSTGRES_URI={DSN}", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1"}},
		{args: []string{"migrate", "postgres", "current"}, env: []string{"POSTGRES_URI={DSN}", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1"}}}},
	"internal/pgmigrate/downgrade.go":            {binary: "dho", runs: []openerRun{{args: []string{"migrate", "postgres", "downgrade", "-1"}, env: []string{"POSTGRES_URI={DSN}", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1"}}}},
	"internal/pgmigrate/indexbuild.go":           {binary: "dho", runs: []openerRun{{args: []string{"migrate", "postgres", "build-index", "failure-streak"}, env: []string{"POSTGRES_URI={DSN}", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1"}}}},
	"internal/pgmigrate/preflight.go":            {binary: "dho", runs: []openerRun{{args: []string{"migrate", "postgres", "preflight"}, env: []string{"POSTGRES_URI={DSN}", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1"}}}},
	"internal/rivermigrate/ownermove.go":         {binary: "dho", runs: []openerRun{{args: []string{"migrate", "owner-move", "-schemas", "census", "-from", "a", "-to", "b", "-plan"}, env: []string{"MIGRATION_DATABASE_URI={DSN}", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1"}}}},
	"internal/synccli/jira_stored_credential.go": {binary: "dho", runs: []openerRun{{args: []string{"sync", "teams", "--provider", "jira", "--org", "00000000-0000-0000-0000-000000000001"}, env: []string{"POSTGRES_URI={DSN}", censusClickHouse}}}},
	"tools/record-provider-answers/main.go":      {binary: "recorder", runs: []openerRun{{args: []string{"-provider", "linear", "-dry"}, env: []string{"POSTGRES_URI={DSN}", "SETTINGS_ENCRYPTION_KEY=census-key-census-key-census-key-1"}}}},
}

var (
	censusBinariesOnce sync.Once
	censusDir          string
	censusBuildErr     error
)

// censusBinaries builds the two binaries once per test run, under TMPDIR.
func censusBinaries(t *testing.T) string {
	t.Helper()
	censusBinariesOnce.Do(func() {
		root := repoRoot(t)
		censusDir, censusBuildErr = os.MkdirTemp("", "opener-census-")
		if censusBuildErr != nil {
			return
		}
		for name, pkg := range map[string]string{"dho": "./cmd/dho", "recorder": "./tools/record-provider-answers"} {
			build := exec.Command("go", "build", "-o", filepath.Join(censusDir, name), pkg)
			build.Dir = root
			if out, err := build.CombinedOutput(); err != nil {
				censusBuildErr = fmt.Errorf("build %s: %v\n%s", pkg, err, out)
				return
			}
		}
	})
	if censusBuildErr != nil {
		t.Fatal(censusBuildErr)
	}
	return censusDir
}

func TestMain(m *testing.M) {
	code := m.Run()
	if censusDir != "" {
		_ = os.RemoveAll(censusDir)
	}
	os.Exit(code)
}

func runCensusBinary(t *testing.T, binary string, run openerRun, dsn string) (int, string) {
	t.Helper()
	// The build runs before the clock starts: a cold, loaded runner takes minutes to compile, and that time is not the run's.
	binaryPath := filepath.Join(censusBinaries(t), binary)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binaryPath, run.args...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir()}
	for _, entry := range run.env {
		cmd.Env = append(cmd.Env, strings.ReplaceAll(entry, "{DSN}", dsn))
	}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	code := 0
	if err := cmd.Run(); err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if !ok || ctx.Err() != nil {
			t.Fatalf("%v %v: %v", binary, run.args, err)
		}
		code = exitErr.ExitCode()
	}
	return code, output.String()
}

func TestEveryBoundaryOpenerFailsClosedOnAnUnparsableDSN(t *testing.T) {
	censusBinaries(t) // build once, before any subtest is timed
	for path, class := range openers {
		if class.kind != kindBoundary {
			continue
		}
		driver, ok := openerDrivers[path]
		if !ok {
			t.Errorf("%s is a boundary opener with no driver in openerDrivers: add the verb that opens it, run with a malformed connection string", path)
			continue
		}
		if driver.drivenBy != "" {
			file, name, _ := strings.Cut(driver.drivenBy, "::")
			parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(repoRoot(t), file), nil, 0)
			found := false
			if err == nil {
				for _, decl := range parsed.Decls {
					if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
						found = true
					}
				}
			}
			if !found {
				t.Errorf("%s: drivenBy names %s, which does not exist", path, driver.drivenBy)
			}
			continue
		}
		for index, run := range driver.runs {
			for form, dsn := range censusDSNs {
				t.Run(fmt.Sprintf("%s/%d/%s", path, index, form), func(t *testing.T) {
					code, output := runCensusBinary(t, driver.binary, run, dsn)
					if (code == 0 && run.reaches == "") || output == "" {
						t.Fatalf("exit %d, output %q: the run did not refuse the malformed connection string", code, output)
					}
					for _, leaked := range []string{censusLogin, censusPassword, "[::1"} {
						if strings.Contains(output, leaked) {
							t.Errorf("%s %v leaks %q of an unparsable DSN: %q", path, run.args, leaked, output)
						}
					}
					if run.reaches != "" && !strings.Contains(output, run.reaches) {
						t.Errorf("the output %q does not hold %q: the run stopped before the open", output, run.reaches)
					}
				})
			}
		}
	}
	for path := range openerDrivers {
		if openers[path].kind != kindBoundary {
			t.Errorf("%s has a driver but is not a boundary opener", path)
		}
	}
}
