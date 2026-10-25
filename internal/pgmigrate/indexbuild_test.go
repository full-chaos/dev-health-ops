package pgmigrate_test

import (
	"bytes"
	"context"
	"io"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

var (
	sqlSpaces       = regexp.MustCompile(`\s+`)
	sqlLineComments = regexp.MustCompile(`(?m)--[^\n]*$`)
)

func squash(sql string) string {
	return strings.TrimSpace(sqlSpaces.ReplaceAllString(sqlLineComments.ReplaceAllString(sql, ""), " "))
}

func chainFile(t *testing.T, revision string) pgmigrate.ChainFile {
	t.Helper()
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range chain {
		if file.Revision == revision {
			return file
		}
	}
	t.Fatalf("the chain holds no revision %s", revision)
	return pgmigrate.ChainFile{}
}

// The Job's definition and revision 0159's statement are one: the same name, table, columns, order and predicate, so a
// database the Job built and a database revision 0159 built hold the same index.
func TestFailureStreakIndexDefinitionMatchesRevision0159(t *testing.T) {
	want := squash("CREATE INDEX IF NOT EXISTS " + pgmigrate.FailureStreakIndexBody + ";")
	if got := squash(chainFile(t, "0159").SQL); got != want {
		t.Fatalf("revision 0159 =\n%s\nwant\n%s", got, want)
	}
}

// The Job's open-units definition is contained in revision 0160's statements.
func TestOpenUnitsIndexDefinitionMatchesRevision0160(t *testing.T) {
	want := squash("CREATE INDEX IF NOT EXISTS " + pgmigrate.OpenUnitsIndexBody + ";")
	if got := squash(chainFile(t, "0160").SQL); !strings.Contains(got, want) {
		t.Fatalf("revision 0160 =\n%s\nwant it to contain\n%s", got, want)
	}
}

// The index serves providersync.RecentTerminalUnitsSQL: its status predicate is the query's, its equality columns lead
// and its ORDER BY follows, so the planner can read each source's newest rows from the index with no sort.
func TestFailureStreakIndexServesTheStreakQuery(t *testing.T) {
	query := squash(providersync.RecentTerminalUnitsSQL)
	body := squash(pgmigrate.FailureStreakIndexBody)
	for _, part := range []string{
		"u.status IN ('success', 'failed')",
		"ORDER BY u.updated_at DESC, u.id DESC",
	} {
		if !strings.Contains(query, part) {
			t.Fatalf("the streak query no longer contains %q; revisit the index", part)
		}
	}
	if !strings.Contains(body, "WHERE status IN ('success', 'failed')") {
		t.Fatalf("index predicate is not the query's status predicate: %s", body)
	}
	if !strings.Contains(body, "(org_id, integration_id, source_id, dataset_key, updated_at DESC, id DESC)") {
		t.Fatalf("index columns are not the query's equality columns then its ORDER BY: %s", body)
	}
	for _, column := range []string{"u.org_id = $1", "u.integration_id = $2::uuid", "u.source_id = s.id", "u.dataset_key = $3"} {
		if !strings.Contains(query, column) {
			t.Fatalf("the streak query no longer filters %q; revisit the index", column)
		}
	}
}

func TestRevision0159DownDropsTheIndexOnly(t *testing.T) {
	var down string
	files, err := pgmigrate.LoadDownChain()
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.Revision == "0159" {
			down = squash(file.SQL)
		}
	}
	if down != "DROP INDEX IF EXISTS public.ix_sync_run_units_failure_streak;" {
		t.Fatalf("down 0159 = %q", down)
	}
}

func TestBuildIndexRefusesAnyOtherIndexName(t *testing.T) {
	resolved := false
	resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		resolved = true
		return secrets.NewValue("postgres://never.invalid/x"), "test", true
	})
	var run func(context.Context, cli.Env) int
	for _, child := range pgmigrate.Command(resolve).Children {
		if child.Name == "build-index" {
			run = child.Run
		}
	}
	if run == nil {
		t.Fatal("no build-index verb")
	}
	for _, args := range [][]string{nil, {"other"}, {"failure-streak", "extra"}, {"open-units", "failure-streak"}, {"--lock-timeout=1ms", "failure-streak"}, {"--attempts=0", "failure-streak"}} {
		var stderr bytes.Buffer
		if code := run(context.Background(), cli.Env{Args: args, Stdout: io.Discard, Stderr: &stderr}); code != cli.ExitUsage {
			t.Errorf("args %v: exit %d, want %d (%s)", args, code, cli.ExitUsage, stderr.String())
		}
	}
	if resolved {
		t.Fatal("a refused invocation resolved the DSN")
	}
}

// The chart passes the flags and then the index name; an operator may type them the other way round. Both orders must
// pass the argument parser and reach the DSN resolver (which here refuses, so nothing connects).
func TestBuildIndexAcceptsFlagsBeforeAndAfterTheIndexName(t *testing.T) {
	flags := []string{"--lock-timeout=5s", "--attempts=3", "--retry-delay=15s"}
	for name, args := range map[string][]string{
		"chart order": append(append([]string{}, flags...), "failure-streak"),
		"name first":  append([]string{"failure-streak"}, flags...),
		"flags both":  {"--lock-timeout=5s", "failure-streak", "--attempts=3", "--retry-delay=15s"},
		"name alone":  {"failure-streak"},
	} {
		t.Run(name, func(t *testing.T) {
			resolved := false
			resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
				resolved = true
				return secrets.Value{}, "", false
			})
			var run func(context.Context, cli.Env) int
			for _, child := range pgmigrate.Command(resolve).Children {
				if child.Name == "build-index" {
					run = child.Run
				}
			}
			var stderr bytes.Buffer
			code := run(context.Background(), cli.Env{Args: args, Stdout: io.Discard, Stderr: &stderr})
			if !resolved || code == cli.ExitUsage {
				t.Fatalf("args %v: exit %d resolved=%v (%s)", args, code, resolved, stderr.String())
			}
		})
	}
}
