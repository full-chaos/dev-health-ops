package rivermigrate

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// `dho migrate` carries the flat aliases of the Alembic verbs the Python CLI
// registers directly under `migrate`; `upgrade` stays the verb that runs the migrate
// Job's steps, and each alias is a verb.
func TestMigrateHasTheFlatAliases(t *testing.T) {
	children := map[string]cli.Command{}
	for _, child := range Command().Children {
		children[child.Name] = child
	}
	for _, name := range []string{"current", "heads", "history", "status", "downgrade"} {
		child, ok := children[name]
		if !ok || child.Kind != cli.Verb || child.Run == nil {
			t.Errorf("dho migrate %s: not a verb (%+v)", name, child)
		}
	}
	if !strings.Contains(children["upgrade"].Summary, "Job's steps") {
		t.Errorf("dho migrate upgrade is %q, want the Job-steps verb", children["upgrade"].Summary)
	}
}

// heads, history and downgrade need no database: they answer the same as under
// `migrate postgres`.
func TestFlatAliasesAnswerWithoutADatabase(t *testing.T) {
	var heads, postgresHeads strings.Builder
	run := func(path []string, out *strings.Builder) int {
		var stderr strings.Builder
		return cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
			Args: path, Stdout: out, Stderr: &stderr, Lookup: func(string) (string, bool) { return "", false },
		})
	}
	if code := run([]string{"migrate", "heads"}, &heads); code != cli.ExitOK || heads.Len() == 0 {
		t.Fatalf("migrate heads: exit %d, output %q", code, heads.String())
	}
	if code := run([]string{"migrate", "postgres", "heads"}, &postgresHeads); code != cli.ExitOK || heads.String() != postgresHeads.String() {
		t.Errorf("migrate heads %q differs from migrate postgres heads %q", heads.String(), postgresHeads.String())
	}
	var out strings.Builder
	if code := run([]string{"migrate", "downgrade", "-1"}, &out); code != cli.ExitRefused {
		t.Errorf("migrate downgrade -1: exit %d, want the refusal (3)", code)
	}
	out.Reset()
	if code := run([]string{"migrate", "status"}, &out); code != cli.ExitFailure || out.Len() != 0 {
		t.Errorf("migrate status without a database: exit %d, stdout %q, want exit 1 and nothing printed", code, out.String())
	}
}
