package pgmigrate

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// Aliases are the flat verbs of `dev-hops migrate` (`migrate current`, `migrate
// heads`, `migrate history`, `migrate status`, `migrate downgrade`): the Python
// CLI registers the Alembic verbs both under `migrate postgres` and directly under
// `migrate`, and scripts and runbooks use either. `current`, `heads`, `history` and
// `downgrade` are the verbs of the postgres group; `status` prints what the Python
// verb prints (its text and its --check exit code), not the JSON report of
// `migrate postgres status`. `migrate upgrade` is not one of them: in dho it is the
// verb that runs the migrate Job's steps in order.
func Aliases(resolve ResolveDSN) []cli.Command {
	group := Command(resolve)
	var out []cli.Command
	for _, name := range []string{"current", "heads", "history", "downgrade"} {
		for _, child := range group.Children {
			if child.Name == name {
				out = append(out, child)
			}
		}
	}
	out = append(out, cli.Command{
		Name:    "status",
		Summary: "show whether the required PostgreSQL migrations are applied (--check exits 1 when one is pending), as `dev-hops migrate status` does",
		Kind:    cli.Verb,
		Run:     func(ctx context.Context, env cli.Env) int { return statusText(ctx, resolve, env) },
	})
	return out
}

// downRevisions is the revisions an entry descends from ("<base>" is none).
func (e HistoryEntry) downRevisions() []string {
	if e.Down == "" || e.Down == "<base>" {
		return nil
	}
	parts := strings.Split(e.Down, ",")
	for index := range parts {
		parts[index] = strings.TrimSpace(parts[index])
	}
	return parts
}

// HasRevision is Python's _database_has_revision: whether any current head
// descends from target (or is it), walking the revision graph; a revision the
// graph does not know ends its branch.
func HasRevision(entries []HistoryEntry, current []string, target string) bool {
	graph := map[string]HistoryEntry{}
	for _, entry := range entries {
		graph[entry.Revision] = entry
	}
	pending := append([]string(nil), current...)
	visited := map[string]bool{}
	for len(pending) > 0 {
		revision := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if revision == target {
			return true
		}
		if visited[revision] {
			continue
		}
		visited[revision] = true
		entry, known := graph[revision]
		if !known {
			continue
		}
		pending = append(pending, entry.downRevisions()...)
	}
	return false
}

// RequiredRevisions is _required_postgres_revisions: the application head, and the
// River cutover revision when the cutover is authorized.
func RequiredRevisions(baseline Baseline, chain []ChainFile, cutover bool) []string {
	application := applicationHead(baseline)
	if len(chain) > 0 {
		application = chain[len(chain)-1].Revision
	}
	required := []string{application}
	if cutover {
		required = append(required, cutoverRevision)
	}
	return required
}

// statusText is `dho migrate status`, as `dev-hops migrate status [--check]`:
//
//	Current PostgreSQL revision heads: <recorded, comma separated, or (base)>
//	Required PostgreSQL revisions: <required>
//	Pending required PostgreSQL revisions: <missing>     (exit 1 with --check)
//	PostgreSQL application schema is current.            (when nothing is missing)
func statusText(ctx context.Context, resolve ResolveDSN, env cli.Env) int {
	flags := flag.NewFlagSet("dho migrate status", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	check := flags.Bool("check", false, "exit 1 if a required migration is pending, 0 if the application schema is current; read-only")
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
	baseline, err := LoadBaseline()
	if err != nil {
		return writeError(env.Stderr, "baseline_unavailable", err.Error())
	}
	chain, err := LoadChain()
	if err != nil {
		return writeError(env.Stderr, "chain_unavailable", err.Error())
	}
	entries, err := LoadHistory()
	if err != nil {
		return writeError(env.Stderr, "history_unavailable", err.Error())
	}
	entries = WithChain(entries, baseline, chain)
	current, code, ok := recordedRevisions(ctx, resolve, env)
	if !ok {
		return code
	}
	cutover := false
	if env.Lookup != nil {
		value, _ := env.Lookup(CutoverEnv)
		cutover = value == "1"
	}
	graph := map[string]bool{}
	for _, entry := range entries {
		graph[entry.Revision] = true
	}
	for _, revision := range current {
		// Alembic refuses a recorded revision its script directory does not contain
		// (the Python verb raises before printing anything).
		if !graph[revision] {
			return writeError(env.Stderr, "unknown_revision", fmt.Sprintf("alembic_version records %q, which is not a revision of this release", revision))
		}
	}
	required := RequiredRevisions(baseline, chain, cutover)
	var missing []string
	for _, revision := range required {
		if !HasRevision(entries, current, revision) {
			missing = append(missing, revision)
		}
	}
	shown := strings.Join(current, ", ")
	if shown == "" {
		shown = "(base)"
	}
	fmt.Fprintf(env.Stdout, "Current PostgreSQL revision heads: %s\n", shown)
	fmt.Fprintf(env.Stdout, "Required PostgreSQL revisions: %s\n", strings.Join(required, ", "))
	if len(missing) > 0 {
		fmt.Fprintf(env.Stdout, "Pending required PostgreSQL revisions: %s\n", strings.Join(missing, ", "))
		if *check {
			return cli.ExitFailure
		}
		return cli.ExitOK
	}
	fmt.Fprintln(env.Stdout, "PostgreSQL application schema is current.")
	return cli.ExitOK
}
