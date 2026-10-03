package pgmigrate

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// `history` prints what `alembic history` prints (dev-hops migrate postgres
// history, default form): one line per revision, newest first in the order
// Alembic walks its graph, "<down> -> <revision>" with the branch label, the head
// / branchpoint / mergepoint marks and the revision's docstring. dho does not carry
// the Alembic scripts (the Go migrator is a baseline plus the revisions after
// it), so the walk is the embedded baseline/history.json: what Alembic printed for
// the script directory, one entry per revision in walk order.
// TestHistoryGraphIsTheAlembicChain compares it with the walk of the Python scripts,
// executed once and frozen in testdata/golden/history_walk.json, and fails when the embedded copy drifts.

//go:embed baseline/history.json
var historyFile []byte

// HistoryEntry is one revision of the walk, with the pieces `alembic history`
// formats a line from. Down, Dependencies and Labels are already the text Alembic
// prints ("<base>", a comma-separated list, "" for none).
type HistoryEntry struct {
	Revision     string `json:"revision"`
	Down         string `json:"down"`
	Dependencies string `json:"dependencies,omitempty"`
	Labels       string `json:"labels,omitempty"`
	// RealHead is a revision no other revision refers to; Head is Alembic's
	// is_head (an "effective head" is a Head that is not a RealHead).
	RealHead    bool   `json:"realHead,omitempty"`
	Head        bool   `json:"head,omitempty"`
	BranchPoint bool   `json:"branchPoint,omitempty"`
	MergePoint  bool   `json:"mergePoint,omitempty"`
	Doc         string `json:"doc"`
}

// LoadHistory returns the embedded walk.
func LoadHistory() ([]HistoryEntry, error) {
	var entries []HistoryEntry
	if err := json.Unmarshal(historyFile, &entries); err != nil {
		return nil, fmt.Errorf("decode the history: %w", err)
	}
	if len(entries) == 0 {
		return nil, errors.New("the history is empty")
	}
	return entries, nil
}

// Format is the line `alembic history` prints for the entry.
func (e HistoryEntry) Format() string {
	text := e.Revision
	if e.Dependencies != "" {
		text = fmt.Sprintf("%s (%s) -> %s", e.Down, e.Dependencies, text)
	} else {
		text = fmt.Sprintf("%s -> %s", e.Down, text)
	}
	if e.Labels != "" {
		text += " (" + e.Labels + ")"
	}
	if e.RealHead {
		text += " (head)"
	}
	if e.Head && !e.RealHead {
		text += " (effective head)"
	}
	if e.BranchPoint {
		text += " (branchpoint)"
	}
	if e.MergePoint {
		text += " (mergepoint)"
	}
	return text + ", " + e.Doc
}

// chainDoc is the docstring a chain revision prints: its file's slug as words
// (a chain revision is a .sql file, not an Alembic script), ending in a period.
func chainDoc(file ChainFile) string {
	name := strings.TrimSuffix(file.Name, ".sql")
	if index := strings.Index(name, "_"); index >= 0 {
		name = name[index+1:]
	}
	words := strings.ReplaceAll(name, "_", " ")
	if words == "" {
		return file.Revision + "."
	}
	return strings.ToUpper(words[:1]) + words[1:] + "."
}

// WithChain is the walk when the chain after the baseline holds a revision the
// embedded walk does not have. A chain revision that is an Alembic revision (it is in
// the walk: every revision of the Python scripts is) is already there, with its own
// branch label and marks; only a chain revision of a release with no Alembic script
// is added: it prints newest first ahead of the application branch, the branch's top
// stops being a head, and the first added revision descends from it. An added
// revision has no branch label or marks (it is not an Alembic script).
func WithChain(entries []HistoryEntry, baseline Baseline, chain []ChainFile) []HistoryEntry {
	known := map[string]bool{}
	for _, entry := range entries {
		known[entry.Revision] = true
	}
	var added []ChainFile
	for _, file := range chain {
		if !known[file.Revision] {
			added = append(added, file)
		}
	}
	if len(added) == 0 {
		return entries
	}
	// The application branch's top: the real head that is not the cutover head.
	top := ""
	for _, entry := range entries {
		if entry.RealHead && entry.Revision != cutoverRevision {
			top = entry.Revision
		}
	}
	if top == "" {
		top = applicationHead(baseline)
	}
	var out []HistoryEntry
	for _, entry := range entries {
		if entry.Revision == top {
			for index := len(added) - 1; index >= 0; index-- {
				down := top
				if index > 0 {
					down = added[index-1].Revision
				}
				extra := HistoryEntry{Revision: added[index].Revision, Down: down, Doc: chainDoc(added[index])}
				if index == len(added)-1 {
					extra.RealHead, extra.Head = true, true
				}
				out = append(out, extra)
			}
			entry.RealHead, entry.Head = false, false
		}
		out = append(out, entry)
	}
	return out
}

// WriteHistory prints the walk.
func WriteHistory(out io.Writer, entries []HistoryEntry) error {
	for _, entry := range entries {
		if _, err := fmt.Fprintln(out, entry.Format()); err != nil {
			return err
		}
	}
	return nil
}

// history is `dho migrate postgres history`.
func history(_ context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho migrate postgres history", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	verbose := flags.Bool("verbose", false, "not supported: the Alembic script details are not carried in dho")
	flags.BoolVar(verbose, "v", false, "not supported: the Alembic script details are not carried in dho")
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
	if *verbose {
		fmt.Fprintln(env.Stderr, "argument error: --verbose is not supported: dho does not carry the Alembic script details (use alembic for them)")
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
	if err := WriteHistory(env.Stdout, WithChain(entries, baseline, chain)); err != nil {
		return cli.ExitFailure
	}
	return cli.ExitOK
}
