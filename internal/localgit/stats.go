package localgit

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// CommitStat is one GitCommitStat row of processors/local.py
// _compute_commit_stats_sync.
type CommitStat struct {
	CommitHash  string
	FilePath    string
	Additions   int
	Deletions   int
	OldFileMode string
	NewFileMode string
}

// CommitStats is _compute_commit_stats_sync: the numstat map GitPython's
// Commit.stats builds (empty on any failure), then one row per file of the
// raw diff against the first parent (Diffable.diff). A root commit is diffed
// with `git diff <commit>` -- Diffable.diff(other=None) compares against the
// WORKING TREE, so a root commit's rows are what changed since it -- and its
// numstat comes from `git diff-tree --root`. A diff that fails yields no rows.
func (r Repo) CommitStats(ctx context.Context, commit Commit) []CommitStat {
	stats := r.numstatFiles(ctx, commit)

	var raw []byte
	var err error
	if len(commit.Parents) > 0 {
		raw, err = r.run(ctx, "diff-tree", commit.Parents[0], commit.Hash, "-r", "--abbrev=40", "--full-index", "-M", "--raw", "-z", "--no-color")
	} else {
		raw, err = r.run(ctx, "diff", commit.Hash, "--abbrev=40", "--full-index", "-M", "--raw", "-z", "--no-color")
	}
	if err != nil {
		return nil
	}
	diffs, ok := parseRawDiff(raw)
	if !ok {
		return nil
	}
	rows := make([]CommitStat, 0, len(diffs))
	for _, diff := range diffs {
		path := diff.BPath
		if path == "" {
			path = diff.APath
		}
		if path == "" {
			continue
		}
		file := stats[path]
		rows = append(rows, CommitStat{
			CommitHash: commit.Hash, FilePath: path,
			Additions: file.insertions, Deletions: file.deletions,
			OldFileMode: modeText(diff.OldMode), NewFileMode: modeText(diff.NewMode),
		})
	}
	return rows
}

// modeText is `str(diff.a_mode) if diff.a_mode else "000000"` where a_mode is
// mode_str_to_int(): the octal mode read as an integer, printed in decimal.
func modeText(mode string) string {
	value := 0
	if len(mode) > 6 {
		mode = mode[len(mode)-6:]
	}
	for _, digit := range mode {
		value = value<<3 + int(digit-'0')
	}
	if value == 0 {
		return "000000"
	}
	return strconv.Itoa(value)
}

type fileNumstat struct{ insertions, deletions int }

// numstatFiles is Commit.stats.files: `git diff --numstat --no-renames --raw
// <parent> <commit> --` (root: `git diff-tree ... --root <commit> --` minus its
// first line), the lines zipped with their second half, then Stats parsing.
// Any failure (a shallow boundary, a line that is not four tab-separated
// fields) is an empty map, as `except Exception` makes it in Python.
func (r Repo) numstatFiles(ctx context.Context, commit Commit) map[string]fileNumstat {
	var out []byte
	var err error
	if len(commit.Parents) == 0 {
		out, err = r.run(ctx, "diff-tree", "--numstat", "--no-renames", "--root", "--raw", commit.Hash, "--")
	} else {
		out, err = r.run(ctx, "diff", "--numstat", "--no-renames", "--raw", commit.Parents[0], commit.Hash, "--")
	}
	if err != nil {
		return nil
	}
	lines := pythonparity.SplitLines(strings.TrimSuffix(string(out), "\n"))
	if len(commit.Parents) == 0 && len(lines) > 0 {
		lines = lines[1:]
	}
	// process_lines: zip(lines, lines[len(lines)//2:]) pairs each raw line with
	// a numstat line; the pair count is the length of the shorter list.
	half := len(lines) / 2
	second := lines[half:]
	var text strings.Builder
	for i := 0; i < len(lines) && i < len(second); i++ {
		changeType := lastRune(strings.SplitN(lines[i], "\t", 2)[0])
		fields := strings.Split(second[i], "\t")
		if len(fields) != 3 {
			return nil // `(insertions, deletions, filename) = line.split("\t")`
		}
		text.WriteString(changeType + "\t" + fields[0] + "\t" + fields[1] + "\t" + fields[2] + "\n")
	}
	files := map[string]fileNumstat{}
	for _, line := range pythonparity.SplitLines(text.String()) {
		fields := strings.Split(line, "\t")
		if len(fields) != 4 {
			return nil
		}
		insertions, deletions := numstatCount(fields[1]), numstatCount(fields[2])
		if insertions < 0 || deletions < 0 {
			return nil // int() of a non-number: ValueError
		}
		files[pythonparity.Strip(fields[3])] = fileNumstat{insertions, deletions}
	}
	return files
}

func lastRune(text string) string {
	runes := []rune(text)
	if len(runes) == 0 {
		return ""
	}
	return string(runes[len(runes)-1])
}

// numstatCount is `raw != "-" and int(raw) or 0`; -1 marks an int() failure.
func numstatCount(raw string) int {
	if raw == "-" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return -1
	}
	return value
}

// RawDiff is one Diff of Diff._handle_diff_line.
type RawDiff struct {
	OldMode, NewMode string
	APath, BPath     string
	ChangeType       byte
}

// parseRawDiff is Diff._index_from_raw_format: GitPython hands the output of
// `--raw -z` to _handle_diff_line one readline() chunk at a time, so a NEWLINE
// inside a path splits an entry (the path is cut there, the rest is dropped),
// and every chunk is decoded as strict UTF-8 (a path that is not valid UTF-8
// raises, and so does a chunk _handle_diff_line cannot unpack). It returns
// false when Python would raise: the whole commit then has no rows.
func parseRawDiff(raw []byte) ([]RawDiff, bool) {
	var diffs []RawDiff
	for len(raw) > 0 {
		end := bytes.IndexByte(raw, '\n') + 1
		if end == 0 {
			end = len(raw)
		}
		chunk := raw[:end]
		raw = raw[end:]
		if !utf8.Valid(chunk) {
			return nil, false
		}
		found, ok := handleDiffLine(string(chunk))
		if !ok {
			return nil, false
		}
		diffs = append(diffs, found...)
	}
	return diffs, true
}

// handleDiffLine is Diff._handle_diff_line over one chunk.
func handleDiffLine(text string) ([]RawDiff, bool) {
	_, after, _ := strings.Cut(text, ":")
	var diffs []RawDiff
	for _, line := range strings.Split(after, "\x00:") {
		if line == "" {
			continue
		}
		meta, path, _ := strings.Cut(line, "\x00")
		path = strings.TrimRight(path, "\x00")
		fields := pythonparity.SplitWhitespace(meta)
		if len(fields) < 5 {
			return nil, false
		}
		changeType := fields[4]
		path = strings.Trim(path, "\n")
		diff := RawDiff{OldMode: fields[0], NewMode: fields[1], APath: path, BPath: path, ChangeType: changeType[0]}
		switch changeType[0] {
		case 'C', 'R':
			a, b, ok := strings.Cut(path, "\x00")
			if !ok {
				return nil, false
			}
			diff.APath, diff.BPath = a, b
		}
		diffs = append(diffs, diff)
	}
	return diffs, true
}
