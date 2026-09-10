package gqlgenguard

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/pmezard/go-difflib/difflib"
)

// diffContext is how many unchanged lines surround each hunk in the record.
const diffContext = 3

// recordHeader is the fixed preamble of the expected-drift record. It is part
// of the compared bytes on purpose: an operator who deletes the explanation
// while hand-editing the record fails the check rather than quietly losing it.
const recordHeader = `# gqlgen expected-drift record. GENERATED -- rewrite with:
#     go run ./cmd/gqlgen-guard check-drift -update
#
# What this file is: every difference between the generated files CHECKED IN to
# this repository and what a fresh run of the generator produces. Each one is a
# deliberate hand-edit that regeneration reverts. The guard compares this file
# BYTE FOR BYTE, which makes the comparison exact in both directions at once:
# new drift means something was regenerated over a hand-edit, and a MISSING
# entry means a hand-edit vanished -- which is how a nullability ruling would
# quietly revert. A subset check would see only the first of those.
#
# The digest table is the exact half and the hunks are the readable half. Do
# not edit either by hand: regenerate, then read the diff of THIS file as the
# review.
`

// renderRecord produces the expected-drift record for one generation.
//
// The "a" side of every hunk is read from the WORKING TREE and the "b" side
// from the private copy after generation. The output is a pure function of the
// plan, the recorded digests and those bytes, so two runs on the same inputs
// produce identical text and the byte comparison in CheckDrift is meaningful.
func renderRecord(res *Result, plan *Plan, gen Generator, moduleRoot, copyRoot *os.Root) string {
	var b strings.Builder
	b.WriteString(recordHeader)
	b.WriteString("#\n")
	fmt.Fprintf(&b, "# config: %s\n", plan.ConfigPath)
	for _, s := range plan.Schemas {
		fmt.Fprintf(&b, "# schema: %s\n", s)
	}
	fmt.Fprintf(&b, "# generator: %s\n", gen.Describe())
	fmt.Fprintf(&b, "# declared output paths: %d\n", len(plan.Outputs))
	b.WriteString("#\n")

	b.WriteString("# digests: <status> <path> tree=<sha256> generated=<sha256>\n")
	drifted := make([]Change, 0, len(res.Changes))
	for _, c := range res.Changes {
		status := statusOf(c)
		fmt.Fprintf(&b, "digest %s %s tree=%s generated=%s\n",
			status, c.Path, orNone(c.TreeDigest), orNone(c.GeneratedDigest))
		if c.Drifted() {
			drifted = append(drifted, c)
		}
	}
	b.WriteString("\n")

	if len(drifted) == 0 {
		b.WriteString("# no drift: a fresh generation reproduces every checked-in output exactly.\n")
		return b.String()
	}

	sort.Slice(drifted, func(i, j int) bool { return drifted[i].Path < drifted[j].Path })
	for _, c := range drifted {
		treeText := readForDiff(moduleRoot, c.Path, c.TreeDigest)
		genText := readForDiff(copyRoot, c.Path, c.GeneratedDigest)
		diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:        splitLines(treeText),
			FromFile: "a/" + c.Path,
			B:        splitLines(genText),
			ToFile:   "b/" + c.Path,
			Context:  diffContext,
		})
		if err != nil {
			// difflib only fails on a write error to its internal buffer, which
			// a strings.Builder cannot produce. Surfacing it as record text
			// rather than swallowing it keeps the failure visible in the diff
			// of this file rather than as a silently shorter record.
			fmt.Fprintf(&b, "# ERROR rendering %s: %v\n", c.Path, err)
			continue
		}
		b.WriteString(diff)
	}
	return b.String()
}

// readForDiff returns the text of one side of a comparison, through a root
// handle so the read cannot leave the tree it belongs to. An empty digest
// means that side has no such file, which renders as a pure addition or
// deletion rather than as an unreadable file.
func readForDiff(root *os.Root, rel, digest string) string {
	if digest == "" {
		return ""
	}
	data, err := ReadThroughRoot(root, rel)
	if err != nil {
		return ""
	}
	return string(data)
}

// splitLines splits text into lines that each keep their terminating newline,
// giving the last line one if the file did not end with it.
//
// difflib's own SplitLines appends a newline to the final element, which for a
// file that already ends in one produces a phantom empty line. Doing it here
// keeps the record free of that artefact and keeps a file with no trailing
// newline from producing a mangled hunk.
func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	lines := strings.SplitAfter(s, "\n")
	if last := len(lines) - 1; lines[last] == "" {
		lines = lines[:last]
	} else {
		lines[last] += "\n"
	}
	return lines
}

func statusOf(c Change) string {
	switch {
	case c.TreeDigest == "" && c.GeneratedDigest == "":
		return "absent"
	case c.TreeDigest == "":
		return "added"
	case c.GeneratedDigest == "":
		return "not-written"
	case c.TreeDigest == c.GeneratedDigest:
		return "same"
	default:
		return "drift"
	}
}

func orNone(d string) string {
	if d == "" {
		return "-"
	}
	return d
}

// describeRecordMismatch turns "these two records differ" into something an
// operator can act on: which digest lines moved, and the diff of the record
// itself for everything else.
func describeRecordMismatch(committed, fresh string) string {
	var b strings.Builder

	committedDigests := digestLines(committed)
	freshDigests := digestLines(fresh)

	var paths []string
	seen := map[string]bool{}
	for p := range committedDigests {
		if !seen[p] {
			seen[p] = true
			paths = append(paths, p)
		}
	}
	for p := range freshDigests {
		if !seen[p] {
			seen[p] = true
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)

	var moved []string
	for _, p := range paths {
		c, hasC := committedDigests[p]
		f, hasF := freshDigests[p]
		switch {
		case hasC && !hasF:
			moved = append(moved, fmt.Sprintf("  %s: recorded %q, no longer a declared output", p, c))
		case !hasC && hasF:
			moved = append(moved, fmt.Sprintf("  %s: not in the record, now %q", p, f))
		case c != f:
			moved = append(moved, fmt.Sprintf("  %s:\n    recorded  %s\n    generated %s", p, c, f))
		}
	}

	if len(moved) > 0 {
		b.WriteString("digests that moved:\n")
		b.WriteString(strings.Join(moved, "\n"))
		b.WriteString("\n\n")
	}

	diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        splitLines(committed),
		FromFile: "a/committed-record",
		B:        splitLines(fresh),
		ToFile:   "b/fresh-record",
		Context:  diffContext,
	})
	if err == nil && diff != "" {
		b.WriteString("record diff (committed -> fresh):\n")
		b.WriteString(diff)
	}
	b.WriteString("\nIf every change above is a deliberate hand-edit, rerun with -update and commit the new record.\n")
	return b.String()
}

// digestLines indexes a record's digest table by path.
func digestLines(record string) map[string]string {
	out := map[string]string{}
	for line := range strings.SplitSeq(record, "\n") {
		if !strings.HasPrefix(line, "digest ") {
			continue
		}
		fields := strings.Fields(line)
		// digest <status> <path> tree=<d> generated=<d>
		if len(fields) != 5 {
			continue
		}
		out[fields[2]] = strings.Join(fields[1:], " ")
	}
	return out
}
