// Package bigboyrender holds bigboy's small ci helpers: the dho_api_ch users.d renderer, the ClickHouse grants
// comparison, the Postgres posture deltas of a cut and the values sha rewrite of bump-swap. cmd/cigate runs them as
// `cigate bigboy <verb>`; the ci/bigboy shell scripts call those verbs.
//
// Each was a Python script; the ports keep their arguments, exit codes, output and written files byte for byte
// (testdata/README.md records the Python runs the tests replay). Where Python stopped on an uncaught exception
// (a traceback), the port prints one plain error line and exits 1.
package bigboyrender

import (
	"fmt"
	"io"
	"regexp"
	"strings"
)

const usage = "usage: cigate bigboy render-ch-users|grants-compare|pg-posture api|domain|bump-swap-values [args]"

// Run dispatches one verb of `cigate bigboy` and returns the exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	switch args[0] {
	case "render-ch-users":
		return renderCHUsers(args[1:], stdout, stderr)
	case "grants-compare":
		return grantsCompare(args[1:], stdout, stderr)
	case "pg-posture":
		return pgPosture(args[1:], stdout, stderr)
	case "bump-swap-values":
		return bumpSwapValues(args[1:], stderr)
	}
	fmt.Fprintf(stderr, "cigate bigboy: unknown verb %q\n%s\n", args[0], usage)
	return 2
}

// pyLines splits like Python's str.splitlines.
func pyLines(s string) []string {
	var lines []string
	runes := []rune(s)
	var cur []rune
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch r {
		case '\n', '\v', '\f', 0x1c, 0x1d, 0x1e, 0x85, 0x2028, 0x2029:
		case '\r':
			if i+1 < len(runes) && runes[i+1] == '\n' {
				i++
			}
		default:
			cur = append(cur, r)
			continue
		}
		lines = append(lines, string(cur))
		cur = nil
	}
	if len(cur) > 0 {
		lines = append(lines, string(cur))
	}
	return lines
}

// universalNewlines is what Python's text-mode read does to \r\n and \r.
func universalNewlines(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	return strings.ReplaceAll(s, "\r", "\n")
}

// afterFirst is Python's s.split(sep, 1)[1]; ok is false when sep is absent (IndexError).
func afterFirst(s, sep string) (string, bool) {
	_, rest, ok := strings.Cut(s, sep)
	return rest, ok
}

// beforeFirst is Python's s.split(sep, 1)[0].
func beforeFirst(s, sep string) string {
	head, _, _ := strings.Cut(s, sep)
	return head
}

// apiTable is the CH APIPosture row `{Database: database, Table: "x", Allow...: true}`.
var apiTable = regexp.MustCompile(`\{Database: database, Table: "(\w+)",([^}]*)\}`)

var chPrivileges = [][2]string{{"AllowSelect", "SELECT"}, {"AllowInsert", "INSERT"}, {"AllowDelete", "ALTER DELETE"}}

// chGrant is one privilege on one table of the ClickHouse manifest.
type chGrant struct{ privilege, table string }

// chManifest returns the privilege/table pairs of func APIPosture in manifest order, per table in privilege order.
// ok is false when the source has no func APIPosture (Python raised IndexError there, or yielded nothing).
func chManifest(source string) (grants []chGrant, ok bool) {
	rest, ok := afterFirst(source, "func APIPosture(")
	if !ok {
		return nil, false
	}
	body := beforeFirst(rest, "\n}\n")
	for _, line := range pyLines(body) {
		m := apiTable.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		for _, p := range chPrivileges {
			if strings.Contains(m[2], p[0]+": true") {
				grants = append(grants, chGrant{p[1], m[1]})
			}
		}
	}
	return grants, true
}
