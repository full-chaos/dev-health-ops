package scripts_test

import (
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-13740: under `set -o pipefail`, a pipeline whose last command can exit before its writer ends (grep -q, -m, -l,
// head, sed ...q) is false at random: the reader exits at its first match, the writer's next write gets EPIPE or SIGPIPE,
// and pipefail turns the writer's failure into the pipeline's status. In ci/check_go.sh `printf | grep -Fqx key` sent a
// split part down the wrong test path in hosted CI. A here-string (`grep -Fqx -- "${key}" <<<"${list}"`) has no pipe.
//
// Scope: ci/check_go.sh, every ci/*_test.sh and the files they source (they run under their pipefail). A pipe inside a process substitution
// <(...) is not refused: its status is never read. Text in single quotes, comments and ${...} is not shell code here.

var (
	pipefailReaderEntries = []string{"ci/check_go.sh"}
	pipefailSetPattern    = regexp.MustCompile(`(?m)^[ \t]*set[ \t]+-(?:[A-Za-z]*o[ \t]+pipefail\b)`)
	sourcedScriptPattern  = regexp.MustCompile(`(?m)^[ \t]*(?:\.|source)[ \t]+"\$\{(ROOT|SCRIPT_DIR)\}/([^"]+)"`)
	sedQuitPattern        = regexp.MustCompile(`(?:^|[;{}\s'"])[qQ][0-9]*\s*(?:$|[;}'"])`)
	envAssignPattern      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*=`)
)

type earlyExitHit struct {
	file   string
	line   int
	reader string
}

// pipefailReaderScope returns each entry that sets pipefail and every script it sources by ${ROOT}/ or ${SCRIPT_DIR}/.
func pipefailReaderScope(t *testing.T, root string, entries []string) map[string]string {
	t.Helper()
	all := toolchainScopeFiles(t, root)
	scope := map[string]string{}
	for _, entry := range entries {
		body, ok := all[entry]
		if !ok {
			t.Fatalf("pipefail reader guard: entry %s not found under %s", entry, root)
		}
		if !pipefailSetPattern.MatchString(body) {
			continue
		}
		scope[entry] = body
		for _, match := range sourcedScriptPattern.FindAllStringSubmatch(body, -1) {
			rel := match[2]
			if match[1] == "SCRIPT_DIR" {
				rel = path.Join(path.Dir(entry), rel)
			}
			lib, ok := all[rel]
			if !ok {
				t.Fatalf("pipefail reader guard: %s sources %s, which is not found", entry, rel)
			}
			scope[rel] = lib
		}
	}
	return scope
}

type shellScan struct {
	src  string
	pos  int
	line int
	file string
	hits []earlyExitHit
}

// earlyExitReadersIn returns every pipe in text whose reader can exit before its writer ends.
func earlyExitReadersIn(file, text string) []earlyExitHit {
	s := &shellScan{src: text, line: 1, file: file}
	s.code(true, true)
	return s.hits
}

func (s *shellScan) at(offset int) byte {
	if s.pos+offset < len(s.src) {
		return s.src[s.pos+offset]
	}
	return 0
}

func (s *shellScan) advance(n int) {
	for ; n > 0 && s.pos < len(s.src); n-- {
		if s.src[s.pos] == '\n' {
			s.line++
		}
		s.pos++
	}
}

// code scans shell code to the end, or (unless top) to the ')' that closes it. Pipes count only when checked.
func (s *shellScan) code(checked, top bool) {
	depth := 0
	for s.pos < len(s.src) {
		c := s.at(0)
		switch {
		case c == '\\':
			s.advance(2)
		case c == '\'':
			s.advance(1)
			s.skipPast('\'')
		case c == '`':
			s.advance(1)
			s.skipPast('`')
		case c == '"':
			s.advance(1)
			s.dquote(checked)
		case c == '$' && s.at(1) == '(' && s.at(2) == '(':
			s.advance(3)
			s.arith()
		case c == '$' && s.at(1) == '(':
			s.advance(2)
			s.code(checked, false)
		case c == '$' && s.at(1) == '{':
			s.advance(2)
			s.braces()
		case (c == '<' || c == '>') && s.at(1) == '(':
			s.advance(2)
			s.code(false, false)
		case c == '#' && (s.pos == 0 || strings.IndexByte(" \t\n;&|(", s.src[s.pos-1]) >= 0):
			for s.pos < len(s.src) && s.src[s.pos] != '\n' {
				s.pos++
			}
		case c == '(':
			s.advance(1)
			depth++
		case c == ')':
			s.advance(1)
			if depth > 0 {
				depth--
			} else if !top {
				return
			}
		case c == '|' && s.at(1) == '|':
			s.advance(2)
		case c == '|':
			line := s.line
			s.advance(1)
			if s.at(0) == '&' {
				s.advance(1)
			}
			if !checked {
				continue
			}
			if reader := earlyExitReader(readerWords(s.src[s.pos:])); reader != "" {
				s.hits = append(s.hits, earlyExitHit{file: s.file, line: line, reader: reader})
			}
		default:
			s.advance(1)
		}
	}
}

func (s *shellScan) skipPast(end byte) {
	for s.pos < len(s.src) && s.src[s.pos] != end {
		s.advance(1)
	}
	s.advance(1)
}

func (s *shellScan) dquote(checked bool) {
	for s.pos < len(s.src) {
		c := s.at(0)
		switch {
		case c == '\\':
			s.advance(2)
		case c == '"':
			s.advance(1)
			return
		case c == '`':
			s.advance(1)
			s.skipPast('`')
		case c == '$' && s.at(1) == '(' && s.at(2) == '(':
			s.advance(3)
			s.arith()
		case c == '$' && s.at(1) == '(':
			s.advance(2)
			s.code(checked, false)
		case c == '$' && s.at(1) == '{':
			s.advance(2)
			s.braces()
		default:
			s.advance(1)
		}
	}
}

func (s *shellScan) braces() {
	for depth := 1; s.pos < len(s.src) && depth > 0; {
		switch s.at(0) {
		case '\\':
			s.advance(1)
		case '{':
			depth++
		case '}':
			depth--
		}
		s.advance(1)
	}
}

func (s *shellScan) arith() {
	for depth := 2; s.pos < len(s.src) && depth > 0; s.advance(1) {
		switch s.at(0) {
		case '(':
			depth++
		case ')':
			depth--
		}
	}
}

// readerWords returns the words of the command that starts text, up to its end (a pipe, ;, &, newline or ')').
func readerWords(text string) []string {
	var words []string
	i := 0
	for len(words) < 16 {
		for i < len(text) && (text[i] == ' ' || text[i] == '\t' || text[i] == '\n' && len(words) == 0 ||
			text[i] == '\\' && i+1 < len(text) && text[i+1] == '\n') {
			if text[i] == '\\' {
				i++
			}
			i++
		}
		if i >= len(text) || strings.IndexByte("|;&\n)", text[i]) >= 0 {
			return words
		}
		start := i
		for i < len(text) && strings.IndexByte(" \t|;&\n)", text[i]) < 0 {
			switch text[i] {
			case '\\':
				i += 2
				continue
			case '\'', '"':
				end := strings.IndexByte(text[i+1:], text[i])
				if end < 0 {
					return append(words, text[start:])
				}
				i += end + 2
				continue
			case '$':
				if i+1 < len(text) && (text[i+1] == '(' || text[i+1] == '{') {
					open, closer := text[i+1], byte(')')
					if open == '{' {
						closer = '}'
					}
					depth := 0
					for i++; i < len(text); i++ {
						if text[i] == open {
							depth++
						} else if text[i] == closer {
							depth--
							if depth == 0 {
								break
							}
						}
					}
				}
			}
			i++
		}
		if i > len(text) {
			i = len(text)
		}
		words = append(words, text[start:i])
	}
	return words
}

// earlyExitReader names the reader when the command can exit before reading all of its input, else "".
func earlyExitReader(words []string) string {
	for len(words) > 0 && (words[0] == "{" || words[0] == "!" || envAssignPattern.MatchString(words[0])) {
		words = words[1:]
	}
	if len(words) == 0 {
		return ""
	}
	named := strings.Join(words[:min(len(words), 3)], " ")
	switch words[0] {
	case "head":
		return named
	case "grep", "egrep", "fgrep":
		for _, arg := range words[1:] {
			switch {
			case arg == "--":
				return ""
			case arg == "--quiet" || arg == "--silent" || strings.HasPrefix(arg, "--max-count") ||
				arg == "--files-with-matches" || arg == "--files-without-match":
				return named
			case len(arg) > 1 && arg[0] == '-' && arg[1] != '-' && strings.ContainsAny(arg, "qlLm"):
				return named
			}
		}
	case "sed":
		for _, arg := range words[1:] {
			if (arg[0] == '\'' || arg[0] == '"') && sedQuitPattern.MatchString(strings.Trim(arg, `'"`)) {
				return named
			}
		}
	}
	return ""
}

func earlyExitHitsUnder(t *testing.T, root string, entries []string) []earlyExitHit {
	t.Helper()
	scope := pipefailReaderScope(t, root, entries)
	var hits []earlyExitHit
	for _, name := range sortedKeys(scope) {
		hits = append(hits, earlyExitReadersIn(name, scope[name])...)
	}
	return hits
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func pipefailReaderEntriesUnder(t *testing.T, root string) []string {
	t.Helper()
	cells, err := filepath.Glob(filepath.Join(root, "ci", "*_test.sh"))
	if err != nil {
		t.Fatal(err)
	}
	entries := slices.Clone(pipefailReaderEntries)
	for _, cell := range cells {
		rel, err := filepath.Rel(root, cell)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, filepath.ToSlash(rel))
	}
	return entries
}

func TestCiScriptsPipefailHaveNoEarlyExitReader(t *testing.T) {
	root := repoRoot(t)
	entries := pipefailReaderEntriesUnder(t, root)
	scope := pipefailReaderScope(t, root, entries)
	for _, want := range []string{"ci/check_go.sh", "ci/lib/venue_oracle_registry.sh", "ci/retry_test.sh", "ci/pull_base_image_test.sh"} {
		if _, ok := scope[want]; !ok {
			t.Errorf("pipefail reader guard scans no %s: the scope is wrong (pipefail no longer set, or the source line moved)", want)
		}
	}
	for _, hit := range earlyExitHitsUnder(t, root, entries) {
		t.Errorf("%s:%d pipes into `%s` under pipefail: the reader can exit before the writer ends, the writer then fails (EPIPE or SIGPIPE) and the pipeline reads false at random; use a here-string (grep -Fqx -- \"${key}\" <<<\"${list}\") or read the whole input", hit.file, hit.line, hit.reader)
	}
}

func TestPipefailReaderGuardCatchesPlantedForms(t *testing.T) {
	root := t.TempDir()
	write := func(rel string, lines ...string) {
		t.Helper()
		file := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("ci/plant.sh",
		"#!/usr/bin/env bash",
		"set -euo pipefail",
		`. "${ROOT}/ci/lib/plant_lib.sh"`,
		`printf '%s\n' "${list}" | grep -Fqx -- "${key}" || exit 1`,
		`first="$(printf '%s\n' "${list}" | head -n 1)"`,
		`if printf '%s\n' "${list}" | grep -m1 x; then :; fi`,
		`printf '%s\n' "${list}" | LC_ALL=C grep --quiet x || true`,
		`printf '%s\n' "${list}" \`,
		`  | grep -qx y \`,
		`  || exit 1`,
		`seq 9 | sed -n '2{p;q}'`,
		`file="$(grep -lE x ./*_test.go | head -n1)"`,
		`grep -Fqx -- "${key}" <<<"${list}" || exit 1`,
		`grep -q x "${file}"`,
		`n="$(printf '%s\n' "${list}" | grep -c x || true)"`,
		`printf '%s\n' "${list}" | grep -v x | awk -F'\t' '$1 == "x" { f = 1 } END { exit !f }'`,
		`true || grep -q x <<<"${list}"`,
		`jq -r '.a | select(.q)' "${file}"`,
		`printf 'a | grep -q b\n' "${x#*|}"`,
		`echo "use x | head here: $(printf '%s' "${list}" | wc -l)"`,
		`# printf | grep -q in a comment`,
		`while read -r line; do :; done < <(printf '%s\n' "${list}" | head -n 3)`,
		`case "$1" in -h|--help) ;; esac`,
	)
	write("ci/lib/plant_lib.sh", `printf '%s\n' "${list}" | grep -qx z`)
	write("ci/no_pipefail.sh", "#!/usr/bin/env bash", "set -eu", `printf x | grep -q x`)

	got := map[string]bool{}
	for _, hit := range earlyExitHitsUnder(t, root, []string{"ci/plant.sh", "ci/no_pipefail.sh"}) {
		got[hit.file+":"+strconv.Itoa(hit.line)] = true
	}
	want := map[string]bool{
		"ci/plant.sh:4": true, "ci/plant.sh:5": true, "ci/plant.sh:6": true, "ci/plant.sh:7": true,
		"ci/plant.sh:9": true, "ci/plant.sh:11": true, "ci/plant.sh:12": true, "ci/lib/plant_lib.sh:1": true,
	}
	for hit := range want {
		if !got[hit] {
			t.Errorf("planted early-exit reader not refused: %s (got %v)", hit, got)
		}
	}
	for hit := range got {
		if !want[hit] {
			t.Errorf("an accepted form was refused: %s (a here-string, a file read, a whole-input reader, quoted text, a comment, a process substitution, a case pattern, or a script without pipefail)", hit)
		}
	}
}
