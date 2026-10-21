package docstools

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"
)

// check-code-prerequisites: a docs page listed in docs/code-prerequisite-scope.yml that has a fenced code sample setting
// CLICKHOUSE_URI or POSTGRES_URI must link to getting-started.md. Was scripts/check_code_prerequisites.py. The scope
// file is absent on main, so the gate passes trivially, as the Python did.

func init() {
	register("check-code-prerequisites", codePrereqVerb)
}

const (
	codePrereqDefaultDocsRoot = "docs"
	codePrereqDefaultScope    = "docs/code-prerequisite-scope.yml"
	codePrereqProg            = "check-code-prerequisites"
	codePrereqUsage           = "usage: cigate docs check-code-prerequisites [--docs-root DIR] [--scope FILE]\n"
)

var (
	codePrereqFenceRe = regexp.MustCompile("(?s)```[a-zA-Z]*\n(.*?)```")
	codePrereqLinkRe  = regexp.MustCompile(`\]\([^)]*getting-started\.md[^)]*\)`)
)

func codePrereqVerb(args []string, stdout, stderr io.Writer) int {
	docsRoot, scopePath, rc, done := codePrereqParseArgs(args, stdout, stderr)
	if done {
		return rc
	}
	if docsRoot == "" && scopePath == "" {
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Fprintln(stderr, codePrereqProg+":", err)
			return 1
		}
		docsRoot = cwd + "/" + codePrereqDefaultDocsRoot
		scopePath = cwd + "/" + codePrereqDefaultScope
	}
	scope, rc := codePrereqLoadScope(scopePath, stderr)
	if rc != 0 {
		return rc
	}
	var missing []string
	for _, rel := range scope {
		doc := codePrereqJoin(docsRoot, rel)
		isFile, err := codePrereqIsFile(doc)
		if err != nil {
			kind, msg := a1OSError(err)
			return a1Traceback(stderr, kind, msg)
		}
		if !isFile {
			missing = append(missing, rel+": listed in scope but the page does not exist")
			continue
		}
		text, err := a1ReadText(doc)
		if err != nil {
			return codePrereqReadFail(stderr, err)
		}
		if codePrereqHasCredentialedSample(text) && !codePrereqLinkRe.MatchString(text) {
			missing = append(missing, rel)
		}
	}
	if len(missing) > 0 {
		for _, entry := range missing {
			fmt.Fprintf(stdout, "ERROR: %s\n", entry)
		}
		return 1
	}
	fmt.Fprintln(stdout, "Code prerequisite check passed")
	return 0
}

func codePrereqHasCredentialedSample(text string) bool {
	for _, m := range codePrereqFenceRe.FindAllStringSubmatch(text, -1) {
		if codePrereqHasCredential(m[1]) {
			return true
		}
	}
	return false
}

func codePrereqReadFail(stderr io.Writer, err error) int {
	var de a1DecodeError
	if errors.As(err, &de) {
		return a1ReadFail(stderr, codePrereqProg+":", err)
	}
	kind, msg := a1OSError(err)
	return a1Traceback(stderr, kind, msg)
}

// codePrereqJoin is Path(root) / rel: an absolute rel replaces the root, ".." is not collapsed.
func codePrereqJoin(root, rel string) string {
	if strings.HasPrefix(rel, "/") {
		return a1PathStr(rel)
	}
	r := a1PathStr(root)
	n := a1PathStr(rel)
	if r == "." {
		return n
	}
	return r + "/" + n
}

// codePrereqIsFile is Path.is_file: a missing path, a path through a non-directory, a symlink loop and a NUL are "no";
// any other stat failure is an error.
func codePrereqIsFile(path string) (bool, error) {
	if strings.ContainsRune(path, 0) {
		return false, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENOTDIR) || errors.Is(err, syscall.ELOOP) || errors.Is(err, syscall.EBADF) {
			return false, nil
		}
		return false, err
	}
	return info.Mode().IsRegular(), nil
}

// codePrereqParseArgs parses --docs-root and --scope the argparse way (one stderr line and exit 2 on a usage error,
// no abbreviations). done is true when the verb must return rc now.
func codePrereqParseArgs(args []string, stdout, stderr io.Writer) (docsRoot, scopePath string, rc int, done bool) {
	usageErr := func(msg string) (string, string, int, bool) {
		fmt.Fprintf(stderr, "%s: error: %s\n", codePrereqProg, msg)
		return "", "", 2, true
	}
	root, scope := codePrereqDefaultDocsRoot, codePrereqDefaultScope
	rootSet, scopeSet := false, false
	var unknown []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		name, value, hasValue := a, "", false
		if strings.HasPrefix(a, "--") {
			if eq := strings.IndexByte(a, '='); eq >= 0 {
				name, value, hasValue = a[:eq], a[eq+1:], true
			}
		}
		switch name {
		case "-h", "--help":
			if hasValue {
				return usageErr("argument -h/--help: ignored explicit argument " + strconv.Quote(value))
			}
			fmt.Fprint(stdout, codePrereqUsage)
			return "", "", 0, true
		case "--docs-root", "--scope":
			if !hasValue {
				if i+1 >= len(args) || codePrereqOptionLike(args[i+1]) {
					return usageErr("argument " + name + ": expected one argument")
				}
				i++
				value = args[i]
			}
			if name == "--docs-root" {
				root, rootSet = value, true
			} else {
				scope, scopeSet = value, true
			}
		default:
			unknown = append(unknown, a)
		}
	}
	if len(unknown) > 0 {
		return usageErr("unrecognized arguments: " + strings.Join(unknown, " "))
	}
	if !rootSet && !scopeSet {
		return "", "", 0, false
	}
	if !rootSet {
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Fprintln(stderr, codePrereqProg+":", err)
			return "", "", 1, true
		}
		root = cwd + "/" + root
	}
	if !scopeSet {
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Fprintln(stderr, codePrereqProg+":", err)
			return "", "", 1, true
		}
		scope = cwd + "/" + scope
	}
	return root, scope, 0, false
}

var codePrereqNegativeNumberRe = regexp.MustCompile(`^-\p{Nd}+$|^-\p{Nd}*\.\p{Nd}+$`)

// codePrereqOptionLike is argparse's _parse_optional for an argument that is no known option: only "-", a negative
// number of the form -1 or -.5, and an argument with a space are values.
func codePrereqOptionLike(a string) bool {
	if len(a) < 2 || a[0] != '-' || codePrereqNegativeNumberRe.MatchString(a) || strings.Contains(a, " ") {
		return false
	}
	return true
}

// codePrereqLoadScope is load_scope: a missing or non-regular file is an empty scope; the entries are sorted and
// deduplicated.
func codePrereqLoadScope(path string, stderr io.Writer) ([]string, int) {
	isFile, err := codePrereqIsFile(path)
	if err != nil {
		kind, msg := a1OSError(err)
		return nil, a1Traceback(stderr, kind, msg)
	}
	if !isFile {
		return nil, 0
	}
	text, err := a1ReadText(path)
	if err != nil {
		return nil, codePrereqReadFail(stderr, err)
	}
	display := a1PathStr(path)
	items, kind := codePrereqScopeItems(text)
	switch kind {
	case scopeYAMLError:
		return nil, a1Traceback(stderr, "YAMLError", display+": not loadable by yaml.safe_load")
	case scopeNotList:
		return nil, a1Traceback(stderr, "ValueError", display+" must be a YAML list of relative doc paths")
	}
	seen := map[string]bool{}
	var out []string
	for _, it := range items {
		if !seen[it] {
			seen[it] = true
			out = append(out, it)
		}
	}
	sort.Strings(out)
	return out, 0
}

const (
	scopeOK = iota
	scopeYAMLError
	scopeNotList
)

// codePrereqScopeItems reads the scope file. It accepts only an empty file, or a block sequence at column 0 of plain,
// single-quoted or double-quoted scalars, with whole-line # comments and blank lines. Everything else is refused as a
// load error (scopeYAMLError): this is stricter than PyYAML safe_load, which also accepts tags, anchors, aliases, merge
// keys, flow collections, block scalars, several scalar styles and escapes. An item the PyYAML 1.1 resolvers read as a
// non-string (yes, 5, 1.5, a date, an empty item) is scopeNotList, as in Python.
func codePrereqScopeItems(text string) ([]string, int) {
	for _, r := range text {
		if (r < 0x20 && r != '\n') || r == 0x7f || r == 0x85 || r == 0x2028 || r == 0x2029 || r == 0xfeff || r == 0xfffe || r == 0xffff {
			return nil, scopeYAMLError
		}
	}
	var items []string
	notList := false
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimLeft(line, " ")
		if trimmed == "" || trimmed[0] == '#' {
			continue
		}
		if line[0] == ' ' {
			return nil, scopeYAMLError
		}
		if line == "-" {
			notList = true
			continue
		}
		if !strings.HasPrefix(line, "- ") {
			return nil, scopeYAMLError
		}
		rest := strings.TrimRight(strings.TrimLeft(line[2:], " "), " ")
		if rest == "" {
			notList = true
			continue
		}
		value, kind := codePrereqItem(rest)
		switch kind {
		case scopeYAMLError:
			return nil, scopeYAMLError
		case scopeNotList:
			notList = true
		default:
			items = append(items, value)
		}
	}
	if notList {
		return nil, scopeNotList
	}
	return items, scopeOK
}

// codePrereqItem parses one non-empty item text of a block sequence line.
func codePrereqItem(rest string) (string, int) {
	switch rest[0] {
	case '\'':
		inner, ok := strings.CutSuffix(rest[1:], "'")
		if !ok || len(rest) < 2 || strings.Contains(strings.ReplaceAll(inner, "''", ""), "'") {
			return "", scopeYAMLError
		}
		return strings.ReplaceAll(inner, "''", "'"), scopeOK
	case '"':
		inner, ok := strings.CutSuffix(rest[1:], "\"")
		if !ok || len(rest) < 2 || strings.ContainsAny(inner, "\"\\") {
			return "", scopeYAMLError
		}
		return inner, scopeOK
	case ',', '[', ']', '{', '}', '#', '&', '*', '!', '|', '>', '%', '@', '`', '?', ':':
		return "", scopeYAMLError
	}
	if strings.HasPrefix(rest, "- ") || rest == "-" || strings.Contains(rest, ": ") || strings.HasSuffix(rest, ":") || strings.Contains(rest, " #") {
		return "", scopeYAMLError
	}
	if rest == "<<" || rest == "=" {
		return "", scopeYAMLError
	}
	if pyBoolRe.MatchString(rest) || pyFloatRe.MatchString(rest) || pyIntRe.MatchString(rest) || pyNullRe.MatchString(rest) || pyTimeRe.MatchString(rest) {
		return "", scopeNotList
	}
	return rest, scopeOK
}

// PyYAML 1.1 implicit resolvers of the safe loader: a plain scalar they match is not a str.
var (
	pyBoolRe  = regexp.MustCompile(`^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$`)
	pyFloatRe = regexp.MustCompile(`^(?:[-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+][0-9]+)?|\.[0-9][0-9_]*(?:[eE][-+][0-9]+)?|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$`)
	pyIntRe   = regexp.MustCompile(`^(?:[-+]?0b[0-1_]+|[-+]?0[0-7_]+|[-+]?(?:0|[1-9][0-9_]*)|[-+]?0x[0-9a-fA-F_]+|[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)$`)
	pyNullRe  = regexp.MustCompile(`^(?:~|null|Null|NULL|)$`)
	pyTimeRe  = regexp.MustCompile(`^(?:[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?(?:[Tt]|[ \t]+)[0-9][0-9]?:[0-9][0-9]:[0-9][0-9](?:\.[0-9]*)?(?:[ \t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?)$`)
)

// codePrereqIsWord is Python's \w for the \b of the credential pattern.
func codePrereqIsWord(r rune) bool {
	if !(unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_') {
		return false
	}
	for _, rg := range codePrereqGoOnlyWord {
		if r >= rg[0] && r <= rg[1] {
			return false
		}
	}
	return true
}

// codePrereqHasCredential is CREDENTIAL_ENV_RE.search: NAME= at a word boundary, for CLICKHOUSE_URI and POSTGRES_URI.
func codePrereqHasCredential(block string) bool {
	for _, name := range []string{"CLICKHOUSE_URI=", "POSTGRES_URI="} {
		for off := 0; ; {
			i := strings.Index(block[off:], name)
			if i < 0 {
				break
			}
			at := off + i
			if at == 0 {
				return true
			}
			if r, _ := utf8.DecodeLastRuneInString(block[:at]); !codePrereqIsWord(r) {
				return true
			}
			off = at + 1
		}
	}
	return false
}
