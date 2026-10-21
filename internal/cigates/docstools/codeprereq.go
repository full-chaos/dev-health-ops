package docstools

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"gopkg.in/yaml.v3"
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
	// Python \b: a word character is a Unicode letter, number or underscore.
	codePrereqCredentialRe = regexp.MustCompile(`(?:^|[^\p{L}\p{N}_])(?:CLICKHOUSE_URI|POSTGRES_URI)=`)
	codePrereqLinkRe       = regexp.MustCompile(`\]\([^)]*getting-started\.md[^)]*\)`)
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
		if codePrereqCredentialRe.MatchString(m[1]) {
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

// codePrereqScopeItems is yaml.safe_load(text) or []: the list of strings, or why it is not one.
func codePrereqScopeItems(text string) ([]string, int) {
	dec := yaml.NewDecoder(strings.NewReader(text))
	var doc yaml.Node
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, scopeOK
		}
		return nil, scopeYAMLError
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, scopeYAMLError
	}
	root := codePrereqResolve(doc.Content[0])
	falsy, class := codePrereqFalsy(root)
	if class == scopeYAMLError {
		return nil, scopeYAMLError
	}
	if falsy {
		return nil, scopeOK
	}
	if root.Kind != yaml.SequenceNode {
		if codePrereqHasUnsafeTag(root) {
			return nil, scopeYAMLError
		}
		return nil, scopeNotList
	}
	if codePrereqHasUnsafeTag(root) {
		return nil, scopeYAMLError
	}
	var items []string
	notList := false
	for _, n := range root.Content {
		n = codePrereqResolve(n)
		if n.Kind == yaml.ScalarNode && codePrereqIsString(n) {
			items = append(items, n.Value)
		} else {
			notList = true
		}
	}
	if notList {
		return nil, scopeNotList
	}
	return items, scopeOK
}

func codePrereqResolve(n *yaml.Node) *yaml.Node {
	for n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	return n
}

var codePrereqSafeTags = map[string]bool{
	"!!str": true, "!!int": true, "!!float": true, "!!bool": true, "!!null": true, "!!timestamp": true, "!!binary": true,
	"!!seq": true, "!!map": true, "!!set": true, "!!omap": true, "!!pairs": true,
}

// codePrereqHasUnsafeTag reports a node yaml.safe_load refuses: a custom or !!python tag, a safe tag on a node of the
// wrong kind (a mapping tag on a sequence), and a plain scalar the safe constructors have no constructor for ("<<", "=").
// Aliases may form a cycle, so each node is visited once.
func codePrereqHasUnsafeTag(n *yaml.Node) bool {
	return codePrereqUnsafe(n, map[*yaml.Node]bool{})
}

func codePrereqUnsafe(n *yaml.Node, seen map[*yaml.Node]bool) bool {
	n = codePrereqResolve(n)
	if seen[n] {
		return false
	}
	seen[n] = true
	if n.Style&yaml.TaggedStyle != 0 {
		if !codePrereqSafeTags[n.Tag] || !codePrereqTagFitsKind(n) {
			return true
		}
	}
	if n.Kind == yaml.ScalarNode && n.Style&yaml.TaggedStyle == 0 && codePrereqPlain(n) && (n.Value == "<<" || n.Value == "=") {
		return true
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			if codePrereqIsMergeKey(k) {
				if !codePrereqMergeable(v) || codePrereqUnsafe(v, seen) {
					return true
				}
				continue
			}
			if codePrereqUnsafe(k, seen) || codePrereqUnsafe(v, seen) {
				return true
			}
		}
		return false
	}
	for _, c := range n.Content {
		if codePrereqUnsafe(c, seen) {
			return true
		}
	}
	return false
}

func codePrereqIsMergeKey(k *yaml.Node) bool {
	k = codePrereqResolve(k)
	return k.Kind == yaml.ScalarNode && k.Value == "<<" && codePrereqPlain(k) && k.Style&yaml.TaggedStyle == 0
}

// codePrereqMergeable is flatten_mapping's test of a merge value: a mapping, or a sequence of mappings.
func codePrereqMergeable(v *yaml.Node) bool {
	v = codePrereqResolve(v)
	if v.Kind == yaml.MappingNode {
		return true
	}
	if v.Kind != yaml.SequenceNode {
		return false
	}
	for _, c := range v.Content {
		if codePrereqResolve(c).Kind != yaml.MappingNode {
			return false
		}
	}
	return true
}

// codePrereqMappingEmpty is whether the constructed dict has no key: merge keys contribute the keys of their mappings.
func codePrereqMappingEmpty(n *yaml.Node, depth int) bool {
	n = codePrereqResolve(n)
	if depth > 64 {
		return false
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if !codePrereqIsMergeKey(n.Content[i]) {
			return false
		}
		v := codePrereqResolve(n.Content[i+1])
		parts := []*yaml.Node{v}
		if v.Kind == yaml.SequenceNode {
			parts = v.Content
		}
		for _, p := range parts {
			if !codePrereqMappingEmpty(p, depth+1) {
				return false
			}
		}
	}
	return true
}

// codePrereqTagFitsKind is the node-kind check of the PyYAML safe constructors for an explicit tag.
func codePrereqTagFitsKind(n *yaml.Node) bool {
	switch n.Tag {
	case "!!seq":
		return n.Kind == yaml.SequenceNode
	case "!!map", "!!set":
		return n.Kind == yaml.MappingNode
	case "!!omap", "!!pairs":
		if n.Kind != yaml.SequenceNode {
			return false
		}
		for _, c := range n.Content {
			c = codePrereqResolve(c)
			if c.Kind != yaml.MappingNode || len(c.Content) != 2 {
				return false
			}
		}
		return true
	}
	return n.Kind == yaml.ScalarNode
}

func codePrereqPlain(n *yaml.Node) bool {
	return n.Style&(yaml.DoubleQuotedStyle|yaml.SingleQuotedStyle|yaml.LiteralStyle|yaml.FoldedStyle) == 0
}

// PyYAML 1.1 implicit resolvers of the safe loader.
var (
	pyBoolRe  = regexp.MustCompile(`^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$`)
	pyFloatRe = regexp.MustCompile(`^(?:[-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+][0-9]+)?|\.[0-9][0-9_]*(?:[eE][-+][0-9]+)?|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$`)
	pyIntRe   = regexp.MustCompile(`^(?:[-+]?0b[0-1_]+|[-+]?0[0-7_]+|[-+]?(?:0|[1-9][0-9_]*)|[-+]?0x[0-9a-fA-F_]+|[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)$`)
	pyNullRe  = regexp.MustCompile(`^(?:~|null|Null|NULL|)$`)
	pyTimeRe  = regexp.MustCompile(`^(?:[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?(?:[Tt]|[ \t]+)[0-9][0-9]?:[0-9][0-9]:[0-9][0-9](?:\.[0-9]*)?(?:[ \t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?)$`)
)

// codePrereqIsString is true when the safe loader constructs a str for the scalar.
func codePrereqIsString(n *yaml.Node) bool {
	if n.Style&yaml.TaggedStyle != 0 {
		return n.Tag == "!!str"
	}
	if !codePrereqPlain(n) {
		return true
	}
	v := n.Value
	return !(pyBoolRe.MatchString(v) || pyFloatRe.MatchString(v) || pyIntRe.MatchString(v) || pyNullRe.MatchString(v) || pyTimeRe.MatchString(v))
}

// codePrereqFalsy is `value or []`: whether the loaded root is falsy. class is scopeYAMLError when the root cannot be
// loaded at all.
func codePrereqFalsy(n *yaml.Node) (bool, int) {
	if codePrereqHasUnsafeTag(n) {
		return false, scopeYAMLError
	}
	switch n.Kind {
	case yaml.SequenceNode:
		return len(n.Content) == 0, scopeOK
	case yaml.MappingNode:
		return codePrereqMappingEmpty(n, 0), scopeOK
	case yaml.ScalarNode:
		return codePrereqScalarFalsy(n), scopeOK
	}
	return false, scopeOK
}

func codePrereqScalarFalsy(n *yaml.Node) bool {
	v := n.Value
	tag := ""
	if n.Style&yaml.TaggedStyle != 0 {
		tag = n.Tag
	} else if codePrereqPlain(n) {
		switch {
		case pyNullRe.MatchString(v):
			tag = "!!null"
		case pyBoolRe.MatchString(v):
			tag = "!!bool"
		case pyIntRe.MatchString(v):
			tag = "!!int"
		case pyFloatRe.MatchString(v):
			tag = "!!float"
		}
	}
	switch tag {
	case "":
		return v == ""
	case "!!str":
		return v == ""
	case "!!null":
		return true
	case "!!binary":
		return strings.TrimSpace(v) == ""
	case "!!bool":
		low := strings.ToLower(v)
		return low == "no" || low == "false" || low == "off"
	case "!!int":
		return codePrereqIntZero(v)
	case "!!float":
		return codePrereqFloatZero(v)
	}
	return false
}

func codePrereqIntZero(v string) bool {
	s := strings.ReplaceAll(v, "_", "")
	s = strings.TrimLeft(s, "+-")
	if strings.Contains(s, ":") {
		return false
	}
	base := 10
	switch {
	case strings.HasPrefix(s, "0b"):
		s, base = s[2:], 2
	case strings.HasPrefix(s, "0x"):
		s, base = s[2:], 16
	case len(s) > 1 && s[0] == '0':
		s, base = s[1:], 8
	}
	z, ok := new(big.Int).SetString(s, base)
	return ok && z.Sign() == 0
}

func codePrereqFloatZero(v string) bool {
	s := strings.ReplaceAll(v, "_", "")
	s = strings.TrimLeft(s, "+-")
	if strings.Contains(s, ":") {
		total := 0.0
		for _, part := range strings.Split(s, ":") {
			f, err := strconv.ParseFloat(part, 64)
			if err != nil {
				return false
			}
			total = total*60 + f
		}
		return total == 0
	}
	f, err := strconv.ParseFloat(s, 64)
	return err == nil && f == 0
}
