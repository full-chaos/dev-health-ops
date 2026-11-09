package cigates

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

var authSegment = regexp.MustCompile(`^(auth|audit)`)

// Why a package is an auth scope candidate.
const (
	SourceSegment    = "segment"    // a path segment starting with auth or audit
	SourceImport     = "import"     // the auth schema adapter in Imports (non-test files)
	SourceTestImport = "testimport" // the auth schema adapter in TestImports or XTestImports
)

func stringList(m map[string]any, key string) []string {
	list, _ := m[key].([]any)
	out := make([]string, 0, len(list))
	for _, v := range list {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func authScopeSources(module string, in io.Reader, stderr io.Writer) (map[string]map[string]bool, int) {
	pkgs, err := jsonStream(in)
	if err != nil {
		fmt.Fprintf(stderr, "auth scope: unreadable go list output: %v\n", err)
		return nil, 1
	}
	adapter := module + "/internal/storage/postgres/authschema"
	found := map[string]map[string]bool{}
	add := func(path, source string) {
		if found[path] == nil {
			found[path] = map[string]bool{}
		}
		found[path][source] = true
	}
	seen := 0
	for _, p := range pkgs {
		path, ok := p["ImportPath"].(string)
		if !ok {
			fmt.Fprintln(stderr, "auth scope: package without ImportPath")
			return nil, 1
		}
		if !strings.HasPrefix(path, module) {
			continue
		}
		seen++
		relative := strings.TrimLeft(path[len(module):], "/")
		for _, seg := range strings.Split(relative, "/") {
			if seg != "" && authSegment.MatchString(seg) {
				add(path, SourceSegment)
			}
		}
		for _, imp := range stringList(p, "Imports") {
			if imp == adapter {
				add(path, SourceImport)
			}
		}
		for _, imp := range append(stringList(p, "TestImports"), stringList(p, "XTestImports")...) {
			if imp == adapter {
				add(path, SourceTestImport)
			}
		}
	}
	if seen == 0 {
		fmt.Fprintln(stderr, "auth scope: go list reported no package of the module")
		return nil, 1
	}
	return found, 0
}

func sortedKeys(m map[string]map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// AuthScopeCandidates reads `go list -json` objects from in and prints the auth-looking package paths of module.
func AuthScopeCandidates(module string, in io.Reader, stdout, stderr io.Writer) int {
	found, rc := authScopeSources(module, in, stderr)
	if rc != 0 {
		return rc
	}
	for _, p := range sortedKeys(found) {
		fmt.Fprintln(stdout, p)
	}
	return 0
}

// AuthScopeCandidateSources is AuthScopeCandidates with one "path<TAB>source" line per reason.
func AuthScopeCandidateSources(module string, in io.Reader, stdout, stderr io.Writer) int {
	found, rc := authScopeSources(module, in, stderr)
	if rc != 0 {
		return rc
	}
	for _, p := range sortedKeys(found) {
		for _, s := range []string{SourceSegment, SourceImport, SourceTestImport} {
			if found[p][s] {
				fmt.Fprintf(stdout, "%s\t%s\n", p, s)
			}
		}
	}
	return 0
}

func nonBlankLines(r io.Reader) ([]string, error) {
	var out []string
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		if line := strings.TrimRight(sc.Text(), "\r"); strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out, sc.Err()
}

// Exemption classes of ci/auth_gate_scope_exempt.txt (package<TAB>class<TAB>reason).
const (
	ExemptTestOnly = "test-only" // the auth schema adapter may appear in the package's tests only
	ExemptPackage  = "package"   // the whole package is exempt; the reason says why
)

type scopeExemption struct {
	pkg, class, reason string
	tabbed             bool
}

// AuthScopeVerdict is the auth scope selftest's decision. gated holds the gate's package paths, sources the
// "path<TAB>source" lines of every discovery pass (duplicates allowed), exempt the ci/auth_gate_scope_exempt.txt rows.
// It returns 0 and prints the count line when every candidate is gated or exempt, else 2.
func AuthScopeVerdict(gated, sources, exempt io.Reader, stdout, stderr io.Writer) int {
	fail := func(format string, a ...any) int {
		fmt.Fprintf(stderr, "auth scope: "+format+"\n", a...)
		return 2
	}
	gatedLines, err := nonBlankLines(gated)
	if err != nil {
		return fail("unreadable gate package list: %v", err)
	}
	if len(gatedLines) == 0 {
		return fail("the gate selects no package")
	}
	gatedSet := map[string]bool{}
	for _, l := range gatedLines {
		gatedSet[strings.TrimSpace(l)] = true
	}
	sourceLines, err := nonBlankLines(sources)
	if err != nil {
		return fail("unreadable candidate sources: %v", err)
	}
	if len(sourceLines) == 0 {
		return fail("no candidate package found; the selftest would measure nothing")
	}
	candidates := map[string]map[string]bool{}
	for _, l := range sourceLines {
		path, source, ok := strings.Cut(l, "\t")
		switch {
		case !ok || path == "":
			return fail("malformed candidate source line %q (want path<TAB>source)", l)
		case source != SourceSegment && source != SourceImport && source != SourceTestImport:
			return fail("unknown candidate source %q for %s", source, path)
		}
		if candidates[path] == nil {
			candidates[path] = map[string]bool{}
		}
		candidates[path][source] = true
	}
	exemptLines, err := nonBlankLines(exempt)
	if err != nil {
		return fail("unreadable exemption file: %v", err)
	}
	var rows []scopeExemption
	exempted := map[string]bool{}
	for _, l := range exemptLines {
		pkg, rest, tabbed := strings.Cut(l, "\t")
		if strings.HasPrefix(pkg, "#") {
			continue
		}
		class, reason, _ := strings.Cut(rest, "\t")
		rows = append(rows, scopeExemption{pkg: pkg, class: class, reason: reason, tabbed: tabbed})
		if tabbed {
			exempted[pkg] = true
		}
	}
	var missing []string
	for _, p := range sortedKeys(candidates) {
		if !gatedSet[p] && !exempted[p] {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		return fail("package(s) look like auth or import the auth schema but are neither in the gate nor exempt with a reason in ci/auth_gate_scope_exempt.txt: %s", strings.Join(missing, " "))
	}
	var classless []string
	for _, r := range rows {
		if r.class != ExemptTestOnly && r.class != ExemptPackage {
			classless = append(classless, r.pkg)
		}
	}
	if len(classless) > 0 {
		return fail("exemption row(s) with no or unknown class (want package<TAB>%s|%s<TAB>reason): %s", ExemptTestOnly, ExemptPackage, strings.Join(classless, " "))
	}
	var stale []string
	for _, r := range rows {
		if strings.TrimSpace(r.reason) == "" {
			stale = append(stale, r.pkg+"(no reason)")
		}
		if gatedSet[r.pkg] || candidates[r.pkg] == nil {
			stale = append(stale, r.pkg)
		}
	}
	if len(stale) > 0 {
		return fail("stale or reasonless exemption(s) (gated, no longer a candidate, or no reason): %s", strings.Join(stale, " "))
	}
	var nonTest, authPath, overBroad []string
	for _, r := range rows {
		src := candidates[r.pkg]
		switch {
		case r.class == ExemptTestOnly && src[SourceImport]:
			nonTest = append(nonTest, r.pkg)
		case r.class == ExemptTestOnly && src[SourceSegment]:
			authPath = append(authPath, r.pkg)
		case r.class == ExemptPackage && len(src) == 1 && src[SourceTestImport]:
			overBroad = append(overBroad, r.pkg)
		}
	}
	if len(nonTest) > 0 {
		return fail("test-only exemption(s) with a non-test auth schema import: %s", strings.Join(nonTest, " "))
	}
	if len(authPath) > 0 {
		return fail("test-only exemption(s) on an auth-looking path: %s", strings.Join(authPath, " "))
	}
	if len(overBroad) > 0 {
		return fail("over-broad exemption(s) (test imports only; use test-only): %s", strings.Join(overBroad, " "))
	}
	fmt.Fprintf(stdout, "auth scope: %d candidate packages, all gated or exempt\n", len(candidates))
	return 0
}
