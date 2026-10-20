package cigates

import (
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

var authSegment = regexp.MustCompile(`^(auth|audit)`)

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

// AuthScopeCandidates reads `go list -json` objects from in and prints the auth-looking package paths of module.
func AuthScopeCandidates(module string, in io.Reader, stdout, stderr io.Writer) int {
	pkgs, err := jsonStream(in)
	if err != nil {
		fmt.Fprintf(stderr, "auth scope: unreadable go list output: %v\n", err)
		return 1
	}
	adapter := module + "/internal/storage/postgres/authschema"
	found := map[string]bool{}
	seen := 0
	for _, p := range pkgs {
		path, ok := p["ImportPath"].(string)
		if !ok {
			fmt.Fprintln(stderr, "auth scope: package without ImportPath")
			return 1
		}
		if !strings.HasPrefix(path, module) {
			continue
		}
		seen++
		relative := strings.TrimLeft(path[len(module):], "/")
		imports := append(append(stringList(p, "Imports"), stringList(p, "TestImports")...), stringList(p, "XTestImports")...)
		match := false
		for _, seg := range strings.Split(relative, "/") {
			if seg != "" && authSegment.MatchString(seg) {
				match = true
			}
		}
		for _, imp := range imports {
			if imp == adapter {
				match = true
			}
		}
		if match {
			found[path] = true
		}
	}
	if seen == 0 {
		fmt.Fprintln(stderr, "auth scope: go list reported no package of the module")
		return 1
	}
	paths := make([]string, 0, len(found))
	for p := range found {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		fmt.Fprintln(stdout, p)
	}
	return 0
}
