package affected_test

import (
	"fmt"
	"strings"
	"testing"
)

// CHAOS-10440: moduleroot.Caller(0) of the module's own internal/testsupport/moduleroot package is runtime.Caller(0) one
// frame deeper, so it names the calling file as runtime.Caller(0) does. A read built on it is a read below the package
// directory. Another skip, a variable skip, a package of the same name at another path and another function name keep
// their dynreads row.
const (
	mrStub  = "package moduleroot\n\nfunc Caller(skip int) (pc uintptr, file string, line int, ok bool) { return }\n"
	mrOther = "package moduleroot\n\nfunc Caller(skip int) (pc uintptr, file string, line int, ok bool) { return }\n\nfunc Other(skip int) (pc uintptr, file string, line int, ok bool) { return }\n"
	mrSrc   = "package x\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n\n\t\"%s\"\n)\n\nfunc read(%s) ([]byte, error) {\n\t_, file, _, _ := moduleroot.%s(%s)\n\treturn os.ReadFile(filepath.Join(filepath.Dir(file), \"testdata\", \"a.txt\"))\n}\n"
	mrOwn   = "m/internal/testsupport/moduleroot"
)

func mrFile(pkg, params, fn, skip string) string { return fmt.Sprintf(mrSrc, pkg, params, fn, skip) }

func TestNonGoPlanModulerootCallerSkipZeroIsTheOwnFile(t *testing.T) {
	own := map[string]string{"internal/testsupport/moduleroot/moduleroot.go": mrStub, "x/x_own_test.go": mrFile(mrOwn, "", "Caller", "0")}
	runNongo(t, []nongoCase{
		{name: "skip_zero_needs_no_dynreads_row", head: own, changed: []string{"docs/a.md"}, want: "AFFECTED 1", keys: []string{"a"}},
	})
}

func TestNonGoPlanModulerootCallerNearMissesKeepTheirRow(t *testing.T) {
	miss := func(name string, head map[string]string) nongoCase {
		return nongoCase{name: name, head: head, changed: []string{"docs/a.md"}, want: "FULL *", readsCode: 1, readsWant: []string{"x\tx_own_test.go"}}
	}
	runNongo(t, []nongoCase{
		miss("skip_one", map[string]string{"internal/testsupport/moduleroot/moduleroot.go": mrStub, "x/x_own_test.go": mrFile(mrOwn, "", "Caller", "1")}),
		miss("variable_skip", map[string]string{"internal/testsupport/moduleroot/moduleroot.go": mrStub, "x/x_own_test.go": mrFile(mrOwn, "n int", "Caller", "n")}),
		miss("package_of_the_same_name_at_another_path", map[string]string{"y/moduleroot/moduleroot.go": strings.Replace(mrStub, "moduleroot", "moduleroot", 1), "x/x_own_test.go": mrFile("m/y/moduleroot", "", "Caller", "0")}),
		miss("another_function_of_the_package", map[string]string{"internal/testsupport/moduleroot/moduleroot.go": mrOther, "x/x_own_test.go": mrFile(mrOwn, "", "Other", "0")}),
	})
}
