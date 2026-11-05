//go:build !race

package integrationshards_test

import (
	"fmt"
	"strings"
	"testing"
)

const utf8Prelude = `
export LC_ALL=C.UTF-8
probe='Ö'
[ "${#probe}" -eq 1 ] || { echo "no UTF-8 locale: the byte count cannot be told from the character count" >&2; exit 97; }
`

func repeatedRuneTest(pkg, rune string, n int) map[string]string {
	return map[string]string{
		pkg + "/tag_test.go": "//go:build integration\n\npackage " + pkg + "\n\nimport \"testing\"\n\nfunc Test" + strings.Repeat(rune, n) + "(t *testing.T) {}\n",
	}
}

func mergeFiles(parts ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, p := range parts {
		for k, v := range p {
			out[k] = v
		}
	}
	return out
}

// The -run budget is in bytes (Linux refuses one argument over 128 KiB), so a name of two-byte letters weighs two per
// letter even where the shell counts characters (CHAOS-11483).
func TestTheRunBudgetCountsBytesNotCharactersUnderAUTF8Locale(t *testing.T) {
	t.Parallel()
	t.Run("two packages that fit by character count and not by byte count open a second group", func(t *testing.T) {
		t.Parallel()
		files := mergeFiles(repeatedRuneTest("p1", "Ö", 20000), repeatedRuneTest("p2", "Ä", 20000))
		got := runDiscoveryFixture(t, files, utf8Prelude+"run_integration_packages --dry-run . fixture ./p1 ./p2\n")
		if g := lineValues(got, "  SHARD-GROUP . "); len(g) != 2 {
			t.Errorf("%d go test group(s) %v, want 2: p1 and p2 are 80014 bytes together, over the 60000-byte budget", len(g), g)
		}
		if !strings.Contains(got, "  SHARD-SPLIT . 2 p2 1 budget 80014\n") {
			t.Errorf("no budget split of p2 at 80014 bytes:\n%.200s", got)
		}
	})
	t.Run("one package over the budget by byte count alone fails by name", func(t *testing.T) {
		t.Parallel()
		res := discoveryHarness(t, repeatedRuneTest("big", "Ö", 31000), "", utf8Prelude+"run_integration_packages --dry-run . fixture ./big\n")
		if res.code == 0 || !strings.Contains(res.stderr, "big") || !strings.Contains(res.stderr, "budget") {
			t.Errorf("exit %d stderr %.300q: want a failure that names big and the budget", res.code, res.stderr)
		}
	})
}

// The shard runs every group; a failing group does not stop the next one, and its status is the shard's status.
func TestAShardRunsEveryGroupAfterARedGroupAndKeepsTheFirstFailure(t *testing.T) {
	t.Parallel()
	files := map[string]string{
		"a/plain_test.go": "package a\n\nimport \"testing\"\n\nfunc TestA(t *testing.T) {}\n",
		"a/tag_test.go":   "//go:build integration\n\npackage a\n\nimport \"testing\"\n\nfunc TestShared(t *testing.T) {}\n",
		"b/plain_test.go": "package b\n\nimport \"testing\"\n\nfunc TestShared(t *testing.T) {}\n",
		"b/tag_test.go":   "//go:build integration\n\npackage b\n\nimport \"testing\"\n\nfunc TestB(t *testing.T) {}\n",
	}
	for _, c := range []struct {
		name     string
		rcs      string
		wantRC   int
		wantRuns int
	}{
		{"red then green", "3 0", 3, 2},
		{"green then red", "0 3", 3, 2},
		{"red then another red keeps the first", "3 5", 3, 2},
		{"green then green", "0 0", 0, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := runDiscoveryFixture(t, files, fmt.Sprintf(`
rcs=(%s)
n=0
run_integration_go_test() { printf 'GOTEST %%s\n' "$*" >> calls.txt; n=$((n + 1)); return "${rcs[$((n - 1))]}"; }
rc=0
run_integration_packages run . fixture ./a ./b > receipts.txt || rc=$?
printf 'RC %%s\n' "${rc}"
sed 's/^/CALL /' calls.txt
grep -c '^  SHARD-GROUP ' receipts.txt | sed 's/^/GROUPS /'
`, c.rcs))
			if g := lineValues(got, "GROUPS "); len(g) != 1 || g[0] != "2" {
				t.Fatalf("the fixture plans %v group(s), want 2: the case measures nothing\n%s", g, got)
			}
			if rc := lineValues(got, "RC "); len(rc) != 1 || rc[0] != fmt.Sprint(c.wantRC) {
				t.Errorf("run_integration_packages status %v, want %d", rc, c.wantRC)
			}
			if calls := lineValues(got, "CALL GOTEST "); len(calls) != c.wantRuns {
				t.Errorf("%d go test call(s) %v, want %d: a red group must not skip the next group", len(calls), calls, c.wantRuns)
			}
		})
	}
}

// A tagged-only file's Example (with an output comment) and Fuzz target are go test names the shard -run regex must
// carry: go test -list under the regex returns exactly them (CHAOS-11483 item 1).
func TestATaggedOnlyExampleAndFuzzTargetAreSelectedByTheShardRegex(t *testing.T) {
	t.Parallel()
	files := map[string]string{
		"p/plain_test.go": "package p\n\nimport \"testing\"\n\nfunc TestPlain(t *testing.T) {}\n",
		"p/tag_test.go": "//go:build integration\n\npackage p\n\nimport (\n\t\"fmt\"\n\t\"testing\"\n)\n\nfunc TestTagged(t *testing.T) {}\n\n" +
			"func FuzzTagged(f *testing.F) { f.Fuzz(func(t *testing.T, s string) {}) }\n\n" +
			"func Example_tagged() {\n\tfmt.Println(\"t\")\n\t// Output: t\n}\n\nfunc Example_tagNoOutput() {\n\tfmt.Println(\"never run\")\n}\n",
		"p/ext_test.go": "//go:build integration\n\npackage p_test\n\nimport \"fmt\"\n\nfunc Example_externalTagged() {\n\tfmt.Println(\"x\")\n\t// Output: x\n}\n",
	}
	got := runDiscoveryFixture(t, files, "run_integration_packages --dry-run . fixture ./p > receipts.txt\ncat receipts.txt\n"+groupListBody)
	want := map[string]bool{"p TestTagged": true, "p FuzzTagged": true, "p Example_tagged": true, "p Example_externalTagged": true}
	selected, listed := map[string]bool{}, map[string]bool{}
	for _, v := range lineValues(got, "  SHARD-TEST ") {
		selected[v] = true
	}
	for _, v := range lineValues(got, "LISTEDG ") {
		listed[v] = true
	}
	for name := range want {
		if !selected[name] || !listed[name] {
			t.Errorf("%s: selected %v, run by the group regex %v", name, selected[name], listed[name])
		}
	}
	for name := range selected {
		if !want[name] {
			t.Errorf("%s is selected but is no tagged-only test, fuzz target or example with output", name)
		}
	}
	for name := range listed {
		if !want[name] {
			t.Errorf("the group regex runs %s, which is not a tagged-only name", name)
		}
	}
}
