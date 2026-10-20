package routing_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cigates/routing"
)

// A recursive YAML alias is refused with one stderr line naming the alias and its anchor, exit 1, nothing on stdout and
// no file written. PyYAML builds a cyclic value there and the Python scripts went on; refusing is an intended
// divergence (these tools read the repository's own values files, where a recursive alias is a defect).
//
// The rows run in a child test process with a small stack and a deadline: a loader that expands aliases without
// cycle detection overflows its stack, and that must fail this test, not kill the test binary.

const aliasChildEnv = "ROUTING_ALIAS_CHILD_CASE"

// The reviewer's exact input: a recursive alias in a field the generator never reads.
const reviewerInput = "unused: &unused\n  - *unused\ningress:\n  goApiPaths:\n    - path: /api/v1/items\n  queryApiPaths:\n    - path: /api/v1/graphql\n      pathType: Exact\n"

type aliasCase struct {
	name, tool, values, want string
}

var aliasCases = []aliasCase{
	{"reviewer input, plane-split-router", "plane-split-router", reviewerInput,
		"yaml: recursive alias *unused (line 2) inside its own anchor &unused (line 1) is refused\n"},
	{"mapping that names itself, query-api-flags with --out", "query-api-flags",
		"ops:\n  queryApi:\n    extraEnv:\n      - name: GO_API_A_ENABLED\n        value: \"true\"\n      - name: S\n        valueFrom: {secretKeyRef: {key: K}}\nm: &m\n  self: *m\n",
		"yaml: recursive alias *m (line 9) inside its own anchor &m (line 8) is refused\n"},
	{"merge key that names its own mapping", "route-coverage",
		"a: &a\n  x: 1\n  <<: *a\n",
		"yaml: recursive alias *a (line 3) inside its own anchor &a (line 1) is refused\n"},
	{"merge list that names its own mapping", "web-env-parity",
		"a: &a\n  x: 1\n  <<: [*a]\n",
		"yaml: recursive alias *a (line 3) inside its own anchor &a (line 1) is refused\n"},
	{"list inside a mapping that names the mapping", "plane-split-router",
		"ingress: &i\n  goApiPaths: [*i]\n  queryApiPaths: []\n",
		"yaml: recursive alias *i (line 2) inside its own anchor &i (line 1) is refused\n"},
}

type aliasResult struct {
	Code           int
	Stdout, Stderr string
	Written        bool
}

func runAliasCase(t *testing.T, c aliasCase) aliasResult {
	dir := t.TempDir()
	values := writeFile(t, dir, "values.yaml", []byte(c.values))
	out := filepath.Join(dir, "out")
	var args []string
	tool := map[string]func([]string, routing.Deps, *bytes.Buffer, *bytes.Buffer) int{
		"plane-split-router": gen, "query-api-flags": flagsTool, "route-coverage": coverage, "web-env-parity": webEnv,
	}[c.tool]
	switch c.tool {
	case "query-api-flags":
		args = []string{values, "--out", out}
	case "web-env-parity":
		args = []string{values, writeFile(t, dir, "compose.yml", []byte("services: {}\n"))}
	case "route-coverage":
		args = []string{values, "--go-api-url", "http://127.0.0.1:1", "--query-api-url", "http://127.0.0.1:1"}
	default:
		args = []string{values, "--format", "dynamic"}
	}
	r := run(tool, args...)
	_, err := os.Stat(out)
	return aliasResult{r.code, r.stdout, r.stderr, err == nil}
}

func TestARecursiveAliasIsRefusedWithOneLine(t *testing.T) {
	if name := os.Getenv(aliasChildEnv); name != "" {
		debug.SetMaxStack(64 << 20)
		for _, c := range aliasCases {
			if c.name == name {
				res, _ := json.Marshal(runAliasCase(t, c))
				fmt.Printf("ALIAS-RESULT %s\n", res)
				return
			}
		}
		t.Fatalf("no case %q", name)
	}
	for _, c := range aliasCases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestARecursiveAliasIsRefusedWithOneLine$", "-test.count=1")
			cmd.Env = append(os.Environ(), aliasChildEnv+"="+c.name)
			raw, err := cmd.CombinedOutput()
			var got aliasResult
			_, line, found := strings.Cut(string(raw), "ALIAS-RESULT ")
			if !found {
				t.Fatalf("the loader did not return (err %v, deadline %v): %.300s", err, ctx.Err(), raw)
			}
			line, _, _ = strings.Cut(line, "\n")
			if err := json.Unmarshal([]byte(line), &got); err != nil {
				t.Fatal(err)
			}
			if got.Code != 1 || got.Stderr != c.want || got.Stdout != "" || got.Written {
				t.Fatalf("got exit %d stdout %q written %v stderr %q, want exit 1, no stdout, no file, stderr %q",
					got.Code, got.Stdout, got.Written, got.Stderr, c.want)
			}
		})
	}
}

// A shared anchor is built once however many aliases name it: 30 levels of two aliases each is 2^30 expansions when
// every alias is built again.
func TestASharedAnchorIsBuiltOnce(t *testing.T) {
	var b strings.Builder
	b.WriteString("l0: &l0 [x, x]\n")
	for i := 1; i <= 30; i++ {
		fmt.Fprintf(&b, "l%d: &l%d [*l%d, *l%d]\n", i, i, i-1, i-1)
	}
	b.WriteString("m: &m {a: 1}\nn: {<<: [*m, *m], b: 2}\n")
	done := make(chan error, 1)
	var doc any
	go func() {
		var err error
		doc, err = routing.LoadYAML([]byte(b.String()), false)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the shared anchor is built again for every alias")
	}
	if got := routing.PyStr(routing.Lookup(doc, "n")); got != "{'a': 1, 'b': 2}" {
		t.Fatalf("merge of a shared anchor: %s", got)
	}
}
