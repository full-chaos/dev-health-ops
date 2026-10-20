package routing_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// Every YAML tag but the core str, int, float, bool, null, seq and map tags is refused: exit 1, one stderr line naming
// the tag and its line, nothing on stdout, no --out file. PyYAML's safe_load built sets, ordered maps, pairs, bytes and
// datetimes from these tags and the Python scripts went on; refusing is a ruled divergence (the repository's own files
// use none of them).

const flagsHead = "ops:\n  queryApi:\n    extraEnv:\n      - {name: GO_API_A_ENABLED, value: \"true\"}\n"

func TestANonCoreTagIsRefusedWithOneLine(t *testing.T) {
	refused := func(tag string, line int) string {
		return "yaml: tag " + tag + " (line " + strconv.Itoa(line) + ") is refused: only the core str, int, float, bool, null, seq and map tags are read\n"
	}
	for _, c := range []struct {
		name, values, want string
	}{
		{"an unused !!set (the reviewer's input shape)", flagsHead + "      - {name: S, valueFrom: {secretKeyRef: {key: K}}}\nunused: !!set {a, b}\n", refused("!!set", 6)},
		{"a !!binary secret key", flagsHead + "      - {name: S, valueFrom: {secretKeyRef: {key: !!binary bXlTZWN1cmVLZXk=}}}\n", refused("!!binary", 5)},
		{"a !!timestamp", flagsHead + "      - {name: S, valueFrom: {secretKeyRef: {key: !!timestamp 2002-12-14}}}\n", refused("!!timestamp", 5)},
		{"an !!omap", flagsHead + "unused: !!omap [a: 1]\n", refused("!!omap", 5)},
		{"!!pairs", flagsHead + "unused: !!pairs [a: 1]\n", refused("!!pairs", 5)},
		{"a python tag", flagsHead + "unused: !!python/object:os.system {}\n", refused("!!python/object:os.system", 5)},
		{"a local tag outside a compose file", flagsHead + "unused: !reset null\n", refused("!reset", 5)},
		{"!!seq on a mapping", flagsHead + "unused: !!seq {a: 1}\n", refused("!!seq", 5)},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			out := filepath.Join(dir, "out")
			r := run(flagsTool, writeFile(t, dir, "v.yaml", []byte(c.values)), "--out", out)
			if _, err := os.Stat(out); err == nil {
				t.Fatal("the --out file was written")
			}
			if r.code != 1 || r.stdout != "" || r.stderr != c.want {
				t.Fatalf("exit %d stdout %q stderr %q, want exit 1, no stdout, stderr %q", r.code, r.stdout, r.stderr, c.want)
			}
		})
	}
	// The other three commands read their values through the same loader.
	values := writeFile(t, t.TempDir(), "v.yaml", []byte("unused: !!set {a}\ningress: {goApiPaths: [{path: /a}], queryApiPaths: [{path: /q, pathType: Exact}]}\n"))
	for _, r := range []result{
		run(gen, values, "--format", "dynamic"),
		run(coverage, values, "--go-api-url", "http://127.0.0.1:1", "--query-api-url", "http://127.0.0.1:1"),
		run(webEnv, values, writeFile(t, t.TempDir(), "c.yml", []byte("services: {}\n"))),
	} {
		if r.code != 1 || r.stdout != "" || r.stderr != refused("!!set", 1) {
			t.Errorf("exit %d stdout %q stderr %q", r.code, r.stdout, r.stderr)
		}
	}
}

// An explicit !!bool reads only the YAML 1.1 bool words, as PyYAML's bool_values lookup does (KeyError otherwise).
func TestAnExplicitBoolReadsOnlyBoolWords(t *testing.T) {
	r := run(flagsTool, writeFile(t, t.TempDir(), "v.yaml", []byte(flagsHead+"unused: !!bool maybe\n")), "--out", filepath.Join(t.TempDir(), "o"))
	wantCode(t, r, 1)
	wantIn(t, r.stderr, "KeyError: 'maybe' (line 5)")
}
