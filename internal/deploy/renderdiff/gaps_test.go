package renderdiff

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func writeFiles(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(dir)
	return dir
}

func runMain(args []string, stdin string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	rc := Main(args, strings.NewReader(stdin), &stdout, &stderr)
	return rc, stdout.String(), stderr.String()
}

func renderPair(env, cmKey, cmToken, uri string) string {
	return `apiVersion: apps/v1
kind: Deployment
metadata: {name: svc}
spec:
  template:
    spec:
      containers:
      - name: app
        args: ["--x", "` + uri + `"]
        env:
        - {name: DB_PASSWORD, value: "` + env + `"}
        - {name: PLAIN, value: same}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: cfg}
data:
  REMOVED_KEY: "` + cmKey + `"
  api_token: "` + cmToken + `"
  LOG_LEVEL: info
`
}

// Gap 1: a changed redacted value (env PASSWORD, ConfigMap token key, args/URI with user info) is ONE row
// "<doc> <path> CHANGED <redacted>"; the same redacted value on both sides is no row.
func TestAChangedRedactedValueIsOneRow(t *testing.T) {
	writeFiles(t, map[string]string{
		"a.yaml": renderPair("pw-one", "k-one", "t-one", "postgres://u:p1@db/x"),
		"b.yaml": renderPair("pw-two", "k-one", "t-two", "postgres://u:p2@db/x"),
	})
	rc, out, errOut := runMain([]string{"a.yaml", "b.yaml", "--rc-a", "0", "--rc-b", "0", "--info"}, "")
	if rc != 0 || errOut != "" {
		t.Fatalf("rc %d stderr %q", rc, errOut)
	}
	for _, want := range []string{
		"Deployment/svc .spec.template.spec.containers[name=app].env[name=DB_PASSWORD].value CHANGED <redacted>\n",
		"ConfigMap/cfg .data.api_token CHANGED <redacted>\n",
		"Deployment/svc .spec.template.spec.containers[name=app].args[1] CHANGED <redacted>\n",
	} {
		if strings.Count(out, want) != 1 {
			t.Errorf("want exactly one %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "REMOVED_KEY") || strings.Contains(out, "PLAIN") {
		t.Errorf("an unchanged value gave a row:\n%s", out)
	}
	if !strings.Contains(out, "; changed field rows 3; ") {
		t.Errorf("the hidden rows are CHANGED rows in the counts:\n%s", out)
	}
}

// Gap 2: the over-redacted ConfigMap key (a name holding "key") with a changed value is the same CHANGED row.
func TestAnOverRedactedConfigMapKeyIsOneRow(t *testing.T) {
	writeFiles(t, map[string]string{
		"a.yaml": renderPair("pw", "k-one", "t", "x"),
		"b.yaml": renderPair("pw", "k-two", "t", "x"),
	})
	rc, out, _ := runMain([]string{"a.yaml", "b.yaml", "--rc-a", "0", "--rc-b", "0", "--info"}, "")
	if rc != 0 || strings.Count(out, "ConfigMap/cfg .data.REMOVED_KEY CHANGED <redacted>\n") != 1 {
		t.Fatalf("rc %d, want one REMOVED_KEY CHANGED <redacted> row:\n%s", rc, out)
	}
}

// Gap 3: --live counts Secret docs by Kind/name, so the render line and --min-docs read every doc.
func TestLiveCountsIncludeSecretDocs(t *testing.T) {
	render := "kind: Secret\nmetadata: {name: s1}\nstringData: {k: v}\n---\nkind: ConfigMap\nmetadata: {name: c}\ndata: {a: b}\n"
	writeFiles(t, map[string]string{"a.yaml": render, "b.yaml": render})
	rc, out, _ := runMain([]string{"a.yaml", "b.yaml", "--rc-a", "0", "--rc-b", "0", "--info", "--live", "--min-docs", "2"}, "")
	if rc != 0 || !strings.HasPrefix(out, "render A: helm rc=0 docs=2\nrender B: helm rc=0 docs=2\n") {
		t.Fatalf("rc %d, want docs=2 on both sides and the floor held:\n%s", rc, out)
	}
	if !strings.Contains(out, "docs (non-Secret) A 1 B 1;") {
		t.Errorf("the non-Secret summary line keeps its own count:\n%s", out)
	}
}

// sentinelRender plants one sentinel in each place the kit hides: an env PASSWORD value, a ConfigMap *key* value,
// a Secret stringData value, and a scheme://user@ URI (an env value of a plain name, a ConfigMap value, an arg).
func sentinelRender(s [4]string) string {
	return `apiVersion: apps/v1
kind: Deployment
metadata: {name: svc}
spec:
  template:
    spec:
      containers:
      - name: app
        args: ["--db=https://` + s[3] + `@db/x"]
        env:
        - {name: DB_PASSWORD, value: "` + s[0] + `"}
        - {name: LINK, value: "redis://` + s[3] + `@cache:6379"}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: cfg}
data:
  REMOVED_KEY: "` + s[1] + `"
  URL: "amqp://` + s[3] + `@mq/"
---
apiVersion: v1
kind: Secret
metadata: {name: creds}
stringData: {token: "` + s[2] + `"}
`
}

// TestNoSentinelOrItsHashReachesAnyOutput: after a full diff in every mode, 0 bytes of any sentinel, of its sha256
// hex, or of any 12-hex window of that hex are in stdout, stderr or a file the run leaves.
func TestNoSentinelOrItsHashReachesAnyOutput(t *testing.T) {
	a := [4]string{"SENTpwAAAA1111", "SENTkeyAAAA2222", "SENTsecAAAA3333", "SENTuriAAAA4444"}
	b := [4]string{"SENTpwBBBB5555", "SENTkeyBBBB6666", "SENTsecBBBB7777", "SENTuriBBBB8888"}
	var needles []string
	for _, s := range append(a[:], b[:]...) {
		sum := sha256.Sum256([]byte(s))
		h := hex.EncodeToString(sum[:])
		needles = append(needles, s, h, strings.ToUpper(h))
		for i := 0; i+12 <= len(h); i++ {
			needles = append(needles, h[i:i+12], strings.ToUpper(h[i:i+12]))
		}
	}
	rows := `{"added": [], "removed": [], "rows": [
 {"doc": "Deployment/svc", "path": ".spec.template.spec.containers[name=app].env[name=DB_PASSWORD].value", "verb": "CHANGED", "before": "` + a[0] + `", "after": "` + b[0] + `", "show": true},
 {"doc": "ConfigMap/cfg", "path": ".data.REMOVED_KEY", "verb": "CHANGED", "show": true},
 {"doc": "ConfigMap/cfg", "path": ".data.URL", "verb": "CHANGED", "show": true},
 {"doc": "Deployment/svc", "path": ".spec.template.spec.containers[name=app].args[0]", "verb": "CHANGED", "show": true},
 {"doc": "Deployment/svc", "path": ".spec.template.spec.containers[name=app].env[name=LINK].value", "verb": "CHANGED", "show": true}]}`
	dir := writeFiles(t, map[string]string{"a.yaml": sentinelRender(a), "b.yaml": sentinelRender(b), "rows.json": rows,
		"na.names": "Deployment/svc\nConfigMap/cfg\nSecret/creds\n", "nb.names": "Deployment/svc\nConfigMap/cfg\nSecret/creds\n"})
	before := listDir(t, dir)
	base := []string{"a.yaml", "b.yaml", "--rc-a", "0", "--rc-b", "0"}
	runs := [][]string{
		append(append([]string{}, base...), "--info"),
		append(append([]string{}, base...), "--info", "--live"),
		append(append([]string{}, base...), "--expect-added", ""),
		append(append([]string{}, base...), "--expect-rows", "rows.json"),
		append(append([]string{}, base...), "--expect-rows", "rows.json", "--live", "--names", "na.names", "nb.names", "--expect-names-added", ""),
		{"a.yaml", "a.yaml", "--rc-a", "0", "--rc-b", "0", "--expect-rows", "rows.json"},
	}
	var all strings.Builder
	sawHidden := 0
	for _, args := range runs {
		_, out, errOut := runMain(args, "")
		all.WriteString(out + errOut)
		sawHidden += strings.Count(out, "CHANGED <redacted>")
	}
	for _, f := range []string{"a.yaml", "b.yaml"} {
		raw, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			t.Fatal(err)
		}
		for _, mode := range []string{"filter", "names"} {
			_, out, errOut := runMain([]string{mode}, string(raw))
			all.WriteString(out + errOut)
		}
	}
	if sawHidden == 0 {
		t.Fatal("no CHANGED <redacted> row was printed: the cell does not reach the hidden rows")
	}
	if after := listDir(t, dir); strings.Join(after, ",") != strings.Join(before, ",") {
		t.Fatalf("the runs left files: %v -> %v", before, after)
	}
	text := all.String()
	for _, n := range needles {
		if strings.Contains(text, n) {
			t.Errorf("planted %q reached the output", n)
		}
	}
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

// A redacted value keeps the kit's forms: a scheme://user@ string in a plain field is a hidden row too, a change of
// the raw type alone (1 vs true, equal under Python ==) is a change, and a list keyed by a redacted name stays keyed by "<redacted>".
func TestRedactedValuesKeepTheKitForms(t *testing.T) {
	doc := func(url, pw, item string) string {
		return "kind: ConfigMap\nmetadata: {name: cfg}\ndata: {URL: \"" + url + "\"}\n---\n" +
			"kind: Pod\nmetadata: {name: p}\nspec:\n  containers:\n  - name: c\n    env:\n    - {name: DB_PASSWORD, value: " + pw + "}\n" +
			"  items:\n  - {name: \"ssh://u@h\", v: " + item + "}\n"
	}
	writeFiles(t, map[string]string{
		"a.yaml": doc("amqp://u1@mq/", "1", "1"),
		"b.yaml": doc("amqp://u2@mq/", "true", "2"),
	})
	rc, out, _ := runMain([]string{"a.yaml", "b.yaml", "--rc-a", "0", "--rc-b", "0", "--info"}, "")
	for _, want := range []string{
		"ConfigMap/cfg .data.URL CHANGED <redacted>\n",
		"Pod/p .spec.containers[name=c].env[name=DB_PASSWORD].value CHANGED <redacted>\n",
		"Pod/p .spec.items[name=<redacted>].v CHANGED\n",
	} {
		if rc != 0 || strings.Count(out, want) != 1 {
			t.Errorf("rc %d, want exactly one %q in:\n%s", rc, want, out)
		}
	}
}

// hiddenIn parses one input as a render and returns the raw values redact() hides (as str) minus those the same
// inputs also hold un-redacted (the kit prints those as plain text), plus the plain values seen. No other needle is
// skipped: a short redacted fixture value is a unique canary (syn-redact-<n>), so it cannot match kit text by chance.
func hiddenIn(raw []byte, hidden, plain map[string]bool) {
	defer func() { _ = recover() }()
	text, err := pythonText(raw)
	if err != nil {
		return
	}
	all, err := loadAll(text, composeDepthFile)
	if err != nil {
		return
	}
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case *redactedValue:
			hidden[pyStr(x.raw)] = true
		case *pyDict:
			for i, k := range x.keys {
				plain[pyStr(k)] = true
				walk(x.vals[i])
			}
		case []any:
			for _, e := range x {
				walk(e)
			}
		default:
			plain[pyStr(x)] = true
		}
	}
	for _, d := range docsOf(all) {
		walk(redact(d))
	}
}

func TestNoRedactedValueOrItsHashInAnyGoldenOutput(t *testing.T) {
	dirs, err := filepath.Glob(filepath.Join("testdata", "goldens-*"))
	if err != nil || len(dirs) == 0 {
		t.Fatalf("no golden dirs: %v", err)
	}
	checked, hiddenRows := 0, 0
	for _, dir := range dirs {
		dir, err := filepath.Abs(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(filepath.Base(dir), func(t *testing.T) {
			in := filepath.Join(dir, "in")
			t.Chdir(kitInputs(t, in))
			for _, c := range readManifest(t, dir) {
				hidden, plain := map[string]bool{}, map[string]bool{}
				for _, f := range append(append([]string{}, c.args...), c.stdin) {
					if raw, err := os.ReadFile(f); err == nil && f != "" {
						hiddenIn(raw, hidden, plain)
					}
				}
				var needles []string
				for v := range hidden {
					if plain[v] {
						continue
					}
					sum := sha256.Sum256([]byte(v))
					h := hex.EncodeToString(sum[:])
					// The empty string holds no byte that could leak; its hash windows are still checked.
					if v != "" {
						needles = append(needles, v)
					}
					for i := 0; i+12 <= len(h); i++ {
						needles = append(needles, h[i:i+12], strings.ToUpper(h[i:i+12]))
					}
				}
				var stdin []byte
				if c.stdin != "" {
					if stdin, err = os.ReadFile(c.stdin); err != nil {
						t.Fatal(err)
					}
				}
				var stdout, stderr bytes.Buffer
				Main(c.args, bytes.NewReader(stdin), &stdout, &stderr)
				out := stdout.String() + stderr.String()
				hiddenRows += strings.Count(out, "CHANGED <redacted>")
				for _, n := range needles {
					if strings.Contains(out, n) {
						t.Errorf("%s: a hidden value or its hash (%d bytes) reached the output", c.name, len(n))
					}
				}
				if len(needles) > 0 {
					checked++
				}
			}
		})
	}
	if checked == 0 || hiddenRows == 0 {
		t.Fatalf("the cell reached %d cases with hidden values and %d CHANGED <redacted> rows: it checks nothing", checked, hiddenRows)
	}
}
