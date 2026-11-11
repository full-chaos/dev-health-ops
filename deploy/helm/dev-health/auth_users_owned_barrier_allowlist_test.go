package devhealth_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-13103: BARRIER_ALLOWLIST. The prod-shaped world is the ticket's executed repro (2026-11-05T14:22:51Z): 3 finished
// ClickHouse grant Jobs (the real Job runs `/bin/bash -c` with `${...}` and `||`, so the scan cannot split its row), the acr
// init container and the pgbouncer exporter (both `sh -c`). The example file next to the script is the D6710 allowed set.
const exampleAllowlist = "../../scripts/auth-users-owned-barrier.allowlist-d6710.txt"

const (
	grantImage    = "clickhouse/clickhouse-server:26.6.8.7@sha256:2c870d77ea76e955fb1c8dad536ad40a1236b6093db0c4966e22d21d4428982b"
	acrInitImage  = "docker.io/library/busybox@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028"
	exporterImage = "quay.io/prometheuscommunity/pgbouncer-exporter:v0.12.1@sha256:30f31b6c2efdad3647f8182cc7c1a3a19e42bae5d17387694989f969371c230d"
	zeroDigest    = "@sha256:0000000000000000000000000000000000000000000000000000000000000000"
	acrRS         = "dev-health-acr-5784b5b46f"
	acrInitCmd    = "install -m 0400 /in/entitlement.jwt /out/entitlement.jwt"
	pgbRS         = "dev-health-ops-go-pgbouncer-transaction-7ffbf75b8"
	pgbPodName    = pgbRS + "-zjwdn"
	exporterCmd   = "while true; do pgbouncer_exporter; echo exporter exited rc=$?; sleep 5; done"
)

var grantJobs = []struct{ job, pod string }{
	{"ch-m5-grant17", "ch-m5-grant17-6fnbg"}, {"ch-m5-grant17b", "ch-m5-grant17b-qdj8n"}, {"ch-m5-grant17c", "ch-m5-grant17c-b94bs"},
}

// grantScript has the shape of the real grant Job's script: a shell, `${...}`, a function and `||`.
const grantScript = `set -uo pipefail
umask 077
cd /work || exit 1
yq1() { local s="${1//\'/\'\'}"; printf "'%s'" "$s"; }
clickhouse-client --config-file /work/dst.yaml -q "GRANT SHOW COLUMNS ON system.query_log TO otel_querylog" </dev/null || { echo "GRANT FAILED"; exit 1; }
echo "GRANT OK"`

func grantSpec(image, script string) map[string]any {
	return map[string]any{"containers": []any{map[string]any{"name": "grant", "image": image,
		"command": bwStrs("/bin/bash", "-c"), "args": bwStrs(script)}}}
}

// grantJob: a finished Job (succeeded 1, completionTime set) unless active > 0 or done is false.
func grantJob(name string, spec map[string]any, active int, done bool) map[string]any {
	j := bwJobItem(name, spec)
	st := map[string]any{"succeeded": 1}
	if active > 0 {
		st["active"] = active
	}
	if done {
		st["completionTime"] = "2026-10-23T17:00:00Z"
	}
	j["status"] = st
	return j
}

func bwPhase(pod map[string]any, phase string) map[string]any {
	pod["status"] = map[string]any{"phase": phase}
	return pod
}

func grantPod(name, job string, spec map[string]any, phase string) map[string]any {
	return bwPhase(bwOwned(bwPodAt(name, "", "", spec), "Job", job), phase)
}

func acrInit(cmd string) map[string]any {
	return map[string]any{"name": "prepare-entitlement-token", "image": acrInitImage, "command": bwStrs("sh", "-c", cmd)}
}

func acrPod(name, component string, init map[string]any) map[string]any {
	return bwPhase(bwOwned(bwPodAt(name, component, "", map[string]any{"initContainers": []any{init},
		"containers": []any{map[string]any{"name": "acr", "image": "ghcr.io/example/dev-health-acr:v1", "args": bwStrs("serve")}}}), "ReplicaSet", acrRS), "Running")
}

func exporterCtr(image, cmd string) map[string]any {
	return map[string]any{"name": "exporter", "image": image, "command": bwStrs("sh", "-c", cmd)}
}

func pgbPod(exporter map[string]any) map[string]any {
	return bwPhase(bwOwned(bwPodAt(pgbPodName, "pgbouncer-transaction", "", map[string]any{"containers": []any{
		map[string]any{"name": "pgbouncer", "image": "edoburu/pgbouncer:1.25.2@sha256:4c1ca296ef525f108f5d3552cc337c0c09587cf8dae7f0067fd93349e47dc1cd"},
		exporter}}), "ReplicaSet", pgbRS), "Running")
}

func rsOwnedBy(name string, deployments ...string) map[string]any {
	refs := []any{}
	for _, d := range deployments {
		refs = append(refs, map[string]any{"kind": "Deployment", "name": d})
	}
	return map[string]any{"metadata": map[string]any{"name": name, "ownerReferences": refs}}
}

type prodParts struct{ grants, acr, exporter bool }

func prodWorld(p prodParts) *bworld {
	w := bwNewWorld()
	if p.grants {
		for _, g := range grantJobs {
			w.jobs = append(w.jobs, grantJob(g.job, grantSpec(grantImage, grantScript), 0, true))
			w.otherPods = append(w.otherPods, grantPod(g.pod, g.job, grantSpec(grantImage, grantScript), "Succeeded"))
		}
	}
	if p.acr {
		w.otherRS = append(w.otherRS, rsOwnedBy(acrRS, "dev-health-acr"))
		w.otherPods = append(w.otherPods, acrPod(acrRS+"-g4424", "acr", acrInit(acrInitCmd)), acrPod(acrRS+"-thltb", "acr", acrInit(acrInitCmd)))
	}
	if p.exporter {
		w.otherRS = append(w.otherRS, rsOwnedBy(pgbRS, "dev-health-ops-go-pgbouncer-transaction"))
		w.otherPods = append(w.otherPods, pgbPod(exporterCtr(exporterImage, exporterCmd)))
	}
	return w
}

func allProd() *bworld { return prodWorld(prodParts{true, true, true}) }

// exampleEntries: the entry lines of the example file, all of them or those holding one of the words.
func exampleEntries(t *testing.T, words ...string) string {
	t.Helper()
	raw, err := os.ReadFile(exampleAllowlist)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		keep := len(words) == 0
		for _, w := range words {
			keep = keep || strings.Contains(line, w)
		}
		if keep {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n") + "\n"
}

// pyJSON renders a string list the way the fake kubectl prints it (python json.dumps, compact, ASCII); nil prints nothing.
func pyJSON(v any) string {
	if v == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("[")
	for i, x := range v.([]any) {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`"`)
		for _, r := range x.(string) {
			switch {
			case r == '"':
				b.WriteString(`\"`)
			case r == '\\':
				b.WriteString(`\\`)
			case r == '\n':
				b.WriteString(`\n`)
			case r == '\r':
				b.WriteString(`\r`)
			case r == '\t':
				b.WriteString(`\t`)
			case r < 0x20 || r > 0x7e:
				fmt.Fprintf(&b, `\u%04x`, r)
			default:
				b.WriteRune(r)
			}
		}
		b.WriteString(`"`)
	}
	b.WriteString("]")
	return b.String()
}

func sha(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

// ctrHash: the sha256 of a container's command+args as the scan reads them (the `shell -c` entry field).
func ctrHash(c map[string]any) string { return sha(pyJSON(c["command"]) + pyJSON(c["args"])) }

// rowHash: the sha256 of a Job row after its third field (containers|initContainers|); its pod (no ephemeral container) gives
// the same bytes (the `malformed row` entry field).
func rowHash(spec map[string]any) string {
	part := func(k string) string {
		out := ""
		if l, ok := spec[k].([]any); ok {
			for _, x := range l {
				c := x.(map[string]any)
				out += c["name"].(string) + "@@" + c["image"].(string) + "@@" + pyJSON(c["command"]) + pyJSON(c["args"]) + "<<c>>"
			}
		}
		return out
	}
	return sha(part("containers") + "|" + part("initContainers") + "|")
}

func grantKeys(h string, jobs ...string) map[string]string {
	m := map[string]string{}
	for _, j := range jobs {
		m["pod|Job/"+j+"|-|malformed row"] = h
		m["job|Job/"+j+"|-|malformed row"] = h
	}
	return m
}

const (
	acrKey      = "pod|Deployment/dev-health-acr|prepare-entitlement-token|shell -c"
	exporterKey = "pod|Deployment/dev-health-ops-go-pgbouncer-transaction|exporter|shell -c"
)

// prodHashes: the content hash of each entry key, computed from the prod-shaped fixtures.
func prodHashes() map[string]string {
	m := grantKeys(rowHash(grantSpec(grantImage, grantScript)), "ch-m5-grant17", "ch-m5-grant17b", "ch-m5-grant17c")
	m[acrKey] = ctrHash(acrInit(acrInitCmd))
	m[exporterKey] = ctrHash(exporterCtr(exporterImage, exporterCmd))
	return m
}

// pinnedEntries: the example entries (all, or those holding one of the words) with their unset sha256 written from
// prodHashes, then from over: what an operator writes after the read-only review.
func pinnedEntries(t *testing.T, over map[string]string, words ...string) string {
	t.Helper()
	h := prodHashes()
	for k, v := range over {
		h[k] = v
	}
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(exampleEntries(t, words...)), "\n") {
		f := strings.Split(line, "|")
		if len(f) != 6 || f[5] != "unset" {
			t.Fatalf("example entry is not `...|unset`: %s", line)
		}
		f[5] = h[strings.Join(f[:4], "|")]
		out = append(out, strings.Join(f, "|"))
	}
	return strings.Join(out, "\n") + "\n"
}

func allowFile(t *testing.T, text string) []string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "allow.txt")
	if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return []string{"BARRIER_ALLOWLIST=" + p}
}

var clearedLine = regexp.MustCompile(`(?m)^cleared-by-allowlist (.+) object=(\S+)$`)

// clearedKeys: the sorted distinct keys and the number of cleared lines.
func clearedKeys(stdout string) ([]string, int) {
	set := map[string]bool{}
	m := clearedLine.FindAllStringSubmatch(stdout, -1)
	for _, x := range m {
		set[x[1]] = true
	}
	keys := []string{}
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, len(m)
}

func entryKeys(entries string) []string {
	keys := []string{}
	for _, line := range strings.Split(strings.TrimSpace(entries), "\n") {
		f := strings.Split(line, "|")
		keys = append(keys, strings.Join(f[:4], "|"))
	}
	sort.Strings(keys)
	return keys
}

// The prod repro after U1, line for line: without an allowlist the run is the run of today (an empty value is no allowlist).
func TestBarrierWithoutAllowlistStopsOnTheProdShapedWorldAsToday(t *testing.T) {
	want := `pod ch-m5-grant17-6fnbg: malformed row (a field separator in an argument?)
pod ch-m5-grant17b-qdj8n: malformed row (a field separator in an argument?)
pod ch-m5-grant17c-b94bs: malformed row (a field separator in an argument?)
pod dev-health-acr-5784b5b46f-g4424 container prepare-entitlement-token: runs a shell with -c
pod dev-health-acr-5784b5b46f-thltb container prepare-entitlement-token: runs a shell with -c
pod dev-health-ops-go-pgbouncer-transaction-7ffbf75b8-zjwdn container exporter: runs a shell with -c
barrier check failed: a pod in the namespace holds an admin users writer or a container the scan cannot clear (see above)
job ch-m5-grant17: malformed row (a field separator in an argument?)
job ch-m5-grant17b: malformed row (a field separator in an argument?)
job ch-m5-grant17c: malformed row (a field separator in an argument?)
barrier check failed: a job in the namespace holds an admin users writer or a container the scan cannot clear (see above)
barrier FAILED: 2 check(s) failed; do not run the next step
`
	unset := runBarrier(t, allProd().files(), nil, "up-go-api")
	if unset.rc != 1 || unset.stdout != "" || unset.stderr != want {
		t.Fatalf("rc=%d stdout=%q stderr=\n%s", unset.rc, unset.stdout, unset.stderr)
	}
	empty := runBarrier(t, allProd().files(), []string{"BARRIER_ALLOWLIST="}, "up-go-api")
	if empty.rc != unset.rc || empty.stdout != unset.stdout || empty.stderr != unset.stderr || empty.argv != unset.argv {
		t.Fatalf("an empty BARRIER_ALLOWLIST differs from none: rc=%d stdout=%q stderr=%q", empty.rc, empty.stdout, empty.stderr)
	}
}

var hashMismatch = regexp.MustCompile(`(?m)^allowlist-hash-mismatch (.+) object=(\S+) sha256=([0-9a-f]{64})$`)

// The example file pins no content yet (sha256 unset): on the prod-shaped world it STOPs and prints, for each of the 9 objects,
// the hash of the content the scan read (equal to the hash of the fixture, computed here on its own). With those hashes written
// in, it clears in every mode: 9 lines (3 grant pods, 3 grant Jobs, 2 acr pods, 1 exporter pod), the 8 keys of the file,
// nothing on stderr, and barrier_ok stays the last line.
func TestBarrierExampleAllowlistClearsTheProdShapedWorld(t *testing.T) {
	r := runBarrier(t, allProd().files(), []string{"BARRIER_ALLOWLIST=" + exampleAllowlist}, "up-go-api")
	m := hashMismatch.FindAllStringSubmatch(r.stderr, -1)
	if r.rc == 0 || bwPassed(r) || len(m) != 9 || clearedLine.MatchString(r.stdout) {
		t.Fatalf("the unset example file: rc=%d mismatch lines=%d stdout=%q stderr=%q", r.rc, len(m), r.stdout, r.stderr)
	}
	for _, x := range m {
		if want := prodHashes()[x[1]]; x[3] != want {
			t.Errorf("%s %s: printed sha256 %s, the fixture hashes to %s", x[1], x[2], x[3], want)
		}
	}
	for _, mode := range []string{"up-go-api", "up-owned", "down-auth"} {
		r := runBarrier(t, allProd().files(), allowFile(t, pinnedEntries(t, nil)), mode)
		if !bwPassed(r) || r.stderr != "" {
			t.Fatalf("%s: rc=%d stdout=%q stderr=%q", mode, r.rc, r.stdout, r.stderr)
		}
		keys, n := clearedKeys(r.stdout)
		if n != 9 || strings.Join(keys, "\n") != strings.Join(entryKeys(exampleEntries(t)), "\n") {
			t.Errorf("%s: %d cleared lines, keys %q", mode, n, keys)
		}
		lines := strings.Split(strings.TrimSpace(r.stdout), "\n")
		if !strings.HasPrefix(lines[len(lines)-1], "barrier_ok mode="+mode+" ") {
			t.Errorf("%s: barrier_ok is not the last line: %q", mode, r.stdout)
		}
	}
}

// Each of the 3 classes alone, with only its own entries.
func TestBarrierAllowlistClearsEachClass(t *testing.T) {
	cases := []struct {
		name  string
		parts prodParts
		word  string
		lines int
	}{
		{"finished grant Jobs and their pods (malformed row)", prodParts{grants: true}, "ch-m5-grant", 6},
		{"acr init container (shell -c)", prodParts{acr: true}, "dev-health-acr", 2},
		{"pgbouncer exporter (shell -c)", prodParts{exporter: true}, "pgbouncer-exporter", 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if r := runBarrier(t, prodWorld(c.parts).files(), nil, "up-go-api"); r.rc == 0 {
				t.Fatalf("control: the class does not STOP without an allowlist: %q", r.stdout)
			}
			entries := pinnedEntries(t, nil, c.word)
			r := runBarrier(t, prodWorld(c.parts).files(), allowFile(t, entries), "up-go-api")
			if !bwPassed(r) || r.stderr != "" {
				t.Fatalf("rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
			keys, n := clearedKeys(r.stdout)
			if n != c.lines || strings.Join(keys, "\n") != strings.Join(entryKeys(entries), "\n") {
				t.Errorf("%d cleared lines, keys %q", n, keys)
			}
		})
	}
}

// Each change of a fact STOPs with the full example file; want names the line that must stay.
func TestBarrierAllowlistStillStops(t *testing.T) {
	t.Parallel()
	grantPodAt := func(w *bworld, job string) map[string]any {
		for _, p := range w.otherPods {
			pod := p.(map[string]any)
			if strings.HasPrefix(pod["metadata"].(map[string]any)["name"].(string), job+"-") {
				return pod
			}
		}
		t.Fatalf("no pod of %s", job)
		return nil
	}
	setJob := func(w *bworld, job string, j map[string]any) {
		for i, x := range w.jobs {
			if x.(map[string]any)["metadata"].(map[string]any)["name"] == job {
				w.jobs[i] = j
			}
		}
	}
	setAcrInit := func(w *bworld, cmd string) {
		for i, p := range w.otherPods {
			if strings.HasPrefix(p.(map[string]any)["metadata"].(map[string]any)["name"].(string), acrRS) {
				name := p.(map[string]any)["metadata"].(map[string]any)["name"].(string)
				w.otherPods[i] = acrPod(name, "acr", acrInit(cmd))
			}
		}
	}
	setExporter := func(w *bworld, image, cmd string) {
		for i, p := range w.otherPods {
			if p.(map[string]any)["metadata"].(map[string]any)["name"] == pgbPodName {
				w.otherPods[i] = pgbPod(exporterCtr(image, cmd))
			}
		}
	}
	regrant := func(w *bworld, job, image, query string) {
		spec := grantSpec(image, query)
		setJob(w, job, grantJob(job, spec, 0, true))
		pod := grantPodAt(w, job)
		pod["spec"] = spec
	}
	withInit := grantSpec(grantImage, grantScript)
	withInit["initContainers"] = []any{map[string]any{"name": "pre", "image": "registry.example/tool:1", "args": bwStrs("x")}}
	changedArgs := "install -m 0444 /in/entitlement.jwt /out/entitlement.jwt"
	changedScript := strings.Replace(grantScript, "TO otel_querylog", "TO otel_querylog2", 1)
	// over: the entry hashes written for the changed content, so the cell meets the check it names and not the hash check
	cases := []struct {
		name string
		mut  func(w *bworld)
		want string
		over map[string]string
	}{
		{"same image, changed args of an allowlisted shell -c container", func(w *bworld) { setAcrInit(w, changedArgs) },
			"allowlist-hash-mismatch " + acrKey + " object=" + acrRS + "-g4424 sha256=" + ctrHash(acrInit(changedArgs)), nil},
		{"same Job name and image, changed row bytes", func(w *bworld) { regrant(w, "ch-m5-grant17", grantImage, changedScript) },
			"allowlist-hash-mismatch job|Job/ch-m5-grant17|-|malformed row object=ch-m5-grant17 sha256=" + rowHash(grantSpec(grantImage, changedScript)), nil},
		{"an allowlisted Job with succeeded 0", func(w *bworld) {
			j := grantJob("ch-m5-grant17c", grantSpec(grantImage, grantScript), 0, true)
			j["status"].(map[string]any)["succeeded"] = 0
			setJob(w, "ch-m5-grant17c", j)
		}, "job ch-m5-grant17c: malformed row", nil},
		{"changed image digest of the exporter", func(w *bworld) {
			setExporter(w, "quay.io/prometheuscommunity/pgbouncer-exporter:v0.12.1"+zeroDigest, exporterCmd)
		}, "zjwdn container exporter: runs a shell with -c", nil},
		{"the exporter image without its digest", func(w *bworld) {
			setExporter(w, "quay.io/prometheuscommunity/pgbouncer-exporter:v0.12.1", exporterCmd)
		}, "zjwdn container exporter: runs a shell with -c", nil},
		{"changed image digest of a grant Job and its pod", func(w *bworld) {
			regrant(w, "ch-m5-grant17b", "clickhouse/clickhouse-server:26.6.8.7"+zeroDigest, grantScript)
		}, "job ch-m5-grant17b: malformed row", grantKeys(rowHash(grantSpec("clickhouse/clickhouse-server:26.6.8.7"+zeroDigest, grantScript)), "ch-m5-grant17b")},
		{"a second container on another image in a grant Job", func(w *bworld) {
			setJob(w, "ch-m5-grant17", grantJob("ch-m5-grant17", withInit, 0, true))
		}, "job ch-m5-grant17: malformed row", map[string]string{"job|Job/ch-m5-grant17|-|malformed row": rowHash(withInit)}},
		{"a forged segment in the args does not hide a changed image", func(w *bworld) {
			regrant(w, "ch-m5-grant17c", "registry.example/evil:1"+zeroDigest, "x<<c>>|grant@@"+grantImage+"@@y | z")
		}, "job ch-m5-grant17c: malformed row", grantKeys(rowHash(grantSpec("registry.example/evil:1"+zeroDigest, "x<<c>>|grant@@"+grantImage+"@@y | z")), "ch-m5-grant17c")},
		{"an allowlisted image with the segment separator appended", func(w *bworld) {
			regrant(w, "ch-m5-grant17", grantImage+"@@x", grantScript)
		}, "job ch-m5-grant17: malformed row", grantKeys(rowHash(grantSpec(grantImage+"@@x", grantScript)), "ch-m5-grant17")},
		{"a pod of an allowlisted Job labelled go-api", func(w *bworld) {
			grantPodAt(w, "ch-m5-grant17")["metadata"].(map[string]any)["labels"] = map[string]any{"app.kubernetes.io/component": "go-api"}
		}, "pod ch-m5-grant17-6fnbg: malformed row", nil},
		{"a pod of an allowlisted Job labelled auth-service", func(w *bworld) {
			grantPodAt(w, "ch-m5-grant17")["metadata"].(map[string]any)["labels"] = map[string]any{"app.kubernetes.io/component": "auth-service"}
		}, "pod ch-m5-grant17-6fnbg: malformed row", nil},
		{"a new item", func(w *bworld) {
			w.otherRS = append(w.otherRS, rsOwnedBy("other-5d9f", "other"))
			w.otherPods = append(w.otherPods, bwOwned(bwPodAt("x1", "other", "", map[string]any{"containers": []any{
				map[string]any{"name": "c", "image": acrInitImage, "command": bwStrs("sh", "-c", "sleep 1")}}}), "ReplicaSet", "other-5d9f"))
		}, "pod x1 container c: runs a shell with -c", nil},
		{"the allowlisted container on another Deployment", func(w *bworld) {
			w.otherRS = append(w.otherRS, rsOwnedBy("dev-health-acr2-5d9f", "dev-health-acr2"))
			w.otherPods = append(w.otherPods, bwOwned(bwPodAt("x1", "acr", "", map[string]any{"initContainers": []any{acrInit(acrInitCmd)},
				"containers": []any{map[string]any{"name": "acr", "image": "ghcr.io/example/dev-health-acr:v1"}}}), "ReplicaSet", "dev-health-acr2-5d9f"))
		}, "pod x1 container prepare-entitlement-token: runs a shell with -c", nil},
		{"another container name in an allowlisted Deployment pod", func(w *bworld) {
			pod := acrPod(acrRS+"-x1", "acr", acrInit(acrInitCmd))
			spec := pod["spec"].(map[string]any)
			spec["initContainers"] = append(spec["initContainers"].([]any), map[string]any{"name": "second", "image": acrInitImage, "command": bwStrs("sh", "-c", acrInitCmd)})
			w.otherPods = append(w.otherPods, pod)
		}, "x1 container second: runs a shell with -c", nil},
		{"a running pod of an allowlisted Job", func(w *bworld) {
			bwPhase(grantPodAt(w, "ch-m5-grant17"), "Running")
			setJob(w, "ch-m5-grant17", grantJob("ch-m5-grant17", grantSpec(grantImage, grantScript), 1, false))
		}, "pod ch-m5-grant17-6fnbg: malformed row", nil},
		{"a Pending pod of a finished allowlisted Job", func(w *bworld) {
			bwPhase(grantPodAt(w, "ch-m5-grant17"), "Pending")
		}, "pod ch-m5-grant17-6fnbg: malformed row", nil},
		{"an allowlisted Job with active 1", func(w *bworld) {
			setJob(w, "ch-m5-grant17c", grantJob("ch-m5-grant17c", grantSpec(grantImage, grantScript), 1, true))
		}, "job ch-m5-grant17c: malformed row", nil},
		{"an allowlisted Job without completionTime", func(w *bworld) {
			setJob(w, "ch-m5-grant17c", grantJob("ch-m5-grant17c", grantSpec(grantImage, grantScript), 0, false))
		}, "job ch-m5-grant17c: malformed row", nil},
		{"a pod of an allowlisted Job with a second owner", func(w *bworld) {
			pod := grantPodAt(w, "ch-m5-grant17")
			pod["metadata"].(map[string]any)["ownerReferences"] = []any{map[string]any{"kind": "Job", "name": "ch-m5-grant17"}, map[string]any{"kind": "Job", "name": "x"}}
		}, "pod ch-m5-grant17-6fnbg: malformed row", nil},
		{"dho in an allowlisted shell -c container", func(w *bworld) { setAcrInit(w, "dho version") },
			"g4424 container prepare-entitlement-token: runs a shell with -c", map[string]string{acrKey: ctrHash(acrInit("dho version"))}},
		{"DHO in upper case in an allowlisted shell -c container", func(w *bworld) { setAcrInit(w, "exec /opt/DHO/bin/tool") },
			"g4424 container prepare-entitlement-token: runs a shell with -c", map[string]string{acrKey: ctrHash(acrInit("exec /opt/DHO/bin/tool"))}},
		{"users and admin in an allowlisted shell -c container", func(w *bworld) {
			setExporter(w, exporterImage, "while true; do tool --role=Admin --table=superUsers; sleep 5; done")
		}, "zjwdn container exporter: runs a shell with -c", map[string]string{exporterKey: ctrHash(exporterCtr(exporterImage, "while true; do tool --role=Admin --table=superUsers; sleep 5; done"))}},
		{"users and admin in an allowlisted Job", func(w *bworld) {
			regrant(w, "ch-m5-grant17", grantImage, "GRANT ALL ON *.* TO admin; SELECT 1 FROM system.users | x")
		}, "job ch-m5-grant17: malformed row", grantKeys(rowHash(grantSpec(grantImage, "GRANT ALL ON *.* TO admin; SELECT 1 FROM system.users | x")), "ch-m5-grant17")},
		{"dho in an allowlisted Job", func(w *bworld) { regrant(w, "ch-m5-grant17", grantImage, "SELECT 'dho' | x") },
			"pod ch-m5-grant17-6fnbg: malformed row", grantKeys(rowHash(grantSpec(grantImage, "SELECT 'dho' | x")), "ch-m5-grant17")},
		{"a substitution in an allowlisted shell -c container", func(w *bworld) { setAcrInit(w, "echo $(id)") },
			"g4424 container prepare-entitlement-token: uses a command substitution", map[string]string{acrKey: ctrHash(acrInit("echo $(id)"))}},
		{"an allowlisted Deployment pod labelled go-api", func(w *bworld) {
			w.otherPods = append(w.otherPods, acrPod(acrRS+"-x1", "go-api", acrInit(acrInitCmd)))
		}, "x1 container prepare-entitlement-token: runs a shell with -c", nil},
		{"an allowlisted Deployment pod labelled auth-service", func(w *bworld) {
			w.otherPods = append(w.otherPods, acrPod(acrRS+"-x1", "auth-service", acrInit(acrInitCmd)))
		}, "x1 container prepare-entitlement-token: runs a shell with -c", nil},
		{"the acr ReplicaSet owned by two Deployments", func(w *bworld) {
			for i, rs := range w.otherRS {
				if rs.(map[string]any)["metadata"].(map[string]any)["name"] == acrRS {
					w.otherRS[i] = rsOwnedBy(acrRS, "dev-health-acr", "dev-health-acr")
				}
			}
		}, "g4424 container prepare-entitlement-token: runs a shell with -c", nil},
		{"an allowlisted object is gone (entry unused)", func(w *bworld) {
			kept := []any{}
			for _, p := range w.otherPods {
				if p.(map[string]any)["metadata"].(map[string]any)["name"] != pgbPodName {
					kept = append(kept, p)
				}
			}
			w.otherPods = kept
		}, "allowlist-unused pod|Deployment/dev-health-ops-go-pgbouncer-transaction|exporter|shell -c", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := allProd()
			c.mut(w)
			r := runBarrier(t, w.files(), allowFile(t, pinnedEntries(t, c.over)), "up-go-api")
			if bwPassed(r) || r.rc == 0 || !strings.Contains(r.stderr, c.want) {
				t.Fatalf("want a STOP with %q: rc=%d stdout=%q stderr=%q", c.want, r.rc, r.stdout, r.stderr)
			}
			if !strings.Contains(r.stderr, "barrier FAILED:") {
				t.Errorf("no FAILED line: %q", r.stderr)
			}
		})
	}
}

// The allowlist never reaches a go-api or auth-service check: with the full file, the go-api lines of the prod repro before
// U1 stay, and only they (the scan items are cleared); an owned auth-service pod still STOPs up-owned; an entry that names
// a go-api or auth-service workload refuses the file before any kubectl call.
func TestBarrierAllowlistNeverTouchesTheGoAPIAndAuthServiceChecks(t *testing.T) {
	w := allProd()
	w.goPods[1] = bwPodAt("p2", "go-api", "", map[string]any{"containers": []any{bwServerGoAPI(map[string]any{"name": "X", "value": "1"})}})
	r := runBarrier(t, w.files(), allowFile(t, pinnedEntries(t, nil)), "up-go-api")
	want := "barrier check failed: go-api pod p2: AUTH_USERS_OWNED is not true\nbarrier check failed: go-api pod p2: AUTH_PROXY_CREDENTIALS is not true\nbarrier FAILED: 2 check(s) failed; do not run the next step\n"
	if r.rc != 1 || r.stderr != want {
		t.Fatalf("rc=%d stderr=%q", r.rc, r.stderr)
	}
	if _, n := clearedKeys(r.stdout); n != 9 {
		t.Errorf("want the 9 scan items cleared, got %d: %q", n, r.stdout)
	}

	w = allProd()
	w.authPods[0] = bwAuthPod("a1", "true")
	if r := runBarrier(t, w.files(), allowFile(t, pinnedEntries(t, nil)), "up-owned"); r.rc == 0 || !strings.Contains(r.stderr, "an auth-service pod is already owned") {
		t.Fatalf("up-owned with an owned auth-service pod: rc=%d stderr=%q", r.rc, r.stderr)
	}

	for _, line := range []string{
		"pod|Deployment/a-dev-health-go-api|go-api|shell -c|" + acrInitImage + "|unset",
		"pod|Deployment/dev-health-ops-go-api|sidecar|shell -c|" + acrInitImage + "|unset",
		"pod|Deployment/a-dev-health-auth-service|stage-signing-key|shell -c|" + acrInitImage + "|unset",
		"pod|Deployment/x|auth-service|shell -c|" + acrInitImage + "|unset",
	} {
		r := runBarrier(t, allProd().files(), allowFile(t, pinnedEntries(t, nil)+line+"\n"), "up-go-api")
		if r.rc == 0 || r.argv != "" || !strings.Contains(r.stderr, "go-api or auth-service") {
			t.Errorf("%s: rc=%d argv=%q stderr=%q", line, r.rc, r.argv, r.stderr)
		}
	}
}

// A malformed file STOPs before any kubectl call; nothing is ignored, nothing is a pattern.
func TestBarrierRefusesAMalformedAllowlist(t *testing.T) {
	h := strings.Repeat("a", 64)
	e := func(fields ...string) string { return strings.Join(fields, "|") + "|" + h }
	good := e("pod", "Deployment/dev-health-acr", "prepare-entitlement-token", "shell -c", acrInitImage)
	cases := map[string]string{
		"4 fields":                                  "pod|Deployment/dev-health-acr|prepare-entitlement-token|shell -c",
		"5 fields (no sha256)":                      "pod|Deployment/dev-health-acr|prepare-entitlement-token|shell -c|" + acrInitImage,
		"7 fields":                                  good + "|x",
		"empty sha256":                              strings.TrimSuffix(good, h),
		"a short sha256":                            good[:len(good)-1],
		"upper case sha256":                         strings.TrimSuffix(good, h) + strings.Repeat("A", 64),
		"a sha256 that is not hex":                  strings.TrimSuffix(good, h) + strings.Repeat("g", 64),
		"duplicate key":                             good + "\n" + good,
		"duplicate key with another image":          good + "\n" + strings.Replace(good, "9532d8c3", "9532d8c4", 1),
		"duplicate key with another sha256":         good + "\n" + strings.TrimSuffix(good, h) + strings.Repeat("b", 64),
		"empty digest":                              e("pod", "Deployment/dev-health-acr", "prepare-entitlement-token", "shell -c", ""),
		"image without a digest":                    e("pod", "Deployment/dev-health-acr", "prepare-entitlement-token", "shell -c", "docker.io/library/busybox:1.36"),
		"a short digest":                            e("pod", "Deployment/dev-health-acr", "prepare-entitlement-token", "shell -c", acrInitImage[:len(acrInitImage)-1]),
		"upper case hex":                            strings.Replace(good, "9532d8c3", "9532D8C3", 1),
		"an empty repository":                       e("pod", "Deployment/dev-health-acr", "prepare-entitlement-token", "shell -c", zeroDigest),
		"a wildcard owner":                          e("job", "Job/ch-m5-grant*", "-", "malformed row", grantImage),
		"a wildcard container":                      e("pod", "Deployment/dev-health-acr", "*", "shell -c", acrInitImage),
		"a question mark":                           e("job", "Job/ch-m5-grant1?", "-", "malformed row", grantImage),
		"a bracket class":                           e("job", "Job/ch-m5-grant1[7]", "-", "malformed row", grantImage),
		"a regex anchor":                            e("job", "Job/ch-m5-grant17$", "-", "malformed row", grantImage),
		"kind cronjob":                              e("cronjob", "Job/ch-m5-grant17", "-", "malformed row", grantImage),
		"kind deployment":                           e("deployment", "Deployment/dev-health-acr", "prepare-entitlement-token", "shell -c", acrInitImage),
		"a reason that is never cleared":            e("pod", "Deployment/dev-health-acr", "prepare-entitlement-token", "mentions dho", acrInitImage),
		"the full reason text":                      e("pod", "Deployment/dev-health-acr", "prepare-entitlement-token", "runs a shell with -c", acrInitImage),
		"malformed row of a Deployment":             e("pod", "Deployment/dev-health-acr", "-", "malformed row", acrInitImage),
		"malformed row with a container":            e("pod", "Job/ch-m5-grant17", "grant", "malformed row", grantImage),
		"shell -c of a Job":                         e("job", "Job/ch-m5-grant17", "grant", "shell -c", grantImage),
		"shell -c of a Job pod":                     e("pod", "Job/ch-m5-grant17", "grant", "shell -c", grantImage),
		"shell -c without a container":              e("pod", "Deployment/dev-health-acr", "-", "shell -c", acrInitImage),
		"an owner without a kind":                   e("job", "ch-m5-grant17", "-", "malformed row", grantImage),
		"an owner kind StatefulSet":                 e("pod", "StatefulSet/x", "c", "shell -c", acrInitImage),
		"an owner with an empty name":               e("job", "Job/", "-", "malformed row", grantImage),
		"a container with upper case":               e("pod", "Deployment/dev-health-acr", "Prepare", "shell -c", acrInitImage),
		"a leading space":                           " " + good,
		"a trailing space":                          good + " ",
		"a tab":                                     strings.Replace(good, "|shell -c|", "|shell\t-c|", 1),
		"a CRLF line end":                           good + "\r",
		"only comments":                             "# nothing here",
		"empty file":                                "",
		"a bad line after good ones (none ignored)": exampleEntries(t) + "pod|Deployment/x|c|shell -c|" + acrInitImage,
	}
	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			r := runBarrier(t, allProd().files(), allowFile(t, text+"\n"), "up-go-api")
			if r.rc == 0 || r.argv != "" || !strings.Contains(r.stderr, "barrier FAILED: the allowlist") {
				t.Fatalf("rc=%d argv=%q stderr=%q", r.rc, r.argv, r.stderr)
			}
		})
	}
	for name, path := range map[string]string{"a missing file": filepath.Join(t.TempDir(), "none.txt"), "a directory": t.TempDir()} {
		r := runBarrier(t, allProd().files(), []string{"BARRIER_ALLOWLIST=" + path}, "up-go-api")
		if r.rc == 0 || r.argv != "" || !strings.Contains(r.stderr, "BARRIER_ALLOWLIST is not a readable file") {
			t.Errorf("%s: rc=%d argv=%q stderr=%q", name, r.rc, r.argv, r.stderr)
		}
	}
}

// With an allowlist the run stays read-only (get -o jsonpath, request timeout) and reads the facts a clear needs.
func TestBarrierAllowlistReadsOnlyWithGetJSONPath(t *testing.T) {
	r := runBarrier(t, allProd().files(), allowFile(t, pinnedEntries(t, nil)), "up-go-api")
	shape := regexp.MustCompile(`^-n dev-health --request-timeout=30s get (deploy|rs|pods|jobs|cronjobs|statefulsets) (-l app\.kubernetes\.io/instance=a\S* )?-o jsonpath=`)
	for _, line := range strings.Split(strings.TrimSpace(r.argv), "\n") {
		if !shape.MatchString(line) {
			t.Errorf("kubectl call outside the read-only shape: %s", line)
		}
	}
	for _, part := range []string{"get pods -o jsonpath={range .items[*]}{.metadata.name}|{.status.phase}", "get rs -o jsonpath={range .items[*]}{.metadata.name}|{range .metadata.ownerReferences[*]}",
		"get jobs -o jsonpath={range .items[*]}{.metadata.name}|{.status.active}|{.status.completionTime}"} {
		if !strings.Contains(r.argv, part) {
			t.Errorf("no fact read %q", part)
		}
	}
	none := runBarrier(t, allProd().files(), nil, "up-go-api")
	if strings.Contains(none.argv, "{.status.phase}") || strings.Contains(none.argv, "{.status.completionTime}") {
		t.Errorf("a run without an allowlist reads the facts: %s", none.argv)
	}
}

// A finished Job cannot run again. A `malformed row` clear rests on Job complete (active 0, completionTime set), pods
// Succeeded, the image and no mention of dho or of users with admin; not on the shell text the scan cannot split (D6710
// cleared the real grant Jobs, `bash -c` with `${...}`, on these facts). The same text in a Job that runs still STOPs.
func TestBarrierAllowlistClearsAFinishedJobWhateverItsShell(t *testing.T) {
	scripts := map[string]string{
		"a shell -c (the command)": "true || exit 1",
		"a command substitution":   "echo $(id) || exit 1",
		"a backtick":               "echo `id` || exit 1",
		"a variable":               "echo ${X} || exit 1",
		"eval":                     "eval x || exit 1",
	}
	for name, script := range scripts {
		t.Run(name, func(t *testing.T) {
			world := func(active int) *bworld {
				w := bwNewWorld()
				for _, g := range grantJobs {
					w.jobs = append(w.jobs, grantJob(g.job, grantSpec(grantImage, script), active, active == 0))
					w.otherPods = append(w.otherPods, grantPod(g.pod, g.job, grantSpec(grantImage, script), map[bool]string{true: "Succeeded", false: "Running"}[active == 0]))
				}
				return w
			}
			allow := allowFile(t, pinnedEntries(t, grantKeys(rowHash(grantSpec(grantImage, script)), "ch-m5-grant17", "ch-m5-grant17b", "ch-m5-grant17c"), "ch-m5-grant"))
			r := runBarrier(t, world(0).files(), allow, "up-go-api")
			if _, n := clearedKeys(r.stdout); !bwPassed(r) || n != 6 {
				t.Fatalf("finished: rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
			r = runBarrier(t, world(1).files(), allow, "up-go-api")
			if bwPassed(r) || r.rc == 0 || !strings.Contains(r.stderr, "job ch-m5-grant17: malformed row") || !strings.Contains(r.stderr, "pod ch-m5-grant17-6fnbg: malformed row") {
				t.Fatalf("running: rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
			}
		})
	}
}

// A running pod whose row or container the scan cannot split is never cleared, even with an entry pinned to the hash of its
// command+args: an argument that holds the scan's own separators (`|`, `@@`, `<<c>>`) makes the parse ambiguous. Fail closed,
// as without an allowlist; only a finished Job's row is cleared that way (pinned by the row hash).
func TestBarrierAllowlistNeverClearsARunningPodItCannotSplit(t *testing.T) {
	for _, sep := range []string{"|", "@@", "<<c>>"} {
		for _, where := range []string{"command", "args"} {
			t.Run(sep+" in "+where, func(t *testing.T) {
				c := acrInit("printf 'a" + sep + "b'")
				if where == "args" {
					c = map[string]any{"name": "prepare-entitlement-token", "image": acrInitImage, "command": bwStrs("sh", "-c"), "args": bwStrs("printf 'a" + sep + "b'")}
				}
				w := prodWorld(prodParts{acr: true})
				for i, p := range w.otherPods {
					w.otherPods[i] = acrPod(p.(map[string]any)["metadata"].(map[string]any)["name"].(string), "acr", c)
				}
				r := runBarrier(t, w.files(), allowFile(t, pinnedEntries(t, map[string]string{acrKey: ctrHash(c)}, "dev-health-acr")), "up-go-api")
				if bwPassed(r) || r.rc == 0 || clearedLine.MatchString(r.stdout) || !strings.Contains(r.stderr, "allowlist-unused "+acrKey) {
					t.Fatalf("rc=%d stdout=%q stderr=%q", r.rc, r.stdout, r.stderr)
				}
			})
		}
	}
}
