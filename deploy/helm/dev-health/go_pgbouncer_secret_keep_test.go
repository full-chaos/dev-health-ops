package devhealth_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The chart-owned PgBouncer credentials Secret is kept on `helm uninstall`/removal, and moving it to an operator-owned
// Secret with the SAME name (secret.create=false + externalSecretName) changes no pod template. Every render compare
// below shows the document count per side, the helm rc and an independent `diff -u`; an empty render never passes.

const (
	keepSecretName = "a-dev-health-go-pgbouncer"
	keepPolicyKey  = "helm.sh/resource-policy"
)

var keepDummyPasswords = []string{
	"--set", "goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=dummy-domain-0001",
	"--set", "goWorkers.pgbouncer.secret.data.RIVER_QUEUE_DATABASE_PASSWORD=dummy-queue-0002",
	"--set", "goWorkers.pgbouncer.secret.data.RIVER_COORDINATOR_DATABASE_PASSWORD=dummy-coord-0003",
	"--set", "goWorkers.pgbouncer.secret.data.RIVER_KEDA_READONLY_PASSWORD=dummy-keda-0004",
}

var keepEmptyPasswords = []string{
	"--set", "goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=",
	"--set", "goWorkers.pgbouncer.secret.data.RIVER_QUEUE_DATABASE_PASSWORD=",
	"--set", "goWorkers.pgbouncer.secret.data.RIVER_COORDINATOR_DATABASE_PASSWORD=",
	"--set", "goWorkers.pgbouncer.secret.data.RIVER_KEDA_READONLY_PASSWORD=",
}

var keepExternalSameName = []string{
	"--set", "goWorkers.pgbouncer.secret.create=false",
	"--set", "goWorkers.pgbouncer.secret.externalSecretName=" + keepSecretName,
}

type keepRender struct {
	label string
	raw   string
	rc    int
	docs  []map[string]any
}

func keepHelm(t *testing.T, label string, args ...string) keepRender {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	cmd := exec.Command("helm", append([]string{"template", "a", "."}, args...)...)
	var stdout strings.Builder
	cmd.Stdout = &stdout
	rc := 0
	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			rc = ee.ExitCode()
		} else {
			t.Fatalf("%s: helm did not run: %v", label, err)
		}
	}
	r := keepRender{label: label, raw: stdout.String(), rc: rc}
	dec := yaml.NewDecoder(strings.NewReader(r.raw))
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			break
		}
		if doc != nil {
			r.docs = append(r.docs, doc)
		}
	}
	return r
}

func keepMustRender(t *testing.T, label string, args ...string) keepRender {
	t.Helper()
	r := keepHelm(t, label, args...)
	t.Logf("%s: helm rc=%d docs=%d", label, r.rc, len(r.docs))
	if r.rc != 0 || len(r.docs) == 0 {
		t.Fatalf("%s: helm rc=%d, %d documents: an empty or failed render is never a pass", label, r.rc, len(r.docs))
	}
	return r
}

// keepWorkloadText is every pod-bearing document, sorted by kind/name, as the text helm printed.
func keepWorkloadText(r keepRender) (string, int) {
	var parts []string
	for _, d := range r.docs {
		switch d["kind"] {
		case "Deployment", "StatefulSet", "Job", "CronJob":
			b, _ := yaml.Marshal(d)
			parts = append(parts, keepStr(at(d, "kind"))+"/"+keepStr(at(d, "metadata", "name"))+"\n"+string(b))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "\n---\n"), len(parts)
}

func keepUnifiedDiff(t *testing.T, a, b string) (string, int) {
	t.Helper()
	dir := t.TempDir()
	pa, pb := filepath.Join(dir, "a.yaml"), filepath.Join(dir, "b.yaml")
	if err := os.WriteFile(pa, []byte(a), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pb, []byte(b), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("diff", "-u", pa, pb).CombinedOutput()
	rc := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			rc = ee.ExitCode()
		} else {
			t.Fatalf("diff did not run: %v", err)
		}
	}
	return string(out), rc
}

func keepStr(v any) string {
	s, _ := v.(string)
	return s
}

func keepSecret(r keepRender) map[string]any { return authDoc(r.docs, "Secret", keepSecretName) }

func TestPgbouncerSecretCarriesTheKeepPolicy(t *testing.T) {
	r := keepMustRender(t, "create (chart-owned Secret)", keepDummyPasswords...)
	s := keepSecret(r)
	if s == nil {
		t.Fatalf("the chart-owned Secret %s did not render (%d documents)", keepSecretName, len(r.docs))
	}
	if got := at(s, "metadata", "annotations", keepPolicyKey); got != "keep" {
		t.Fatalf("Secret %s annotation %s = %v, want keep", keepSecretName, keepPolicyKey, got)
	}
}

func TestPgbouncerExternalSameNameChangesNoPodTemplate(t *testing.T) {
	create := keepMustRender(t, "create", keepDummyPasswords...)
	external := keepMustRender(t, "external same name", append(append([]string{}, keepDummyPasswords...), keepExternalSameName...)...)
	if keepSecret(create) == nil {
		t.Fatalf("create side: Secret %s missing", keepSecretName)
	}
	if keepSecret(external) != nil {
		t.Fatalf("external side: Secret %s still rendered", keepSecretName)
	}
	if len(external.docs) != len(create.docs)-1 {
		t.Fatalf("documents: create=%d external=%d, want exactly the Secret removed", len(create.docs), len(external.docs))
	}
	a, na := keepWorkloadText(create)
	b, nb := keepWorkloadText(external)
	t.Logf("workloads compared: create=%d external=%d", na, nb)
	if na == 0 || nb == 0 || na != nb {
		t.Fatalf("workload documents: create=%d external=%d: an empty or unequal side cannot pass", na, nb)
	}
	if out, rc := keepUnifiedDiff(t, a, b); rc != 0 {
		t.Fatalf("diff -u of the %d workloads: rc=%d, a pod template differs:\n%.1500s", na, rc, out)
	}
	for i := range create.docs {
		d := create.docs[i]
		if d["kind"] == "Deployment" || d["kind"] == "StatefulSet" || d["kind"] == "Job" || d["kind"] == "CronJob" {
			name := keepStr(at(d, "metadata", "name"))
			e := authDoc(external.docs, keepStr(d["kind"]), name)
			if e == nil {
				t.Fatalf("%s/%s is missing on the external side", d["kind"], name)
			}
			ad, _ := yaml.Marshal(d["spec"])
			ae, _ := yaml.Marshal(e["spec"])
			if string(ad) != string(ae) {
				t.Errorf("%s/%s: spec differs between create and external", d["kind"], name)
			}
		}
	}
}

func TestPgbouncerExternalModeRendersWithEmptyPasswordValues(t *testing.T) {
	ext := keepMustRender(t, "external same name, empty values", append(append([]string{}, keepEmptyPasswords...), keepExternalSameName...)...)
	withValues := keepMustRender(t, "external same name, dummy values", append(append([]string{}, keepDummyPasswords...), keepExternalSameName...)...)
	if keepSecret(ext) != nil {
		t.Fatalf("external mode still rendered Secret %s", keepSecretName)
	}
	if len(ext.docs) != len(withValues.docs) {
		t.Fatalf("documents: empty values=%d, dummy values=%d", len(ext.docs), len(withValues.docs))
	}
	if out, rc := keepUnifiedDiff(t, ext.raw, withValues.raw); rc != 0 {
		t.Fatalf("diff -u: the password values change the external render (rc=%d):\n%.1500s", rc, out)
	}
	ctl := keepHelm(t, "control: create with empty values", keepEmptyPasswords...)
	t.Logf("control: helm rc=%d docs=%d", ctl.rc, len(ctl.docs))
	if ctl.rc == 0 || len(ctl.docs) != 0 {
		t.Fatalf("control: create=true with empty passwords must fail (required); rc=%d docs=%d", ctl.rc, len(ctl.docs))
	}
}
