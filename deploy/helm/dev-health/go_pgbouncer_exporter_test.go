package devhealth_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The transaction pooler's optional pgbouncer_exporter sidecar (SHOW POOLS: cl_waiting per pool). Off by default, and then the
// pooler objects render byte-for-byte as before the knob existed (golden recorded from the chart without it). On, it is a second
// container in the transaction pod only, credential-free (console login over a pod-local Unix socket), digest-pinned, without
// probes, and its command is a supervisor loop so a failing exporter binary never ends the container (a pod is Ready only when
// every container is: an exiting exporter took the pooler out of its Service in the k8s proof).

const exporterTestImage = "quay.io/prometheuscommunity/pgbouncer-exporter:v0.12.1@sha256:30f31b6c2efdad3647f8182cc7c1a3a19e42bae5d17387694989f969371c230d"

var exporterOn = []string{
	"--set", "goWorkers.pgbouncer.transaction.exporter.enabled=true",
	"--set", "goWorkers.pgbouncer.transaction.exporter.image=" + exporterTestImage,
}

var collectorSelector = `[{"matchLabels":{"app.kubernetes.io/name":"otel-collector","app.kubernetes.io/component":"collector"}}]`

var poolerNames = []string{
	"a-dev-health-go-pgbouncer-transaction",
	"a-dev-health-go-pgbouncer-queue-session",
	"a-dev-health-go-pgbouncer-coordinator-session",
}

func renderPoolers(t *testing.T, extra ...string) []map[string]any {
	t.Helper()
	docs, out, err := renderAuth(t, authWith(append(append([]string{}, authNetworkPolicy...), extra...)...)...)
	if err != nil {
		t.Fatalf("render: %v\n%.600s", err, out)
	}
	return docs
}

// poolerSnapshot: every object of the three poolers (Deployment, Service, NetworkPolicy) and the users ConfigMap, in a fixed order.
func poolerSnapshot(docs []map[string]any) []map[string]any {
	var snap []map[string]any
	for _, name := range poolerNames {
		for _, kind := range []string{"Deployment", "Service", "NetworkPolicy"} {
			if d := authDoc(docs, kind, name); d != nil {
				snap = append(snap, d)
			}
		}
	}
	if cm := authDoc(docs, "ConfigMap", usersConfigMap); cm != nil {
		snap = append(snap, cm)
	}
	return snap
}

func containerNamed(pod any, name string) any {
	cs, _ := at(pod, "containers").([]any)
	for _, c := range cs {
		if at(c, "name") == name {
			return c
		}
	}
	return nil
}

func TestPgbouncerExporterOffByDefaultKeepsThePoolerObjects(t *testing.T) {
	docs := renderPoolers(t)
	raw, err := json.MarshalIndent(poolerSnapshot(docs), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	path := filepath.Join("testdata", "pgbouncer_exporter", "poolers-default.golden.json")
	if os.Getenv("PGBOUNCER_EXPORTER_UPDATE_GOLDEN") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (PGBOUNCER_EXPORTER_UPDATE_GOLDEN=1 writes it from the chart without the exporter): %v", err)
	}
	if string(want) != string(raw) {
		t.Fatalf("with the exporter off the pooler objects differ from %s:\n got %s\nwant %s", path, raw, want)
	}
	pod := at(authDoc(docs, "Deployment", transactionPooler), "spec", "template", "spec")
	if cs, _ := at(pod, "containers").([]any); len(cs) != 1 {
		t.Errorf("off: the transaction pod must have one container, got %d", len(cs))
	}
	if at(pod, "initContainers") != nil {
		t.Error("off: no initContainers")
	}
	for _, v := range at(pod, "volumes").([]any) {
		if at(v, "name") == "console-socket" {
			t.Error("off: no console-socket volume")
		}
	}
	if envNames(containerNamed(pod, "pgbouncer"))["UNIX_SOCKET_DIR"] {
		t.Error("off: no UNIX_SOCKET_DIR env")
	}
}

func TestPgbouncerExporterOnIsASupervisedCredentialFreeSidecar(t *testing.T) {
	docs := renderPoolers(t, exporterOn...)
	pod := at(authDoc(docs, "Deployment", transactionPooler), "spec", "template", "spec")
	cs, _ := at(pod, "containers").([]any)
	if len(cs) != 2 || at(cs[0], "name") != "pgbouncer" || at(cs[1], "name") != "exporter" {
		t.Fatalf("on: containers must be [pgbouncer exporter], got %v", cs)
	}
	if at(pod, "initContainers") != nil {
		t.Error("on: the exporter is a regular container, never an init container")
	}
	e := cs[1]
	if at(e, "image") != exporterTestImage {
		t.Errorf("exporter image = %v", at(e, "image"))
	}
	cmd, _ := at(e, "command").([]any)
	if len(cmd) != 4 || cmd[0] != "/bin/sh" || cmd[1] != "-c" {
		t.Fatalf("exporter command must be /bin/sh -c <loop> <name>, got %v", cmd)
	}
	loop, _ := cmd[2].(string)
	for _, want := range []string{`trap 'kill "$p" 2>/dev/null; exit 0' TERM INT`, `while :; do /bin/pgbouncer_exporter "$@" & p=$!; wait "$p"`, `echo "exporter exited rc=$?"`, `sleep 5`} {
		if !strings.Contains(loop, want) {
			t.Errorf("the supervisor loop must contain %q, got %q", want, loop)
		}
	}
	args, _ := at(e, "args").([]any)
	wantArgs := []any{"--pgBouncer.connectionString=user=pgbouncer dbname=pgbouncer host=/var/run/pgbouncer port=6432 sslmode=disable", "--web.listen-address=:9127"}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("exporter args = %v, want %v", args, wantArgs)
	}
	for _, probe := range []string{"readinessProbe", "livenessProbe", "startupProbe"} {
		if at(e, probe) != nil {
			t.Errorf("the exporter must have no %s: its state must never gate the pooler's readiness", probe)
		}
	}
	if at(e, "env") != nil || at(e, "envFrom") != nil {
		t.Error("the exporter carries no env: it needs no credential")
	}
	if at(e, "ports", 0, "containerPort") != 9127 || at(e, "ports", 0, "name") != "metrics" {
		t.Errorf("exporter port = %v", at(e, "ports"))
	}
	sc := at(e, "securityContext")
	if at(sc, "readOnlyRootFilesystem") != true || at(sc, "allowPrivilegeEscalation") != false || !reflect.DeepEqual(at(sc, "capabilities", "drop"), []any{"ALL"}) {
		t.Errorf("exporter securityContext = %v", sc)
	}
	if at(e, "resources", "requests", "cpu") != "10m" || at(e, "resources", "requests", "memory") != "16Mi" ||
		at(e, "resources", "limits", "cpu") != "50m" || at(e, "resources", "limits", "memory") != "64Mi" {
		t.Errorf("exporter resources = %v", at(e, "resources"))
	}
	em, _ := at(e, "volumeMounts").([]any)
	if len(em) != 1 || at(em[0], "name") != "console-socket" || at(em[0], "mountPath") != "/var/run/pgbouncer" || at(em[0], "readOnly") != true {
		t.Errorf("the exporter mounts only the console socket, read-only: %v", em)
	}
	p := cs[0]
	gotDir := ""
	for _, ev := range at(p, "env").([]any) {
		if at(ev, "name") == "UNIX_SOCKET_DIR" {
			gotDir, _ = at(ev, "value").(string)
		}
	}
	if gotDir != "/var/run/pgbouncer" {
		t.Errorf("pgbouncer UNIX_SOCKET_DIR = %q", gotDir)
	}
	rw := false
	for _, m := range at(p, "volumeMounts").([]any) {
		if at(m, "name") == "console-socket" && at(m, "mountPath") == "/var/run/pgbouncer" && at(m, "readOnly") == nil {
			rw = true
		}
	}
	if !rw {
		t.Error("pgbouncer mounts the console socket dir read-write (it creates the socket)")
	}
	found := false
	for _, v := range at(pod, "volumes").([]any) {
		if at(v, "name") == "console-socket" {
			found = true
			if ed, ok := at(v, "emptyDir").(map[string]any); !ok || len(ed) != 0 {
				t.Errorf("console-socket must be a plain emptyDir: %v", v)
			}
		}
	}
	if !found {
		t.Error("no console-socket volume")
	}
}

func TestPgbouncerExporterChangesOnlyTheTransactionPod(t *testing.T) {
	off := renderPoolers(t)
	on := renderPoolers(t, exporterOn...)
	if len(off) != len(on) {
		t.Fatalf("the exporter adds or removes objects: %d -> %d", len(off), len(on))
	}
	for i := range off {
		meta, _ := off[i]["metadata"].(map[string]any)
		if off[i]["kind"] == "Deployment" && meta["name"] == transactionPooler {
			continue
		}
		if !reflect.DeepEqual(off[i], on[i]) {
			t.Errorf("%v %v changes with the exporter on (no metricsFrom): only the transaction Deployment may", off[i]["kind"], meta["name"])
		}
	}
}

func TestPgbouncerExporterImageMustBeDigestPinned(t *testing.T) {
	for _, image := range []string{"", "quay.io/prometheuscommunity/pgbouncer-exporter:v0.12.1", "quay.io/prometheuscommunity/pgbouncer-exporter@sha256:not-a-digest"} {
		_, out, err := renderAuth(t, authWith("--set", "goWorkers.pgbouncer.transaction.exporter.enabled=true", "--set", "goWorkers.pgbouncer.transaction.exporter.image="+image)...)
		if err == nil || !strings.Contains(out, "goWorkers.pgbouncer.transaction.exporter.image") {
			t.Errorf("image %q must be refused naming the key, got err=%v\n%.300s", image, err, out)
		}
	}
	if _, out, err := renderAuth(t, authWith(exporterOn...)...); err != nil {
		t.Errorf("a tag@digest image must render: %v\n%.300s", err, out)
	}
}

func TestPgbouncerExporterMetricsFromAdmitsOnlyTheScraperOnItsPort(t *testing.T) {
	docs := renderPoolers(t, append(append([]string{}, exporterOn...), "--set-json", "goWorkers.pgbouncer.transaction.exporter.metricsFrom="+collectorSelector)...)
	off := renderPoolers(t)
	np := authDoc(docs, "NetworkPolicy", transactionPooler)
	var rule any
	ruleAt := -1
	for i, r := range at(np, "spec", "ingress").([]any) {
		if at(r, "ports", 0, "port") == 9127 {
			rule, ruleAt = r, i
		}
	}
	if rule == nil {
		t.Fatalf("no ingress rule for the exporter port: %v", at(np, "spec", "ingress"))
	}
	if !reflect.DeepEqual(at(rule, "from"), []any{map[string]any{"podSelector": map[string]any{"matchLabels": map[string]any{"app.kubernetes.io/name": "otel-collector", "app.kubernetes.io/component": "collector"}}}}) {
		t.Errorf("exporter rule from = %v", at(rule, "from"))
	}
	if ps, _ := at(rule, "ports").([]any); len(ps) != 1 || at(ps[0], "protocol") != "TCP" {
		t.Errorf("exporter rule ports = %v", at(rule, "ports"))
	}
	var rest []any
	for i, r := range at(np, "spec", "ingress").([]any) {
		if i != ruleAt {
			rest = append(rest, r)
		}
	}
	if !reflect.DeepEqual(rest, at(authDoc(off, "NetworkPolicy", transactionPooler), "spec", "ingress")) {
		t.Error("the pooler port rules must stay as they are")
	}
	for _, name := range poolerNames[1:] {
		if !reflect.DeepEqual(authDoc(docs, "NetworkPolicy", name), authDoc(off, "NetworkPolicy", name)) {
			t.Errorf("%s NetworkPolicy must not change", name)
		}
	}
	noFrom := renderPoolers(t, exporterOn...)
	for _, r := range at(authDoc(noFrom, "NetworkPolicy", transactionPooler), "spec", "ingress").([]any) {
		if at(r, "ports", 0, "port") == 9127 {
			t.Error("no metricsFrom: no rule for the exporter port")
		}
	}
	_, out, err := renderAuth(t, authWith(append(append(append([]string{}, authNetworkPolicy...), exporterOn...), "--set-json", `goWorkers.pgbouncer.transaction.exporter.metricsFrom=[{}]`)...)...)
	if err == nil || !strings.Contains(out, "metricsFrom") {
		t.Errorf("an empty selector must be refused: err=%v\n%.300s", err, out)
	}
}

func TestPgbouncerExporterSchemaRefusesUnknownKeys(t *testing.T) {
	for _, set := range []string{"goWorkers.pgbouncer.transaction.exporter.bogus=1", "goWorkers.pgbouncer.transaction.exporter.port=80"} {
		_, out, err := renderAuth(t, authWith(append(append([]string{}, exporterOn...), "--set", set)...)...)
		if err == nil {
			t.Errorf("%s must be refused by the schema\n%.300s", set, out)
		}
	}
}

// The exporter shares the pod network namespace with PgBouncer: its listen port must not be the pooler port.
func TestPgbouncerExporterPortMustDifferFromThePoolerPort(t *testing.T) {
	const msg = "goWorkers.pgbouncer.transaction.exporter.port must differ from the transaction pooler port"
	for _, set := range [][]string{
		{"--set", "goWorkers.pgbouncer.transaction.exporter.port=6432"},
		{"--set", "goWorkers.pgbouncer.transaction.port=9127"},
	} {
		_, out, err := renderAuth(t, authWith(append(append([]string{}, exporterOn...), set...)...)...)
		if err == nil || !strings.Contains(out, msg) {
			t.Errorf("%v: the render must fail with %q, got err=%v\n%.300s", set, msg, err, out)
		}
	}
	docs, out, err := renderAuth(t, authWith(append(append([]string{}, exporterOn...), "--set", "goWorkers.pgbouncer.transaction.exporter.port=9127")...)...)
	if err != nil {
		t.Fatalf("exporter port 9127 must render: %v\n%.300s", err, out)
	}
	if at(containerNamed(at(authDoc(docs, "Deployment", transactionPooler), "spec", "template", "spec"), "exporter"), "ports", 0, "containerPort") != 9127 {
		t.Error("the exporter must listen on 9127")
	}
	if _, out, err := renderAuth(t, authWith("--set", "goWorkers.pgbouncer.transaction.exporter.port=6432")...); err != nil {
		t.Errorf("a disabled exporter is not checked: %v\n%.300s", err, out)
	}
}
