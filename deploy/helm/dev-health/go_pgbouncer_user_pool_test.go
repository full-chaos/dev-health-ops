package devhealth_test

import (
	"strings"
	"testing"
)

const (
	userPoolSecretsets = "goWorkers.pgbouncer.secret.data.K1=x"
	transactionPooler  = "a-dev-health-go-pgbouncer-transaction"
	usersConfigMap     = "a-dev-health-go-pgbouncer-transaction-users"
)

func extraUsersJSON(entries string) []string {
	return []string{"--set-json", "goWorkers.pgbouncer.transaction.extraUsers=[" + entries + "]", "--set", userPoolSecretsets}
}

func poolerContainer(docs []map[string]any, name string) any {
	return at(authDoc(docs, "Deployment", name), "spec", "template", "spec", "containers", 0)
}

func TestPgbouncerExtraUserPoolSizeRendersUsersFragment(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	cm := authDoc(docs, "ConfigMap", usersConfigMap)
	if cm == nil {
		t.Fatal("the default KEDA user has a pool size, so its [users] file must render")
	}
	want := "%include /etc/pgbouncer/pgbouncer.ini\n[users]\ndevhealth_keda_readonly = pool_size=4\n"
	if got := at(cm, "data", "pgbouncer.ini"); got != want {
		t.Fatalf("users ini = %q, want %q", got, want)
	}
	c := poolerContainer(docs, transactionPooler)
	args, _ := at(c, "args").([]any)
	if len(args) != 2 || args[0] != "/usr/bin/pgbouncer" || args[1] != "/etc/pgbouncer-users/pgbouncer.ini" {
		t.Fatalf("transaction pooler args = %v, want the users ini as its config", args)
	}
	mounts, _ := at(c, "volumeMounts").([]any)
	found := false
	for _, m := range mounts {
		if at(m, "name") == "users" && at(m, "mountPath") == "/etc/pgbouncer-users" && at(m, "readOnly") == true {
			found = true
		}
	}
	if !found {
		t.Fatalf("the users ini is not mounted read-only: %v", mounts)
	}
	if sum, _ := at(authDoc(docs, "Deployment", transactionPooler), "spec", "template", "metadata", "annotations", "checksum/users-ini").(string); sum == "" {
		t.Fatal("the pod template must carry a checksum of the users ini so a size change rolls the pooler")
	}
	for _, name := range []string{"a-dev-health-go-pgbouncer-queue-session", "a-dev-health-go-pgbouncer-coordinator-session"} {
		if a := at(poolerContainer(docs, name), "args"); a != nil {
			t.Errorf("%s must keep the image default args, got %v", name, a)
		}
	}
	env := envNames(c)
	if !env["DEFAULT_POOL_SIZE"] {
		t.Error("the own role keeps DEFAULT_POOL_SIZE")
	}
}

func TestPgbouncerExtraUsersWithoutPoolSizeRenderNothingNew(t *testing.T) {
	docs, out, err := renderAuth(t, authWith(extraUsersJSON(`{"role":"devhealth_keda_readonly","passwordKey":"RIVER_KEDA_READONLY_PASSWORD"}`)...)...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	if authDoc(docs, "ConfigMap", usersConfigMap) != nil {
		t.Error("no user has a pool size: no users ConfigMap")
	}
	c := poolerContainer(docs, transactionPooler)
	if a := at(c, "args"); a != nil {
		t.Errorf("args must stay the image default, got %v", a)
	}
	for _, m := range at(c, "volumeMounts").([]any) {
		if at(m, "name") == "users" {
			t.Error("no users mount without a sized user")
		}
	}
	if at(authDoc(docs, "Deployment", transactionPooler), "spec", "template", "metadata", "annotations", "checksum/users-ini") != nil {
		t.Error("no checksum annotation without a sized user")
	}
}

func TestPgbouncerExtraUserPoolSizeChangesTheChecksum(t *testing.T) {
	sum := func(size string) string {
		docs, out, err := renderAuth(t, authWith(extraUsersJSON(`{"role":"devhealth_keda_readonly","passwordKey":"RIVER_KEDA_READONLY_PASSWORD","poolSize":`+size+`}`)...)...)
		if err != nil {
			t.Fatalf("render: %v\n%.400s", err, out)
		}
		s, _ := at(authDoc(docs, "Deployment", transactionPooler), "spec", "template", "metadata", "annotations", "checksum/users-ini").(string)
		return s
	}
	if a, b := sum("4"), sum("5"); a == "" || a == b {
		t.Fatalf("checksum must differ per size: %q vs %q", a, b)
	}
}

func TestPgbouncerExtraUserPoolSizeIsValidated(t *testing.T) {
	for name, entry := range map[string]string{
		"zero":     `{"role":"r1","passwordKey":"K1","poolSize":0}`,
		"negative": `{"role":"r1","passwordKey":"K1","poolSize":-1}`,
		"string":   `{"role":"r1","passwordKey":"K1","poolSize":"4"}`,
		"fraction": `{"role":"r1","passwordKey":"K1","poolSize":1.5}`,
	} {
		_, out, err := renderAuth(t, authWith(extraUsersJSON(`{"role":"devhealth_keda_readonly","passwordKey":"RIVER_KEDA_READONLY_PASSWORD"},`+entry)...)...)
		if err == nil || !strings.Contains(out, "poolSize") {
			t.Errorf("%s: want a schema refusal naming poolSize: err=%v out=%.300s", name, err, out)
		}
	}
	_, out, err := renderAuth(t, authWith(extraUsersJSON(`{"role":"devhealth_keda_readonly","passwordKey":"RIVER_KEDA_READONLY_PASSWORD"},{"role":"bad role","passwordKey":"K1","poolSize":2}`)...)...)
	if err == nil || !strings.Contains(out, "transaction.extraUsers") || !strings.Contains(out, "bad role") {
		t.Errorf("a sized role that cannot be a [users] key must fail naming the entry: err=%v out=%.300s", err, out)
	}
}
