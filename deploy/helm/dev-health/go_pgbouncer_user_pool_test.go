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

var poolSizeKeys = map[string][]string{
	"extra user":  nil,
	"transaction": {"--set-json", "goWorkers.pgbouncer.transaction.poolSize=%s"},
	"queue":       {"--set-json", "goWorkers.pgbouncer.queueSession.poolSize=%s"},
	"coordinator": {"--set-json", "goWorkers.pgbouncer.coordinatorSession.poolSize=%s"},
}

func poolSizeArgs(name, size string) []string {
	if name == "extra user" {
		return extraUsersJSON(`{"role":"devhealth_keda_readonly","passwordKey":"RIVER_KEDA_READONLY_PASSWORD","poolSize":` + size + `}`)
	}
	set := poolSizeKeys[name]
	return []string{set[0], strings.Replace(set[1], "%s", size, 1)}
}

// A pool size above 1000 is refused by the schema, and by the budget helper
// itself when the schema did not run, so the budget sum cannot wrap.
func TestPoolSizeIsBoundedBySchemaAndByTheBudgetHelper(t *testing.T) {
	for name := range poolSizeKeys {
		for _, size := range []string{"1001", "9223372036854775807"} {
			_, out, err := renderAuth(t, authWith(poolSizeArgs(name, size)...)...)
			if err == nil || !strings.Contains(out, "poolSize") || !strings.Contains(out, "schema") {
				t.Errorf("%s %s: want a schema refusal naming poolSize: err=%v out=%.300s", name, size, err, out)
			}
			args := append(authWith("--set", "authService.database.connectionBudget=97", "--skip-schema-validation"), poolSizeArgs(name, size)...)
			_, out, err = renderAuth(t, args...)
			if err == nil || !strings.Contains(out, "poolSize must be a whole number from 1 to 1000") {
				t.Errorf("%s %s without the schema: want the helper refusal naming the key: err=%v out=%.300s", name, size, err, out)
			}
		}
	}
}

func TestAuthServiceReplicasAreBoundedForTheBudget(t *testing.T) {
	for _, size := range []string{"101", "9223372036854775807"} {
		_, out, err := renderAuth(t, authWith("--set-json", "authService.replicas="+size)...)
		if err == nil || !strings.Contains(out, "replicas") {
			t.Errorf("%s: want a schema refusal naming replicas: err=%v out=%.300s", size, err, out)
		}
		_, out, err = renderAuth(t, authWith("--set-json", "authService.replicas="+size, "--set", "authService.database.connectionBudget=97", "--skip-schema-validation")...)
		if err == nil || !strings.Contains(out, "authService.replicas must be a whole number from 0 to 100") {
			t.Errorf("%s without the schema: want the helper refusal: err=%v out=%.300s", size, err, out)
		}
	}
}

// 1000 is the largest accepted size; it still fails a 97 budget with the sum shown.
func TestPoolSizeAtTheBoundStillFailsASmallBudget(t *testing.T) {
	if _, out, err := renderAuth(t, authWith(poolSizeArgs("extra user", "1000")...)...); err != nil {
		t.Fatalf("1000 renders with the budget off: %v\n%.300s", err, out)
	}
	_, out, err := renderAuth(t, authWith(append(poolSizeArgs("extra user", "1000"), "--set", "authService.database.connectionBudget=97")...)...)
	// 2 x (16 + 1000) + 50 + 14 + (2+1) x 8 = 2120
	if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "2120") {
		t.Fatalf("1000 must fail the 97 budget with the sum 2120: err=%v out=%.300s", err, out)
	}
}
