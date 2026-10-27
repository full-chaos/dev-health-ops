package devhealth_test

import (
	"reflect"
	"strings"
	"testing"
)

// The bundled PostgreSQL reads its client authentication from the file initdb
// wrote on the volume, which trusts the local socket and loopback: any process in
// the pod connects as any role without a password. postgresql.hba.enabled points
// the server at the chart's own pg_hba.conf, which asks every client for a
// scram-sha-256 password. The ConfigMap renders with the store, so a chart pin
// adds only that object; turning the switch on changes only the StatefulSet.

var (
	pgStoreOn = append(append([]string{}, authBase...), "--set", "postgresql.enabled=true")
	pgHbaOn   = append(append([]string{}, pgStoreOn...), "--set", "postgresql.hba.enabled=true")
)

const (
	pgHbaConfigMap = "a-dev-health-postgresql-hba"
	pgHbaFileArg   = "hba_file=/etc/postgresql/hba/pg_hba.conf"
	pgHbaMountPath = "/etc/postgresql/hba"
)

func postgresRender(t *testing.T, args ...string) (map[string]any, map[string]any) {
	t.Helper()
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	return authDoc(docs, "StatefulSet", "a-dev-health-postgresql"), authDoc(docs, "ConfigMap", pgHbaConfigMap)
}

func postgresContainer(t *testing.T, sts map[string]any) any {
	t.Helper()
	if sts == nil {
		t.Fatal("no postgresql StatefulSet rendered")
	}
	c := at(sts, "spec", "template", "spec", "containers", 0)
	if at(c, "name") != "postgresql" {
		t.Fatalf("container 0 is %v, want postgresql", at(c, "name"))
	}
	return c
}

func namedEntry(list any, name string) any {
	items, _ := list.([]any)
	for _, item := range items {
		if at(item, "name") == name {
			return item
		}
	}
	return nil
}

func TestPostgresHbaConfigMapRendersWithTheStoreAndIsInertByDefault(t *testing.T) {
	requireHelmUnderCI(t)
	if _, cm := postgresRender(t, authBase...); cm != nil {
		t.Errorf("ConfigMap %s renders while postgresql is disabled", pgHbaConfigMap)
	}
	sts, cm := postgresRender(t, pgStoreOn...)
	if cm == nil {
		t.Fatalf("ConfigMap %s does not render with postgresql enabled", pgHbaConfigMap)
	}
	if hba, _ := at(cm, "data", "pg_hba.conf").(string); hba == "" {
		t.Errorf("ConfigMap %s has no pg_hba.conf key", pgHbaConfigMap)
	}
	c := postgresContainer(t, sts)
	if args := at(c, "args"); args != nil {
		t.Errorf("postgresql args = %v with postgresql.hba.enabled unset, want none", args)
	}
	if v := at(sts, "spec", "template", "spec", "volumes"); v != nil {
		t.Errorf("postgresql pod volumes = %v with postgresql.hba.enabled unset, want none", v)
	}
	mounts, _ := at(c, "volumeMounts").([]any)
	if len(mounts) != 1 || at(mounts[0], "name") != "postgres-data" {
		t.Errorf("postgresql volumeMounts = %v, want only postgres-data", mounts)
	}
}

func TestPostgresHbaOnPointsTheServerAtTheChartFile(t *testing.T) {
	requireHelmUnderCI(t)
	cases := map[string][]string{
		"persistence on":  pgHbaOn,
		"persistence off": append(append([]string{}, pgHbaOn...), "--set", "postgresql.persistence.enabled=false"),
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			sts, cm := postgresRender(t, args...)
			if cm == nil {
				t.Fatalf("ConfigMap %s does not render", pgHbaConfigMap)
			}
			c := postgresContainer(t, sts)
			if got, want := toStrings(at(c, "args")), []string{"-c", pgHbaFileArg}; !reflect.DeepEqual(got, want) {
				t.Errorf("postgresql args = %q, want %q", got, want)
			}
			mount := namedEntry(at(c, "volumeMounts"), "pg-hba")
			if at(mount, "mountPath") != pgHbaMountPath || at(mount, "readOnly") != true {
				t.Errorf("pg-hba mount = %v, want %s read-only", mount, pgHbaMountPath)
			}
			if path, _ := at(mount, "mountPath").(string); "hba_file="+path+"/pg_hba.conf" != pgHbaFileArg {
				t.Errorf("the pg-hba mount %q does not hold the file %s names", path, pgHbaFileArg)
			}
			volume := namedEntry(at(sts, "spec", "template", "spec", "volumes"), "pg-hba")
			if at(volume, "configMap", "name") != pgHbaConfigMap {
				t.Errorf("pg-hba volume = %v, want ConfigMap %s", volume, pgHbaConfigMap)
			}
			persisted := namedEntry(at(c, "volumeMounts"), "postgres-data") != nil
			if want := name == "persistence on"; persisted != want {
				t.Errorf("postgres-data mounted = %v, want %v", persisted, want)
			}
		})
	}
}

// Exactly these rules: the socket and both loopback addresses are named so a
// probe or script in the pod is held to the same password as the pod network.
// No replication line: the store has no replica.
func TestPostgresHbaRulesAreScramOnly(t *testing.T) {
	requireHelmUnderCI(t)
	_, cm := postgresRender(t, pgHbaOn...)
	hba, _ := at(cm, "data", "pg_hba.conf").(string)
	var rules [][]string
	for _, line := range strings.Split(hba, "\n") {
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		if fields := strings.Fields(line); len(fields) > 0 {
			rules = append(rules, fields)
		}
	}
	want := [][]string{
		{"local", "all", "all", "scram-sha-256"},
		{"host", "all", "all", "127.0.0.1/32", "scram-sha-256"},
		{"host", "all", "all", "::1/128", "scram-sha-256"},
		{"host", "all", "all", "0.0.0.0/0", "scram-sha-256"},
		{"host", "all", "all", "::/0", "scram-sha-256"},
	}
	if !reflect.DeepEqual(rules, want) {
		t.Errorf("pg_hba.conf rules = %q, want %q", rules, want)
	}
	for _, rule := range rules {
		switch method := rule[len(rule)-1]; method {
		case "trust", "md5", "password", "peer", "ident", "reject", "cert":
			t.Errorf("rule %q uses %s", rule, method)
		}
	}
}

// The flip is a data-store step of its own: it changes the postgresql
// StatefulSet and no other document.
func TestPostgresHbaSwitchChangesOnlyThePostgresStatefulSet(t *testing.T) {
	requireHelmUnderCI(t)
	got := changedDocs(t, pgStoreOn, pgHbaOn)
	if want := []string{"StatefulSet/a-dev-health-postgresql"}; !reflect.DeepEqual(got, want) {
		t.Errorf("documents changed by postgresql.hba.enabled = %v, want %v", got, want)
	}
}
