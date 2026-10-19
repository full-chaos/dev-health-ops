package venueoracle

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExternalStoresValidate(t *testing.T) {
	good := ExternalStores{PostgresURI: "postgres://u:p@h:5432/authdb", ClickHouseURI: "clickhouse://u:p@h:9000/authdb", ValkeyURI: "redis://h:6379/1"}
	if err := good.Validate(true); err != nil {
		t.Fatalf("complete stores refused: %v", err)
	}
	if good.Validate(false) == nil {
		t.Error("external stores were accepted for a live venue: the Python plane needs its own copy")
	}
	for name, mutate := range map[string]func(*ExternalStores){
		"no postgres":                     func(e *ExternalStores) { e.PostgresURI = "" },
		"no clickhouse":                   func(e *ExternalStores) { e.ClickHouseURI = "" },
		"no valkey":                       func(e *ExternalStores) { e.ValkeyURI = "" },
		"postgres no db":                  func(e *ExternalStores) { e.PostgresURI = "postgres://u:p@h:5432" },
		"clickhouse no db":                func(e *ExternalStores) { e.ClickHouseURI = "clickhouse://u:p@h:9000" },
		"valkey malformed":                func(e *ExternalStores) { e.ValkeyURI = "not a url" },
		"valkey wrong scheme":             func(e *ExternalStores) { e.ValkeyURI = "http://h:6379/1" },
		"clickhouse http scheme":          func(e *ExternalStores) { e.ClickHouseURI = "http://h:8123/authdb" },
		"clickhouse extra path":           func(e *ExternalStores) { e.ClickHouseURI = "clickhouse://u:p@h:9000/authdb/extra" },
		"clickhouse conflicting database": func(e *ExternalStores) { e.ClickHouseURI = "clickhouse://u:p@h:9000/authdb?database=other" },
		"postgres extra path":             func(e *ExternalStores) { e.PostgresURI = "postgres://u:p@h:5432/authdb/extra" },
		"postgres bad uri":                func(e *ExternalStores) { e.PostgresURI = "://" },
	} {
		stores := good
		mutate(&stores)
		if stores.Validate(true) == nil {
			t.Errorf("%s: unusable stores were accepted", name)
		}
	}
}

func TestExternalFromEnv(t *testing.T) {
	for _, name := range []string{EnvExternalPostgres, EnvExternalClickHouse, EnvExternalValkey} {
		t.Setenv(name, "")
	}
	if _, ok := ExternalFromEnv(); ok {
		t.Error("external stores found with the environment empty")
	}
	t.Setenv(EnvExternalPostgres, "postgres://u:p@h:5432/authdb")
	t.Setenv(EnvExternalClickHouse, "clickhouse://u:p@h:9000/authdb")
	t.Setenv(EnvExternalValkey, "redis://h:6379/1")
	stores, ok := ExternalFromEnv()
	if !ok || stores.PostgresURI != "postgres://u:p@h:5432/authdb" || stores.ClickHouseURI != "clickhouse://u:p@h:9000/authdb" || stores.ValkeyURI != "redis://h:6379/1" {
		t.Errorf("ExternalFromEnv = %+v, %v", stores, ok)
	}
}

func TestExternalRequiredFollowsTheBaseURL(t *testing.T) {
	for _, name := range []string{EnvExternalPostgres, EnvExternalClickHouse, EnvExternalValkey, EnvExternalSetEnvCmd} {
		t.Setenv(name, "")
	}
	if _, ok, err := ExternalRequired(""); ok || err != nil {
		t.Errorf("no base URL: got ok=%v err=%v, want neither", ok, err)
	}
	if _, _, err := ExternalRequired("http://target.test"); err == nil {
		t.Error("a base URL with no stores was accepted")
	}
	t.Setenv(EnvExternalPostgres, "postgres://u:p@h:5432/authdb")
	t.Setenv(EnvExternalClickHouse, "clickhouse://u:p@h:9000/authdb")
	t.Setenv(EnvExternalValkey, "http://h:6379/1")
	if _, _, err := ExternalRequired("http://target.test"); err == nil {
		t.Error("a base URL with unusable stores was accepted")
	}
	t.Setenv(EnvExternalValkey, "redis://h:6379/1")
	if _, _, err := ExternalRequired("http://target.test"); err == nil {
		t.Error("an external run with no target-environment command was accepted: the corpus changes the target's environment mid-run")
	}
	t.Setenv(EnvExternalSetEnvCmd, "/bin/true")
	for _, bad := range []string{"not-a-url", "ftp://target.test", "http://", "target.test:8080"} {
		if _, _, err := ExternalRequired(bad); err == nil {
			t.Errorf("malformed base URL %q was accepted", bad)
		}
	}
	if stores, ok, err := ExternalRequired("http://target.test"); err != nil || !ok || stores.ValkeyURI != "redis://h:6379/1" {
		t.Errorf("complete stores: %+v ok=%v err=%v", stores, ok, err)
	}
}

func TestSetTargetEnvInProcess(t *testing.T) {
	t.Setenv("ORACLE_TEST_NAME", "before")
	SetTargetEnv(t, false, "ORACLE_TEST_NAME", " OFF ")
	if got := os.Getenv("ORACLE_TEST_NAME"); got != " OFF " {
		t.Errorf("in-process set: %q", got)
	}
	SetTargetEnv(t, false, "ORACLE_TEST_NAME", "")
	if _, set := os.LookupEnv("ORACLE_TEST_NAME"); set {
		t.Error("in-process unset left the variable set")
	}
}

func TestSetTargetEnvExternalRunsTheCommand(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "calls")
	script := filepath.Join(dir, "set-env.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s=%s\\n' \"$1\" \"$2\" >> "+log+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvExternalSetEnvCmd, script)
	t.Setenv("ORACLE_TEST_NAME", "untouched")
	SetTargetEnv(t, true, "ORACLE_TEST_NAME", " OFF ")
	if got := os.Getenv("ORACLE_TEST_NAME"); got != " OFF " {
		t.Errorf("after the set the process holds %q: the golden's environment key would not follow the target", got)
	}
	SetTargetEnv(t, true, "ORACLE_TEST_NAME", "")
	got, err := os.ReadFile(log)
	if err != nil {
		t.Fatalf("the command did not run: %v", err)
	}
	if string(got) != "ORACLE_TEST_NAME= OFF \nORACLE_TEST_NAME=\n" {
		t.Errorf("command calls = %q", got)
	}
	// The frozen golden keys its calls by this process's environment, so it follows the target's.
	if _, set := os.LookupEnv("ORACLE_TEST_NAME"); set {
		t.Errorf("after the unset the process still holds ORACLE_TEST_NAME=%q", os.Getenv("ORACLE_TEST_NAME"))
	}
}

// The frozen golden keys each call by the process environment (callEnvKey). The auth-flow golden was recorded with
// the auto-create-org setting changed in the process, so an external run must key the call the same way, or the
// run stops at the first changed batch ("recorded under another extra environment").
func TestExternalSetTargetEnvKeysTheGoldenCallLikeInProcess(t *testing.T) {
	script := filepath.Join(t.TempDir(), "set-env.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvExternalSetEnvCmd, script)
	golden, err := openGolden(GoldenSpec{Path: t.TempDir() + "/g.json", PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := golden.bindPythonEnv(Options{JWTKey: "k"}); err != nil {
		t.Fatal(err)
	}
	keyAfter := func(external bool) (key string) {
		t.Run(map[bool]string{false: "in-process", true: "external"}[external], func(sub *testing.T) {
			SetTargetEnv(sub, external, "AUTH_AUTO_CREATE_ORG_ON_REGISTER", " OFF ")
			var err error
			if key, err = golden.callEnvKey(nil); err != nil {
				sub.Fatal(err)
			}
		})
		return key
	}
	inProcess, external := keyAfter(false), keyAfter(true)
	if inProcess == "" {
		t.Fatal("the in-process change is not in the call key: the test measures nothing")
	}
	if external != inProcess {
		t.Errorf("external call key %q differs from the in-process key %q", external, inProcess)
	}
}
