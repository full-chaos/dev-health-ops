package venueoracle

import (
	"os"
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
	for _, name := range []string{EnvExternalPostgres, EnvExternalClickHouse, EnvExternalValkey} {
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

func TestSetTargetEnvExternalSkipsTheCase(t *testing.T) {
	t.Setenv("ORACLE_TEST_NAME", "untouched")
	reached := false
	t.Run("external", func(sub *testing.T) {
		SetTargetEnv(sub, true, "ORACLE_TEST_NAME", " OFF ")
		reached = true
	})
	if reached {
		t.Error("an env-dependent case ran against an external target, whose configuration the test cannot change")
	}
	if os.Getenv("ORACLE_TEST_NAME") != "untouched" {
		t.Error("an external run changed the test process environment")
	}
}
