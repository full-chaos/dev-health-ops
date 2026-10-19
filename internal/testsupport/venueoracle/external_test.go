package venueoracle

import "testing"

func TestExternalStoresValidate(t *testing.T) {
	good := ExternalStores{PostgresURI: "postgres://u:p@h:5432/authdb", ClickHouseURI: "clickhouse://u:p@h:9000/authdb", ValkeyURI: "redis://h:6379/1"}
	if err := good.Validate(true); err != nil {
		t.Fatalf("complete stores refused: %v", err)
	}
	if good.Validate(false) == nil {
		t.Error("external stores were accepted for a live venue: the Python plane needs its own copy")
	}
	for name, mutate := range map[string]func(*ExternalStores){
		"no postgres":      func(e *ExternalStores) { e.PostgresURI = "" },
		"no clickhouse":    func(e *ExternalStores) { e.ClickHouseURI = "" },
		"no valkey":        func(e *ExternalStores) { e.ValkeyURI = "" },
		"postgres no db":   func(e *ExternalStores) { e.PostgresURI = "postgres://u:p@h:5432" },
		"clickhouse no db": func(e *ExternalStores) { e.ClickHouseURI = "clickhouse://u:p@h:9000" },
		"postgres bad uri": func(e *ExternalStores) { e.PostgresURI = "://" },
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
