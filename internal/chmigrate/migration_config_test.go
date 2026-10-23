package chmigrate

import "testing"

func TestMigrationConfigSessionSettings(t *testing.T) {
	config := migrationConfig("clickhouse://u:p@h:9000/db")
	want := map[string]any{"alter_sync": 2, "mutations_sync": 2, "max_execution_time": 0}
	if len(config.Settings) != len(want) {
		t.Fatalf("settings = %v, want %v", config.Settings, want)
	}
	for k, v := range want {
		if config.Settings[k] != v {
			t.Fatalf("settings[%s] = %v, want %v", k, config.Settings[k], v)
		}
	}
	if config.MaxOpenConns != 1 || config.MaxIdleConns != 1 || config.ReadTimeout != migrationReadTimeout {
		t.Fatalf("pool/timeout changed: %+v", config)
	}
}
