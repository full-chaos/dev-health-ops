package config

import (
	"strings"
	"testing"
)

func TestServiceClickHouseDSNRefusesNonNativeScheme(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"CLICKHOUSE_URI", "API_CLICKHOUSE_URI"} {
		for _, scheme := range []string{"http", "https"} {
			t.Run(key+"/"+scheme, func(t *testing.T) {
				t.Parallel()
				dsn := scheme + "://analytics:hunter2-secret@ch.internal:8123/default"
				_, err := Load(workerSpec(map[string]string{key: dsn}))
				if err == nil {
					t.Fatalf("%s accepted a %s:// ClickHouse DSN", key, scheme)
				}
				if !strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), scheme) {
					t.Fatalf("error does not name the key and scheme: %v", err)
				}
				for _, leak := range []string{"hunter2-secret", "analytics", "ch.internal", dsn} {
					if strings.Contains(err.Error(), leak) {
						t.Fatalf("error leaks %q: %v", leak, err)
					}
				}
			})
		}
		t.Run(key+"/clickhouse", func(t *testing.T) {
			t.Parallel()
			if _, err := Load(workerSpec(map[string]string{key: "clickhouse://analytics:secret@ch.internal:9000/default"})); err != nil {
				t.Fatalf("%s refused a clickhouse:// DSN: %v", key, err)
			}
		})
	}
}

func TestResolveDSNStillAcceptsHTTPClickHouseForCLIs(t *testing.T) {
	t.Parallel()

	for _, dsn := range []string{"http://ch:ch@127.0.0.1:8123/default", "https://ch:ch@127.0.0.1:8443/default"} {
		got, configured, err := ResolveDSN(lookup(map[string]string{"CLICKHOUSE_URI": dsn}), "CLICKHOUSE_URI", ClickHouseSpec)
		if err != nil || !configured || got.Reveal() != dsn {
			t.Fatalf("ResolveDSN(%q) = %v, %v, %v", dsn, got.Configured(), configured, err)
		}
	}
}
