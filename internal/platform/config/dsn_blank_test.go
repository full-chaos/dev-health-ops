package config

import "testing"

// A worker group blanks the database DSN names it does not hold with an explicit
// empty env entry (the chart's go-workers template). That is only safe if an
// empty value behaves exactly as an absent one for every one of those names in
// every class: no mutual-exclusion error with a *_FILE name, no presence branch,
// no change to what is configured. This pins it per class and per blanked name.
func TestBlankDSNNamesBehaveAsAbsentInEveryWorkerClass(t *testing.T) {
	t.Parallel()

	base := map[string]string{
		"POSTGRES_URI":         "postgresql://domain:pw@db:5432/app",
		"DEV_HEALTH_HTTP_ADDR": ":8080",
	}
	classes := map[string]struct {
		spec Spec
		env  map[string]string
	}{
		"river":   {Spec{Service: "dev-health-worker", RequireQueues: true, Queues: []string{"metrics"}}, map[string]string{"WORKER_DATABASE_URI": "postgresql://queue:pw@db:5432/app", "DEV_HEALTH_QUEUE_CONCURRENCY": "metrics=1"}},
		"control": {Spec{Service: "dev-health-reconciler"}, map[string]string{"WORKER_DATABASE_URI": "postgresql://queue:pw@db:5432/app", "COORDINATOR_DATABASE_URI": "postgresql://coord:pw@db:5432/app"}},
		"stream":  {Spec{Service: "dev-health-stream-runner", Profile: "ingest"}, map[string]string{"CLICKHOUSE_URI": "clickhouse://ch:pw@ch:9000/default", "VALKEY_URI": "redis://valkey:6379/1"}},
	}
	blanked := []string{
		"WORKER_DATABASE_URI", "COORDINATOR_DATABASE_URI", "API_DATABASE_URI",
		"MIGRATION_DATABASE_URI", "DATABASE_URI", "DATABASE_URL", "POSTGRES_URL",
		"GO_API_REGISTRY_POSTGRES_URI", "AUTH_DATABASE_URI", "AUTH_MIGRATION_DATABASE_URI",
	}
	for class, test := range classes {
		for _, name := range blanked {
			t.Run(class+"/"+name, func(t *testing.T) {
				t.Parallel()
				absent := map[string]string{}
				for k, v := range base {
					absent[k] = v
				}
				for k, v := range test.env {
					absent[k] = v
				}
				// The name is not held by the class in this case: remove any value it
				// has, then compare absent against explicitly empty.
				delete(absent, name)
				blank := map[string]string{name: ""}
				for k, v := range absent {
					blank[k] = v
				}
				absentSpec, blankSpec := test.spec, test.spec
				absentSpec.LookupEnv, blankSpec.LookupEnv = lookup(absent), lookup(blank)
				want, wantErr := Load(absentSpec)
				got, gotErr := Load(blankSpec)
				if wantErr != nil || gotErr != nil {
					t.Fatalf("Load errors: absent=%v blank=%v", wantErr, gotErr)
				}
				for key, pair := range map[string][2]any{
					"domain":      {want.DomainDatabaseURI.Configured(), got.DomainDatabaseURI.Configured()},
					"queue":       {want.QueueDatabaseURI.Configured(), got.QueueDatabaseURI.Configured()},
					"coordinator": {want.CoordinatorDatabaseURI.Configured(), got.CoordinatorDatabaseURI.Configured()},
					"api":         {want.APIDatabaseURI.Configured(), got.APIDatabaseURI.Configured()},
					"queue form":  {want.QueueDatabaseForm, got.QueueDatabaseForm},
					"coord form":  {want.CoordinatorDatabaseForm, got.CoordinatorDatabaseForm},
				} {
					if pair[0] != pair[1] {
						t.Errorf("%s differs between an absent and an empty %s: %v vs %v", key, name, pair[0], pair[1])
					}
				}
			})
		}
	}

	t.Run("a stream Load with an empty WORKER_DATABASE_URI succeeds", func(t *testing.T) {
		t.Parallel()
		// A stream group never needs the queue DSN: Load accepts it empty.
		spec := Spec{Service: "dev-health-stream-runner", Profile: "ingest", LookupEnv: lookup(map[string]string{
			"POSTGRES_URI": base["POSTGRES_URI"], "CLICKHOUSE_URI": "clickhouse://ch:pw@ch:9000/default",
			"VALKEY_URI": "redis://valkey:6379/1", "WORKER_DATABASE_URI": "",
		})}
		if _, err := Load(spec); err != nil {
			t.Fatalf("stream Load with an empty WORKER_DATABASE_URI: %v", err)
		}
	})
}
