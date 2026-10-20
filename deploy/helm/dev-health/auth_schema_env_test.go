package devhealth_test

import "testing"

// The ops migrator (the `migrate` hook Job running dho) and the auth migrator
// (the `auth-migrate` hook Job) must be told the same auth schema name: the ops
// migrator exempts exactly the configured auth schema from its foreign-database
// check, so a non-default schema that only one of them knows turns a fresh
// install into a refusal.
func TestBothMigrationJobsCarryTheSameAuthSchema(t *testing.T) {
	for _, schema := range []string{"auth", "identity_plane"} {
		t.Run(schema, func(t *testing.T) {
			args := append([]string{"--set", "authService.database.schema=" + schema}, authBase...)
			docs, out, err := renderAuth(t, args...)
			if err != nil {
				t.Fatalf("render failed: %v\n%.600s", err, out)
			}
			values := map[string]string{}
			for jobName, container := range map[string]string{
				"a-dev-health-auth-migrate": "auth-migrate",
				"a-dev-health-migrate":      "migrate",
			} {
				job := authDoc(docs, "Job", jobName)
				if job == nil {
					t.Fatalf("no Job %s in the render", jobName)
				}
				containers, _ := at(job, "spec", "template", "spec", "containers").([]any)
				for _, c := range containers {
					if at(c, "name") != container {
						continue
					}
					env, _ := at(c, "env").([]any)
					for _, e := range env {
						if at(e, "name") == "AUTH_DATABASE_SCHEMA" {
							values[jobName], _ = at(e, "value").(string)
						}
					}
				}
			}
			for jobName, value := range values {
				if value != schema {
					t.Errorf("%s carries AUTH_DATABASE_SCHEMA=%q, want %q", jobName, value, schema)
				}
			}
			if len(values) != 2 {
				t.Errorf("AUTH_DATABASE_SCHEMA is missing from a migration Job: %v", values)
			}
		})
	}
}
