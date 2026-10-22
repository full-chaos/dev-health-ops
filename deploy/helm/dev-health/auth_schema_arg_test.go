package devhealth_test

import (
	"fmt"
	"testing"
)

// auth-service names its schema with --database-schema: the same name the two
// migration Jobs carry as AUTH_DATABASE_SCHEMA, and "auth" when the value is
// empty.
func TestAuthServiceDatabaseSchemaArgMatchesTheMigrationJobs(t *testing.T) {
	for _, tc := range []struct{ name, set, want string }{
		{"default", "", "auth"},
		{"empty value", "authService.database.schema=", "auth"},
		{"named schema", "authService.database.schema=identity_plane", "identity_plane"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{}, authBase...)
			if tc.set != "" {
				args = append(args, "--set", tc.set)
			}
			docs, out, err := renderAuth(t, args...)
			if err != nil {
				t.Fatalf("render failed: %v\n%.600s", err, out)
			}
			deployment := authDoc(docs, "Deployment", "a-dev-health-auth-service")
			if deployment == nil {
				t.Fatal("no auth-service Deployment in the render")
			}
			want := fmt.Sprintf("--database-schema=%s", tc.want)
			found := false
			containers, _ := at(deployment, "spec", "template", "spec", "containers").([]any)
			for _, container := range containers {
				if at(container, "name") != "auth-service" {
					continue
				}
				args, _ := at(container, "args").([]any)
				for _, arg := range args {
					found = found || arg == want
				}
			}
			if !found {
				t.Errorf("the auth-service container has no %s argument", want)
			}
			for _, jobName := range []string{"a-dev-health-auth-migrate", "a-dev-health-migrate"} {
				if value, _ := containerEnvValue(t, authDoc(docs, "Job", jobName), "AUTH_DATABASE_SCHEMA"); value != tc.want {
					t.Errorf("%s AUTH_DATABASE_SCHEMA = %q, want %q", jobName, value, tc.want)
				}
			}
		})
	}
}
