package devhealth_test

import (
	"strings"
	"testing"
)

// CHAOS-12081: the provision-roles hook Job (`dho migrate roles`) provisions the go-api login when
// API_DATABASE_ROLE / API_DATABASE_PASSWORD are set. Until apiRole existed the Job never passed them, so a fresh
// stack had no devhealth_api login. The password is read from an operator-created Secret by reference only: it
// is never a literal in the manifest.

var provisionRolesJob = "a-dev-health-provision-roles"

func provisionRolesArgs(extra ...string) []string {
	return queryAPIRoleArgs(riverHookSets, extra)
}

func jobEnvEntry(t *testing.T, job map[string]any, name string) map[string]any {
	t.Helper()
	containers, _ := at(job, "spec", "template", "spec", "containers").([]any)
	for _, container := range containers {
		env, _ := at(container, "env").([]any)
		for _, entry := range env {
			if at(entry, "name") == name {
				m, _ := entry.(map[string]any)
				return m
			}
		}
	}
	return nil
}

func TestProvisionRolesJobPassesTheAPILoginWhenNamed(t *testing.T) {
	docs, out, err := renderAuth(t, provisionRolesArgs(
		"--set", "migrations.hook.provisionRoles.apiRole.name=devhealth_api",
		"--set", "migrations.hook.provisionRoles.apiRole.secretName=dev-health-api-login",
		"--set", "migrations.hook.provisionRoles.apiRole.secretKey=API_DATABASE_PASSWORD")...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	job := authDoc(docs, "Job", provisionRolesJob)
	if job == nil {
		t.Fatal("provision-roles Job did not render")
	}
	role := jobEnvEntry(t, job, "API_DATABASE_ROLE")
	if role == nil || role["value"] != "devhealth_api" {
		t.Fatalf("API_DATABASE_ROLE = %v, want value devhealth_api", role)
	}
	password := jobEnvEntry(t, job, "API_DATABASE_PASSWORD")
	if password == nil {
		t.Fatal("API_DATABASE_PASSWORD is not set on the Job")
	}
	if _, literal := password["value"]; literal {
		t.Errorf("API_DATABASE_PASSWORD must be a secretKeyRef, not a literal: %v", password)
	}
	if at(password, "valueFrom", "secretKeyRef", "name") != "dev-health-api-login" || at(password, "valueFrom", "secretKeyRef", "key") != "API_DATABASE_PASSWORD" {
		t.Errorf("API_DATABASE_PASSWORD secretKeyRef = %v", at(password, "valueFrom", "secretKeyRef"))
	}
}

func TestProvisionRolesJobHasNoAPILoginByDefault(t *testing.T) {
	docs, out, err := renderAuth(t, provisionRolesArgs()...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	job := authDoc(docs, "Job", provisionRolesJob)
	if job == nil {
		t.Fatal("provision-roles Job did not render")
	}
	for _, name := range []string{"API_DATABASE_ROLE", "API_DATABASE_PASSWORD"} {
		if jobEnvEntry(t, job, name) != nil {
			t.Errorf("apiRole.name is empty, the Job still sets %s", name)
		}
	}
}

func TestProvisionRolesAPILoginNeedsItsSecret(t *testing.T) {
	for name, sets := range map[string][]string{
		"no secretName": {"--set", "migrations.hook.provisionRoles.apiRole.name=devhealth_api"},
		"no secretKey": {
			"--set", "migrations.hook.provisionRoles.apiRole.name=devhealth_api",
			"--set", "migrations.hook.provisionRoles.apiRole.secretName=dev-health-api-login",
			"--set", "migrations.hook.provisionRoles.apiRole.secretKey=",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, provisionRolesArgs(sets...)...)
			if err == nil || !strings.Contains(out, "migrations.hook.provisionRoles.apiRole") {
				t.Fatalf("expected a render refusal naming apiRole, got err=%v\n%.500s", err, out)
			}
		})
	}
}
