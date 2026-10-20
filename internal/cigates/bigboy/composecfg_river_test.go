package bigboy_test

import (
	"fmt"
	"strings"
	"testing"
)

// River role provisioning is delivered by the Go operator image (`dho migrate roles`), never by a host file: a relative
// bind-mount source that is absent on the host becomes an empty directory instead of an error. The SQL stays in the tree
// for the callers that have not moved yet (the chart's provision-roles Job, ci/lib/go_worker_fixture.sh).

func ccfgRiverSQLExists(g *guard) {
	g.expect(exists(g.path(ccfgProvisionSQL)), "%s is missing: the callers that still apply it have nothing to read", ccfgProvisionSQL)
}

func ccfgRiverNoSQLBindMount(g *guard) {
	svc := ccfgService(g, ccfgServices(g), "go-river-provision")
	for _, v := range list(svc["volumes"]) {
		g.expect(!strings.Contains(fmt.Sprint(v), "provision_river_roles.sql"), "go-river-provision still mounts the SQL from the host: %v", v)
	}
}

func ccfgRiverProvisionsWithTheGoLeg(g *guard) {
	svc := ccfgService(g, ccfgServices(g), "go-river-provision")
	_, hasEntrypoint := svc["entrypoint"]
	g.expect(!hasEntrypoint, "go-river-provision sets an entrypoint %v: it must run the operator image's own", svc["entrypoint"])
	g.expect(equal(strs(svc["command"]), []string{"migrate", "roles"}), "go-river-provision command is %v, want [migrate roles]", svc["command"])
	g.expect(str(at(svc, "build", "target")) == "operator", "go-river-provision builds target %q, want operator", str(at(svc, "build", "target")))
}

func ccfgRiverProvisionBeforeMigrate(g *guard) {
	svc := ccfgService(g, ccfgServices(g), "go-river-migrate")
	g.expect(ccfgCondition(svc, "go-river-provision") == "service_completed_successfully",
		"go-river-migrate waits for go-river-provision with condition %q, want service_completed_successfully", ccfgCondition(svc, "go-river-provision"))
}

var ccfgRiverChecks = []check{
	{"the_provisioning_sql_source_still_exists", ccfgRiverSQLExists},
	{"the_overlay_does_not_bind_mount_the_provisioning_sql", ccfgRiverNoSQLBindMount},
	{"the_overlay_provisions_with_the_go_leg_not_the_packaged_sql", ccfgRiverProvisionsWithTheGoLeg},
	{"provisioning_runs_before_the_river_schema_migration", ccfgRiverProvisionBeforeMigrate},
}

func TestComposeCfgRiverRealTree(t *testing.T) { runReal(t, ccfgRiverChecks...) }

func TestComposeCfgRiverPlanted(t *testing.T) {
	c := ccfgCompose
	files := []string{ccfgCompose, ccfgProvisionSQL}
	runPlanted(t, []planted{
		{name: "the_sql_source_is_deleted", copy: []string{ccfgCompose}, check: ccfgRiverSQLExists, want: "is missing"},
		{name: "the_provisioning_sql_is_bind_mounted_from_the_host", copy: files, check: ccfgRiverNoSQLBindMount, want: "still mounts the SQL",
			mutate: ccfgMutAfter(c, "  go-river-provision:\n", "    command: [migrate, roles]\n", "    command: [migrate, roles]\n    volumes:\n      - ./scripts/worker/provision_river_roles.sql:/opt/provision_river_roles.sql:ro\n")},
		{name: "go_river_provision_gets_a_shell_entrypoint", copy: files, check: ccfgRiverProvisionsWithTheGoLeg, want: "sets an entrypoint",
			mutate: ccfgMutAfter(c, "  go-river-provision:\n", "    command: [migrate, roles]\n", "    entrypoint: [/bin/sh, -c]\n    command: [migrate, roles]\n")},
		{name: "go_river_provision_runs_another_command", copy: files, check: ccfgRiverProvisionsWithTheGoLeg, want: "want [migrate roles]",
			mutate: ccfgMutAfter(c, "  go-river-provision:\n", "    command: [migrate, roles]\n", "    command: [migrate, up]\n")},
		{name: "go_river_provision_builds_another_target", copy: files, check: ccfgRiverProvisionsWithTheGoLeg, want: "want operator",
			mutate: ccfgMutAfter(c, "  go-river-provision:\n", "      target: operator\n", "      target: runtime\n")},
		{name: "go_river_migrate_stops_waiting_for_the_provisioning", copy: files, check: ccfgRiverProvisionBeforeMigrate, want: "go-river-migrate waits for go-river-provision",
			mutate: ccfgMutAfter(c, "  go-river-migrate:\n", "      go-river-provision:\n        condition: service_completed_successfully\n", "      go-river-provision:\n        condition: service_started\n")},
	})
}
