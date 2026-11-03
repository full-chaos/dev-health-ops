package roleprovision

import "testing"

// CHAOS-12614: the ops migration login is optional, listed under its own label, and its name follows the runtime
// identifier rule (river and the owner pin read it).
func TestMigratorRoleIsConfiguredOnlyWhenNamedAndLabelled(t *testing.T) {
	t.Parallel()
	options := valid()
	for _, entry := range options.configured() {
		if entry.label == migratorLabel {
			t.Fatal("an unset migrator role must not be configured")
		}
	}
	options.Migrator = Role{"devhealth_migrator", "pw-mig"}
	if err := options.Validate(); err != nil {
		t.Fatalf("a valid migrator role refused: %v", err)
	}
	found := false
	for _, entry := range options.configured() {
		found = found || (entry.label == migratorLabel && entry.role.Name == "devhealth_migrator")
	}
	if !found {
		t.Fatal("a named migrator role must be configured under the migrator label")
	}
	for name, role := range map[string]Role{
		"a quoted name":    {`Odd "Mig"`, "pw"},
		"no password":      {"devhealth_migrator", ""},
		"the domain login": {options.Domain.Name, "pw"},
	} {
		bad := options
		bad.Migrator = role
		if err := bad.Validate(); err == nil {
			t.Errorf("%s: Validate accepted the migrator role", name)
		}
	}
}
