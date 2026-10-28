package pgschema

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func noEnv(string) (string, bool) { return "", false }

func fixtureChain() []pgmigrate.ChainFile {
	return []pgmigrate.ChainFile{
		{Revision: "0001", Name: "0001_a.sql", SQL: "CREATE TABLE a ();"},
		{Revision: "0002", Name: "0002_b.sql", SQL: "CREATE TABLE b ();"},
	}
}

func fixtureBaseline() pgmigrate.Baseline {
	return pgmigrate.Baseline{RiverSchema: "river", Heads: []string{"0000"}, Schema: "CREATE TABLE x ();", Data: ""}
}

func TestKeyIsStableAndHex(t *testing.T) {
	a := KeyFor(fixtureBaseline(), fixtureChain(), "img", noEnv)
	b := KeyFor(fixtureBaseline(), fixtureChain(), "img", noEnv)
	if a != b {
		t.Errorf("same input gave two keys: %s %s", a, b)
	}
	if len(a) != 64 || a != lowerHex(a) {
		t.Errorf("key %q is not 64 lowercase hex digits", a)
	}
}

func lowerHex(s string) string {
	out := []byte(s)
	for i, c := range out {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			out[i] = '?'
		}
	}
	return string(out)
}

func TestKeyChangesWithFileContent(t *testing.T) {
	base := KeyFor(fixtureBaseline(), fixtureChain(), "img", noEnv)
	chain := fixtureChain()
	chain[1].SQL = "CREATE TABLE b (id int);"
	if KeyFor(fixtureBaseline(), chain, "img", noEnv) == base {
		t.Error("a changed migration body kept the key")
	}
	chain = fixtureChain()
	chain[1].Name = "0002_c.sql"
	if KeyFor(fixtureBaseline(), chain, "img", noEnv) == base {
		t.Error("a renamed migration file kept the key")
	}
	if KeyFor(fixtureBaseline(), fixtureChain()[:1], "img", noEnv) == base {
		t.Error("a shorter chain kept the key")
	}
	baseline := fixtureBaseline()
	baseline.Schema = "CREATE TABLE y ();"
	if KeyFor(baseline, fixtureChain(), "img", noEnv) == base {
		t.Error("a changed baseline kept the key")
	}
}

func TestKeyChangesWithFileOrder(t *testing.T) {
	base := KeyFor(fixtureBaseline(), fixtureChain(), "img", noEnv)
	chain := fixtureChain()
	chain[0], chain[1] = chain[1], chain[0]
	if KeyFor(fixtureBaseline(), chain, "img", noEnv) == base {
		t.Error("the same files in another order kept the key")
	}
}

func TestKeyChangesWithImageAndEachEnvValue(t *testing.T) {
	base := KeyFor(fixtureBaseline(), fixtureChain(), "img", noEnv)
	if KeyFor(fixtureBaseline(), fixtureChain(), "img2", noEnv) == base {
		t.Error("another image kept the key")
	}
	for _, name := range KeyEnv {
		lookup := func(got string) (string, bool) { return "set-" + name, got == name }
		if KeyFor(fixtureBaseline(), fixtureChain(), "img", lookup) == base {
			t.Errorf("setting %s kept the key", name)
		}
	}
	unsetVersusEmpty := func(got string) (string, bool) { return "", got == KeyEnv[0] }
	if KeyFor(fixtureBaseline(), fixtureChain(), "img", unsetVersusEmpty) == base {
		t.Error("an env var set to the empty string and an unset one gave one key")
	}
}

func TestKeyEnvListIsWhatUpgradeReads(t *testing.T) {
	want := map[string]bool{pgmigrate.AuthSchemaEnv: true, pgmigrate.RiverSchemaEnv: true, pgmigrate.CutoverEnv: true}
	for _, name := range KeyEnv {
		delete(want, name)
	}
	if len(want) != 0 {
		t.Errorf("KeyEnv lacks %v", want)
	}
}

// The key is read at the time of the call, not at process start.
func TestKeyReadsTheEnvironmentAtCallTime(t *testing.T) {
	t.Setenv(pgmigrate.AuthSchemaEnv, "")
	os.Unsetenv(pgmigrate.AuthSchemaEnv)
	first, err := Key()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(pgmigrate.AuthSchemaEnv, "auth_other")
	second, err := Key()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Errorf("%s changed between two calls and the key did not", pgmigrate.AuthSchemaEnv)
	}
	t.Setenv(pgmigrate.AuthSchemaEnv, "")
	os.Unsetenv(pgmigrate.AuthSchemaEnv)
	again, err := Key()
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Error("restoring the environment did not restore the key")
	}
}

func TestKeyUsesTheHarnessImage(t *testing.T) {
	real, err := Key()
	if err != nil {
		t.Fatal(err)
	}
	baseline, _ := pgmigrate.LoadBaseline()
	chain, _ := pgmigrate.LoadChain()
	if want := KeyFor(baseline, chain, containers.PostgresImage, os.LookupEnv); real != want {
		t.Errorf("Key() = %s, want KeyFor over the embedded chain and %s", real, containers.PostgresImage)
	}
}

// The key sees what the build role sees: a role default for search_path or another role
// builds the same chain into other places or under another owner.
func TestKeyChangesWithTheSessionState(t *testing.T) {
	state := SessionState{CurrentUser: "worker_test", SearchPath: `"$user", public`}
	base := KeyForSession(fixtureBaseline(), fixtureChain(), "img", noEnv, state)
	if again := KeyForSession(fixtureBaseline(), fixtureChain(), "img", noEnv, state); again != base {
		t.Errorf("the same state gave two keys: %s %s", base, again)
	}
	if len(base) != 64 {
		t.Errorf("key %q is not 64 hex digits", base)
	}
	for name, changed := range map[string]SessionState{
		"search_path": {CurrentUser: "worker_test", SearchPath: "other_schema"},
		"role":        {CurrentUser: "other_role", SearchPath: `"$user", public`},
	} {
		if KeyForSession(fixtureBaseline(), fixtureChain(), "img", noEnv, changed) == base {
			t.Errorf("another %s kept the key", name)
		}
	}
	if KeyForSession(fixtureBaseline(), fixtureChain(), "img", noEnv, SessionState{}) == base {
		t.Error("an empty session state and a real one gave one key")
	}
}
