package containers

import (
	"strings"
	"testing"
)

func TestDatabaseNameReadsTheRealConnectedDatabase(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"container path":      "postgres://worker_test:worker_test_password@127.0.0.1:55432/worker_test?sslmode=disable",
		"remote/kiac scratch": "postgres://devhealth:acr-trial-dev@192.168.65.4:30500/lane_4661_scratch_20d2d1267138ddb3?sslmode=disable",
	}
	want := map[string]string{
		"container path":      "worker_test",
		"remote/kiac scratch": "lane_4661_scratch_20d2d1267138ddb3",
	}
	for name, uri := range cases {
		got, err := DatabaseName(uri)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != want[name] {
			t.Errorf("%s: DatabaseName = %q, want %q", name, got, want[name])
		}
	}
}

func TestDatabaseNameRejectsAnUnparseableURI(t *testing.T) {
	t.Parallel()

	if _, err := DatabaseName("not a dsn at all"); err == nil {
		t.Fatal("want an error for an unparseable URI, got nil")
	}
}

// TestDatabaseNameRejectsAnUnsafeDatabaseName is the injection-defense case:
// every caller concatenates DatabaseName's return value directly into DDL,
// so this function must refuse a database name outside the safe identifier
// class itself, rather than trusting that only this package's own
// scratchName() ever produces the URIs it is given.
func TestDatabaseNameRejectsAnUnsafeDatabaseName(t *testing.T) {
	t.Parallel()

	cases := []string{
		"postgres://u:p@h:5432/robert'; DROP TABLE users;--?sslmode=disable",
		"postgres://u:p@h:5432/Has-A-Dash?sslmode=disable",
		"postgres://u:p@h:5432/UPPERCASE?sslmode=disable",
	}
	for _, uri := range cases {
		if _, err := DatabaseName(uri); err == nil {
			t.Errorf("DatabaseName(%q): want an error for an unsafe database name, got nil", uri)
		}
	}
}

func TestRoleSuffixIsBoundedAndDiffersAcrossCalls(t *testing.T) {
	t.Parallel()

	a := &Instance{URI: "postgres://u:p@h:5432/lane_4661_scratch_20d2d1267138ddb3?sslmode=disable"}
	b := &Instance{URI: "postgres://u:p@h:5432/lane_4661_scratch_ffffffffffffffff?sslmode=disable"}

	suffixA, err := RoleSuffix(a)
	if err != nil {
		t.Fatal(err)
	}
	suffixB, err := RoleSuffix(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(suffixA) != roleSuffixLen {
		t.Errorf("RoleSuffix length = %d, want %d", len(suffixA), roleSuffixLen)
	}
	if suffixA == suffixB {
		t.Errorf("RoleSuffix collided across two different scratch databases: %q", suffixA)
	}

	// A short database name (the container path's fixed "worker_test") is
	// returned unchanged rather than padded or errored -- there is nothing
	// else to disambiguate it from, and nothing needs there to be: a fresh
	// container has no pre-existing role to collide with either way.
	container := &Instance{URI: "postgres://worker_test:worker_test_password@127.0.0.1:55432/worker_test?sslmode=disable"}
	suffix, err := RoleSuffix(container)
	if err != nil {
		t.Fatal(err)
	}
	if suffix != "worker_test" {
		t.Errorf("RoleSuffix(container) = %q, want %q", suffix, "worker_test")
	}
}

func TestRoleSuffixRejectsANilInstance(t *testing.T) {
	t.Parallel()

	if _, err := RoleSuffix(nil); err == nil {
		t.Fatal("want an error for a nil instance, got nil")
	}
}

func TestRoleNameComposesAndValidates(t *testing.T) {
	t.Parallel()

	instance := &Instance{URI: "postgres://u:p@h:5432/lane_4661_scratch_20d2d1267138ddb3?sslmode=disable"}
	name, err := RoleName("workerctl_coordinator_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	suffix, err := RoleSuffix(instance)
	if err != nil {
		t.Fatal(err)
	}
	if want := "workerctl_coordinator_runtime_" + suffix; name != want {
		t.Errorf("RoleName = %q, want %q", name, want)
	}
	if len(name) != 42 {
		t.Errorf("RoleName length = %d, want 42 (this repo's longest role literal, 29, plus '_' plus the 12-byte suffix)", len(name))
	}
}

// TestRoleNameRejectsAPrefixThatWouldOverflowNamedatalen is the mutation
// target this test pins: RoleName must FAIL a name that would exceed
// PostgreSQL's 63-byte NAMEDATALEN, never silently truncate it. Truncating
// instead would let two different long prefixes -- or two different calls
// whose composed names happen to share a 63-byte cut point -- collide on the
// identical stored role name, which is exactly the bug this package exists
// to prevent.
func TestRoleNameRejectsAPrefixThatWouldOverflowNamedatalen(t *testing.T) {
	t.Parallel()

	instance := &Instance{URI: "postgres://u:p@h:5432/lane_4661_scratch_20d2d1267138ddb3?sslmode=disable"}
	// prefix + "_" + 12-byte suffix must land at 64 bytes, one over the
	// 63-byte limit -- built by repeat rather than by hand-counting a
	// literal, so the test can't itself be off by one.
	overlong := strings.Repeat("x", maxIdentifierLength-1-roleSuffixLen+1)
	name, err := RoleName(overlong, instance)
	if err == nil {
		t.Fatalf("RoleName(%d-byte prefix) = %q (%d bytes): want an error, not a name at or over 63 bytes",
			len(overlong), name, len(name))
	}
}
