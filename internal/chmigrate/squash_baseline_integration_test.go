//go:build integration

package chmigrate_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// squashEnv is one ClickHouse server for the squash proofs: each database is a fresh scratch database on it.
type squashEnv struct {
	ctx      context.Context
	instance *containers.Instance
	admin    driver.Conn
}

func newSquashEnv(t *testing.T) *squashEnv {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close clickhouse: %v", err)
		}
	})
	return &squashEnv{ctx: ctx, instance: instance, admin: openDatabase(t, instance.URI, "")}
}

// build makes a fresh database and runs Upgrade over it with baseline and chain.
func (e *squashEnv) build(t *testing.T, baseline chmigrate.Baseline, chain []chmigrate.ChainFile) (string, driver.Conn, chmigrate.DB, chmigrate.Result) {
	t.Helper()
	database := scratchDatabase(t, e.admin)
	conn := openDatabase(t, e.instance.URI, database)
	db, _, err := chmigrate.NewConnDB(e.ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	result, err := chmigrate.Upgrade(e.ctx, db, baseline, chain)
	if err != nil {
		t.Fatalf("upgrade %s: %v", database, err)
	}
	return database, conn, db, result
}

// catalogProbe is one system-table read that a CREATE statement might not show.
type catalogProbe struct{ name, query string }

var catalogProbes = []catalogProbe{
	{"system.columns", "SELECT concat(table, '|', name, '|', toString(position), '|', type, '|', default_kind, '|', default_expression, '|', comment, '|', compression_codec, '|', " +
		"toString(is_in_partition_key), toString(is_in_sorting_key), toString(is_in_primary_key), toString(is_in_sampling_key)) " +
		"FROM system.columns WHERE database = ? AND table NOT LIKE '.inner%' ORDER BY table, position"},
	{"system.tables", "SELECT concat(name, '|', engine, '|', engine_full, '|', partition_key, '|', sorting_key, '|', primary_key, '|', sampling_key, '|', comment) " +
		"FROM system.tables WHERE database = ? AND name NOT LIKE '.inner%' ORDER BY name"},
	{"system.data_skipping_indices", "SELECT concat(table, '|', name, '|', type, '|', expr, '|', toString(granularity)) " +
		"FROM system.data_skipping_indices WHERE database = ? ORDER BY table, name"},
	{"system.dictionaries", "SELECT concat(name, '|', toString(source)) FROM system.dictionaries WHERE database = ? ORDER BY name"},
	{"view dependencies", "SELECT concat(name, '|', arrayStringConcat(arraySort(dependencies_table), ',')) " +
		"FROM system.tables WHERE database = ? AND name NOT LIKE '.inner%' ORDER BY name"},
	{"inner tables", "SELECT toString(count()) FROM system.tables WHERE database = ? AND name LIKE '.inner%'"},
}

// catalog reads every probe of database, with its name replaced, so two databases compare.
func catalog(t *testing.T, ctx context.Context, conn driver.Conn, database string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, probe := range catalogProbes {
		rows, err := conn.Query(ctx, probe.query, database)
		if err != nil {
			t.Fatalf("%s: %v", probe.name, err)
		}
		lines := []string{}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, strings.ReplaceAll(line, database, "<db>"))
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: %v", probe.name, err)
		}
		rows.Close()
		out[probe.name] = lines
	}
	return out
}

func compareCatalogs(t *testing.T, label string, got, want map[string][]string) {
	t.Helper()
	for _, probe := range catalogProbes {
		if !reflect.DeepEqual(got[probe.name], want[probe.name]) {
			t.Errorf("%s: %s differs (%d vs %d rows), first %s", label, probe.name, len(got[probe.name]), len(want[probe.name]),
				firstDifference(got[probe.name], want[probe.name]))
		}
	}
}

// versionRows reads schema_migrations whole, applied_at included, so a run that wrote a row is seen.
func versionRows(t *testing.T, ctx context.Context, conn driver.Conn, database string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT concat(version, '|', toString(applied_at)) FROM `"+database+"`.schema_migrations ORDER BY version, applied_at")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		out = append(out, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestBaselineEqualsTheArchivedChain is the squash's equivalence proof (CHAOS-11751): a fresh database built the old
// way (the archived 098 baseline, then every archived chain file) and one built from the head baseline alone hold
// the same objects with the same CREATE statements (SHOW CREATE: columns, codecs, TTL, settings, indices, projections,
// view queries), the same seed rows and versions, and the same catalog rows. Any edit of head.json or of an
// archived file that changes the schema fails here.
func TestBaselineEqualsTheArchivedChain(t *testing.T) {
	env := newSquashEnv(t)
	archivedBaseline, archivedChain := loadArchivedBaseline(t), loadArchivedChain(t)
	head, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	oldDB, oldConn, _, oldResult := env.build(t, archivedBaseline, archivedChain)
	if oldResult.Action != "baseline_applied" || len(oldResult.Applied) != len(archivedChain) {
		t.Fatalf("the archived path = %+v, want baseline_applied with %d chain files", oldResult, len(archivedChain))
	}
	newDB, newConn, _, newResult := env.build(t, head, nil)
	if newResult.Action != "baseline_applied" || len(newResult.Created) != len(head.Objects) || len(newResult.Applied) != 0 {
		t.Fatalf("the head baseline = %+v, want baseline_applied creating %d objects and applying no chain file", newResult, len(head.Objects))
	}
	if diff := compare(capture(t, env.ctx, newConn, newDB, productionContract), capture(t, env.ctx, oldConn, oldDB, productionContract)); diff != "" {
		t.Fatalf("the head baseline builds a different database than the archived chain: %s", diff)
	}
	if diff := compare(capture(t, env.ctx, oldConn, oldDB, productionContract), head); diff != "" {
		t.Fatalf("head.json is not the capture of the archived chain: %s", diff)
	}
	compareCatalogs(t, "head baseline vs archived chain", catalog(t, env.ctx, newConn, newDB), catalog(t, env.ctx, oldConn, oldDB))
}

// TestAPreSquashHeadDatabaseRunsNoDDL: a database the old way built to its head (prod, bigboy and every dev database
// reach the squash this way) is at this build's head, nothing pending, and an upgrade of it changes nothing: no
// object, no seed row, no version row.
func TestAPreSquashHeadDatabaseRunsNoDDL(t *testing.T) {
	env := newSquashEnv(t)
	head, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	live, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	database, conn, db, _ := env.build(t, loadArchivedBaseline(t), loadArchivedChain(t))
	beforeCatalog, beforeVersions := catalog(t, env.ctx, conn, database), versionRows(t, env.ctx, conn, database)
	beforeCapture := capture(t, env.ctx, conn, database, productionContract)

	status, err := chmigrate.ReadStatus(env.ctx, db, head, live)
	if err != nil || status.State != "at_head" || len(status.Pending) != 0 || status.Head != chmigrate.HeadVersion(head, live) {
		t.Fatalf("status of a pre-squash head database = %+v, %v; want at_head with nothing pending", status, err)
	}
	result, err := chmigrate.Upgrade(env.ctx, db, head, live)
	if err != nil || result.Action != "up_to_date" || len(result.Created)+len(result.Seeded)+len(result.Applied) != 0 {
		t.Fatalf("upgrade of a pre-squash head database = %+v, %v; want up_to_date, nothing created, seeded or applied", result, err)
	}
	if got := versionRows(t, env.ctx, conn, database); !reflect.DeepEqual(got, beforeVersions) {
		t.Errorf("schema_migrations changed: %s", firstDifference(got, beforeVersions))
	}
	if diff := compare(capture(t, env.ctx, conn, database, productionContract), beforeCapture); diff != "" {
		t.Errorf("the upgrade changed the database: %s", diff)
	}
	compareCatalogs(t, "after the upgrade", catalog(t, env.ctx, conn, database), beforeCatalog)
}

// TestADatabaseBelowTheSquashedBaselineIsRefused: a database the old way left part-way through the archived chain
// cannot reach the head with this build (its missing files are archived). It is refused as below the head, naming
// the missing versions and the remedy, and nothing is written.
func TestADatabaseBelowTheSquashedBaselineIsRefused(t *testing.T) {
	env := newSquashEnv(t)
	head, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	live, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	archivedChain := loadArchivedChain(t)
	last := archivedChain[len(archivedChain)-1].Version
	database, conn, db, _ := env.build(t, loadArchivedBaseline(t), archivedChain[:len(archivedChain)-1])
	beforeCatalog, beforeVersions := catalog(t, env.ctx, conn, database), versionRows(t, env.ctx, conn, database)

	status, err := chmigrate.ReadStatus(env.ctx, db, head, live)
	if err != nil || status.State != "below_head" || !reflect.DeepEqual(status.Missing, []string{last}) {
		t.Fatalf("status = %+v, %v; want below_head missing [%s]", status, err, last)
	}
	_, err = chmigrate.Upgrade(env.ctx, db, head, live)
	var below chmigrate.BelowHeadError
	if !errors.As(err, &below) || !reflect.DeepEqual(below.Missing, []string{last}) {
		t.Fatalf("upgrade = %v, want below the head naming %s", err, last)
	}
	for _, phrase := range []string{"squashed", "pre-squash build", "re-create"} {
		if !strings.Contains(err.Error(), phrase) {
			t.Errorf("the refusal does not say %q: %v", phrase, err)
		}
	}
	if got := versionRows(t, env.ctx, conn, database); !reflect.DeepEqual(got, beforeVersions) {
		t.Errorf("a refused upgrade changed schema_migrations: %s", firstDifference(got, beforeVersions))
	}
	compareCatalogs(t, "after the refusal", catalog(t, env.ctx, conn, database), beforeCatalog)
}

// regenerateHeadEnv set to "1" makes TestRegenerateTheHeadBaseline write baseline/head.json from the archive. It is
// the only producer of head.json; TestBaselineEqualsTheArchivedChain then holds the file to the archive.
const regenerateHeadEnv = "CHMIGRATE_REGENERATE_HEAD"

// TestRegenerateTheHeadBaseline writes baseline/head.json: the capture of a fresh database built the old way (the
// archived 098 baseline, then every archived chain file), in the archived file's own JSON form.
func TestRegenerateTheHeadBaseline(t *testing.T) {
	if os.Getenv(regenerateHeadEnv) != "1" {
		t.Skipf("set %s=1 to regenerate baseline/head.json", regenerateHeadEnv)
	}
	archived, err := os.ReadFile(filepath.Join(archiveDir, "baseline-098", "head.json"))
	if err != nil {
		t.Fatal(err)
	}
	if again, err := json.MarshalIndent(loadArchivedBaseline(t), "", "  "); err != nil || string(again)+"\n" != string(archived) {
		t.Fatalf("the archived baseline is not in json.MarshalIndent form (%v): the regenerated file would not be in the same form", err)
	}
	env := newSquashEnv(t)
	database, conn, _, _ := env.build(t, loadArchivedBaseline(t), loadArchivedChain(t))
	data, err := json.MarshalIndent(capture(t, env.ctx, conn, database, productionContract), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("baseline", "head.json"), append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
