//go:build integration

package remaining

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// This file is the second mandated guard for CHAOS-3092 R1 (Option C).
//
// # What it is defending against, and why a smaller fixture cannot
//
// The native DORA executor reads current operational rows through one of two
// SQL shapes, chosen by OPERATIONAL_ORDERING_CONTRACT: legacy `FINAL`, or the
// contract-2 `ORDER BY source_revision DESC ... LIMIT 1 BY org_id, id` that
// migration 067 introduced.
//
// Over SINGLE-VERSION data the two shapes return exactly the same rows. That
// is the whole problem: a parity fixture in which every incident appears once
// reports EQUAL for a port that picked the WRONG branch, and reports EQUAL
// again the day the contract flips. The comparison is not weak, it is VACUOUS
// -- the two hypotheses "this port reads current rows correctly" and "this
// port reads them incorrectly" predict identical observations, so the
// observation cannot choose between them. Every assertion here would still
// pass against an executor hard-coded to the wrong contract.
//
// This fixture therefore carries a keyed row with MORE THAN ONE version, and
// asserts that it does (anti-vacuity, below) before asserting anything about
// projections -- otherwise a later fixture edit that collapses the versions
// degrades the test back to the false pass without failing.
//
// # The measured divergence
//
// Migration 067 does not merely swap the SQL. It rewrites the table to
// ReplacingMergeTree(ingest_revision) with ORDER BY
// (org_id, id, source_revision, source_conflict_key). Because source_revision
// is now part of the sorting key, two versions of one incident are DISTINCT
// primary keys, and `FINAL` no longer collapses them at all.
//
// So the failure mode is not "picks the older version". It is "sees one
// incident as two incidents", which inflates incident counts and therefore
// time_to_restore_service and change_failure_rate. Legacy is the default when
// the environment variable is unset, so it is reachable by omission rather
// than by misconfiguration.
//
// TestOrderingContractSelectsDifferentSQL (dora_native_test.go) proves the two
// branches emit different text. Only this test proves they return different
// DATA, against the real migrated schema, which is the claim that matters.
//
// # Why BOTH schema shapes are exercised, not just the newest
//
// Migration 067 reads the SAME environment variable the query builder does and
// raises MigrationDeferred when it says legacy (067_operational_ordering
// _contract.py:323). The schema shape is therefore chosen at MIGRATION time by
// configuration, which means the contract-1 table is not a historical state
// that every deployment has left behind -- it is a currently reachable one,
// and a bridge left on contract 1 stays on the legacy table indefinitely.
//
// That was discovered here rather than assumed: the first run of this test
// applied the chain with the variable unset and got a contract-1 table, which
// is why the fixture below migrates each container deliberately instead of
// trusting the chain head to mean one thing.
//
// So the guard has to be right in BOTH directions -- refusing legacy-on-v2 AND
// contract-2-on-v1 -- and a guard tested in only one direction would be
// indistinguishable from a blanket refusal. The truth table below is the whole
// point.

const (
	orderingTestOrg      = "11111111-1111-4111-8111-111111111111"
	multiVersionIncident = "incident-multi-version"
	singleVersionInciden = "incident-single-version"
)

func TestOrderingContractBranchesDivergeOnMultiVersionRows(t *testing.T) {
	ctx := context.Background()
	conn := migratedClickHouse(t, ctx, OperationalOrderingRevision)

	seedIncidentVersions(t, ctx, conn)

	// ---- Anti-vacuity gate ------------------------------------------------
	//
	// Everything below is only evidence if the fixture actually produces a
	// version collision. Asserting this FIRST, and failing hard, is what stops
	// the test silently degrading into the single-version false pass it exists
	// to rule out.
	versions := maxVersionsPerKeyedRow(t, ctx, conn)
	if versions < 2 {
		t.Fatalf(
			"fixture is VACUOUS: the most-versioned (org_id, id) has %d version(s). "+
				"With one version per key both ordering branches return identical "+
				"rows, so every assertion below would pass against an executor on "+
				"the WRONG contract. Restore the superseded incident version.",
			versions,
		)
	}

	revisionRows := projectedIncidentIDs(t, ctx, conn, OperationalOrderingRevision)
	legacyRows := projectedIncidentIDs(t, ctx, conn, OperationalOrderingLegacy)

	// ---- Where they MUST agree -------------------------------------------
	//
	// The single-version incident is the control. If the branches disagreed
	// here the divergence below would be explained by something other than
	// version resolution, and the test would prove nothing about contracts.
	if got := revisionRows[singleVersionInciden]; got != 1 {
		t.Errorf("contract 2 returned %d rows for the single-version incident, want 1", got)
	}
	if got := legacyRows[singleVersionInciden]; got != 1 {
		t.Errorf("legacy returned %d rows for the single-version incident, want 1", got)
	}

	// ---- Where they MUST diverge -----------------------------------------
	if got := revisionRows[multiVersionIncident]; got != 1 {
		t.Errorf(
			"contract 2 returned %d rows for the multi-version incident, want 1 -- "+
				"the revision ordering is what collapses versions to a current row",
			got,
		)
	}
	if got := legacyRows[multiVersionIncident]; got < 2 {
		t.Errorf(
			"legacy returned %d rows for the multi-version incident, want >1. "+
				"If this now returns 1 the premise of the construction-time guard "+
				"in NewDORAExecutor has changed and the guard must be re-justified, "+
				"not deleted",
			got,
		)
	}

	// ---- And the winner must be the NEWER SOURCE revision ----------------
	//
	// Ingest order is deliberately inverted in the fixture (the newer
	// source_revision was ingested FIRST), so a branch that resolved by
	// arrival time rather than by source revision would pick the other title
	// and still return exactly one row -- passing every count assertion above.
	if title := currentIncidentTitle(t, ctx, conn, OperationalOrderingRevision); title != "NEWER_SOURCE_REVISION" {
		t.Errorf(
			"contract 2 resolved the current row to %q, want NEWER_SOURCE_REVISION -- "+
				"resolution must follow source_revision, not ingest order",
			title,
		)
	}
}

// TestOrderingContractGuardMatchesTheDeployedSchema is the fail-closed half,
// and it is a TRUTH TABLE rather than a pair of refusals on purpose.
//
// The projection test above shows what a worker on the wrong branch would
// compute. This one shows it never gets that far. Both directions are checked
// because both are reachable (see the migration-gate note in the file header):
// a bridge on contract 1 keeps a contract-1 table, so "refuse contract 2 here"
// is as real a requirement as "refuse legacy on a v2 table". Testing only the
// refusals would leave a guard that rejected everything scoring full marks.
func TestOrderingContractGuardMatchesTheDeployedSchema(t *testing.T) {
	ctx := context.Background()
	stores := map[OperationalOrderingContract]driver.Conn{
		OperationalOrderingLegacy:   migratedClickHouse(t, ctx, OperationalOrderingLegacy),
		OperationalOrderingRevision: migratedClickHouse(t, ctx, OperationalOrderingRevision),
	}

	cases := []struct {
		name      string
		configure func(*testing.T)
		schema    OperationalOrderingContract
		wantBuild bool
		wantErr   error
	}{
		{
			// The reachable-by-omission path: nothing set at all, so the query
			// builder defaults to legacy while the table is v2.
			name:      "unset against a v2 schema is refused",
			configure: unsetContract,
			schema:    OperationalOrderingRevision,
			wantErr:   ErrOrderingContractMismatch,
		},
		{
			name:      "explicit legacy against a v2 schema is refused",
			configure: setContract("1"),
			schema:    OperationalOrderingRevision,
			wantErr:   ErrOrderingContractMismatch,
		},
		{
			name:      "contract 2 against a legacy schema is refused",
			configure: setContract("2"),
			schema:    OperationalOrderingLegacy,
			wantErr:   ErrOrderingContractMismatch,
		},
		{
			// Exported-but-empty is a CONFIGURATION error, not a silent
			// fallback to legacy. It refuses on both schemas, and for a
			// different reason than a mismatch -- which is why the expected
			// error is asserted rather than just "some error".
			name:      "an empty value is a configuration error, not legacy",
			configure: setContract(""),
			schema:    OperationalOrderingLegacy,
			wantErr:   ErrOrderingContractUnparseable,
		},
		{
			name:      "a typo is refused rather than resolved to legacy",
			configure: setContract("3"),
			schema:    OperationalOrderingLegacy,
			wantErr:   ErrOrderingContractUnparseable,
		},
		{
			name:      "contract 2 against a v2 schema builds",
			configure: setContract("2"),
			schema:    OperationalOrderingRevision,
			wantBuild: true,
		},
		{
			name:      "legacy against a legacy schema builds",
			configure: setContract("1"),
			schema:    OperationalOrderingLegacy,
			wantBuild: true,
		},
		{
			// A deployment that has never set the variable and has never run
			// 067 is CONSISTENT, not broken. The guard must not turn it into
			// an outage.
			name:      "unset against a legacy schema builds",
			configure: unsetContract,
			schema:    OperationalOrderingLegacy,
			wantBuild: true,
		},
	}

	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			test.configure(t)
			_, err := NewDORAExecutor(ctx, stores[test.schema], nil)
			switch {
			case test.wantBuild && err != nil:
				t.Fatalf(
					"a consistent configuration must build -- a guard that "+
						"refuses everything is not a guard: %v", err,
				)
			case !test.wantBuild && err == nil:
				t.Fatal(
					"an inconsistent configuration must refuse at construction " +
						"rather than compute wrong numbers job after job",
				)
			case !test.wantBuild && !errors.Is(err, test.wantErr):
				t.Fatalf("expected %v, got: %v", test.wantErr, err)
			}
		})
	}
}

// setContract exports the variable for one subtest.
func setContract(value string) func(*testing.T) {
	return func(t *testing.T) { t.Setenv(operationalOrderingContractEnv, value) }
}

// unsetContract removes the variable entirely.
//
// t.Setenv(name, "") is NOT this: it leaves the variable exported with an empty
// value, which the strict parser treats as a configuration error rather than as
// absence -- exactly the distinction Python draws between None and "". Using
// t.Setenv here would have silently tested the wrong row of the table.
func unsetContract(t *testing.T) {
	t.Helper()
	previous, had := os.LookupEnv(operationalOrderingContractEnv)
	if err := os.Unsetenv(operationalOrderingContractEnv); err != nil {
		t.Fatalf("unset %s: %v", operationalOrderingContractEnv, err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(operationalOrderingContractEnv, previous)
		} else {
			_ = os.Unsetenv(operationalOrderingContractEnv)
		}
	})
}

// seedIncidentVersions writes the multi-version fixture.
//
// The two versions of the multi-version incident are ingested OUT OF ORDER on
// purpose: the newer source_revision carries the LOWER ingest_revision. A
// resolver keyed on arrival would therefore pick the stale row, and the title
// assertion catches exactly that.
func seedIncidentVersions(t *testing.T, ctx context.Context, conn driver.Conn) {
	t.Helper()
	observed := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	rows := []struct {
		id             string
		title          string
		sourceRevision int64
		ingestRevision int64
	}{
		// Newer source revision, ingested FIRST.
		{multiVersionIncident, "NEWER_SOURCE_REVISION", 2, 1},
		// Superseded source revision, ingested SECOND.
		{multiVersionIncident, "OLDER_SOURCE_REVISION", 1, 2},
		{singleVersionInciden, "ONLY_VERSION", 1, 1},
	}
	batch, err := conn.PrepareBatch(ctx, `
        INSERT INTO operational_incidents (
            org_id, provider, provider_instance_id, source_entity_type, external_id,
            source_version_at, id, observed_at, last_synced, title, is_deleted,
            started_at, resolved_at,
            source_revision, source_conflict_key, ingest_revision, ordering_contract
        )`)
	if err != nil {
		t.Fatalf("prepare incident batch: %v", err)
	}
	for _, row := range rows {
		started := observed
		resolved := observed.Add(2 * time.Hour)
		if err := batch.Append(
			orderingTestOrg, "pagerduty", "instance-1", "incident", row.id,
			observed, row.id, observed, observed, row.title, uint8(0),
			&started, &resolved,
			// source_revision and ingest_revision are UInt128; the driver
			// takes those as big integers, not as a Go word-sized value.
			big.NewInt(row.sourceRevision), "", big.NewInt(row.ingestRevision),
			// ordering_contract carries a CHECK constraint pinning it to 2;
			// omitting it is rejected with VIOLATED_CONSTRAINT rather than
			// defaulted, which is itself a schema-level statement that no
			// legacy-shaped row belongs in this table.
			uint8(2),
		); err != nil {
			t.Fatalf("append incident %s/%d: %v", row.id, row.sourceRevision, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send incident batch: %v", err)
	}
}

// maxVersionsPerKeyedRow reads the fixture's actual version multiplicity from
// the STORED rows rather than from the seed literal above, so a schema or
// engine change that silently collapses them fails the anti-vacuity gate.
func maxVersionsPerKeyedRow(t *testing.T, ctx context.Context, conn driver.Conn) uint64 {
	t.Helper()
	var most uint64
	if err := conn.QueryRow(ctx, `
        SELECT max(versions) FROM (
            SELECT count() AS versions
            FROM operational_incidents
            WHERE org_id = {org_id:String}
            GROUP BY org_id, id
        )`, clickhouse.Named("org_id", orderingTestOrg)).Scan(&most); err != nil {
		t.Fatalf("count versions per keyed row: %v", err)
	}
	return most
}

// projectedIncidentIDs runs the PRODUCTION projection builder -- not a query
// retyped for the test -- under one contract, and returns rows per incident.
func projectedIncidentIDs(
	t *testing.T, ctx context.Context, conn driver.Conn, contract OperationalOrderingContract,
) map[string]int {
	t.Helper()
	query := fmt.Sprintf(
		"SELECT id, count() AS rows FROM (%s) "+
			"WHERE org_id = {org_id:String} GROUP BY id",
		currentOperationalRowsSQL("operational_incidents", nil, contract),
	)
	// Named parameters, not positional: the production builder emits
	// {name:Type} placeholders, and the driver refuses a query that mixes the
	// two styles.
	result, err := conn.Query(ctx, query, clickhouse.Named("org_id", orderingTestOrg))
	if err != nil {
		t.Fatalf("project incidents under contract %d: %v", contract, err)
	}
	defer func() { _ = result.Close() }()

	counts := map[string]int{}
	for result.Next() {
		var id string
		var rows uint64
		if err := result.Scan(&id, &rows); err != nil {
			t.Fatalf("scan projection row: %v", err)
		}
		counts[id] = int(rows)
	}
	if err := result.Err(); err != nil {
		t.Fatalf("iterate projection: %v", err)
	}
	return counts
}

func currentIncidentTitle(
	t *testing.T, ctx context.Context, conn driver.Conn, contract OperationalOrderingContract,
) string {
	t.Helper()
	query := fmt.Sprintf(
		"SELECT title FROM (%s) "+
			"WHERE org_id = {org_id:String} AND id = {incident_id:String}",
		currentOperationalRowsSQL("operational_incidents", nil, contract),
	)
	var title string
	if err := conn.QueryRow(
		ctx, query,
		clickhouse.Named("org_id", orderingTestOrg),
		clickhouse.Named("incident_id", multiVersionIncident),
	).Scan(&title); err != nil {
		t.Fatalf("read the current incident title: %v", err)
	}
	return title
}

// migratedClickHouse returns a scratch store migrated to the schema shape the
// GIVEN contract produces, building each shape at most once per package run.
//
// The contract is exported into the migration subprocess's environment rather
// than passed as an argument because that is how the real chain reads it
// (chschema.Apply inherits os.Environ). os.Setenv is used instead of t.Setenv
// so the value is scoped to the migration call itself: these containers are
// shared across tests, and a t.Setenv restore firing at the end of whichever
// test happened to build one would leave the others reading a different
// variable than the schema they were handed.
//
// Hand-typed DDL would defeat the point entirely: the divergence under test is
// a property of what migration 067 does to the sorting key, so a test-authored
// table could only ever confirm what the test itself declared.
var (
	migratedStores     = map[OperationalOrderingContract]driver.Conn{}
	migratedStoresLock sync.Mutex
	// migratedInstances holds the dependency behind each cached conn so it can
	// be released when the package finishes.
	//
	// The conn is cached for the whole package on purpose -- the migration
	// chain is expensive -- so it must outlive any single test and t.Cleanup
	// is the wrong hook: it would drop the store while other tests are still
	// reading through the cached conn. TestMain is the only scope that
	// matches the cache's lifetime.
	//
	// This mattered only once the harness gained a remote-DSN path. Against a
	// container, discarding the Instance was harmless: the daemon reaps the
	// container when the run ends. Against a shared server the Instance holds
	// the ONLY closure that drops the scratch database, so discarding it
	// leaks a database per contract on every run -- silently, and on passing
	// runs.
	migratedInstances []*containers.Instance
)

func TestMain(m *testing.M) {
	code := m.Run()
	closeMigratedStores()
	os.Exit(code)
}

func closeMigratedStores() {
	migratedStoresLock.Lock()
	defer migratedStoresLock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, instance := range migratedInstances {
		if err := instance.Close(ctx); err != nil {
			// Cannot fail the run from here -- m.Run has already returned --
			// so make the orphan findable by name instead of silent.
			fmt.Fprintf(os.Stderr, "close migrated ClickHouse store: %v\n", err)
		}
	}
	migratedInstances = nil
}

func migratedClickHouse(
	t *testing.T, ctx context.Context, contract OperationalOrderingContract,
) driver.Conn {
	t.Helper()
	migratedStoresLock.Lock()
	defer migratedStoresLock.Unlock()
	if existing, ok := migratedStores[contract]; ok {
		return existing
	}

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	// Register for teardown the moment the store exists, NOT after the
	// migration chain below. Everything between here and the end of this
	// function can t.Fatalf, and a fatal after an unregistered create leaks
	// the scratch database on a shared server.
	migratedInstances = append(migratedInstances, instance)

	previous, had := os.LookupEnv(operationalOrderingContractEnv)
	if err := os.Setenv(
		operationalOrderingContractEnv, strconv.Itoa(int(contract)),
	); err != nil {
		t.Fatalf("set %s: %v", operationalOrderingContractEnv, err)
	}
	chschema.Apply(ctx, t, instance)
	if had {
		_ = os.Setenv(operationalOrderingContractEnv, previous)
	} else {
		_ = os.Unsetenv(operationalOrderingContractEnv)
	}

	conn := openClickHouse(t, ctx, instance)

	// The migration is configuration-gated, so "the chain ran" does not imply
	// "the intended schema exists". Verify the shape actually landed, using
	// the same production reader the executor's guard uses, before any test
	// draws a conclusion from this container.
	// Every table the projection reads, not just incidents: 067 rebuilds them
	// in a loop, so "the chain ran" does not imply they all landed on the same
	// contract.
	for _, table := range doraContractTables {
		deployed, err := schemaOrderingContract(ctx, conn, table)
		if err != nil {
			t.Fatalf("read the deployed contract for %s: %v", table, err)
		}
		if deployed != contract {
			t.Fatalf(
				"asked the chain for a contract-%d schema and %s came back as "+
					"contract-%d -- the fixture is not testing what it claims to test",
				contract, table, deployed,
			)
		}
	}

	migratedStores[contract] = conn
	return conn
}

func openClickHouse(
	t *testing.T, ctx context.Context, instance *containers.Instance,
) driver.Conn {
	t.Helper()
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	password, _ := parsed.User.Password()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{parsed.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(parsed.Path, "/"),
			Username: parsed.User.Username(),
			Password: password,
		},
	})
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	return conn
}

// TestAPartiallyMigratedSchemaIsRefused is the regression test for the gap that
// guarding only operational_incidents left open.
//
// Migration 067 rebuilds each operational table in a LOOP, through
// non-transactional ClickHouse DDL (shadow table, copy, EXCHANGE TABLES). There
// is no transaction wrapping the loop, so an interrupted or failed run can
// genuinely leave one table on contract 2 and the next still on legacy. That is
// not a hypothetical: it is the ordinary failure mode of a multi-statement DDL
// migration in an engine without transactional DDL.
//
// With the guard reading only incidents, that state passed construction. The
// projection then wrapped BOTH tables in the contract-2 form, so the query
// either referenced source_revision on a table that does not have it, or
// resolved mapping versions under semantics the table was not built for --
// wrong repository attribution rather than a clean failure. Wrong attribution
// is the worse outcome, because it still produces numbers.
//
// The fixture builds the partial state honestly: it takes the LEGACY table's
// real DDL from a legacy-migrated store and installs it into a
// contract-2-migrated store, rather than hand-writing a table that only
// resembles one.
func TestAPartiallyMigratedSchemaIsRefused(t *testing.T) {
	ctx := context.Background()

	legacyDDL := showCreateTable(t, ctx,
		migratedClickHouse(t, ctx, OperationalOrderingLegacy),
		partiallyMigratedTable)

	// A dedicated store: this one gets mutated into an inconsistent state and
	// must not be shared with the tests that rely on a coherent schema.
	partial := freshMigratedClickHouse(t, ctx, OperationalOrderingRevision)

	if err := partial.Exec(ctx,
		"DROP TABLE `"+partiallyMigratedTable+"`"); err != nil {
		t.Fatalf("drop %s: %v", partiallyMigratedTable, err)
	}
	if err := partial.Exec(ctx, legacyDDL); err != nil {
		t.Fatalf("install the legacy %s: %v", partiallyMigratedTable, err)
	}

	// Prove the fixture really is INCONSISTENT before asserting on the guard.
	// If both tables ended up on the same contract, the test below would pass
	// for the ordinary mismatch reason and prove nothing about partial states.
	incidents, err := schemaOrderingContract(ctx, partial, "operational_incidents")
	if err != nil {
		t.Fatalf("read incidents contract: %v", err)
	}
	mappings, err := schemaOrderingContract(ctx, partial, partiallyMigratedTable)
	if err != nil {
		t.Fatalf("read mappings contract: %v", err)
	}
	if incidents == mappings {
		t.Fatalf(
			"fixture is not partial: both tables are on contract %d, so this "+
				"would exercise the ordinary mismatch path instead", incidents,
		)
	}

	// The configured contract AGREES with incidents. Under a guard that checked
	// only incidents this construction succeeded.
	t.Setenv(operationalOrderingContractEnv, "2")
	_, err = NewDORAExecutor(ctx, partial, nil)
	if err == nil {
		t.Fatal(
			"a partially migrated schema was accepted. The projection reads " +
				"both tables through the ordering contract, so this worker " +
				"would have queried a legacy mappings table under contract-2 " +
				"semantics and produced wrong repository attribution rather " +
				"than failing",
		)
	}
	if !errors.Is(err, ErrOrderingContractMismatch) {
		t.Fatalf("expected a contract mismatch, got: %v", err)
	}
	if !strings.Contains(err.Error(), partiallyMigratedTable) {
		t.Errorf(
			"the refusal must NAME the table that disagrees, or an operator "+
				"cannot tell which half of the migration to finish: %v", err,
		)
	}
}

const partiallyMigratedTable = "operational_service_repository_mappings"

func showCreateTable(
	t *testing.T, ctx context.Context, conn driver.Conn, table string,
) string {
	t.Helper()
	var ddl string
	if err := conn.QueryRow(
		ctx, "SHOW CREATE TABLE `"+table+"`").Scan(&ddl); err != nil {
		t.Fatalf("show create table %s: %v", table, err)
	}
	return ddl
}

// freshMigratedClickHouse builds an UNCACHED store, for tests that mutate the
// schema. Handing one of the shared containers to a test that then breaks its
// schema would make every later test's result depend on execution order.
func freshMigratedClickHouse(
	t *testing.T, ctx context.Context, contract OperationalOrderingContract,
) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })

	previous, had := os.LookupEnv(operationalOrderingContractEnv)
	if err := os.Setenv(
		operationalOrderingContractEnv, strconv.Itoa(int(contract)),
	); err != nil {
		t.Fatalf("set %s: %v", operationalOrderingContractEnv, err)
	}
	chschema.Apply(ctx, t, instance)
	if had {
		_ = os.Setenv(operationalOrderingContractEnv, previous)
	} else {
		_ = os.Unsetenv(operationalOrderingContractEnv)
	}
	return openClickHouse(t, ctx, instance)
}
