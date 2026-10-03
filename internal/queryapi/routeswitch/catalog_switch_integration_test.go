//go:build integration

package routeswitch

// CHAOS-8517: the catalog rule over the REAL go_api_routing_state (the migrated schema, a real
// Postgres). catalog_switch_test.go runs the same row states at the unit tier over an in-memory table;
// what only this file can show is that Postgres reads the two statements the way that table does.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

const olderSchemaDigest = "sha256:an-older-schema-digest"

func insertRoutingStateAt(t *testing.T, pool *pgxpool.Pool, schemaDigest, documentDigest, operation, mode string) {
	t.Helper()
	pgseed.RoutingState(context.Background(), t, pool, schemaDigest, documentDigest, operation, mode)
}

// (1) An empty table: the catalog switch serves a registered operation. Every other switch built on
// the same type keeps its refusal -- the plain one (what this route used before), the proof switch,
// and the class-row switch for every allowlisted MCP root.
func TestCatalogSwitch_EmptyTableServesARegisteredOperation(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	digests := map[string]string{"securityAlerts": digestA, "hotspots": digestB}

	catalog := NewCatalogSwitchWithLegacy(pool, testSchemaDigest, digests, nil)
	for operation := range digests {
		if !catalog.Enabled(operation) {
			t.Errorf("empty table: the catalog switch did not serve the registered operation %s", operation)
		}
		if NewPostgresSwitchWithLegacy(pool, testSchemaDigest, digests, nil).Enabled(operation) {
			t.Errorf("empty table: the switch WITHOUT the catalog rule served %s (the old answer is a refusal)", operation)
		}
		if NewProofSwitchWithLegacy(pool, testSchemaDigest, digests, nil).Enabled(operation) {
			t.Errorf("empty table: the proof switch served %s; a measurement route still needs a row", operation)
		}
	}

	// (5) A name the process does not register is refused, and so is a class key asked of this switch.
	for _, operation := range []string{"notInTheCatalog", "mcp:securityAlerts"} {
		if catalog.Enabled(operation) {
			t.Errorf("empty table: the catalog switch served %q, which it does not register", operation)
		}
	}

	// (4) The class-row switch (server/class_row_gate.go builds exactly this) over the same empty table:
	// every MCP root is dark, securityAlerts with them.
	class := NewPostgresSwitch(pool, testSchemaDigest, mcpclass.Digests())
	for _, root := range mcpclass.SortedRoots() {
		if class.Enabled(mcpclass.Operation(root)) {
			t.Errorf("empty table: the class-row switch enabled %s", mcpclass.Operation(root))
		}
	}
}

// (2) and the non-served modes: an operation that has a row at its live key answers by that row,
// exactly as the plain switch does over the same row.
func TestCatalogSwitch_ALiveRowKeepsItsAnswer(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	for mode, want := range map[string]bool{"python": false, "shadow": false, "canary": true, "primary": true, "disabled": false} {
		operation, documentDigest := "catalog_"+mode, "doc-catalog-"+mode
		insertRoutingState(t, pool, documentDigest, operation, mode)
		digests := map[string]string{operation: documentDigest}
		if got := NewCatalogSwitchWithLegacy(pool, testSchemaDigest, digests, nil).Enabled(operation); got != want {
			t.Errorf("mode=%s: the catalog switch answered %v, want %v", mode, got, want)
		}
		if got := NewPostgresSwitchWithLegacy(pool, testSchemaDigest, digests, nil).Enabled(operation); got != want {
			t.Errorf("mode=%s: the plain switch answered %v, want %v (the row's answer did not change)", mode, got, want)
		}
	}
}

// A row that is not at the live key still counts as a row: the operation is not served, in any mode,
// and it keeps the digest miss it always had. (6) covers the document clause: a canary row under a
// document digest the process does not register for the operation serves nothing.
func TestCatalogSwitch_ARowElsewhereHoldsTheOperationDark(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	digests := map[string]string{}
	for _, mode := range []string{"python", "shadow", "canary", "primary", "disabled"} {
		operation := "stale_" + mode
		digests[operation] = "doc-" + operation
		insertRoutingStateAt(t, pool, olderSchemaDigest, digests[operation], operation, mode)
	}
	digests["otherDocument"] = digestA
	insertRoutingState(t, pool, digestB, "otherDocument", "canary")
	legacy := map[string][]string{"legacyRow": {"doc-legacy-old"}}
	digests["legacyRow"] = "doc-legacy-current"
	insertRoutingState(t, pool, "doc-legacy-old", "legacyRow", "canary")

	var misses []string
	previous := recordDigestMiss
	recordDigestMiss = func(_ context.Context, operation, _, _ string) { misses = append(misses, operation) }
	defer func() { recordDigestMiss = previous }()

	sw := NewCatalogSwitchWithLegacy(pool, testSchemaDigest, digests, legacy)
	for operation := range digests {
		want := operation == "legacyRow" // a canary row under an ACCEPTED legacy digest is a live row
		if got := sw.Enabled(operation); got != want {
			t.Errorf("Enabled(%q) = %v, want %v", operation, got, want)
		}
	}
	if len(misses) != len(digests)-1 || strings.Contains(strings.Join(misses, " "), "legacyRow") {
		t.Errorf("digest-miss signals = %v, want one for each of the %d operations whose only row is elsewhere", misses, len(digests)-1)
	}
}

// (3) securityAlerts stays dark for the MCP caller class, on production-like rows and on an empty table.
// Production holds a canary document row for every catalog operation, a canary class row for 13 roots,
// and the securityAlerts class row only at an OLDER schema digest, in shadow (CHAOS-8143). The class-row
// switch refuses it in each state; the catalog switch never answers for a class key.
func TestCatalogSwitch_SecurityAlertsClassRootStaysDark(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	documents := map[string]string{"securityAlerts": digestA}
	securityAlerts := mcpclass.Operation("securityAlerts")

	insertRoutingState(t, pool, digestA, "securityAlerts", "canary")
	for _, root := range mcpclass.SortedRoots() {
		if root != "securityAlerts" {
			insertRoutingState(t, pool, mcpclass.DocumentDigest(), mcpclass.Operation(root), "canary")
		}
	}
	insertRoutingStateAt(t, pool, olderSchemaDigest, mcpclass.DocumentDigest(), securityAlerts, "shadow")

	class := NewPostgresSwitch(pool, testSchemaDigest, mcpclass.Digests())
	catalog := NewCatalogSwitchWithLegacy(pool, testSchemaDigest, documents, nil)
	check := func(state string, wantOtherRoots bool) {
		t.Helper()
		if class.Enabled(securityAlerts) {
			t.Errorf("%s: the class-row switch enabled %s", state, securityAlerts)
		}
		if catalog.Enabled(securityAlerts) {
			t.Errorf("%s: the catalog switch answered for the class key %s", state, securityAlerts)
		}
		if !catalog.Enabled("securityAlerts") {
			t.Errorf("%s: the catalog switch did not serve the DOCUMENT operation securityAlerts", state)
		}
		for _, root := range mcpclass.SortedRoots() {
			if root == "securityAlerts" {
				continue
			}
			if got := class.Enabled(mcpclass.Operation(root)); got != wantOtherRoots {
				t.Errorf("%s: class root %s enabled=%v, want %v", state, root, got, wantOtherRoots)
			}
		}
	}
	check("production-like rows (securityAlerts shadow at an older schema digest)", true)

	insertRoutingState(t, pool, mcpclass.DocumentDigest(), securityAlerts, "shadow")
	check("a shadow class row at the live schema digest", true)

	if _, err := pool.Exec(context.Background(), `DELETE FROM go_api_routing_state`); err != nil {
		t.Fatal(err)
	}
	check("an empty table (a fresh stack)", false)
}

// The decision is observable where the digest miss is: the counter, read back through a real reader, and
// the real log record. (The counter is read through a meter of the test's own, readServedWithoutRow says
// why; the digest-miss test of this package owns the global provider.)
func TestCatalogSwitch_ServedWithoutRowEmitsCounterAndLog(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	read := readServedWithoutRow(t)

	var misses []string
	previous := recordDigestMiss
	recordDigestMiss = func(_ context.Context, operation, _, _ string) { misses = append(misses, operation) }
	defer func() { recordDigestMiss = previous }()

	var logged bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	defer slog.SetDefault(previousLogger)

	sw := NewCatalogSwitchWithLegacy(pool, testSchemaDigest, map[string]string{"hotspots": digestA}, nil)
	if !sw.Enabled("hotspots") || !sw.Enabled("hotspots") {
		t.Fatal("empty table: the catalog switch did not serve hotspots")
	}

	record := findDigestMissLogRecord(t, logged.Bytes()) // the first record of this package, whatever it is
	if record["level"] != "INFO" || record["operation"] != "hotspots" || record["reason"] != ReasonCatalogNoRow {
		t.Errorf("log record = %v, want INFO operation=hotspots reason=%s", record, ReasonCatalogNoRow)
	}
	if strings.Count(logged.String(), "routeswitch") != 1 {
		t.Errorf("two decisions on one operation wrote more than one record:\n%s", logged.String())
	}
	if value := read("hotspots"); value != 2 {
		t.Errorf("%s{operation=hotspots,reason=%s} = %d, want 2 (-1 = no such data point)", servedWithoutRowMetric, ReasonCatalogNoRow, value)
	}
	if len(misses) != 0 {
		t.Errorf("a served operation fired the digest miss: %v", misses)
	}
}
