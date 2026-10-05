//go:build integration

package goapiproof

import (
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// CHAOS-8649: query-api's switch reads an operation's rows under its current AND its registered legacy
// document digests (CHAOS-8000 dual accept). The census must count exactly those rows: a carried row under
// a registered legacy digest is MATCH with its own mode and build, and a row under any other digest stays
// unreachable. The switch itself is the oracle: for every operation, the census's answer is checked against
// what routeswitch.NewPostgresSwitchWithLegacy (the row-reading switch) says over the same rows and the same catalog data.
func TestRoutingStatusRowsWithLegacyAgreesWithTheSwitchOnLegacyDigestRows(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)

	const (
		currentBuild = "1111111111111111111111111111111111111111"
		legacyBuild  = "2222222222222222222222222222222222222222"
	)
	digest := func(fill string) string { return strings.Repeat(fill, 64) }
	catalog := map[string]string{
		"currentRow":   digest("a"),
		"legacyRow":    digest("b"),
		"strayRow":     digest("c"),
		"bothRows":     digest("d"),
		"otherLegacy":  digest("e"),
		"legacyShadow": digest("f"),
	}
	legacy := map[string][]string{
		"legacyRow":    {digest("1")},
		"strayRow":     {digest("2")},
		"bothRows":     {digest("3")},
		"otherLegacy":  {digest("4")},
		"legacyShadow": {digest("5")},
	}
	seedRow(t, ctx, "currentRow", digest("a"), "canary", currentBuild, pool)
	seedRow(t, ctx, "legacyRow", digest("1"), "canary", legacyBuild, pool)
	// A digest nothing registers for this operation: dead, whatever its mode.
	seedRow(t, ctx, "strayRow", digest("9"), "canary", legacyBuild, pool)
	// Two accepted rows: the current one off, the legacy one on. The switch serves when ANY is on.
	seedRow(t, ctx, "bothRows", digest("d"), "disabled", currentBuild, pool)
	seedRow(t, ctx, "bothRows", digest("3"), "primary", legacyBuild, pool)
	// Another operation's legacy digest is not this operation's: legacy is per operation.
	seedRow(t, ctx, "otherLegacy", digest("1"), "canary", legacyBuild, pool)
	// A legacy row that holds its operation dark is MATCH and not reachable, exactly like a current one.
	seedRow(t, ctx, "legacyShadow", digest("5"), "shadow", legacyBuild, pool)

	statuses, err := RoutingStatusRowsWithLegacy(ctx, pool, testSchemaDigest, "", catalog, nil, legacy)
	if err != nil {
		t.Fatalf("RoutingStatusRowsWithLegacy: %v", err)
	}
	byOperation := map[string]OperationStatus{}
	for _, status := range statuses {
		byOperation[status.Operation] = status
	}
	if len(byOperation) != len(catalog) {
		t.Fatalf("census reported %d operations, want the %d catalog operations: %+v", len(byOperation), len(catalog), statuses)
	}

	type want struct {
		state, class, rowDigest, mode, build string
		reachable                            bool
		accepted, unreachable                []string
	}
	wants := map[string]want{
		"currentRow":   {DigestMatch, DocumentClassCurrent, digest("a"), "canary", currentBuild, true, []string{digest("a")}, nil},
		"legacyRow":    {DigestMatch, DocumentClassLegacy, digest("1"), "canary", legacyBuild, true, []string{digest("1")}, nil},
		"strayRow":     {DigestStale, "", "", "", "", false, nil, []string{digest("9")}},
		"bothRows":     {DigestMatch, DocumentClassLegacy, digest("3"), "primary", legacyBuild, true, []string{digest("3"), digest("d")}, nil},
		"otherLegacy":  {DigestStale, "", "", "", "", false, nil, []string{digest("1")}},
		"legacyShadow": {DigestMatch, DocumentClassLegacy, digest("5"), "shadow", legacyBuild, false, []string{digest("5")}, nil},
	}
	for operation, w := range wants {
		got := byOperation[operation]
		if got.DigestState != w.state || got.DocumentClass != w.class || got.RowDocumentDigest != w.rowDigest ||
			got.Mode != w.mode || got.CurrentCandidateBuild != w.build || got.Reachable() != w.reachable {
			t.Errorf("%s: state=%s class=%q row=%s mode=%q build=%q reachable=%t, want state=%s class=%q row=%s mode=%q build=%q reachable=%t",
				operation, got.DigestState, got.DocumentClass, got.RowDocumentDigest, got.Mode, got.CurrentCandidateBuild, got.Reachable(),
				w.state, w.class, w.rowDigest, w.mode, w.build, w.reachable)
		}
		if !reflect.DeepEqual(got.AcceptedDocumentDigests, w.accepted) || !reflect.DeepEqual(got.UnreachableDocumentDigests, w.unreachable) {
			t.Errorf("%s: accepted=%v unreachable=%v, want accepted=%v unreachable=%v",
				operation, got.AcceptedDocumentDigests, got.UnreachableDocumentDigests, w.accepted, w.unreachable)
		}
		if got.DocumentDigest != catalog[operation] {
			t.Errorf("%s: document_digest=%s, want the catalog's current digest %s unchanged", operation, got.DocumentDigest, catalog[operation])
		}
	}

	// The oracle: the row-reading switch (the class-row and proof switches), over the same rows and the
	// same catalog data. The census's Reachable must equal its Enabled exactly. query-api's /query and
	// /graphql switch reads no row any more (CHAOS-8702).
	sw := routeswitch.NewPostgresSwitchWithLegacy(pool, testSchemaDigest, catalog, legacy)
	for operation := range catalog {
		status := byOperation[operation]
		if served := sw.Enabled(operation); served != status.Reachable() || status.ServedWithoutRow() {
			t.Errorf("%s: the switch serves=%t, the census says reachable=%t served_without_row=%t (state %s)",
				operation, served, status.Reachable(), status.ServedWithoutRow(), status.DigestState)
		}
	}
}

// RoutingStatusRowsWithKinds passes no legacy digests, so a caller of it judges every operation by its
// current text alone; a caller that has the catalog's legacy entries must pass them.
func TestRoutingStatusRowsWithKindsIsTheLegacyCensusWithNoLegacyDigests(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	legacyDigest := strings.Repeat("1", 64)
	seedRow(t, ctx, "legacyRow", legacyDigest, "canary", testCandidateBuild, pool)
	catalog := map[string]string{"legacyRow": testDocumentDigest}

	withKinds, err := RoutingStatusRowsWithKinds(ctx, pool, testSchemaDigest, "", catalog, nil)
	if err != nil {
		t.Fatal(err)
	}
	withNilLegacy, err := RoutingStatusRowsWithLegacy(ctx, pool, testSchemaDigest, "", catalog, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(withKinds, withNilLegacy) {
		t.Fatalf("WithKinds = %+v, WithLegacy(nil) = %+v: want the same census", withKinds, withNilLegacy)
	}
	if got := withKinds[0]; got.DigestState != DigestStale || !reflect.DeepEqual(got.UnreachableDocumentDigests, []string{legacyDigest}) {
		t.Fatalf("with no legacy digests the row is %+v, want STALE with %s unreachable", got, legacyDigest)
	}
}
