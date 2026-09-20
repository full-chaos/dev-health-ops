package main

import (
	"os"
	"testing"
)

// TestRegisteredFeatureFlagsDocument_MatchesCapturedWireFixture is
// CHAOS-4696's evidence-bar requirement 1: a fixture captured off a REAL
// HTTP request, not rebuilt from source and not produced by invoking
// urql inside a test harness (that shortcut would reproduce the exact
// circularity that hid this bug -- every existing test in the epic built
// its request from the same bytes it compared against).
//
// testdata/wire_capture/featureflags_captured.graphql was captured by
// running this repo's OWN unmodified web-side graphqlFetch
// (web/src/lib/graphql/server.ts) against a real local HTTP listener and
// recording the raw query bytes a real fetch() call sent -- see
// testdata/wire_capture/README.md for the exact mechanism
// (web/scripts/capture-graphql-wire-fixture.ts) and both digests.
//
// This test is independent of registeredFeatureFlagsDocument's own doc
// comment: it does not trust the comment's claim that the const equals
// the wire form, it proves it against a fixture this file never
// generates or touches.
func TestRegisteredFeatureFlagsDocument_MatchesCapturedWireFixture(t *testing.T) {
	captured, err := os.ReadFile("testdata/wire_capture/featureflags_captured.graphql")
	if err != nil {
		t.Fatalf("read captured wire fixture: %v", err)
	}

	gotDigest := digestHex(string(captured))
	wantDigest := digestHex(registeredFeatureFlagsDocument)

	if gotDigest != wantDigest {
		t.Fatalf(
			"registeredFeatureFlagsDocument digest %s does NOT match the digest of a REAL captured request (%s) -- "+
				"a real client's featureFlags request would 404 against this route (CHAOS-4696). "+
				"captured fixture:\n%s\n\nregistered const:\n%s",
			wantDigest, gotDigest, string(captured), registeredFeatureFlagsDocument,
		)
	}

	// The captured fixture must ALSO differ from the raw, unprinted web
	// source text's digest (555bc9f8...) -- if it did not, the capture
	// mechanism itself would be broken (either the real client stopped
	// reflowing/adding __typename, or the capture script silently fell
	// back to source text). This is the negative control CHAOS-4696's
	// evidence bar calls for: proof the gate can tell REAL wire bytes
	// apart from source-copied bytes, not just proof they currently
	// happen to agree.
	const rawSourceDigest = "555bc9f82339b8321f309a26d310c4a7e41e79b9b155da41f62d8e97b50da8b7"
	if gotDigest == rawSourceDigest {
		t.Fatalf(
			"captured wire fixture digests to the RAW SOURCE TEXT digest (%s) -- "+
				"the capture mechanism is not observing urql's real print()+__typename transforms; "+
				"this test would pass even if the underlying defect returned",
			rawSourceDigest,
		)
	}
}

// TestRegisteredFeatureFlagEventsDocument_MatchesCapturedWireFixture is
// CHAOS-5523's evidence-bar requirement, modelled directly on
// TestRegisteredFeatureFlagsDocument_MatchesCapturedWireFixture above:
// registeredFeatureFlagEventsDocument must digest to the SAME value as a
// fixture captured off a real HTTP request produced by this repo's own
// unmodified web-side graphqlFetch -- not rebuilt from source, not
// trusted from the const's own doc comment.
//
// testdata/wire_capture/featureflagevents_captured.graphql was captured
// the same way featureflags_captured.graphql was (see
// testdata/wire_capture/README.md's featureFlagEvents section for the
// exact mechanism and both digests) -- this test proves the const
// against that fixture independently of the doc comment's claim.
func TestRegisteredFeatureFlagEventsDocument_MatchesCapturedWireFixture(t *testing.T) {
	captured, err := os.ReadFile("testdata/wire_capture/featureflagevents_captured.graphql")
	if err != nil {
		t.Fatalf("read captured wire fixture: %v", err)
	}

	gotDigest := digestHex(string(captured))
	wantDigest := digestHex(registeredFeatureFlagEventsDocument)

	if gotDigest != wantDigest {
		t.Fatalf(
			"registeredFeatureFlagEventsDocument digest %s does NOT match the digest of a REAL captured request (%s) -- "+
				"a real client's featureFlagEvents request would 404 against this route (CHAOS-4696 class). "+
				"captured fixture:\n%s\n\nregistered const:\n%s",
			wantDigest, gotDigest, string(captured), registeredFeatureFlagEventsDocument,
		)
	}

	// Same negative control as the featureFlags test above: the captured
	// fixture must differ from the raw, unprinted web source text's
	// digest, proving this test can tell real wire bytes apart from a
	// source-copied guess.
	const rawSourceDigestEvents = "4a3e3f98adb538466e4a7963af6ba88bc1b1fbcc4c08c56895f838c1d00210b9"
	if gotDigest == rawSourceDigestEvents {
		t.Fatalf(
			"captured wire fixture digests to the RAW SOURCE TEXT digest (%s) -- "+
				"the capture mechanism is not observing urql's real print()+__typename transforms; "+
				"this test would pass even if the underlying defect returned",
			rawSourceDigestEvents,
		)
	}
}

// TestRegisteredPrDetailDocument_MatchesCapturedWireFixture is CHAOS-4991's
// evidence-bar requirement for the `pr` operation, same discipline as
// TestRegisteredFeatureFlagsDocument_MatchesCapturedWireFixture above.
//
// testdata/wire_capture/pr_captured.graphql was produced by IMPORTING the
// web repo's own, live, pinned `wireForm()` (scripts/graphql-wire-parity.ts
// -- the exact function that repo's own CI wire-parity gate calls) and
// invoking it directly against the real `PR_DETAIL_QUERY` export
// (web/src/lib/graphql/queries.ts:94-138) via `tsx`, so `@urql/core` was
// resolved from the web repo's own pinned node_modules -- the same
// `createRequest` -> `formatDocument` -> `stringifyDocument` pipeline
// `fetchExchange` calls in production, not a hand-rolled reimplementation
// of its rules. This differs from featureflags_captured.graphql's
// mechanism (an actual HTTP request captured off a real listener) only in
// HOW the real urql code was invoked -- both paths call the identical
// pinned `@urql/core` functions in the identical order; see this file's
// own package-level precedent doc comment and
// testdata/wire_capture/README.md's "pr" section for the full method and
// why a live-HTTP capture was not required to meet the same evidence bar.
func TestRegisteredPrDetailDocument_MatchesCapturedWireFixture(t *testing.T) {
	captured, err := os.ReadFile("testdata/wire_capture/pr_captured.graphql")
	if err != nil {
		t.Fatalf("read captured wire fixture: %v", err)
	}

	gotDigest := digestHex(string(captured))
	wantDigest := digestHex(registeredPrDetailDocument)

	if gotDigest != wantDigest {
		t.Fatalf(
			"registeredPrDetailDocument digest %s does NOT match the digest of the captured wire form (%s) -- "+
				"a real client's pr request would 404 against this route once/if enabled (CHAOS-4696-class defect). "+
				"captured fixture:\n%s\n\nregistered const:\n%s",
			wantDigest, gotDigest, string(captured), registeredPrDetailDocument,
		)
	}
}

// TestRegisteredCapturedDocuments_MatchCapturedWireFixtures proves each
// registered security document digests to the wire-form text produced by
// the web repo's own wire-parity tooling, so a real client's request is
// accepted by the route.
func TestRegisteredCapturedDocuments_MatchCapturedWireFixtures(t *testing.T) {
	for _, c := range []struct{ fixture, registered string }{
		{"testdata/wire_capture/securityoverview_captured.graphql", registeredSecurityOverviewDocument},
		{"testdata/wire_capture/securityalerts_captured.graphql", registeredSecurityAlertsDocument},
		{"testdata/wire_capture/busfactor_captured.graphql", registeredBusFactorDocument},
		{"testdata/wire_capture/saved_reports_captured.graphql", registeredSavedReportsDocument},
		{"testdata/wire_capture/saved_report_captured.graphql", registeredSavedReportDocument},
		{"testdata/wire_capture/report_runs_captured.graphql", registeredReportRunsDocument},
		{"testdata/wire_capture/aiimpactsummary_captured.graphql", registeredAiImpactSummaryDocument},
		{"testdata/wire_capture/aicomparison_captured.graphql", registeredAiComparisonDocument},
		{"testdata/wire_capture/aireviewload_captured.graphql", registeredAiReviewLoadDocument},
		{"testdata/wire_capture/compoundingrisk_captured.graphql", registeredCompoundingRiskDocument},
		{"testdata/wire_capture/releaseimpact_captured.graphql", registeredReleaseImpactDocument},
	} {
		captured, err := os.ReadFile(c.fixture)
		if err != nil {
			t.Fatalf("read captured wire fixture: %v", err)
		}
		if got, want := digestHex(string(captured)), digestHex(c.registered); got != want {
			t.Errorf("%s: registered document digest %s does not match the captured wire form %s", c.fixture, want, got)
		}
	}
}

// TestRegisteredCatalogDocuments_MatchCapturedWireFixtures asserts each
// registered catalog document digests to the wire form the web client's own
// pinned urql prints for the matching query source. See
// testdata/wire_capture/README.md for how the fixtures were produced.
func TestRegisteredCatalogDocuments_MatchCapturedWireFixtures(t *testing.T) {
	cases := []struct {
		fixture    string
		registered string
	}{
		{"testdata/wire_capture/catalog_values_captured.graphql", registeredCatalogValuesDocument},
		{"testdata/wire_capture/acr_repository_scopes_captured.graphql", registeredAcrRepositoryScopesDocument},
	}
	for _, tc := range cases {
		captured, err := os.ReadFile(tc.fixture)
		if err != nil {
			t.Fatalf("read captured wire fixture %s: %v", tc.fixture, err)
		}
		if got, want := digestHex(string(captured)), digestHex(tc.registered); got != want {
			t.Fatalf("registered document digest %s does not match the captured wire form %s (%s): a real client's request would miss this route\ncaptured:\n%s\n\nregistered:\n%s",
				want, got, tc.fixture, string(captured), tc.registered)
		}
	}
}

// TestRegisteredDataHealthDocuments_MatchCapturedWireFixtures asserts each
// registered data-health document digests to the wire form the web client's
// own pinned urql prints for the generated query text.
func TestRegisteredDataHealthDocuments_MatchCapturedWireFixtures(t *testing.T) {
	cases := []struct {
		fixture    string
		registered string
	}{
		{"testdata/wire_capture/data_health_connectors_captured.graphql", registeredConnectorsDataHealthDocument},
		{"testdata/wire_capture/data_health_identity_captured.graphql", registeredDataHealthIdentityDocument},
		{"testdata/wire_capture/data_health_metric_lineage_captured.graphql", registeredMetricLineageDocument},
		{"testdata/wire_capture/data_health_mapping_coverage_captured.graphql", registeredMappingCoverageHealthDocument},
	}
	for _, tc := range cases {
		captured, err := os.ReadFile(tc.fixture)
		if err != nil {
			t.Fatalf("read captured wire fixture %s: %v", tc.fixture, err)
		}
		if got, want := digestHex(string(captured)), digestHex(tc.registered); got != want {
			t.Fatalf("registered document digest %s does not match the captured wire form %s (%s): a real client's request would miss this route\ncaptured:\n%s\n\nregistered:\n%s",
				want, got, tc.fixture, string(captured), tc.registered)
		}
	}
}

// TestRegisteredExperimentsDocument_MatchesCapturedWireFixture asserts the
// registered experiments document digests to the wire form the web client's own
// pinned urql prints for the generated query text.
func TestRegisteredExperimentsDocument_MatchesCapturedWireFixture(t *testing.T) {
	const fixture = "testdata/wire_capture/experiments_captured.graphql"
	captured, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatalf("read captured wire fixture %s: %v", fixture, err)
	}
	if got, want := digestHex(string(captured)), digestHex(registeredExperimentsDocument); got != want {
		t.Fatalf("registered document digest %s does not match the captured wire form %s (%s): a real client's request would miss this route", want, got, fixture)
	}
}

// TestRegisteredProductTelemetryDocuments_MatchCapturedWireFixtures asserts each
// registered product telemetry document digests to the wire form the web
// client's own pinned urql prints for the generated query text.
func TestRegisteredProductTelemetryDocuments_MatchCapturedWireFixtures(t *testing.T) {
	cases := []struct {
		fixture    string
		registered string
	}{
		{"testdata/wire_capture/product_telemetry_dashboard_captured.graphql", registeredProductTelemetryDashboardDocument},
		{"testdata/wire_capture/product_telemetry_platform_dashboard_captured.graphql", registeredProductTelemetryPlatformDashboardDocument},
	}
	for _, tc := range cases {
		captured, err := os.ReadFile(tc.fixture)
		if err != nil {
			t.Fatalf("read captured wire fixture %s: %v", tc.fixture, err)
		}
		if got, want := digestHex(string(captured)), digestHex(tc.registered); got != want {
			t.Fatalf("registered document digest %s does not match the captured wire form %s (%s): a real client's request would miss this route", want, got, tc.fixture)
		}
	}
}
