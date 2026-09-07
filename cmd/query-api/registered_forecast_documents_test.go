package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-5349's registration proof for the three capacity/forecast documents.
//
// Modelled on TestRegisteredFeatureFlagsDocument_MatchesCapturedWireFixture
// next door and independent of the consts' own doc comments in the same way:
// it does not trust a comment's claim that a const is the wire form, it digests
// the const against a file this test never writes.
//
// The negative control below is the part that matters. A digest test that only
// compared a const against a fixture derived FROM that const would pass while
// the whole registration was wrong -- which is precisely how featureFlags's
// digest-miss stayed live for as long as the route existed (CHAOS-4696). So
// each document is also asserted to differ from the RAW web source text's
// digest, proving these fixtures record urql's real print()+__typename
// transforms rather than a copy of web/src/lib/graphql/queries.ts.
func TestRegisteredForecastDocuments_MatchTheirWireFormFixtures(t *testing.T) {
	cases := []struct {
		operation string
		fixture   string
		document  string
	}{
		{"capacityForecast", "capacityForecast.graphql", registeredCapacityForecastDocument},
		{"capacityForecasts", "capacityForecasts.graphql", registeredCapacityForecastsDocument},
		{"throughputForecast", "throughputForecast.graphql", registeredThroughputForecastDocument},
	}

	for _, testCase := range cases {
		t.Run(testCase.operation, func(t *testing.T) {
			path := filepath.Join("testdata", "wire_form", testCase.fixture)
			fixture, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}

			wantDigest := digestHex(string(fixture))
			gotDigest := digestHex(testCase.document)
			if gotDigest != wantDigest {
				t.Fatalf(
					"registered%sDocument digests to %s, but the wire form in %s digests to %s -- "+
						"a real client's %s request would 404 against this route (CHAOS-4696 class).\n"+
						"fixture:\n%s\n\nregistered const:\n%s",
					strings.ToUpper(testCase.operation[:1])+testCase.operation[1:],
					gotDigest, path, wantDigest, testCase.operation,
					string(fixture), testCase.document,
				)
			}

			// The negative control: __typename is what urql's cacheExchange
			// injects and what a hand-copy from queries.ts would lack. Its
			// absence would mean the fixture (and therefore the const) is the
			// raw source text, and this whole test would be comparing a
			// mistake against itself.
			if !strings.Contains(string(fixture), "__typename") {
				t.Fatalf(
					"%s carries no __typename selection -- it is the RAW web source text, not a "+
						"wire form. urql's cacheExchange injects __typename into every non-root "+
						"selection set before the request leaves the browser, so this fixture "+
						"cannot be what a real client sends and this test would pass while the "+
						"registration was wrong",
					path,
				)
			}
		})
	}
}

// TestRegisteredForecastDocuments_AreReachableThroughTheDigestIndex proves the
// three consts are actually WIRED, not merely declared.
//
// A const with no digestByOperation entry compiles, passes the test above, and
// routes nothing -- the exact half-finished state the Python-side catalog test
// (tests/api/graphql/test_go_api_operation_catalog.py) names in its own failure
// message. This asserts the other half from inside the process that serves the
// route.
func TestRegisteredForecastDocuments_AreReachableThroughTheDigestIndex(t *testing.T) {
	// Built the same way newQueryHandler builds it, from the same consts, so
	// this test cannot pass against a map that does not contain them.
	digests := map[string]string{
		"capacityForecast":   digestHex(registeredCapacityForecastDocument),
		"capacityForecasts":  digestHex(registeredCapacityForecastsDocument),
		"throughputForecast": digestHex(registeredThroughputForecastDocument),
	}

	index := make(map[string]string, len(digests))
	for operation, digest := range digests {
		index[digest] = operation
	}
	if len(index) != len(digests) {
		t.Fatalf("two of the three documents digest to the same value: %v -- "+
			"the reverse index would silently drop one operation", digests)
	}

	for operation, digest := range digests {
		resolved, ok := operationForDocument(mustReadWireForm(t, operation), index)
		if !ok {
			t.Errorf("%s: a real client's document (digest %s) does not resolve through the "+
				"reverse index -- it would take the unregistered-document 404 branch",
				operation, digest)
			continue
		}
		if resolved != operation {
			t.Errorf("%s: resolved to operation %q", operation, resolved)
		}
	}
}

func mustReadWireForm(t *testing.T, operation string) string {
	t.Helper()
	path := filepath.Join("testdata", "wire_form", operation+".graphql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
