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
