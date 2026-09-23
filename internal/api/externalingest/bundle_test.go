package externalingest

import (
	"github.com/full-chaos/dev-health-ops/internal/api/recordvalidation"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// The ensure_ascii oracle receipt lives in bundle_oracle_test.go's
// TestSchemaBundleMatchesLivePython, which EXECUTES schema_registry.
// compute_etag through ci/check_go.sh's live-python-oracles verb, rather
// than pinning a hash computed once and typed into this file (a digest, not
// execution -- see that test's doc comment). This file keeps the tests that
// do not need a live Python process: canonicalMarshal's astral-rune
// contract (internal/api/pyjson's own tests cover MarshalCanonical
// directly; this pins that the thin wrapper here still calls it), and the
// two Go-only invariants below.

func TestCanonicalMarshalSurrogatePairsAnAstralRune(t *testing.T) {
	// U+1F600 GRINNING FACE, outside the BMP: Python's ensure_ascii emits a
	// UTF-16 surrogate pair for it, not a single \u escape.
	got, err := canonicalMarshal(map[string]any{"x": string(rune(0x1F600))})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"x":"` + `\u` + `d83d` + `\u` + `de00` + `"}`
	if string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestSchemaDocumentEmbedsLiveLimits(t *testing.T) {
	limits := Limits{MaxRecords: 42, MaxBodyBytes: 7}
	document, err := schemaDocument(limits)
	if err != nil {
		t.Fatalf("schemaDocument: %v", err)
	}
	got, ok := document["limits"].(*pyjson.Object)
	if !ok {
		t.Fatalf("no limits key: %+v", document)
	}
	maxRecords, _ := got.Get("maxRecordsPerBatch")
	maxBodyBytes, _ := got.Get("maxBodyBytes")
	if maxRecords != 42 || maxBodyBytes != 7 {
		t.Fatalf("%+v", got)
	}
}

func TestComputeETagIsStableAndSensitiveToLimits(t *testing.T) {
	docA, err := schemaDocument(Limits{MaxRecords: 1000, MaxBodyBytes: 10_000_000})
	if err != nil {
		t.Fatal(err)
	}
	docB, err := schemaDocument(Limits{MaxRecords: 1000, MaxBodyBytes: 10_000_000})
	if err != nil {
		t.Fatal(err)
	}
	etagA, err := computeETag(docA)
	if err != nil {
		t.Fatal(err)
	}
	etagB, err := computeETag(docB)
	if err != nil {
		t.Fatal(err)
	}
	if etagA != etagB {
		t.Fatalf("same content must hash the same: %q != %q", etagA, etagB)
	}
	if etagA[0] != '"' || etagA[len(etagA)-1] != '"' {
		t.Fatalf("etag must be quoted: %q", etagA)
	}

	docC, err := schemaDocument(Limits{MaxRecords: 1, MaxBodyBytes: 10_000_000})
	if err != nil {
		t.Fatal(err)
	}
	etagC, err := computeETag(docC)
	if err != nil {
		t.Fatal(err)
	}
	if etagA == etagC {
		t.Fatal("a changed limit must change the ETag (a 304 must be a correct validator for the whole body)")
	}
}

func TestRecordKindsMatchesTheGoldenBundle(t *testing.T) {
	document, err := decodeGolden()
	if err != nil {
		t.Fatal(err)
	}
	index, ok := document["recordKinds"].(map[string]any)
	if !ok {
		t.Fatalf("golden bundle has no recordKinds: %+v", document)
	}
	if len(index) != len(recordvalidation.RecordKinds()) {
		t.Fatalf("golden bundle has %d record kinds, recordvalidation has %d", len(index), len(recordvalidation.RecordKinds()))
	}
	for kind := range index {
		if !recordvalidation.KnownKind(kind) {
			t.Errorf("golden bundle has kind %q with no Go model", kind)
		}
	}
	for _, kind := range recordvalidation.RecordKinds() {
		if _, ok := index[kind]; !ok {
			t.Errorf("recordvalidation has kind %q missing from the golden bundle", kind)
		}
	}
}
