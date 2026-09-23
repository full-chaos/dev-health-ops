package externalingest

import "testing"

// TestComputeETagMatchesPythonEnsureAscii is the oracle receipt for
// escapeNonASCII: schema_registry.compute_etag, run against the SAME golden
// document merged with the SAME default limits, executed via the real
// Python producer (not hand-computed):
//
//	PYTHONPATH=src .venv/bin/python3 -c \
//	  'from dev_health_ops.api.external_ingest.schema_registry import get_bundle, compute_etag; \
//	   from dev_health_ops.api.external_ingest.schemas import SCHEMA_VERSION; \
//	   bundle = get_bundle(SCHEMA_VERSION); \
//	   body = {**bundle.document, "limits": {"maxRecordsPerBatch": 1000, "maxBodyBytes": 10000000}}; \
//	   print(compute_etag(body))'
//
// The schema descriptions this hashes contain an em dash (copied from a
// docstring), so this is a real exercise of ensure_ascii escaping, not a
// vacuous ASCII-only case -- decoding the golden bundle back into Go values
// and re-marshaling it without escapeNonASCII reintroduces raw UTF-8 and
// produces a DIFFERENT hash (caught this exact regression once while
// writing computeETag).
func TestComputeETagMatchesPythonEnsureAscii(t *testing.T) {
	document, err := schemaDocument(DefaultLimits)
	if err != nil {
		t.Fatalf("schemaDocument: %v", err)
	}
	etag, err := computeETag(document)
	if err != nil {
		t.Fatalf("computeETag: %v", err)
	}
	const pythonETag = `"20c04b9407c0b0d817b0971f307bdc037051e8907f45ff187ef6c6d7b917d990"`
	if etag != pythonETag {
		t.Fatalf("got %s, want the Python-computed %s", etag, pythonETag)
	}
}

func TestEscapeNonASCIISurrogatePairsAnAstralRune(t *testing.T) {
	// U+1F600 GRINNING FACE, outside the BMP: Python's ensure_ascii emits a
	// UTF-16 surrogate pair for it, not a single \u escape.
	got := string(escapeNonASCII([]byte(`"` + string(rune(0x1F600)) + `"`)))
	want := `"` + `\u` + `d83d` + `\u` + `de00` + `"`
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestSchemaDocumentEmbedsLiveLimits(t *testing.T) {
	limits := Limits{MaxRecords: 42, MaxBodyBytes: 7}
	document, err := schemaDocument(limits)
	if err != nil {
		t.Fatalf("schemaDocument: %v", err)
	}
	got, ok := document["limits"].(map[string]any)
	if !ok {
		t.Fatalf("no limits key: %+v", document)
	}
	if got["maxRecordsPerBatch"] != 42 || got["maxBodyBytes"] != 7 {
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
	if len(index) != len(recordKindValidators) {
		t.Fatalf("golden bundle has %d record kinds, recordKindValidators has %d", len(index), len(recordKindValidators))
	}
	for kind := range index {
		if _, ok := recordKindValidators[kind]; !ok {
			t.Errorf("golden bundle has kind %q with no Go validator", kind)
		}
	}
	for kind := range recordKindValidators {
		if _, ok := index[kind]; !ok {
			t.Errorf("recordKindValidators has kind %q missing from the golden bundle", kind)
		}
	}
}
