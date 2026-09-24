package externalingest

import (
	"crypto/sha256"
	_ "embed"
	"fmt"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/recordvalidation"
)

// schemaVersion is the one supported external-ingest schema version
// (SCHEMA_VERSION in schemas.py). Both routers reject any other value.
const schemaVersion = "external-ingest.v1"

// bundleGolden is the JSON Schema bundle GET /schemas/{version} serves,
// generated once from the real producer -- Python's
// schema_registry.get_bundle(SCHEMA_VERSION).document, via
// pydantic.json_schema.models_json_schema() over schemas.py's models -- and
// checked in as a golden asset (AGENTS.md: "Build the oracle case from the
// real producer, never hand-authored JSON"). Regenerate with:
//
//	PYTHONPATH=src .venv/bin/python3 -c \
//	  'import json; from dev_health_ops.api.external_ingest.schema_registry import get_bundle; \
//	   from dev_health_ops.api.external_ingest.schemas import SCHEMA_VERSION; \
//	   json.dump(get_bundle(SCHEMA_VERSION).document, \
//	     open("internal/api/externalingest/testdata/schema_bundle.v1.json","w"), \
//	     separators=(",", ":"))'
//
// No sort_keys: the key order in the file is Python's live dict order
// ($schema, $id, ... $defs), because GET /schemas/{version} serves the
// document in that order and its body is compared byte for byte against the
// live api (TestSchemaBundleMatchesLivePython, the venue oracle).
//
// bundleGoldenRecordKinds is RECORD_KIND_MODELS' keys, sorted -- pinned
// separately (rather than derived from the JSON) so a golden regenerated
// without the Go side also changing is caught by TestBundleRecordKindsMatch.
//
//go:embed testdata/schema_bundle.v1.json
var bundleGolden []byte

// limitsPayload mirrors router.py's _limits_payload(): the live,
// env-overridable ingest limits, served on both GET /schemas* and merged
// into the ETag'd schema document.
func limitsPayload(cfg Limits) *pyjson.Object {
	object := pyjson.NewObject()
	object.Set("maxRecordsPerBatch", cfg.MaxRecords)
	object.Set("maxBodyBytes", cfg.MaxBodyBytes)
	return object
}

// schemaDocument returns the served GET /schemas/{version} body: the golden
// bundle plus the live limits, exactly as router.py's
// `{**bundle.document, "limits": _limits_payload()}` does -- an ordered
// *pyjson.Object in the bundle's own key order, "limits" appended (or
// replaced in place, as a dict spread does).
func schemaDocument(cfg Limits) (*pyjson.Object, error) {
	value, err := pyjson.Decode(bundleGolden)
	if err != nil {
		return nil, fmt.Errorf("decode embedded schema bundle: %w", err)
	}
	document, ok := value.(*pyjson.Object)
	if !ok {
		return nil, fmt.Errorf("decode embedded schema bundle: not an object")
	}
	document.Set("limits", limitsPayload(cfg))
	return document, nil
}

// computeETag replicates schema_registry.compute_etag: a quoted sha256 over
// json.dumps(document, sort_keys=True, separators=(",", ":"),
// ensure_ascii=True).
func computeETag(document *pyjson.Object) (string, error) {
	canonical, err := canonicalMarshal(document)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return fmt.Sprintf("%q", fmt.Sprintf("%x", digest)), nil
}

// canonicalMarshal renders v as json.dumps(v, sort_keys=True,
// separators=(",", ":")) does -- plain json.dumps, whose DEFAULT
// ensure_ascii=True applies, matching Python's json.dumps(sort_keys=True,
// separators=(",", ":"), ensure_ascii=True) exactly. The schema descriptions
// this hashes are copied from docstrings that use an em dash for punctuation
// (TestComputeETagIsStableAndSensitiveToLimits pins the exact escape via
// pyjson.MarshalCanonical's own tests), so this is not a hypothetical case:
// the checked-in golden JSON is ASCII-only on disk (Python already escaped
// it the same way when the file was generated), but decoding it back into
// Go values and re-marshaling without this step would reintroduce raw UTF-8
// and silently diverge from the ETag docs promise ("a 304 must be a correct
// validator for the whole body"). Delegates to the one shared canonicalizer
// (internal/api/pyjson.MarshalCanonical, which accepts this package's own
// stdlib-decoded map[string]any/json.Number shape directly) rather than a
// second copy of the same sort+ensure_ascii logic.
func canonicalMarshal(v any) ([]byte, error) {
	return pyjson.MarshalCanonical(v)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// SchemaVersion is SCHEMA_VERSION, exported for the admin schema proxy.
const SchemaVersion = schemaVersion

// RecordKinds is sorted(RECORD_KIND_MODELS), a fresh copy per call.
func RecordKinds() []string { return recordvalidation.RecordKinds() }
