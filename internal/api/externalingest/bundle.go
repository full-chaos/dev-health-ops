package externalingest

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"unicode/utf8"
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
//	     sort_keys=True, separators=(",", ":"))'
//
// bundleGoldenRecordKinds is RECORD_KIND_MODELS' keys, sorted -- pinned
// separately (rather than derived from the JSON) so a golden regenerated
// without the Go side also changing is caught by TestBundleRecordKindsMatch.
//
//go:embed testdata/schema_bundle.v1.json
var bundleGolden []byte

// recordKinds is RECORD_KIND_MODELS' keys (schemas.py), sorted. Used both to
// answer GET /schemas' recordKinds list and to validate POST /batches'
// _check_all_kinds_known_or_400 / POST /validate's unknown_kind check.
var recordKinds = sortedKeys(recordKindValidators)

// limitsPayload mirrors router.py's _limits_payload(): the live,
// env-overridable ingest limits, served on both GET /schemas* and merged
// into the ETag'd schema document.
func limitsPayload(cfg Limits) map[string]any {
	return map[string]any{
		"maxRecordsPerBatch": cfg.MaxRecords,
		"maxBodyBytes":       cfg.MaxBodyBytes,
	}
}

// schemaDocument returns the served GET /schemas/{version} body: the golden
// bundle plus the live limits, exactly as router.py's
// `{**bundle.document, "limits": _limits_payload()}` does.
func schemaDocument(cfg Limits) (map[string]any, error) {
	document, err := decodeGolden()
	if err != nil {
		return nil, err
	}
	document["limits"] = limitsPayload(cfg)
	return document, nil
}

func decodeGolden() (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(bundleGolden))
	decoder.UseNumber() // preserve the golden's exact digit text on re-marshal
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode embedded schema bundle: %w", err)
	}
	return document, nil
}

// computeETag replicates schema_registry.compute_etag: a quoted sha256 over
// json.dumps(document, sort_keys=True, separators=(",", ":"),
// ensure_ascii=True).
func computeETag(document map[string]any) (string, error) {
	canonical, err := canonicalMarshal(document)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return fmt.Sprintf("%q", fmt.Sprintf("%x", digest)), nil
}

// canonicalMarshal renders v as compact JSON with map keys sorted
// lexicographically, HTML-sensitive runes (<, >, &, U+2028, U+2029) left
// unescaped, and every non-ASCII rune escaped as \uXXXX (surrogate-paired
// above the BMP) -- matching Python's json.dumps(sort_keys=True,
// separators=(",", ":"), ensure_ascii=True) exactly. The schema descriptions
// this hashes are copied from docstrings that use an em dash for punctuation
// (TestComputeETagMatchesPythonEnsureAscii pins the exact escape), so this
// is not a hypothetical case: the checked-in golden JSON is ASCII-only
// on disk (Python already escaped it the same way when the file was
// generated), but decoding it back into Go values and re-marshaling
// without this step would reintroduce raw UTF-8 and silently diverge from
// the ETag docs promise ("a 304 must be a correct validator for the whole
// body").
func canonicalMarshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	// Encode appends a trailing newline; json.dumps does not.
	compact := bytes.TrimRight(buf.Bytes(), "\n")
	return escapeNonASCII(compact), nil
}

// escapeNonASCII rewrites every rune above U+007F in a compact JSON byte
// stream as its \uXXXX escape (a surrogate pair for runes above U+FFFF),
// matching Python's ensure_ascii=True. It is safe to run over an ENTIRE
// JSON document, not just string literals: outside a JSON string, valid
// JSON syntax is ASCII-only by construction, so no non-string byte can be
// mistaken for the start of a multi-byte UTF-8 sequence.
func escapeNonASCII(input []byte) []byte {
	out := make([]byte, 0, len(input))
	for i := 0; i < len(input); {
		b := input[i]
		if b < 0x80 {
			out = append(out, b)
			i++
			continue
		}
		r, size := utf8.DecodeRune(input[i:])
		i += size
		if r > 0xFFFF {
			r -= 0x10000
			high := 0xD800 + (r >> 10)
			low := 0xDC00 + (r & 0x3FF)
			out = append(out, []byte(fmt.Sprintf(`\u%04x\u%04x`, high, low))...)
		} else {
			out = append(out, []byte(fmt.Sprintf(`\u%04x`, r))...)
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
