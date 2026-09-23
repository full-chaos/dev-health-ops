package externalingest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/api/recordvalidation"
	"math/rand"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// pythonEnvelopeProgram runs BatchEnvelope.model_validate_json on each
// base64 body: "ok", the errors as [type, loc, msg], or the TypeError.
const pythonEnvelopeProgram = `
import base64, json, sys
from pydantic import ValidationError
from dev_health_ops.api.external_ingest.schemas import BatchEnvelope
out = []
for raw in json.loads(sys.stdin.read()):
    try:
        BatchEnvelope.model_validate_json(base64.b64decode(raw))
        out.append("ok")
    except ValidationError as exc:
        try:
            rendered = json.dumps([dict(e) for e in exc.errors()], ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        except (TypeError, ValueError):
            rendered = "UNRENDERABLE"
        out.append({"rows": [[e["type"], list(e["loc"]), e["msg"]] for e in exc.errors()], "dicts": rendered})
        continue
    except TypeError:
        out.append("TypeError")
print(json.dumps(out))
`

// envelopeCorpus is JSON syntax errors of every jiter kind, envelope field
// errors in and out of order, and a seeded fuzz: valid envelopes with parts
// replaced, dropped, duplicated, renamed or cut.
func envelopeCorpus() [][]byte {
	valid := `{"schemaVersion": "external-ingest.v1", "idempotencyKey": "k-1", "source": {"system": "github", "instance": "acme/api"}, "window": {"startedAt": "2026-01-01T00:00:00Z", "endedAt": "2026-01-02T00:00:00Z"}, "records": [{"kind": "repository.v1", "externalId": "r-1", "payload": {"externalId": "acme/api"}}]}`
	fixed := []string{
		``, ` `, "\n\n", `{`, `[`, `"`, `"ab`, `{"a"`, `{"a":`, `{"a":1,`, `[1,`, `[1`, `{"a":"x`, `x`, ` x`, "\nx", `{} x`, "{}\n\nx",
		"{}  \n  x", `1 2`, `{"a":1}}`, `{"a" 1}`, `{"a":1 "b"}`, `{1:2}`, `{,}`, `[,1]`, `[1,]`, `{"a":1,}`, `[1 2]`, `{"a":tr}`,
		`{"a":nul}`, `{"a":fals`, `{"a":tx}`, `{"a":-}`, `{"a":01}`, `{"a":1.}`, `{"a":1.e5}`, `{"a":1e}`, `{"a":-a}`, `{"a":"\q"}`,
		`{"a":"\u12"}`, `{"a":"\u12G4"}`, `{"a":"\ud800"}`, `{"a":"\ud800x"}`, `{"a":"\ud800\u0041"}`, `{"a":"\udc00"}`,
		"{\"a\":\"a\x01\"}", "{\"a\":\"\xff\"}", "{\"a\":\"\xc3\"}", "\xff", "{\"\xff\":1}", "{\"a\":\xff}", "{\"a\":1}\n\xff",
		`{"a":NaN}`, `{"a":-NaN}`, `{"a":Infinity}`, `{"a":-Infinity}`, `{"a":+1}`, `{"a":Inf}`, `{"a":nan}`, `{"a":1e999}`,
		`{"a":123456789012345678901234567890}`, `{"a":[1,2,]}`, "\t{}", "\r\n{}\r\nx", `{"a":"\/"}`, "{\"a\":\"\t\"}", "\"\xe2\x82\xac",
		"{\"\xc3\xa9\":1} x", `[]`, `"s"`, `1`, `null`, `true`, `{}`, `{"a":"\ud800\udc00"}`, `{"a":1.5E+3}`, `{"a":-0}`,
		valid,
		`{"schemaVersion":1,"schema_version":"v","idempotencyKey":"","source":{"system":"x","instance":"","type":"y","extra":1,"entityFamily":"z","producer":1},"window":{"startedAt":"2026-01-02T00:00:00Z","endedAt":"2026-01-01T00:00:00Z"},"records":[1,{"kind":1,"externalId":"","payload":[],"q":1}],"zzz":1,"aaa":2}`,
		`{"schemaVersion":"v","idempotencyKey":"k","source":{"system":"github","instance":"i"},"records":[]}`,
		`{"schema_version":5,"idempotencyKey":"k","source":"s","window":1,"records":{}}`,
		`{"schemaVersion":"v","idempotencyKey":"k","source":{"system":"github","instance":"i"},"window":{"startedAt":"2026-01-02T00:00:00","endedAt":"2026-01-01T00:00:00Z"},"records":[{"kind":"k","externalId":"e","payload":{}}]}`,
		`{"schemaVersion":"v","idempotencyKey":"k","source":{"system":"github","instance":"i"},"window":{"startedAt":"2026-01-02T00:00:00","endedAt":"2026-01-01T00:00:00"},"records":[{"kind":"k","externalId":"e","payload":{}}]}`,
		`{"schemaVersion":"v","idempotencyKey":"k","source":{"system":"github","instance":"i"},"window":{"startedAt":1767225600,"endedAt":"2026-01-01T00:00:00+02:00"},"records":[{"kind":"k","externalId":"e","payload":{}}]}`,
		`{"schemaVersion":"v","idempotencyKey":"k","source":{"system":"github","instance":"i"},"window":{"started_at":"x","ended_at":true},"records":[{"kind":"k","external_id":"e","payload":{}}]}`,
		`{"schemaVersion":"v","schemaVersion":5,"idempotencyKey":"k","source":{"system":"github","instance":"i"},"records":[{"kind":"k","externalId":"e","payload":{}}]}`,
	}
	var corpus [][]byte
	for _, body := range fixed {
		corpus = append(corpus, []byte(body))
	}
	// jiter's nesting limit, at and around it: bare arrays, objects under a
	// key, mixed, inside a record payload, and cut off.
	for _, depth := range []int{199, 200, 201, 202, 203, 260} {
		corpus = append(corpus,
			[]byte(strings.Repeat("[", depth)+strings.Repeat("]", depth)),
			[]byte(strings.Repeat(`{"a":`, depth)+"1"+strings.Repeat("}", depth)),
			[]byte(`{"x": `+strings.Repeat(`[{"k": `, depth/2)+"null"+strings.Repeat("}]", depth/2)+`}`),
			[]byte(`{"schemaVersion":"external-ingest.v1","idempotencyKey":"k","source":{"system":"github","instance":"i"},"records":[{"kind":"repository.v1","externalId":"e","payload":{"settings":`+
				strings.Repeat("[", depth)+strings.Repeat("]", depth)+`}}]}`),
			[]byte(strings.Repeat("[", depth)))
	}
	parts := map[string][]string{
		"schemaVersion": {`"external-ingest.v1"`, `"v2"`, `""`, `1`, `null`, `["x"]`},
		"idempotencyKey": {`"k"`, `""`, `"` + strings.Repeat("k", 255) + `"`, `"` + strings.Repeat("é", 256) + `"`, `7`, `null`,
			`"` + strings.Repeat("é", 255) + `"`, `"` + strings.Repeat("😀", 255) + `"`, `"` + strings.Repeat("😀", 256) + `"`},
		"source": {`{"system": "github", "instance": "i"}`, `{"system": "GitHub", "instance": "i"}`, `{"system": "jira", "instance": ""}`,
			`{"type": "customer_push", "system": "custom", "instance": "i", "entityFamily": "operational", "producer": "p", "producerVersion": null}`,
			`{"system": "linear", "instance": "i", "entity_family": "legacy", "producer_version": 1, "x": 1}`, `[]`, `"s"`, `null`,
			`{"system": "github", "instance": "` + strings.Repeat("é", 256) + `", "type": "other"}`,
			`{"system": "github", "instance": "` + strings.Repeat("é", 255) + `"}`},
		"window": {`null`, `{"startedAt": "2026-01-01", "endedAt": "2026-01-01T00:00:00"}`, `{"startedAt": 0, "endedAt": -1}`,
			`{"startedAt": "x", "endedAt": "y"}`, `{"startedAt": "2026-01-01T00:00:00Z"}`, `{"endedAt": "2026-01-01T00:00:00Z", "extra": 1}`, `1`, `[]`,
			`{"startedAt": "2026-01-01T10:00:00+02:00", "endedAt": "2026-01-01T09:00:00+01:00"}`, `{"startedAt": ".5", "endedAt": 1.5}`},
		"records": {`[]`, `[{"kind": "k", "externalId": "e", "payload": {}}]`, `[1, "x", null]`, `{}`, `"r"`,
			`[{"kind": "k", "externalId": "", "payload": []}, {"kind": null, "payload": {}, "extra": 2}]`,
			`[{"kind": "k", "external_id": "e", "externalId": 5, "payload": {"a": NaN}}]`,
			`[{"kind": "k", "externalId": "` + strings.Repeat("e", 513) + `", "payload": {}}]`,
			`[{"kind": "k", "externalId": "` + strings.Repeat("é", 512) + `", "payload": {}}]`,
			`[{"kind": "k", "externalId": "` + strings.Repeat("😀", 513) + `", "payload": {}}]`},
	}
	keys := []string{"schemaVersion", "idempotencyKey", "source", "window", "records"}
	random := rand.New(rand.NewSource(6320))
	for range 1500 {
		var fields []string
		for _, key := range keys {
			if random.Intn(6) == 0 {
				continue
			}
			pool := parts[key]
			name := key
			if random.Intn(8) == 0 {
				name = map[string]string{"schemaVersion": "schema_version", "idempotencyKey": "idempotency_key", "source": "source",
					"window": "window", "records": "records"}[key]
			}
			fields = append(fields, fmt.Sprintf("%q: %s", name, pool[random.Intn(len(pool))]))
		}
		if random.Intn(5) == 0 {
			fields = append(fields, `"extra": 1`)
		}
		random.Shuffle(len(fields), func(a, b int) { fields[a], fields[b] = fields[b], fields[a] })
		body := "{" + strings.Join(fields, ", ") + "}"
		if random.Intn(10) == 0 {
			body = body[:random.Intn(len(body)+1)]
		}
		corpus = append(corpus, []byte(body))
	}
	// Every pool value once, beside the first (valid) value of each other
	// field, so boundary values such as multi-byte strings at a length
	// limit are always compared.
	for _, key := range keys {
		for _, value := range parts[key] {
			fields := make([]string, len(keys))
			for index, other := range keys {
				chosen := parts[other][0]
				if other == key {
					chosen = value
				}
				fields[index] = fmt.Sprintf("%q: %s", other, chosen)
			}
			corpus = append(corpus, []byte("{"+strings.Join(fields, ", ")+"}"))
		}
	}
	return corpus
}

// TestEnvelopeValidationMatchesLivePython compares recordvalidation.ValidateEnvelopeJSON
// with BatchEnvelope.model_validate_json on envelopeCorpus: each error's
// type, loc and msg in order, or success, or the TypeError. jiter's JSON
// syntax texts other than EOF, trailing characters and expected value are
// a named limit: for those, the type and loc must match, and message
// differences are counted and reported.
func TestEnvelopeValidationMatchesLivePython(t *testing.T) {
	root, python := oracleRoot(t)
	corpus := envelopeCorpus()
	encoded := make([]string, len(corpus))
	for index, body := range corpus {
		encoded[index] = base64.StdEncoding.EncodeToString(body)
	}
	input, _ := json.Marshal(encoded)
	output := runPython(t, root, python, pythonEnvelopeProgram, input)
	lines := bytes.Split(bytes.TrimSpace(output), []byte("\n"))
	var want []json.RawMessage
	if err := json.Unmarshal(lines[len(lines)-1], &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d results for %d bodies", len(want), len(corpus))
	}
	mismatches, limited, syntax := 0, 0, 0
	for index, body := range corpus {
		_, errs, typeErr := recordvalidation.ValidateEnvelopeJSON(body)
		got := `"ok"`
		switch {
		case typeErr != nil:
			got = `"TypeError"`
		case len(errs) > 0:
			rows := make([]string, len(errs))
			for i, err := range errs {
				loc, _ := pyjson.Marshal(err.Loc)
				if err.Loc == nil {
					loc = []byte("[]")
				}
				message, _ := json.Marshal(err.Msg)
				rows[i] = fmt.Sprintf("[%q, %s, %s]", err.Type, loc, message)
			}
			got = "[" + strings.Join(rows, ", ") + "]"
		}
		var wantValue, gotValue any
		var wantFull struct {
			Rows  json.RawMessage
			Dicts string
		}
		if json.Unmarshal(want[index], &wantFull) == nil && wantFull.Rows != nil {
			if goDicts := renderDicts(errs); goDicts != wantFull.Dicts {
				mismatches++
				if mismatches <= 15 {
					t.Errorf("%q dicts:\n  go     %s\n  python %s", body, goDicts, wantFull.Dicts)
				}
				continue
			}
			want[index] = wantFull.Rows
		}
		_ = json.Unmarshal(want[index], &wantValue)
		if err := json.Unmarshal([]byte(got), &gotValue); err != nil {
			t.Fatalf("render: %v %s", err, got)
		}
		wantText, _ := json.Marshal(wantValue)
		gotText, _ := json.Marshal(gotValue)
		if bytes.Equal(wantText, gotText) {
			if strings.Contains(string(wantText), "json_invalid") {
				syntax++
			}
			continue
		}
		if isLimitedSyntaxMessage(wantValue, gotValue) {
			limited++
			continue
		}
		mismatches++
		if mismatches <= 15 {
			t.Errorf("%q:\n  go     %s\n  python %s", body, gotText, wantText)
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d bodies differ", mismatches, len(corpus))
	}
	writeOracleProof(t, "externalingest-envelope-validation")
	t.Logf("%d bodies compared (%d JSON syntax errors exact); %d named-limit syntax messages differ; 0 other mismatches",
		len(corpus), syntax, limited)
}

// isLimitedSyntaxMessage reports a json_invalid pair whose messages
// differ only where the named limit allows: neither is an EOF, trailing
// characters or expected value error.
func isLimitedSyntaxMessage(want, got any) bool {
	wantRows, ok1 := want.([]any)
	gotRows, ok2 := got.([]any)
	if !ok1 || !ok2 || len(wantRows) != 1 || len(gotRows) != 1 {
		return false
	}
	w, g := wantRows[0].([]any), gotRows[0].([]any)
	if w[0] != "json_invalid" || g[0] != "json_invalid" {
		return false
	}
	for _, message := range []string{w[2].(string), g[2].(string)} {
		for _, exact := range []string{"EOF while parsing", "trailing characters", "expected value"} {
			if strings.Contains(message, exact) {
				return false
			}
		}
	}
	return true
}

// renderDicts is json.dumps([dict(e) ...]) for errs, or UNRENDERABLE where
// Python's json.dumps raises.
func renderDicts(errs []recordvalidation.PydanticError) string {
	list := make([]pyjson.Value, len(errs))
	for index, err := range errs {
		if err.Unrenderable {
			return "UNRENDERABLE"
		}
		list[index] = err.Dict()
	}
	text, err := pyjson.Marshal(list)
	if err != nil {
		return "UNRENDERABLE"
	}
	return string(text)
}

// pythonDataPlaneParseProgram answers each body as router.py's
// _parse_envelope_or_400 does through the Python api's handlers: 200
// "valid", the 400 external_ingest_error_body JSONResponse, or the
// unhandled 500 when that response cannot be rendered or the validator
// raises.
const pythonDataPlaneParseProgram = `
import base64, json, sys
from pydantic import ValidationError
from starlette.responses import JSONResponse
from dev_health_ops.api.external_ingest.schemas import BatchEnvelope
from dev_health_ops.api.external_ingest.errors import external_ingest_error_body
out = []
for raw in json.loads(sys.stdin.read()):
    body = base64.b64decode(raw)
    try:
        BatchEnvelope.model_validate_json(body)
        out.append([200, "valid"])
        continue
    except ValidationError as exc:
        try:
            response = JSONResponse(status_code=400, content=external_ingest_error_body(
                "invalid_envelope", "Malformed batch envelope", [dict(e) for e in exc.errors()]))
            out.append([400, response.body.decode()])
            continue
        except Exception:
            pass
    except Exception:
        pass
    out.append([500, json.dumps(external_ingest_error_body("internal_error", "Internal Server Error"), separators=(",", ":"))])
print(json.dumps(out))
`

// TestDataPlaneEnvelopeParseMatchesLivePython compares the data plane's
// parseEnvelope answers (status and body bytes, written by
// writeIngestError) with the Python api's on envelopeCorpus.
func TestDataPlaneEnvelopeParseMatchesLivePython(t *testing.T) {
	root, python := oracleRoot(t)
	corpus := envelopeCorpus()
	encoded := make([]string, len(corpus))
	for index, body := range corpus {
		encoded[index] = base64.StdEncoding.EncodeToString(body)
	}
	input, _ := json.Marshal(encoded)
	output := runPython(t, root, python, pythonDataPlaneParseProgram, input)
	lines := bytes.Split(bytes.TrimSpace(output), []byte("\n"))
	var want [][2]any
	if err := json.Unmarshal(lines[len(lines)-1], &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	mismatches, statuses := 0, map[int]int{}
	for index, body := range corpus {
		status, text := 200, "valid"
		if _, err := parseEnvelope(body); err != nil {
			recorder := httptest.NewRecorder()
			writeIngestError(recorder, err.(*ingestError))
			status, text = recorder.Code, strings.TrimSpace(recorder.Body.String())
		}
		statuses[status]++
		wantStatus, wantText := int(want[index][0].(float64)), want[index][1].(string)
		if status != wantStatus || text != wantText {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("%q:\n  go     %d %s\n  python %d %s", body, status, text, wantStatus, wantText)
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d bodies differ", mismatches, len(corpus))
	}
	writeOracleProof(t, "externalingest-parse-dataplane")
	t.Logf("%d bodies compared (statuses %v); 0 mismatches", len(corpus), statuses)
}
