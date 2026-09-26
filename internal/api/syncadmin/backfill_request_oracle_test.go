package syncadmin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonBackfillRequestProgram posts every body, byte for byte, to a FastAPI
// route whose parameter is the api's own BackfillRequest, and prints what the
// route saw (the resolved window and scope) or the 422 body FastAPI answered.
const pythonBackfillRequestProgram = `
import json, sys
from datetime import timedelta, timezone
from fastapi import FastAPI
from fastapi.testclient import TestClient
from dev_health_ops.api.admin.schemas_flat import BackfillRequest

app = FastAPI()

@app.post("/x")
def route(payload: BackfillRequest):
    selector = payload.resolved_selector()
    return {
        "since": selector.since.astimezone(timezone.utc).isoformat(),
        "before": selector.before.astimezone(timezone.utc).isoformat(),
        "structured": payload.selector is not None,
        "since_iso": selector.since.isoformat() if payload.selector is not None else payload.since.isoformat(),
        "before_iso": selector.before.isoformat() if payload.selector is not None else payload.before.isoformat(),
        "history_since": selector.since.date().isoformat() if payload.selector is not None else payload.since.isoformat(),
        "history_before": ((selector.before - timedelta(microseconds=1)).date() if payload.selector is not None else payload.before).isoformat(),
        "days": (selector.before - selector.since).days,
        "source_ids": selector.source_ids if payload.selector is not None else None,
        "dataset_keys": selector.dataset_keys if payload.selector is not None else None,
    }

client = TestClient(app)
out = []
for text in json.loads(sys.stdin.read()):
    response = client.post("/x", content=text.encode("utf-8"), headers={"content-type": "application/json"})
    out.append({"status": response.status_code, "body": response.json()})
print(json.dumps(out))
`

// backfillRequestCorpus is the JSON text of request bodies: every shape of the
// two forms, each field with every date and datetime value the date corpus
// names, and the mixed and missing combinations.
func backfillRequestCorpus() []string {
	dates := []string{`"2026-09-01"`, `"2026-09-30"`, `"2026-09-01T00:00:00Z"`, `"2026-09-01T00:00:01Z"`, `"2026-02-30"`, `"x"`, `""`,
		`1700000000`, `86400`, `1.5`, `true`, `[]`, `{}`, `null`, `"2026-09-01T00:00:00+05:00"`}
	instants := []string{`"2026-09-01T00:00:00Z"`, `"2026-09-30T00:00:00+02:00"`, `"2026-09-01T00:00:00"`, `"2026-09-01"`, `"2026-09-01T00:00:00.123456Z"`,
		`1700000000`, `1700086400`, `"x"`, `true`, `null`, `[]`, `"2026-09-01T00:00:00+24:00"`, `"2026-09-01T00:00:00-05:30"`}
	corpus := []string{`{}`, `[]`, `null`, `"x"`, `1`, `{"since": "2026-09-01"}`, `{"before": "2026-09-30"}`, `{"selector": null}`,
		`{"selector": null, "since": "2026-09-01", "before": "2026-09-30"}`, `{"selector": {}}`, `{"selector": []}`, `{"selector": "x"}`,
		`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-01T00:00:00Z"}}`,
		`{"selector": {"since": "2026-09-02T00:00:00Z", "before": "2026-09-01T00:00:00Z"}}`,
		`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-02T00:00:00Z"}, "since": "2026-09-01"}`,
		`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-02T00:00:00Z"}, "before": "2026-09-02"}`,
		`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-02T00:00:00Z", "source_ids": ["a", "b"], "dataset_keys": []}}`,
		`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-02T00:00:00Z", "source_ids": null, "dataset_keys": ["x"]}}`,
		`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-02T00:00:00Z", "source_ids": "a"}}`,
		`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-02T00:00:00Z", "source_ids": [1, "a", null]}}`,
		`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-02T00:00:00Z", "dataset_keys": {}}}`,
		`{"selector": {"since": "2026-09-01T00:00:00Z", "before": "2026-09-02T00:00:00Z", "extra": 1}, "extra": 2}`,
		`{"since": "2026-09-01", "before": "2026-09-30", "source_ids": ["a"]}`,
		`{"since": "2026-09-30", "before": "2026-09-01"}`, `{"since": "2026-09-01", "before": "2026-09-01"}`,
		`{"since": "0001-01-01", "before": "9999-12-31"}`, `{"since": "2026-09-01", "since": "2026-09-02", "before": "2026-09-30"}`,
	}
	for _, value := range dates {
		corpus = append(corpus, `{"since": `+value+`, "before": "2026-09-30"}`, `{"since": "2026-09-01", "before": `+value+`}`)
	}
	for _, value := range instants {
		corpus = append(corpus,
			`{"selector": {"since": `+value+`, "before": "2026-12-31T00:00:00Z"}}`,
			`{"selector": {"since": "2026-01-01T00:00:00Z", "before": `+value+`}}`,
		)
	}
	return corpus
}

func TestBackfillRequestMatchesTheLiveFastAPIRoute(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := backfillRequestCorpus()
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonBackfillRequestProgram)
	command.Dir = root
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = bytes.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live FastAPI: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []struct {
		Status int
		Body   any
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v: %s", err, output)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d of %d", len(want), len(corpus))
	}
	accepted, refused, mismatches := 0, 0, 0
	for index, text := range corpus {
		body, err := pyjson.DecodeString(text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		// pybody.Read marks an empty body or a JSON null as missing.
		window, problems := parseBackfillRequest(pybody.Body{Value: body, Missing: text == `null`})
		var got any
		status := 200
		if len(problems) > 0 {
			status = 422
			encoded, err := pyjson.Marshal(pybody.Detail(problems))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &got); err != nil {
				t.Fatal(err)
			}
			refused++
		} else {
			accepted++
			got = map[string]any{
				"since": isoUTC(window.Since), "before": isoUTC(window.Before), "structured": window.Structured,
				"since_iso": window.sinceISO(), "before_iso": window.beforeISO(),
				"history_since": window.historySince().Format("2006-01-02"), "history_before": window.historyBefore().Format("2006-01-02"),
				"days":       float64(window.days()),
				"source_ids": nilIfUnset(window.SourceIDs, window.SourceIDsSet), "dataset_keys": nilIfUnset(window.DatasetKeys, window.DatasetKeysSet),
			}
		}
		if status != want[index].Status || !reflect.DeepEqual(got, want[index].Body) {
			mismatches++
			if mismatches <= 30 {
				goJSON, _ := json.Marshal(got)
				pyJSON, _ := json.Marshal(want[index].Body)
				t.Errorf("%s:\n Go     %d %s\n Python %d %s", text, status, goJSON, want[index].Status, pyJSON)
			}
		}
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir != "" {
		_ = os.WriteFile(filepath.Join(proofDir, "api-syncadmin-backfill-request"), []byte("executed"), 0o600)
	}
	if accepted == 0 || refused == 0 {
		t.Fatalf("the corpus must exercise both outcomes (accepted %d, refused %d)", accepted, refused)
	}
	t.Logf("%d bodies compared (%d accepted, %d refused), %d mismatches", len(corpus), accepted, refused, mismatches)
}

func nilIfUnset(values []string, set bool) any {
	if !set {
		return nil
	}
	out := make([]any, len(values))
	for index, value := range values {
		out[index] = value
	}
	return out
}

// isoUTC is datetime.astimezone(utc).isoformat(): microseconds only when
// non-zero.
func isoUTC(at time.Time) string {
	at = at.UTC()
	text := at.Format("2006-01-02T15:04:05")
	if micro := at.Nanosecond() / 1000; micro != 0 {
		text += fmt.Sprintf(".%06d", micro)
	}
	return text + "+00:00"
}
