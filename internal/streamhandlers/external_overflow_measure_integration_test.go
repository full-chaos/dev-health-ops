//go:build integration

package streamhandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// overflowCase is one batch both planes write: records are (kind, externalId,
// payload JSON text) so Python decodes them with json.loads (a 1e1000 becomes
// +inf there) and Go with UseNumber.
type overflowCase struct {
	Name    string
	Records [][3]string
}

func overflowCases() []overflowCase {
	pr := func(external string, number string) [3]string {
		return [3]string{"pull_request.v1", external, `{"repositoryExternalId":"full-chaos/dev-health","number":` + number + `,"state":"open","createdAt":"2026-07-22T12:00:00Z"}`}
	}
	wi := func(external string, points string) [3]string {
		return [3]string{"work_item.v1", external, `{"externalKey":"` + external + `","provider":"github","title":"Issue","type":"issue","status":"todo","createdAt":"2026-07-22T10:00:00Z","repositoryExternalId":"full-chaos/dev-health","storyPoints":` + points + `}`}
	}
	return []overflowCase{
		{"control: pull request number 7", [][3]string{pr("pr-ok", "7")}},
		{"pull request number 2^63 (past int64)", [][3]string{pr("pr-big", "9223372036854775808")}},
		{"pull request number 2^32 (past UInt32, inside int64)", [][3]string{pr("pr-u32", "4294967296")}},
		{"pull request number -1", [][3]string{pr("pr-neg", "-1")}},
		{"work item storyPoints 1e1000 (float64 overflow)", [][3]string{wi("wi-inf", "1e1000")}},
		{"work item storyPoints 1e308 (float64 max region)", [][3]string{wi("wi-big", "1e308")}},
		{"two pull requests, one past int64", [][3]string{pr("pr-a", "7"), pr("pr-b", "9223372036854775808")}},
		{"mixed kinds, bad pull request", [][3]string{pr("pr-c", "7"), pr("pr-d", "9223372036854775808"), wi("wi-c", "3")}},
		{"mixed kinds, work item with a float past range (stored as inf, batch succeeds)", [][3]string{pr("pr-e", "7"), wi("wi-d", "1e1000"), wi("wi-e", "3")}},
		{"work item storyPoints 1.5 (control)", [][3]string{wi("wi-f", "1.5")}},
		{"work item storyPoints -1e1000", [][3]string{wi("wi-g", "-1e1000")}},
		{"work item storyPoints 1e-1000 (underflow)", [][3]string{wi("wi-h", "1e-1000")}},
		{"work item storyPoints NaN token", [][3]string{wi("wi-i", "NaN")}},
		{"work item storyPoints Infinity token", [][3]string{wi("wi-j", "Infinity")}},
		{"work item storyPoints integer literal of 401 digits", [][3]string{wi("wi-k", "1"+strings.Repeat("0", 400))}},
	}
}

func overflowRecords(raw [][3]string) ([]externalSinkRecord, error) {
	records := make([]externalSinkRecord, len(raw))
	for i, item := range raw {
		// The production decode (json.loads' grammar, ordered, exact numbers)
		// then the production conversion to the sink's payload map.
		decoded, err := pyjson.DecodeString(item[2])
		if err != nil {
			return nil, err
		}
		payload, ok := payloadFromOrdered(decoded).(map[string]any)
		if !ok {
			return nil, fmt.Errorf("payload is not an object")
		}
		records[i] = externalSinkRecord{Index: i, Kind: item[0], ExternalID: item[1], Payload: payload}
	}
	return records, nil
}

// TestExternalSinkOutcomesMatchFrozenPythonAgainstClickHouse runs the same records
// through the frozen outcomes of the real Python sink (normalize_batch + write_batch) and the Go sink
// against the same real ClickHouse schema (CHAOS-6415), logs both outcomes as
// data, and asserts: a batch fails on one plane exactly when it fails on the
// other, and the stored rows are equal (a failing kind is skipped whole and
// every other kind is written, on both planes).
func TestExternalSinkOutcomesMatchFrozenPythonAgainstClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cases := overflowCases()
	type pythonCase struct {
		Name    string      `json:"name"`
		Org     string      `json:"org"`
		Records [][3]string `json:"records"`
	}
	pythonOrgs, goOrgs := make([]string, len(cases)), make([]string, len(cases))
	input := make([]pythonCase, len(cases))
	for i, item := range cases {
		// The Python orgs are named: they are part of the request the golden is keyed on.
		pythonOrgs[i], goOrgs[i] = venueoracle.StableUUID("external-overflow/python/"+item.Name), uuid.NewString()
		input[i] = pythonCase{Name: item.Name, Org: pythonOrgs[i], Records: item.Records}
	}
	encoded, _ := json.Marshal(input)

	stored := func(org string) string {
		var parts []string
		for _, query := range []struct{ label, sql string }{
			{"pr", "SELECT toString(number) FROM git_pull_requests FINAL WHERE org_id = ? ORDER BY number"},
			{"wi", "SELECT toString(story_points) FROM work_items FINAL WHERE org_id = ? ORDER BY work_item_id"},
		} {
			rows, err := conn.Query(ctx, query.sql, org)
			if err != nil {
				parts = append(parts, query.label+"=ERR("+err.Error()+")")
				continue
			}
			var values []string
			for rows.Next() {
				var value string
				if err := rows.Scan(&value); err != nil {
					t.Fatal(err)
				}
				values = append(values, value)
			}
			_ = rows.Close()
			parts = append(parts, query.label+"=["+strings.Join(values, ",")+"]")
		}
		return strings.Join(parts, " ")
	}

	// The Python plane outcome and the rows it stored, per case, were executed once on the pinned build and
	// are frozen. The golden holds the outcome and the stored rows of each case.
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/external-overflow-python-outcomes.golden.json",
		PythonBuild: "a4847c5e93607451a0c987b314d37e02fc43ce85",
		SHA256:      "02230f3e2b67fabc3cc754b91ade3c144e8376da9694eda04741ccf84ca11232",
	})
	request := venueoracle.ProgramRequest("external overflow sink outcomes", encoded)
	answers := golden.Produce(t, []venueoracle.Request{request})
	golden.Consumed(t, answers...)
	var frozen struct {
		Results []map[string]any `json:"results"`
		Stored  []string         `json:"stored"`
	}
	if err := json.Unmarshal([]byte(answers[0].Body), &frozen); err != nil {
		t.Fatal(err)
	}
	results := frozen.Results
	if len(results) != len(cases) || len(frozen.Stored) != len(cases) {
		t.Fatalf("the golden holds %d outcomes and %d row sets for %d cases", len(results), len(frozen.Stored), len(cases))
	}

	var skipped []string
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	for i, item := range cases {
		python, _ := json.Marshal(results[i])
		pythonStored := frozen.Stored[i]
		t.Logf("python %-55s %s stored: %s", item.Name, python, pythonStored)
		records, err := overflowRecords(item.Records)
		if err != nil {
			t.Fatal(err)
		}
		pointer := externalTestPointer()
		pointer.OrgID, pointer.IngestionID = goOrgs[i], uuid.New()
		_, writeErr := sink.Write(ctx, externalSinkBatch{Pointer: pointer, SourceID: uuid.New(), Records: records})
		goStored := stored(goOrgs[i])
		t.Logf("go     %-55s err=%v stored: %s", item.Name, writeErr, goStored)

		// A record Python's own shape validation rejects never reaches its
		// sink, so there is no sink outcome to compare. Go's validator does
		// not yet reject every such record (an integer past float64 range in
		// a float field: CHAOS-6491), so this comparison skips them rather
		// than claim they agree; the sink still refuses that value.
		if rejections, _ := results[i]["rejections"].([]any); len(rejections) > 0 {
			skipped = append(skipped, item.Name)
			continue
		}
		sinkErrors, _ := results[i]["sink_errors"].([]any)
		_, raised := results[i]["raised"]
		pythonFailed, goFailed := len(sinkErrors) > 0 || raised, writeErr != nil
		if pythonFailed != goFailed {
			t.Errorf("%s: python failed=%v, go failed=%v (err %v)", item.Name, pythonFailed, goFailed, writeErr)
			continue
		}
		// Python isolates per kind and Go does the same, so what is stored
		// must be equal whether or not the batch failed.
		if pythonStored != goStored {
			t.Errorf("%s: stored rows differ: python %s, go %s", item.Name, pythonStored, goStored)
		}
	}
	// Which cases the comparison skips is part of what the golden pins: a rejection added to or removed
	// from a recorded case would otherwise change the set unseen.
	wantSkipped := []string{"pull request number -1", "work item storyPoints integer literal of 401 digits"}
	if strings.Join(skipped, "\n") != strings.Join(wantSkipped, "\n") {
		t.Errorf("the cases skipped because Python rejects the record are %q, want %q", skipped, wantSkipped)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}
