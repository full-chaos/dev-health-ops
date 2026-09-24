package filteroptions

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
)

// loadGolden decodes testdata/filters_options_org1.json -- captured by
// running the REAL Python fetch_filter_options (api/queries/filters.py)
// once with query_dicts monkeypatched to fixed fixture rows, via:
//
//	uv run python3 - <<'PYEOF'
//	import asyncio, json
//	from unittest import mock
//	from dev_health_ops.api.queries import filters as filters_module
//
//	async def fake_query_dicts(_client, query, params):
//	    assert params.get("org_id") == "org-1", params
//	    if "FROM teams FINAL" in query:
//	        return [{"value": "team-a"}, {"value": "team-b"}]
//	    if "FROM repos" in query:
//	        return [{"value": "api-gateway"}, {"value": "checkout-service"}]
//	    if "author_email AS value" in query:
//	        return [{"value": "dev@example.com"}, {"value": "not-an-email"}, {"value": "second@example.com"}]
//	    if "issue_type_norm AS value" in query:
//	        return [{"value": "bug"}, {"value": "feature"}]
//	    if "status AS value" in query:
//	        return [{"value": "done"}, {"value": "in_progress"}]
//	    raise AssertionError(f"unexpected query: {query}")
//
//	async def main():
//	    with mock.patch.object(filters_module, "query_dicts", fake_query_dicts):
//	        result = await filters_module.fetch_filter_options(object(), org_id="org-1")
//	    print(json.dumps(result, indent=2))
//
//	asyncio.run(main())
//	PYEOF
//
// No .py file is committed anywhere in the tree -- the command above is
// the whole capture, reproducible from this comment alone.
// DisallowUnknownFields makes a field-name mismatch (Python emitted a key
// this Go type does not declare) a hard test failure rather than a silent
// drop.
func loadGolden(t *testing.T, name string) Response {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var resp Response
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return resp
}

// TestGoldenFilterOptionsOrg1 replays the same fixture rows the capture
// command above fed to the real Python builder through this package's own
// BuildResponse, and requires byte-for-byte field equality with Python's
// output: the union/distinct teams list, the repos list, the
// email-filtered developers list (one non-email value dropped), the
// static work_category taxonomy, issue_type, flow_stage, and the always-
// empty services field.
func TestGoldenFilterOptionsOrg1(t *testing.T) {
	want := loadGolden(t, "filters_options_org1.json")

	client := byQueryClient{
		teams:      []string{"team-a", "team-b"},
		repos:      []string{"api-gateway", "checkout-service"},
		developers: []string{"dev@example.com", "not-an-email", "second@example.com"},
		issueTypes: []string{"bug", "feature"},
		flowStages: []string{"done", "in_progress"},
	}

	got, err := BuildResponse(context.Background(), client, "org-1")
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}

	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("Go BuildResponse diverges from Python golden:\n got:  %s\n want: %s", gotJSON, wantJSON)
	}
}
