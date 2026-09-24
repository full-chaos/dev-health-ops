package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// versionRowScanner replays a single-row, single-column "version" result
// -- the only shape this package's one query ever produces.
type versionRowScanner struct {
	version string
	served  bool
}

func (s *versionRowScanner) Next() bool {
	if s.served {
		return false
	}
	s.served = true
	return true
}

func (s *versionRowScanner) Scan(dest ...any) error {
	*(dest[0].(*string)) = s.version
	return nil
}

func (s *versionRowScanner) Err() error   { return nil }
func (s *versionRowScanner) Close() error { return nil }

// fixedVersionClient answers the version() query with a fixed string,
// matching the FakeSink used in the Python capture below.
type fixedVersionClient struct{ version string }

func (c fixedVersionClient) Query(_ context.Context, _ string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return &versionRowScanner{version: c.version}, nil
}

// loadGolden decodes testdata/meta.json -- captured by calling the REAL
// Python main.meta() once with clickhouse_client monkeypatched to a fake
// sink whose query_dicts returns a fixed version string, via:
//
//	CLICKHOUSE_URI="clickhouse://localhost:8123/default" \
//	JWT_SECRET_KEY="test-secret-key" SETTINGS_ENCRYPTION_KEY="test-encryption-key" \
//	uv run python3 - <<'PYEOF'
//	import asyncio, json
//	from contextlib import asynccontextmanager
//	from unittest import mock
//
//	from dev_health_ops.api import main as main_module
//
//	class FakeSink:
//	    def query_dicts(self, query, params):
//	        assert "SELECT version()" in query, query
//	        return [{"version": "24.3.1.2672"}]
//
//	@asynccontextmanager
//	async def fake_clickhouse_client(dsn):
//	    yield FakeSink()
//
//	async def run():
//	    with mock.patch.object(main_module, "clickhouse_client", fake_clickhouse_client):
//	        result = await main_module.meta()
//	    print("BEGIN_GOLDEN")
//	    print(json.dumps(json.loads(result.model_dump_json()), indent=2))
//	    print("END_GOLDEN")
//
//	asyncio.run(run())
//	PYEOF
//
// The BEGIN_GOLDEN/END_GOLDEN markers exist because app startup (tracing
// init, the OTLP exporter's background retry) writes its own JSON log
// lines to the same stdout stream this command captures -- piping through
// `sed -n '/BEGIN_GOLDEN/,/END_GOLDEN/p' | sed '1d;$d'` isolates the
// payload from that surrounding noise before it is compared byte-for-byte
// against testdata/meta.json. No .py file is committed anywhere in the
// tree -- the command above (plus that one pipeline) is the whole
// capture, reproducible from this comment alone.
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

// TestGoldenMeta replays the same fixed version string the capture command
// above fed to the real Python handler through this package's own
// BuildResponse, and requires byte-for-byte field equality with Python's
// output: backend, version, the always-null last_ingest_at, the
// always-empty coverage object, the literal limits, and the literal
// supported_endpoints list.
func TestGoldenMeta(t *testing.T) {
	want := loadGolden(t, "meta.json")

	got := BuildResponse(context.Background(), fixedVersionClient{version: "24.3.1.2672"})

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
