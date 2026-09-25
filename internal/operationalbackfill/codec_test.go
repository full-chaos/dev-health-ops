package operationalbackfill

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// The vectors below were produced by the real Python producer
// (models/operational.py) for the same inputs.

func TestPyJSONASCIIIsPythonsEnsureAscii(t *testing.T) {
	got := pyJSONASCII("q\"uote\\back\x01ctl\x7fdel-é-😀- \x1f")
	want := `"q\"uote\\back\u0001ctl\u007fdel-\u00e9-\ud83d\ude00-\u2028\u001f"`
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestCanonicalIDIsThePythonDigest(t *testing.T) {
	for _, tc := range []struct{ external, want string }{
		{"inc-1", "9adb8a9ff68fe263b1f94b76dad96417d39b00d53807074fd0659ececa8b89db"},
		{"q\"uote\\back\x01ctl\x7fdel-é-😀- \x1f", "85bd1aa97ba809afc41df5c1d9f5826d9664c272f83b18f6d012f3fb9ff9a18b"},
	} {
		got, err := canonicalID("org", "atlassian", "atlassian-ops", familyIncident, tc.external)
		if err != nil || got != tc.want {
			t.Fatalf("canonicalID(%q) = %q, %v; want %s", tc.external, got, err, tc.want)
		}
	}
}

func TestCanonicalIDRefusesAnEmptyComponentLikePython(t *testing.T) {
	_, err := canonicalID("org", "atlassian", "atlassian-ops", familyIncident, "")
	if err == nil || err.Error() != "external_id must be non-empty, got ''" {
		t.Fatalf("got %v", err)
	}
}

func TestMapReproducesThePythonIncidentOrdering(t *testing.T) {
	at := time.Date(2026, 3, 1, 10, 5, 0, 654321000, time.UTC)
	url := "https://x"
	batch, err := Map("org", "atlassian-ops", Legacy{Incidents: []legacyIncident{{
		ID: "inc-1", URL: &url, Summary: "Database down", Status: "open", Severity: "SEV-1",
		CreatedAt: at, LastSynced: at,
	}}}, at)
	if err != nil {
		t.Fatal(err)
	}
	row := batch.Incidents[0]
	if row.ID != "9adb8a9ff68fe263b1f94b76dad96417d39b00d53807074fd0659ececa8b89db" {
		t.Fatalf("id %s", row.ID)
	}
	if row.SourceRevision.String() != "32694262115177916189830091629281993" {
		t.Fatalf("source_revision %s", row.SourceRevision)
	}
	if row.IngestRevision.String() != "32694262115177916078184019023587057" {
		t.Fatalf("ingest_revision %s", row.IngestRevision)
	}
	if row.SourceConflict != "6f7065726174696f6e616c2d636f6e666c6963742d76310000000d656e746974795f66616d696c790006737472696e670100000000000000146f7065726174696f6e616c5f696e636964656e74000000066f72675f69640006737472696e670100000000000000036f72670000000870726f76696465720006737472696e6701000000000000000961746c61737369616e0000001470726f76696465725f696e7374616e63655f69640006737472696e6701000000000000000d61746c61737369616e2d6f707300000012736f757263655f656e746974795f747970650006737472696e6701000000000000001661746c61737369616e5f6f70735f696e636964656e740000000b65787465726e616c5f69640006737472696e67010000000000000005696e632d3100000011736f757263655f76657273696f6e5f617400086461746574696d6501000000000000001b323032362d30332d30315431303a30353a30302e3635343332315a00000009736f757263655f696400046e756c6c0000000000000000000000000a736f757263655f75726c0006737472696e6701000000000000000968747470733a2f2f780000000f736f757263655f6576656e745f617400046e756c6c0000000000000000000000000f736f757263655f6576656e745f696400046e756c6c0000000000000000000000000a7261775f7374617475730006737472696e670100000000000000046f70656e0000000c7261775f73657665726974790006737472696e670100000000000000055345562d310000000c7261775f7072696f7269747900046e756c6c000000000000000000000000116e6f726d616c697a65645f7374617475730006737472696e670100000000000000046f70656e000000136e6f726d616c697a65645f73657665726974790006737472696e67010000000000000008637269746963616c000000136e6f726d616c697a65645f7072696f7269747900046e756c6c0000000000000000000000001772656c6174696f6e736869705f70726f76656e616e636500046e756c6c0000000000000000000000001772656c6174696f6e736869705f636f6e666964656e636500046e756c6c0000000000000000000000000a736572766963655f696400046e756c6c00000000000000000000000013736572766963655f65787465726e616c5f696400046e756c6c00000000000000000000000014657363616c6174696f6e5f706f6c6963795f696400046e756c6c000000000000000000000000057469746c650006737472696e6701000000000000000d446174616261736520646f776e0000000b6465736372697074696f6e00046e756c6c0000000000000000000000000a737461727465645f617400086461746574696d6501000000000000001b323032362d30332d30315431303a30353a30302e3635343332315a0000000b7265736f6c7665645f617400046e756c6c0000000000000000000000000a69735f64656c657465640004626f6f6c010000000000000001000000000a64656c657465645f617400046e756c6c000000000000000000" {
		t.Fatalf("source_conflict_key %s", row.SourceConflict)
	}
	if row.OrderingContract != 2 {
		t.Fatalf("ordering_contract %d", row.OrderingContract)
	}
}

func TestVocabulariesAreThePythonNormalizers(t *testing.T) {
	str := func(value *string) string {
		if value == nil {
			return "<nil>"
		}
		return *value
	}
	for _, tc := range []struct {
		table map[string]string
		hyph  bool
		raw   string
		want  string
	}{
		{statusVocabulary, false, " Resolved ", "resolved"},
		{statusVocabulary, false, "CLOSED", "resolved"},
		{statusVocabulary, false, "Opened", "open"},
		{statusVocabulary, false, "on fire", "<nil>"},
		{statusVocabulary, false, "\x1cactive\x1f", "active"},
		{priorityVocabulary, false, " P3 ", "medium"},
		{priorityVocabulary, false, "urgent", "<nil>"},
		{severityVocabulary, true, "SEV-1", "critical"},
		{severityVocabulary, true, "s-e-v-3", "medium"},
		{severityVocabulary, true, "İnfo", "<nil>"},
		{severityVocabulary, true, "Kow", "<nil>"},
	} {
		if got := str(vocabulary(tc.table, tc.raw, tc.hyph)); got != tc.want {
			t.Errorf("vocabulary(%q) = %s, want %s", tc.raw, got, tc.want)
		}
	}
}

func TestParseContractIsThePythonParser(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		present bool
		want    Contract
		err     string
	}{
		{"", false, ContractLegacy, ""},
		{"1", true, ContractLegacy, ""},
		{"2", true, ContractCurrent, ""},
		{"3", true, 0, "OPERATIONAL_ORDERING_CONTRACT must be exactly '1' or '2', got '3'"},
		{"", true, 0, "OPERATIONAL_ORDERING_CONTRACT must be exactly '1' or '2', got ''"},
		{" 2", true, 0, "OPERATIONAL_ORDERING_CONTRACT must be exactly '1' or '2', got ' 2'"},
	} {
		got, err := ParseContract(tc.raw, tc.present)
		if tc.err != "" {
			if err == nil || err.Error() != tc.err {
				t.Errorf("ParseContract(%q) error %v, want %s", tc.raw, err, tc.err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("ParseContract(%q) = %v, %v", tc.raw, got, err)
		}
	}
}

func TestTableContractClassifiesTheDDL(t *testing.T) {
	legacy := "CREATE TABLE t (org_id String, id String, source_version_at DateTime64(6, 'UTC')) ENGINE = ReplacingMergeTree(source_version_at) ORDER BY (org_id, id)"
	current := "CREATE TABLE t (org_id String, id String, source_revision UInt128, source_conflict_key String, ingest_revision UInt128, ordering_contract UInt8, CONSTRAINT ordering_contract_v2 CHECK ordering_contract = 2) ENGINE = ReplacingMergeTree(ingest_revision) PRIMARY KEY (org_id, id) ORDER BY (org_id, id, source_revision, source_conflict_key)"
	partial := strings.Replace(current, "ordering_contract UInt8, ", "", 1)
	for name, tc := range map[string]struct {
		ddl  string
		want Contract
		err  bool
	}{"legacy": {legacy, ContractLegacy, false}, "current": {current, ContractCurrent, false}, "partial": {partial, 0, true}, "empty": {"", 0, true}} {
		got, err := TableContract(tc.ddl, "t")
		if (err != nil) != tc.err || got != tc.want {
			t.Errorf("%s: got %v, %v", name, got, err)
		}
	}
}

func TestValidateSinkIsThePythonValidator(t *testing.T) {
	for sink, refused := range map[string]bool{"clickhouse": false, "AUTO": false, " clickhouse ": false, "": false, "postgres": true, "MONGO": true, "both": true, "sqlite": true, "redis": true} {
		if got := validateSink(sink) != ""; got != refused {
			t.Errorf("validateSink(%q) refused = %v", sink, got)
		}
	}
}

func TestOperationalRefusesBeforeTouchingClickHouse(t *testing.T) {
	for name, args := range map[string][]string{
		"no org":       {},
		"positional":   {"--org", "o", "extra"},
		"deprecated":   {"--org", "o", "--sink", "postgres"},
		"unknown flag": {"--nope"},
	} {
		var stderr strings.Builder
		code := runOperational(context.Background(), cli.Env{Args: args, Stderr: &stderr, Stdout: &stderr})
		if code != 2 {
			t.Errorf("%s: exit %d, stderr %s", name, code, stderr.String())
		}
	}
}

// goldenSHA256 pins testdata/backfill_operational_golden.json (R24): the rows
// and the summary line that the real Python `dev-hops backfill operational` left
// for every comparable scenario of the integration test, written at commit
// 9b78d78044eadf40e088189057d88bc984ca2b86. The producer is deleted with the Python CLI, so this is a rot guard,
// not a freshness check: the file is only rewritten by
// TestBackfillOperationalVenueOracleMatchesThePythonProducer with
// DHO_BACKFILL_OPERATIONAL_GOLDEN_UPDATE=1, then this digest is updated.
const goldenSHA256 = "c4b1ee9fa22868479cee3e1dfadda31ee963bc95c44ef6e5275de34d4c0941fe"

func TestGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile("testdata/backfill_operational_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != goldenSHA256 {
		t.Fatalf("golden digest = %s, want %s: the golden changed without its digest. It is only rewritten from the live Python producer, then the digest is updated", got, goldenSHA256)
	}
}
