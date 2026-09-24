package streamhandlers

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/google/uuid"
)

// pythonNormalizeBatchProgram runs normalize_batch on each case and prints
// each one's rejection list (index, kind, externalId, code, message, path,
// in normalize_batch's own append order) as JSON. normalize_batch is a
// pure function -- no DB/async context needed to call it directly.
const pythonNormalizeBatchProgram = `
import json, sys, uuid
from dev_health_ops.api.external_ingest.schemas import RecordEnvelope
from dev_health_ops.external_ingest.normalize import normalize_batch

cases = json.loads(sys.stdin.read())
out = []
for case in cases:
    records = [
        RecordEnvelope.model_validate({"kind": r["kind"], "externalId": r["externalId"], "payload": r["payload"]})
        for r in case["records"]
    ]
    result = normalize_batch(
        org_id=case["orgId"],
        source_id=uuid.UUID(case["sourceId"]),
        source_system=case["sourceSystem"],
        source_instance=case["sourceInstance"],
        ingestion_id=uuid.UUID(case["ingestionId"]),
        records=records,
    )
    out.append([
        {
            "index": r.index, "kind": r.kind, "externalId": r.external_id,
            "code": r.code, "message": r.message, "path": r.path,
        }
        for r in result.rejections
    ])
print(json.dumps(out))
`

func normalizeOracleRoot(t *testing.T) (string, string) {
	t.Helper()
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	return root, pyoracle.Resolve(t, root)
}

func runNormalizeOraclePython(t *testing.T, root, python string, stdin []byte) []byte {
	t.Helper()
	command := exec.Command(python, "-c", pythonNormalizeBatchProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = bytes.NewReader(stdin)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	return output
}

func writeNormalizeOracleProof(t *testing.T) {
	t.Helper()
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "streamhandlers-normalize-batch"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// externalMessageNotClaimedExact names rejection codes CHAOS-6345 does not
// touch and never claimed message-exact -- normalize.py interpolates the
// offending value and a master-spec citation into these (e.g.
// "record kind 'repository.v1' is not accepted for source system 'jira'
// (master-spec CC6)"), confirmed live; this file's own messages for them
// are static descriptions, unchanged from before this ticket. Porting
// interpolated, !r-quoted message text for these is real work (its own
// pythonRepr-shaped fix) outside "the worker calls the one exact
// validator" scope this ticket covers -- named here, not silently assumed
// equal.
var externalMessageNotClaimedExact = map[string]bool{
	"unsupported_kind_for_system":    true,
	"entity_family_mismatch":         true,
	"record_outside_source_instance": true,
}

type normalizeOracleRecord struct {
	Kind       string         `json:"kind"`
	ExternalID string         `json:"externalId"`
	Payload    map[string]any `json:"payload"`
}

// normalizeOracleCase is one normalize_batch/normalizeExternalRecords
// comparison. EntityFamily must stay consistent with every record's own
// kind (operational kind <-> "operational", everything else <-> "legacy")
// -- entity_family_mismatch has NO Python counterpart at all (grepped,
// confirmed; CHAOS-6345's design note), so a case that would trigger it
// compares Go's own extra check against nothing, not a real divergence
// this oracle is scoped to catch.
type normalizeOracleCase struct {
	Name           string
	SourceSystem   string
	SourceInstance string
	EntityFamily   string
	Records        []normalizeOracleRecord
}

// normalizeOracleCorpus targets ORCHESTRATION -- which check wins, how a
// rejection row is built, the one added post-shape check -- not per-field
// shape correctness: internal/api/externalingest's own
// TestRecordValidationMatchesLivePython already exhaustively oracles
// ValidateRecords/validate_records field-by-field, so re-fuzzing that here
// would be a second, redundant proof of the same claim.
func normalizeOracleCorpus() []normalizeOracleCase {
	return []normalizeOracleCase{
		{
			// CHAOS-6345's own repro: a whitespace-only canonicalId is a
			// valid plain str for IdentityV1 (no strip validator) --
			// Python accepts it. The pre-fix Go worker's own field-table
			// trimmed required strings and refused it.
			Name: "identity whitespace canonicalId is accepted", SourceSystem: "github", SourceInstance: "acme/repo", EntityFamily: "legacy",
			Records: []normalizeOracleRecord{{Kind: "identity.v1", ExternalID: "u1", Payload: map[string]any{
				"canonicalId": " ", "updatedAt": "2026-07-01T12:00:00Z",
			}}},
		},
		{
			// A shape defect (missing required canonicalId) on a kind
			// ("identity.v1") that IS allowed for this system --
			// unsupported_kind_for_system never fires, so this only pins
			// that the shape error itself is reported, not the reorder.
			Name: "shape error alone on an allowed kind", SourceSystem: "github", SourceInstance: "acme/repo", EntityFamily: "legacy",
			Records: []normalizeOracleRecord{{Kind: "identity.v1", ExternalID: "u1", Payload: map[string]any{
				"updatedAt": "2026-07-01T12:00:00Z",
			}}},
		},
		{
			// The actual reordering pin: work_item.v1 is NOT in jira's...
			// wait, jira DOES allow work_item.v1 (ALLOWED_KINDS_BY_SYSTEM).
			// Use a git-family kind against jira instead, which jira never
			// allows, AND give it a shape defect (missing required
			// externalId) -- Python's shape check must win, reporting the
			// shape error, never unsupported_kind_for_system.
			Name: "shape error wins over unsupported_kind_for_system", SourceSystem: "jira", SourceInstance: "PROJ", EntityFamily: "legacy",
			Records: []normalizeOracleRecord{{Kind: "repository.v1", ExternalID: "r1", Payload: map[string]any{
				"sourceSystem": "github",
			}}},
		},
		{
			// The same kind against the same disallowed system, but with a
			// SHAPE-VALID payload this time: now unsupported_kind_for_system
			// is the only thing left to fire.
			Name: "unsupported_kind_for_system alone", SourceSystem: "jira", SourceInstance: "PROJ", EntityFamily: "legacy",
			Records: []normalizeOracleRecord{{Kind: "repository.v1", ExternalID: "r1", Payload: map[string]any{
				"externalId": "acme/repo", "sourceSystem": "github",
			}}},
		},
		{
			Name: "unknown kind alone", SourceSystem: "github", SourceInstance: "acme/repo", EntityFamily: "legacy",
			Records: []normalizeOracleRecord{{Kind: "nope.v1", ExternalID: "x", Payload: map[string]any{}}},
		},
		{
			// normalize_batch's own post-shape check, outside
			// validate_records: repo_full_name/repo_provider are both
			// Optional at the pydantic model level.
			Name: "service_repository_mapping missing repoFullName", SourceSystem: "pagerduty", SourceInstance: "acme", EntityFamily: "operational",
			Records: []normalizeOracleRecord{{Kind: "service_repository_mapping.v1", ExternalID: "m1", Payload: map[string]any{
				"externalId": "m1", "sourceVersionAt": "2026-07-01T12:00:00Z",
				"serviceExternalId": "svc-1", "repoProvider": "github",
			}}},
		},
		{
			Name: "service_repository_mapping missing repoProvider", SourceSystem: "pagerduty", SourceInstance: "acme", EntityFamily: "operational",
			Records: []normalizeOracleRecord{{Kind: "service_repository_mapping.v1", ExternalID: "m1", Payload: map[string]any{
				"externalId": "m1", "sourceVersionAt": "2026-07-01T12:00:00Z",
				"serviceExternalId": "svc-1", "repoFullName": "acme/repo",
			}}},
		},
		{
			Name: "service_repository_mapping both present is accepted", SourceSystem: "pagerduty", SourceInstance: "acme", EntityFamily: "operational",
			Records: []normalizeOracleRecord{{Kind: "service_repository_mapping.v1", ExternalID: "m1", Payload: map[string]any{
				"externalId": "m1", "sourceVersionAt": "2026-07-01T12:00:00Z",
				"serviceExternalId": "svc-1", "repoFullName": "acme/repo", "repoProvider": "github",
			}}},
		},
		{
			Name: "git family instance mismatch", SourceSystem: "github", SourceInstance: "acme/repo", EntityFamily: "legacy",
			Records: []normalizeOracleRecord{{Kind: "repository.v1", ExternalID: "r1", Payload: map[string]any{
				"externalId": "someone-else/other-repo", "sourceSystem": "github",
			}}},
		},
		{
			// Index correctness across a mixed batch: valid, shape-invalid,
			// unsupported-kind-for-system, valid again.
			Name: "mixed batch preserves index and per-record verdicts", SourceSystem: "github", SourceInstance: "acme/repo", EntityFamily: "legacy",
			Records: []normalizeOracleRecord{
				{Kind: "repository.v1", ExternalID: "r1", Payload: map[string]any{"externalId": "acme/repo", "sourceSystem": "github"}},
				{Kind: "identity.v1", ExternalID: "u1", Payload: map[string]any{"updatedAt": "2026-07-01T12:00:00Z"}},
				{Kind: "team.v1", ExternalID: "t1", Payload: map[string]any{"id": "t1", "name": "Team", "updatedAt": "2026-07-01T12:00:00Z"}},
				{Kind: "commit.v1", ExternalID: "c1", Payload: map[string]any{
					"repositoryExternalId": "acme/repo", "hash": "abc1234", "authorWhen": "2026-07-01T12:00:00Z",
				}},
			},
		},
		{
			Name: "clean valid batch has no rejections", SourceSystem: "github", SourceInstance: "acme/repo", EntityFamily: "legacy",
			Records: []normalizeOracleRecord{
				{Kind: "repository.v1", ExternalID: "r1", Payload: map[string]any{"externalId": "acme/repo", "sourceSystem": "github"}},
			},
		},
	}
}

// TestNormalizeExternalRecordsMatchesLivePythonNormalizeBatch is
// CHAOS-6345's proof: normalizeExternalRecords and normalize_batch agree,
// case by case, on which records are rejected, in what order, and with
// what code/message/path -- the orchestration this ticket changed
// (shape-validation-first precedence, the repository_identity_required
// port, rejection row construction from a ValidationErrorItem), not the
// shape rules themselves (already oracled exhaustively elsewhere).
func TestNormalizeExternalRecordsMatchesLivePythonNormalizeBatch(t *testing.T) {
	root, python := normalizeOracleRoot(t)
	corpus := normalizeOracleCorpus()

	type pythonCase struct {
		OrgID          string                  `json:"orgId"`
		SourceID       string                  `json:"sourceId"`
		SourceSystem   string                  `json:"sourceSystem"`
		SourceInstance string                  `json:"sourceInstance"`
		IngestionID    string                  `json:"ingestionId"`
		Records        []normalizeOracleRecord `json:"records"`
	}
	type wireRecord struct {
		Kind       string         `json:"kind"`
		ExternalID string         `json:"externalId"`
		Payload    map[string]any `json:"payload"`
	}

	orgID := "2b237281-6b27-4b46-8b23-14f14f2cf429"
	sourceID := uuid.New()
	ingestionID := uuid.New()

	pythonCases := make([]pythonCase, len(corpus))
	for i, c := range corpus {
		records := make([]normalizeOracleRecord, len(c.Records))
		for j, r := range c.Records {
			records[j] = normalizeOracleRecord{Kind: r.Kind, ExternalID: r.ExternalID, Payload: r.Payload}
		}
		pythonCases[i] = pythonCase{
			OrgID: orgID, SourceID: sourceID.String(), SourceSystem: c.SourceSystem,
			SourceInstance: c.SourceInstance, IngestionID: ingestionID.String(), Records: records,
		}
	}
	input, err := json.Marshal(pythonCases)
	if err != nil {
		t.Fatal(err)
	}
	output := runNormalizeOraclePython(t, root, python, input)
	lines := bytes.Split(bytes.TrimSpace(output), []byte("\n"))
	var want [][]struct {
		Index      int    `json:"index"`
		Kind       string `json:"kind"`
		ExternalID string `json:"externalId"`
		Code       string `json:"code"`
		Message    string `json:"message"`
		Path       string `json:"path"`
	}
	if err := json.Unmarshal(lines[len(lines)-1], &want); err != nil {
		t.Fatalf("decode python output: %v\n%s", err, output)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d case results, want %d", len(want), len(corpus))
	}

	pointer := externalPointer{IngestionID: ingestionID, OrgID: orgID, SchemaVersion: externalSchemaVersion}
	for i, c := range corpus {
		t.Run(c.Name, func(t *testing.T) {
			wireRecords := make([]wireRecord, len(c.Records))
			for j, r := range c.Records {
				wireRecords[j] = wireRecord{Kind: r.Kind, ExternalID: r.ExternalID, Payload: r.Payload}
			}
			body, err := json.Marshal(map[string]any{
				"schemaVersion": externalSchemaVersion, "idempotencyKey": "oracle-" + c.Name,
				"source": map[string]any{
					"type": "customer_push", "system": c.SourceSystem,
					"instance": c.SourceInstance, "entityFamily": c.EntityFamily,
				},
				"records": wireRecords,
			})
			if err != nil {
				t.Fatal(err)
			}
			envelope, err := parseExternalEnvelope(body)
			if err != nil {
				t.Fatalf("parseExternalEnvelope: %v", err)
			}
			p := pointer
			p.SourceSystem, p.SourceInstance = c.SourceSystem, c.SourceInstance
			_, rejections, _ := normalizeExternalRecords(p, envelope)

			if len(rejections) != len(want[i]) {
				t.Fatalf("rejections = %d, python = %d\ngo:     %#v\npython: %#v", len(rejections), len(want[i]), rejections, want[i])
			}
			for j, got := range rejections {
				py := want[i][j]
				if got.Index != py.Index || got.Kind != py.Kind || got.ExternalID != py.ExternalID ||
					got.Code != py.Code || got.Path != py.Path {
					t.Errorf("rejection[%d]:\n go     = %#v\n python = %#v", j, got, py)
					continue
				}
				// externalMessageNotClaimedExact: unsupported_kind_for_system,
				// entity_family_mismatch and record_outside_source_instance
				// are pre-existing Go-only checks (no CHAOS-6345 change touches
				// them) whose messages were never claimed message-exact --
				// unlike a shapeRejection's Message, which is
				// ValidateRecords' own item.Message verbatim. Python
				// interpolates the offending value and a master-spec
				// citation into these three; Go's stays a static
				// description. Code/index/kind/path -- the parts that decide
				// routing and attribution -- are asserted above regardless.
				if externalMessageNotClaimedExact[got.Code] {
					continue
				}
				if got.Message != py.Message {
					t.Errorf("rejection[%d] message:\n go     = %q\n python = %q", j, got.Message, py.Message)
				}
			}
		})
	}
	writeNormalizeOracleProof(t)
}
