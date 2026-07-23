package jobcontract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateTree(t *testing.T) {
	t.Parallel()
	if err := ValidateTree(contractRoot(t)); err != nil {
		t.Fatalf("ValidateTree() error = %v", err)
	}
}

func TestVersionPolicyRequiresNAndNMinusOne(t *testing.T) {
	t.Parallel()
	root := contractRoot(t)
	registry, err := LoadRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	job := registry.Jobs[0]
	job.CurrentVersion = 2
	job.SupportedVersions = []int{1, 2}
	job.SchemaVersions["2"] = job.SchemaVersions["1"]
	job.Fixtures["2"] = job.Fixtures["1"]
	if err := validateJobDefinition(root, job); err != nil {
		t.Fatalf("N/N-1 definition rejected: %v", err)
	}
	job.SupportedVersions = []int{2}
	if err := validateJobDefinition(root, job); err == nil || !strings.Contains(err.Error(), "N-1") {
		t.Fatalf("missing N-1 error = %v", err)
	}
}

func TestJobDefinitionRejectsRegistrySchemaViolations(t *testing.T) {
	t.Parallel()
	root := contractRoot(t)
	registry, err := LoadRegistry(root)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*JobDefinition)
	}{
		{name: "profile", mutate: func(job *JobDefinition) { job.Profile = "Ops" }},
		{name: "queue", mutate: func(job *JobDefinition) { job.Queue = "queue with spaces" }},
		{name: "handler owner", mutate: func(job *JobDefinition) { job.HandlerOwner = "src/jobs/system" }},
		{name: "timeout upper bound", mutate: func(job *JobDefinition) { job.TimeoutSeconds = 86401 }},
		{name: "attempt upper bound", mutate: func(job *JobDefinition) { job.MaxAttempts = 26 }},
		{name: "retry policy", mutate: func(job *JobDefinition) { job.RetryPolicy = "Bad-Policy" }},
		{name: "concurrency upper bound", mutate: func(job *JobDefinition) { job.Concurrency.Limit = 10001 }},
		{name: "domain link", mutate: func(job *JobDefinition) { job.DomainLink = "bad-link" }},
		{name: "sensitive field", mutate: func(job *JobDefinition) { job.SensitiveFields = []string{"raw-payload"} }},
		{name: "schema version key", mutate: func(job *JobDefinition) {
			job.SchemaVersions = map[string]string{"01": job.SchemaVersions["1"]}
		}},
		{name: "fixture version key", mutate: func(job *JobDefinition) {
			job.Fixtures = map[string][]string{"zero": job.Fixtures["1"]}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job := registry.Jobs[0]
			test.mutate(&job)
			if err := validateJobDefinition(root, job); err == nil {
				t.Fatal("validateJobDefinition() error = nil, want schema violation rejection")
			}
		})
	}
}

func TestCapabilityRolloutChecksEveryLiveReport(t *testing.T) {
	t.Parallel()
	root := contractRoot(t)
	registry, err := LoadRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	state, err := LoadMigrationState(root, registry)
	if err != nil {
		t.Fatal(err)
	}
	report, err := CapabilitiesForProfile(root, registry, "ops")
	if err != nil {
		t.Fatal(err)
	}
	heavy, err := CapabilitiesForProfile(root, registry, "heavy")
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckRollout(root, registry, state, []CapabilityReport{report, report, heavy, heavy}); err != nil {
		t.Fatalf("CheckRollout() error = %v", err)
	}
	oldReplica := report
	oldReplica.Contracts = append([]ContractCapability(nil), report.Contracts...)
	oldReplica.Contracts[0] = ContractCapability{
		Kind: KindHeartbeat, Versions: []int{2},
		SchemaDigests: map[string]string{"2": "sha256:" + strings.Repeat("a", 64)},
	}
	if err := CheckRollout(root, registry, state, []CapabilityReport{report, oldReplica, heavy, heavy}); err == nil {
		t.Fatal("CheckRollout() accepted a live report without producer support")
	}
	staleDigest := report
	staleDigest.Contracts = append([]ContractCapability(nil), report.Contracts...)
	staleDigest.Contracts[0].SchemaDigests = map[string]string{"1": "sha256:" + strings.Repeat("0", 64)}
	if err := CheckRollout(root, registry, state, []CapabilityReport{report, staleDigest, heavy, heavy}); err == nil {
		t.Fatal("CheckRollout() accepted an old schema revision")
	}
	if err := CheckRollout(root, registry, state, nil); err == nil {
		t.Fatal("CheckRollout() accepted missing profile reports")
	}
}

func TestRollingDeploymentHoldsProducerAtNMinusOne(t *testing.T) {
	t.Parallel()
	root := contractRoot(t)
	registry := Registry{EnvelopeSchema: "envelope.schema.json", Jobs: []JobDefinition{{
		Kind:              KindHeartbeat,
		CurrentVersion:    2,
		SupportedVersions: []int{1, 2},
		Profile:           "ops",
		SchemaVersions: map[string]string{
			"1": "schemas/system.heartbeat.v1.schema.json",
			"2": "schemas/system.heartbeat.v1.schema.json",
		},
	}}}
	newBinary, err := CapabilitiesForProfile(root, registry, "ops")
	if err != nil {
		t.Fatal(err)
	}
	oldBinary := CapabilityReport{
		SchemaVersion: 1,
		Profile:       "ops",
		Contracts: []ContractCapability{{
			Kind:          KindHeartbeat,
			Versions:      []int{1},
			SchemaDigests: map[string]string{"1": newBinary.Contracts[0].SchemaDigests["1"]},
		}},
	}
	state := MigrationState{SchemaVersion: 1, Jobs: []MigrationJob{{
		Kind: KindHeartbeat, ProducerVersion: 1, RequiredProfiles: []string{"ops"},
	}}}
	if err := CheckRollout(root, registry, state, []CapabilityReport{oldBinary, newBinary}); err != nil {
		t.Fatalf("N-1 producer should remain safe during rolling deploy: %v", err)
	}
	state.Jobs[0].ProducerVersion = 2
	if err := CheckRollout(root, registry, state, []CapabilityReport{oldBinary, newBinary}); err == nil {
		t.Fatal("N producer advanced while an N-1 binary was still live")
	}
	if err := CheckRollout(root, registry, state, []CapabilityReport{newBinary}); err != nil {
		t.Fatalf("N producer did not advance after N-1 drained: %v", err)
	}
}

func TestBreakingChangeDetection(t *testing.T) {
	t.Parallel()
	base := contractRoot(t)
	candidate := filepath.Join(t.TempDir(), "v1")
	copyTree(t, base, candidate)

	schemaPath := filepath.Join(candidate, "schemas", "system.heartbeat.v1.schema.json")
	var schema map[string]any
	readJSONFile(t, schemaPath, &schema)
	properties := schema["properties"].(map[string]any)
	properties["note"] = map[string]any{"type": "string", "maxLength": json.Number("32")}
	writeJSONFile(t, schemaPath, schema)
	changes, err := CompareTrees(base, candidate)
	if err != nil {
		t.Fatalf("CompareTrees(optional field) error = %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("optional field reported as breaking: %#v", changes)
	}
	baseRegistry, err := LoadRegistry(base)
	if err != nil {
		t.Fatal(err)
	}
	candidateRegistry, err := LoadRegistry(candidate)
	if err != nil {
		t.Fatal(err)
	}
	state, err := LoadMigrationState(candidate, candidateRegistry)
	if err != nil {
		t.Fatal(err)
	}
	oldBinary, err := CapabilitiesForProfile(base, baseRegistry, "ops")
	if err != nil {
		t.Fatal(err)
	}
	newBinary, err := CapabilitiesForProfile(candidate, candidateRegistry, "ops")
	if err != nil {
		t.Fatal(err)
	}
	oldHeavy, err := CapabilitiesForProfile(base, baseRegistry, "heavy")
	if err != nil {
		t.Fatal(err)
	}
	newHeavy, err := CapabilitiesForProfile(candidate, candidateRegistry, "heavy")
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckRollout(candidate, candidateRegistry, state, []CapabilityReport{oldBinary, newBinary, oldHeavy, newHeavy}); err == nil {
		t.Fatal("same-version optional edit advanced while an old schema digest was live")
	}
	if err := CheckRollout(candidate, candidateRegistry, state, []CapabilityReport{newBinary, newHeavy}); err != nil {
		t.Fatalf("same-version optional edit did not advance after old digest drained: %v", err)
	}

	readJSONFile(t, schemaPath, &schema)
	properties = schema["properties"].(map[string]any)
	scheduled := properties["scheduled_for"].(map[string]any)
	scheduled["type"] = "integer"
	writeJSONFile(t, schemaPath, schema)
	first, err := CompareTrees(base, candidate)
	if err != nil {
		t.Fatalf("CompareTrees(type change) error = %v", err)
	}
	second, err := CompareTrees(base, candidate)
	if err != nil {
		t.Fatalf("CompareTrees(repeat) error = %v", err)
	}
	if len(first) == 0 {
		t.Fatal("type change was not reported as breaking")
	}
	if FormatBreakingChanges(first) != FormatBreakingChanges(second) {
		t.Fatalf("breaking output is nondeterministic\nfirst: %s\nsecond: %s", FormatBreakingChanges(first), FormatBreakingChanges(second))
	}
}

func TestEnvelopeOptionalFieldChangesEveryCapabilityDigest(t *testing.T) {
	t.Parallel()
	base := contractRoot(t)
	candidate := filepath.Join(t.TempDir(), "v1")
	copyTree(t, base, candidate)
	envelopePath := filepath.Join(candidate, "envelope.schema.json")
	var schema map[string]any
	readJSONFile(t, envelopePath, &schema)
	properties := schema["properties"].(map[string]any)
	properties["trace_id"] = map[string]any{
		"type": "string", "maxLength": json.Number("128"),
	}
	writeJSONFile(t, envelopePath, schema)
	changes, err := CompareTrees(base, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 {
		t.Fatalf("optional envelope field reported as breaking: %#v", changes)
	}
	baseRegistry, err := LoadRegistry(base)
	if err != nil {
		t.Fatal(err)
	}
	candidateRegistry, err := LoadRegistry(candidate)
	if err != nil {
		t.Fatal(err)
	}
	oldReport, err := CapabilitiesForProfile(base, baseRegistry, "ops")
	if err != nil {
		t.Fatal(err)
	}
	newReport, err := CapabilitiesForProfile(candidate, candidateRegistry, "ops")
	if err != nil {
		t.Fatal(err)
	}
	for index := range oldReport.Contracts {
		oldDigest := oldReport.Contracts[index].SchemaDigests["1"]
		newDigest := newReport.Contracts[index].SchemaDigests["1"]
		if oldDigest == newDigest {
			t.Fatalf("envelope edit did not change %s digest", oldReport.Contracts[index].Kind)
		}
	}
}

func TestFixtureSecurityScan(t *testing.T) {
	t.Parallel()
	for _, fixture := range []string{
		`{"token":"value"}`,
		`{"payload":{"dsn":"postgresql://user:pass@example/db"}}`,
		`{"payload":{"value":"Bearer example"}}`,
	} {
		if err := validateFixtureSafety([]byte(fixture)); err == nil {
			t.Fatalf("validateFixtureSafety(%s) error = nil", fixture)
		}
	}
}

func copyTree(t *testing.T, source, destination string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func readJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		t.Fatal(err)
	}
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
