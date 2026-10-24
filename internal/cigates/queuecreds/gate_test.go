package queuecreds_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/queuecreds"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func valueScanner(t *testing.T, root string) *queuecreds.ValueScanner {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "contracts", "auth", "v1", "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	scanner, err := queuecreds.NewValueScanner(data)
	if err != nil {
		t.Fatal(err)
	}
	return scanner
}

func report(t *testing.T, findings []queuecreds.Finding) {
	t.Helper()
	for _, finding := range findings {
		t.Errorf("%s", finding)
	}
}

// The contract tree is the wire shape of every payload. A measurement that read
// no schema or no example must fail, never pass.
func TestJobContractCarriesNoCredential(t *testing.T) {
	root := repoRoot(t)
	scan, err := queuecreds.ScanContracts(root, valueScanner(t, root))
	if err != nil {
		t.Fatal(err)
	}
	if scan.Schemas < 20 || scan.Examples < 20 {
		t.Fatalf("scanned %d schemas and %d examples; the contract tree is missing or moved", scan.Schemas, scan.Examples)
	}
	report(t, scan.Findings)
}

// Every schema file named by the registry must sit in the scanned directory.
func TestRegistrySchemasAreScanned(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "contracts", "jobs", "v1", "registry.json"))
	if err != nil {
		t.Fatal(err)
	}
	var registry struct {
		Jobs []struct {
			Kind           string            `json:"kind"`
			SchemaVersions map[string]string `json:"schema_versions"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		t.Fatal(err)
	}
	referenced := 0
	for _, job := range registry.Jobs {
		for _, schema := range job.SchemaVersions {
			referenced++
			if !strings.HasPrefix(schema, "schemas/") || strings.Contains(schema[len("schemas/"):], "/") {
				t.Errorf("%s names schema %q outside the scanned schemas/ directory", job.Kind, schema)
				continue
			}
			if _, err := os.Stat(filepath.Join(root, "contracts", "jobs", "v1", filepath.FromSlash(schema))); err != nil {
				t.Errorf("%s names schema %q that does not exist", job.Kind, schema)
			}
		}
	}
	if referenced == 0 {
		t.Fatal("registry names no schema")
	}
}

var payloadTypes = []reflect.Type{
	reflect.TypeOf(jobcontract.DomainLink{}),
	reflect.TypeOf(jobcontract.DimensionFoldPayload{}),
	reflect.TypeOf(jobcontract.LLMBudgetStaleReservationsPayload{}),
	reflect.TypeOf(jobcontract.HeartbeatPayload{}),
	reflect.TypeOf(jobcontract.SyncCoverageRefreshPayload{}),
	reflect.TypeOf(jobcontract.RetentionCleanupPayload{}),
	reflect.TypeOf(jobcontract.BillingNotificationPayload{}),
	reflect.TypeOf(jobcontract.WebhookDeliveryPayload{}),
	reflect.TypeOf(jobcontract.OnDemandReportExecutionPayload{}),
	reflect.TypeOf(jobcontract.ScheduledReportExecutionPayload{}),
	reflect.TypeOf(jobcontract.DailyMetricsDispatchPayload{}),
	reflect.TypeOf(jobcontract.DailyMetricsPartitionPayload{}),
	reflect.TypeOf(jobcontract.DailyMetricsFinalizePayload{}),
	reflect.TypeOf(jobcontract.TeamAutoimportPayload{}),
	reflect.TypeOf(jobcontract.TeamRepoOwnershipDerivationPayload{}),
	reflect.TypeOf(jobcontract.WorkGraphBuildPayload{}),
	reflect.TypeOf(jobcontract.InvestmentMaterializePayload{}),
	reflect.TypeOf(jobcontract.RemainingMetricsPartitionPayload{}),
	reflect.TypeOf(jobcontract.ProviderUnitPayload{}),
}

var queueArgTypes = []reflect.Type{
	reflect.TypeOf(jobruntime.HeartbeatArgs{}),
	reflect.TypeOf(jobruntime.DimensionFoldArgs{}),
	reflect.TypeOf(jobruntime.LLMBudgetStaleReservationsArgs{}),
	reflect.TypeOf(jobruntime.SyncCoverageRefreshArgs{}),
	reflect.TypeOf(jobruntime.RetentionCleanupArgs{}),
	reflect.TypeOf(jobruntime.BillingNotificationArgs{}),
	reflect.TypeOf(jobruntime.WebhookDeliveryArgs{}),
	reflect.TypeOf(jobruntime.OnDemandReportExecutionArgs{}),
	reflect.TypeOf(jobruntime.ScheduledReportExecutionArgs{}),
	reflect.TypeOf(jobruntime.DailyMetricsDispatchArgs{}),
	reflect.TypeOf(jobruntime.DailyMetricsPartitionArgs{}),
	reflect.TypeOf(jobruntime.DailyMetricsFinalizeArgs{}),
	reflect.TypeOf(jobruntime.WorkGraphBuildArgs{}),
	reflect.TypeOf(jobruntime.InvestmentMaterializeArgs{}),
	reflect.TypeOf(jobruntime.RemainingCapacityArgs{}),
	reflect.TypeOf(jobruntime.RemainingComplexityArgs{}),
	reflect.TypeOf(jobruntime.RemainingDORAArgs{}),
	reflect.TypeOf(jobruntime.RemainingMembershipArgs{}),
	reflect.TypeOf(jobruntime.RemainingRecommendationsArgs{}),
	reflect.TypeOf(jobruntime.RemainingReleaseImpactArgs{}),
	reflect.TypeOf(jobruntime.RemainingWorkItemAttributionArgs{}),
	reflect.TypeOf(jobruntime.ProviderUnitArgs{}),
	reflect.TypeOf(syncdispatchruntime.TeamAutoimportJobArgs{}),
	reflect.TypeOf(syncdispatchruntime.TeamRepoOwnershipDerivationJobArgs{}),
	reflect.TypeOf(syncdispatchruntime.DispatchSyncRunArgs{}),
	reflect.TypeOf(syncdispatchruntime.FinalizeSyncRunArgs{}),
	reflect.TypeOf(syncdispatchruntime.PostSyncArgs{}),
	reflect.TypeOf(syncdispatchruntime.ReferenceDiscoveryArgs{}),
	reflect.TypeOf(syncdispatchruntime.Claim{}),
	reflect.TypeOf(syncdispatchruntime.DomainReference{}),
}

func TestQueueTypesCarryNoCredential(t *testing.T) {
	for _, typ := range append(append([]reflect.Type{}, payloadTypes...), queueArgTypes...) {
		report(t, queuecreds.ScanType(typ.PkgPath()[strings.LastIndex(typ.PkgPath(), "/")+1:]+"."+typ.Name(), typ))
	}
}

// A queue type added without a line in the lists above would escape the scan.
// Every *Payload struct and every type with a Kind method in the three queue
// packages must be listed.
func TestEveryQueueTypeIsScanned(t *testing.T) {
	root := repoRoot(t)
	listed := map[string]bool{}
	for _, typ := range append(append([]reflect.Type{}, payloadTypes...), queueArgTypes...) {
		listed[typ.PkgPath()[strings.LastIndex(typ.PkgPath(), "/")+1:]+"."+typ.Name()] = true
	}
	var seen []string
	for _, pkg := range []struct {
		dir     string
		methods bool
	}{{"internal/jobcontract", false}, {"internal/jobruntime", true}, {"internal/syncdispatchruntime", true}} {
		parsed, err := queuecreds.ParsePackage(filepath.Join(root, filepath.FromSlash(pkg.dir)))
		if err != nil {
			t.Fatal(err)
		}
		names := parsed.StructTypesWithSuffix("Payload")
		if pkg.methods {
			names = append(names, parsed.TypesWithMethod("Kind")...)
		}
		for _, name := range names {
			label := filepath.Base(pkg.dir) + "." + name
			seen = append(seen, label)
			if !listed[label] {
				t.Errorf("%s is a queue type that the credential scan does not list", label)
			}
		}
	}
	if len(seen) < 40 {
		t.Fatalf("found only %d queue types; the parse missed the packages", len(seen))
	}
	sort.Strings(seen)
}

// Operator-facing JSON: every json tag in the worker operator CLI and the
// worker presence summary is scanned.
func TestOperatorDiagnosticsCarryNoCredential(t *testing.T) {
	root := repoRoot(t)
	parsed, err := queuecreds.ParsePackage(filepath.Join(root, "internal", "workersctl"))
	if err != nil {
		t.Fatal(err)
	}
	if tagged := len(parsed.TaggedFields()); tagged < 10 {
		t.Fatalf("found only %d json tags in internal/workersctl; the parse missed the package", tagged)
	}
	report(t, parsed.OperatorFindings("internal/workersctl", queuecreds.OperatorExemptions))
	report(t, queuecreds.ScanType("jobruntime.WorkerPresenceSummary", reflect.TypeOf(jobruntime.WorkerPresenceSummary{})))
}

func plantedCredential() string {
	segment := func(text string) string { return base64.RawURLEncoding.EncodeToString([]byte(text)) }
	return segment(`{"alg":"none"}`) + "." + segment(`{"sub":"planted"}`) + "." + segment("planted-signature")
}

// A payload that carries a credential is refused, and the text of the refusal
// (the text the outbox keeps as the row's error detail) never holds the value.
func TestRefusalTextNeverEchoesTheValue(t *testing.T) {
	value := plantedCredential()
	envelope := func(payload map[string]any) []byte {
		data, err := json.Marshal(map[string]any{
			"contract_version": 1,
			"correlation_id":   "job-heartbeat-0001",
			"idempotency_key":  "heartbeat:2026-07-21T12:00:00Z",
			"domain":           map[string]any{"type": "schedule_occurrence", "id": "00000000-0000-4000-8000-000000000001"},
			"payload":          payload,
		})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	cases := map[string]map[string]any{
		"unknown field":  {"scheduled_for": "2026-07-21T12:00:00Z", "access_token": value},
		"known field":    {"scheduled_for": value},
		"nested unknown": {"scheduled_for": "2026-07-21T12:00:00Z", "extra": map[string]any{"bearer": value}},
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := jobcontract.Decode(jobcontract.KindHeartbeat, envelope(payload))
			if err == nil {
				t.Fatal("a payload carrying a credential was accepted")
			}
			if strings.Contains(err.Error(), value) {
				t.Errorf("decode refusal echoes the credential value: %q", err.Error())
			}
		})
	}
	_, err := jobcontract.MarshalCanonical(jobcontract.Envelope{
		ContractVersion: 1, CorrelationID: "job-heartbeat-0001", IdempotencyKey: "heartbeat:2026-07-21T12:00:00Z",
		Domain:  jobcontract.DomainLink{Type: "schedule_occurrence", ID: "00000000-0000-4000-8000-000000000001"},
		Payload: jobcontract.HeartbeatPayload{ScheduledFor: value},
	})
	if err == nil {
		t.Fatal("a canonical envelope with a credential in a typed field was accepted")
	}
	if strings.Contains(err.Error(), value) {
		t.Errorf("canonical refusal echoes the credential value: %q", err.Error())
	}
}

// A type that embeds a queue type serializes its fields and satisfies its
// interfaces without declaring a Kind method of its own, so discovery by
// declaration misses it. Every embedder of a listed type must be listed.
func TestEveryQueueTypeEmbedderIsScanned(t *testing.T) {
	root := repoRoot(t)
	listed := map[string]bool{}
	known := map[string]bool{}
	for _, typ := range append(append([]reflect.Type{}, payloadTypes...), queueArgTypes...) {
		listed[typ.PkgPath()[strings.LastIndex(typ.PkgPath(), "/")+1:]+"."+typ.Name()] = true
		if strings.HasSuffix(typ.Name(), "Args") || strings.HasSuffix(typ.Name(), "Payload") {
			known[typ.Name()] = true
		}
	}
	embedders, err := queuecreds.FindEmbedders(root, []string{"internal", "cmd"}, known)
	if err != nil {
		t.Fatal(err)
	}
	for _, label := range embedders {
		if !listed[label] {
			t.Errorf("%s embeds a queue type and the credential scan does not list it", label)
		}
	}
}

// Probe every string schema with each known credential shape. Only the pinned
// strings may accept one.
func TestCredentialShapedStringsAreAdmittedOnlyWhereAnIdIsFreeText(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "contracts", "auth", "v1", "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	probes, err := queuecreds.CredentialProbes(data)
	if err != nil {
		t.Fatal(err)
	}
	got, err := queuecreds.AdmittingStrings(root, probes)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]string{}, queuecreds.FreeTextIdentifiers...), queuecreds.RuntimeBoundStrings...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("strings that accept a credential-shaped value:\n got  %q\n want %q", got, want)
	}
}

// domain.type accepts a credential-shaped value in the schema; Decode must
// refuse it for every kind, because the value has to equal the kind's
// registered domain_link word, and no registered word may look like a credential.
func TestDomainTypeRefusesCredentialShapedValues(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "contracts", "auth", "v1", "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	probes, err := queuecreds.CredentialProbes(data)
	if err != nil {
		t.Fatal(err)
	}
	values := valueScanner(t, root)
	registry, err := jobcontract.LoadRegistry(filepath.Join(root, "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(registry.Jobs) < 20 {
		t.Fatalf("registry lists %d jobs", len(registry.Jobs))
	}
	for _, job := range registry.Jobs {
		if rule := values.Match(job.DomainLink); rule != "" {
			t.Errorf("%s domain_link %q matches %s", job.Kind, job.DomainLink, rule)
		}
	}
	control, _ := json.Marshal(map[string]any{
		"contract_version": 1, "correlation_id": "job-heartbeat-0001", "idempotency_key": "heartbeat:2026-07-21T12:00:00Z",
		"domain":  map[string]any{"type": "schedule_occurrence", "id": "00000000-0000-4000-8000-000000000001"},
		"payload": map[string]any{"scheduled_for": "2026-07-21T12:00:00Z"},
	})
	if _, err := jobcontract.Decode(jobcontract.KindHeartbeat, control); err != nil {
		t.Fatalf("the control envelope is refused: %v", err)
	}
	for _, probe := range probes {
		domainType := probe
		if len(domainType) > 64 {
			domainType = domainType[:64]
		}
		envelope, err := json.Marshal(map[string]any{
			"contract_version": 1,
			"correlation_id":   "job-heartbeat-0001",
			"idempotency_key":  "heartbeat:2026-07-21T12:00:00Z",
			"domain":           map[string]any{"type": domainType, "id": "00000000-0000-4000-8000-000000000001"},
			"payload":          map[string]any{"scheduled_for": "2026-07-21T12:00:00Z"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := jobcontract.Decode(jobcontract.KindHeartbeat, envelope); err == nil {
			t.Errorf("Decode accepted domain.type %q", domainType)
		}
	}
}

// Every envelope string the schema leaves free-form is bound at decode time by
// the runtime value guard: Decode refuses each known credential shape in each of
// them, so no string is pinned as free text any more.
func TestEveryRuntimeBoundStringRefusesCredentialShapedValues(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "contracts", "auth", "v1", "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	probes, err := queuecreds.CredentialProbes(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(queuecreds.FreeTextIdentifiers) != 0 {
		t.Fatalf("free-text envelope strings remain: %q", queuecreds.FreeTextIdentifiers)
	}
	places := map[string]func(document map[string]any, value string){
		"envelope.schema.json $.properties.correlation_id":         func(d map[string]any, v string) { d["correlation_id"] = v },
		"envelope.schema.json $.properties.idempotency_key":        func(d map[string]any, v string) { d["idempotency_key"] = v },
		"envelope.schema.json $.properties.domain.properties.type": func(d map[string]any, v string) { d["domain"].(map[string]any)["type"] = v },
	}
	if len(places) != len(queuecreds.RuntimeBoundStrings) {
		t.Fatalf("%d strings exercised, %d pinned", len(places), len(queuecreds.RuntimeBoundStrings))
	}
	for _, pinned := range queuecreds.RuntimeBoundStrings {
		place, ok := places[pinned]
		if !ok {
			t.Fatalf("pinned string %q has no refusal check", pinned)
		}
		for _, probe := range probes {
			document := map[string]any{
				"contract_version": 1, "correlation_id": "job-heartbeat-0001", "idempotency_key": "heartbeat:2026-07-21T12:00:00Z",
				"domain":  map[string]any{"type": "schedule_occurrence", "id": "00000000-0000-4000-8000-000000000001"},
				"payload": map[string]any{"scheduled_for": "2026-07-21T12:00:00Z"},
			}
			place(document, probe)
			encoded, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := jobcontract.Decode(jobcontract.KindHeartbeat, encoded); !errors.Is(err, jobcontract.ErrCredentialShapedValue) {
				t.Errorf("%s: Decode error = %v, want the value refusal", pinned, err)
			}
		}
	}
}
