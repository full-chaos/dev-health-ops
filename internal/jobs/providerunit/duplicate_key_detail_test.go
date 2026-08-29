package providerunit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// realDuplicateNaturalKeyError drives a REAL recordGitHubTestsKey rejection
// (two identical test_suite_results rows through the actual
// providersync.TestOpsClickHouseEffects.WriteEffect), rather than
// hand-constructing an error, so this test proves failTerminal against the
// exact error shape production code returns -- not a stand-in.
func realDuplicateNaturalKeyError(t *testing.T) error {
	t.Helper()
	// Field names/tags MUST mirror providersync's unexported testSuiteResultRow
	// (github_tests_reports.go) exactly -- decodeEffectRows unmarshals into
	// that struct's snake_case json tags, and a mismatched tag here would
	// silently decode empty strings instead of reproducing the real row.
	type suiteRow struct {
		OrgID      string    `json:"org_id"`
		RepoID     string    `json:"repo_id"`
		RunID      string    `json:"run_id"`
		SuiteID    string    `json:"suite_id"`
		SuiteName  string    `json:"suite_name"`
		TotalCount int64     `json:"total_count"`
		LastSynced time.Time `json:"last_synced"`
	}
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	row := suiteRow{
		OrgID: "org-acme", RepoID: "repo-1", RunID: "9001",
		SuiteID: "dup-suite", SuiteName: "pytest", LastSynced: now,
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := providersync.BuildEffectBatch(
		"test_suite_results", providersync.EffectReadbackRequired,
		[]json.RawMessage{encoded, encoded},
	)
	if err != nil {
		t.Fatal(err)
	}
	capability, ok := providersync.Capability("github", "cicd")
	if !ok {
		t.Fatal("github/cicd capability not registered")
	}
	claim := providersync.Claim{
		Unit: providersync.Unit{
			ID:               "11111111-1111-4111-8111-111111111111",
			SyncRunID:        "22222222-2222-4222-8222-222222222222",
			OrgID:            "org-acme",
			IntegrationID:    "33333333-3333-4333-8333-333333333333",
			SourceID:         "44444444-4444-4444-8444-444444444444",
			SourceExternalID: "acme/api", SourceName: "acme/api",
			Provider: "github", Dataset: "cicd", CostClass: capability.CostClass,
			Mode:         "incremental",
			CredentialID: "66666666-6666-4666-8666-666666666666",
			AuthSource:   "integration_credential",
		},
		Owner: "55555555-5555-4555-8555-555555555555", Attempt: 1,
		LeaseExpiresAt: now.Add(time.Hour),
	}
	sink := providersync.TestOpsClickHouseEffects{
		Conn:  &acceptingConn{},
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	writeErr := sink.WriteEffect(context.Background(), claim, effect)
	if !errors.Is(writeErr, providersync.ErrDuplicateNaturalKey) {
		t.Fatalf("fixture did not reproduce ErrDuplicateNaturalKey: %v", writeErr)
	}
	return writeErr
}

// acceptingConn is a driver.Conn whose PrepareBatch returns a batch that
// accepts every Append, so WriteEffect reaches recordGitHubTestsKey's
// natural-key check instead of stopping at a nil-Conn guard.
type acceptingConn struct{ driver.Conn }

func (c *acceptingConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &acceptingBatch{}, nil
}

type acceptingBatch struct{ driver.Batch }

func (b *acceptingBatch) Append(...any) error { return nil }
func (b *acceptingBatch) Send() error         { return nil }
func (b *acceptingBatch) Abort() error        { return nil }

// duplicateKeyDetailFake implements DuplicateKeyDetailRepository on top of
// memoryUnitRepository, recording exactly what it was called with so the
// test can assert failTerminal chose the richer path and passed the right
// fields through.
type duplicateKeyDetailFake struct {
	*memoryUnitRepository
	calls        int
	lastCategory string
	lastTable    string
	lastFields   []providersync.DuplicateNaturalKeyField
}

func (f *duplicateKeyDetailFake) FailWithDuplicateKeyDetail(
	_ context.Context, claim providersync.Claim, category string,
	table string, fields []providersync.DuplicateNaturalKeyField,
	_, _ time.Time,
) error {
	f.calls++
	f.lastCategory = category
	f.lastTable = table
	f.lastFields = fields
	f.memoryUnitRepository.mu.Lock()
	f.memoryUnitRepository.failures++
	f.memoryUnitRepository.lastFailCategory = category
	f.memoryUnitRepository.status = "failed"
	f.memoryUnitRepository.mu.Unlock()
	return nil
}

// TestFailTerminalPersistsDuplicateKeyDetailWhenSupported pins CHAOS-4557's
// core wiring: when the repository supports DuplicateKeyDetailRepository AND
// the error carries a structured duplicate-natural-key detail, failTerminal
// must call FailWithDuplicateKeyDetail with the real table and fields --
// not the plain Fail that only ever persisted the bare category.
func TestFailTerminalPersistsDuplicateKeyDetailWhenSupported(t *testing.T) {
	unit := providersync.Unit{ID: "11111111-1111-4111-8111-111111111111", OrgID: "org-acme"}
	fake := &duplicateKeyDetailFake{memoryUnitRepository: newMemoryUnitRepository(unit)}
	handler := &Handler{Repository: fake}
	claim := providersync.Claim{Unit: providersync.Unit{ID: unit.ID, OrgID: unit.OrgID, Provider: "github", Dataset: "cicd"}}
	fake.lastClaim = claim

	err := realDuplicateNaturalKeyError(t)
	now := time.Now()
	if failErr := handler.failTerminal(context.Background(), claim, DuplicateNaturalKeyCategory, err, now, now.Add(time.Second)); failErr != nil {
		t.Fatalf("failTerminal returned %v, want nil", failErr)
	}

	if fake.calls != 1 {
		t.Fatalf("FailWithDuplicateKeyDetail calls=%d, want 1 (plain Fail must not have been used instead)", fake.calls)
	}
	if fake.lastCategory != DuplicateNaturalKeyCategory {
		t.Fatalf("category=%q, want %q", fake.lastCategory, DuplicateNaturalKeyCategory)
	}
	if fake.lastTable != "test_suite_results" {
		t.Fatalf("table=%q, want test_suite_results", fake.lastTable)
	}
	found := map[string]string{}
	for _, field := range fake.lastFields {
		found[field.Name] = field.Value
	}
	for _, want := range []string{"org_id", "repo_id", "run_id", "suite_id"} {
		if _, ok := found[want]; !ok {
			t.Fatalf("fields=%+v missing %q", fake.lastFields, want)
		}
	}
	if found["run_id"] != "9001" || found["suite_id"] != "dup-suite" {
		t.Fatalf("fields=%+v, want run_id=9001 suite_id=dup-suite", fake.lastFields)
	}
}

// TestFailTerminalFallsBackToPlainFailWithoutSupport pins the additive-only
// contract: a repository that does NOT implement DuplicateKeyDetailRepository
// (every existing test double and any older rolling binary) must keep working
// exactly as before -- failTerminal falls back to Fail.
func TestFailTerminalFallsBackToPlainFailWithoutSupport(t *testing.T) {
	unit := providersync.Unit{ID: "11111111-1111-4111-8111-111111111111", OrgID: "org-acme"}
	repository := newMemoryUnitRepository(unit)
	repository.status = "running"
	handler := &Handler{Repository: repository}
	claim := providersync.Claim{Unit: providersync.Unit{ID: unit.ID, OrgID: unit.OrgID, Provider: "github", Dataset: "cicd"}}
	repository.lastClaim = claim

	err := realDuplicateNaturalKeyError(t)
	now := time.Now()
	if failErr := handler.failTerminal(context.Background(), claim, DuplicateNaturalKeyCategory, err, now, now.Add(time.Second)); failErr != nil {
		t.Fatalf("failTerminal returned %v, want nil", failErr)
	}
	if repository.failures != 1 || repository.lastFailCategory != DuplicateNaturalKeyCategory {
		t.Fatalf("plain Fail path did not record the failure: failures=%d lastFailCategory=%q",
			repository.failures, repository.lastFailCategory)
	}
}

// TestFailTerminalUsesPlainFailForNonDuplicateKeyCategories pins that the new
// branch is scoped to duplicate-key errors only -- every other terminal
// category (which carries no DuplicateNaturalKeyDetail) must still go through
// plain Fail even against a repository that DOES support the richer method.
func TestFailTerminalUsesPlainFailForNonDuplicateKeyCategories(t *testing.T) {
	unit := providersync.Unit{ID: "11111111-1111-4111-8111-111111111111", OrgID: "org-acme"}
	fake := &duplicateKeyDetailFake{memoryUnitRepository: newMemoryUnitRepository(unit)}
	fake.status = "running"
	handler := &Handler{Repository: fake}
	claim := providersync.Claim{Unit: providersync.Unit{ID: unit.ID, OrgID: unit.OrgID, Provider: "github", Dataset: "cicd"}}
	fake.lastClaim = claim

	now := time.Now()
	if failErr := handler.failTerminal(
		context.Background(), claim, FeatureDisabledCategory, providersync.ErrIncidentEntitlementDisabled, now, now.Add(time.Second),
	); failErr != nil {
		t.Fatalf("failTerminal returned %v, want nil", failErr)
	}
	if fake.calls != 0 {
		t.Fatalf("FailWithDuplicateKeyDetail calls=%d, want 0 for a non-duplicate-key category", fake.calls)
	}
	if fake.failures != 1 || fake.lastFailCategory != FeatureDisabledCategory {
		t.Fatalf("plain Fail path did not record the failure: failures=%d lastFailCategory=%q",
			fake.failures, fake.lastFailCategory)
	}
}

// TestObserveDuplicateNaturalKeyCollisionCountsPerTable pins the counter half
// of CHAOS-4557: the terminal duplicate_natural_key path must increment
// dev_health_cicd_duplicate_natural_key_total labeled by the ACTUAL colliding
// table, and must not fire for any other category.
func TestObserveDuplicateNaturalKeyCollisionCountsPerTable(t *testing.T) {
	claim := providersync.Claim{Unit: providersync.Unit{Provider: "github", Dataset: "cicd"}}
	err := realDuplicateNaturalKeyError(t)

	metrics := providerfoundation.NewMetrics()
	handler := &Handler{ProviderMetrics: metrics}
	handler.observeDuplicateNaturalKeyCollision(claim, DuplicateNaturalKeyCategory, err)
	// A non-duplicate-key category must never increment this counter, even
	// when the underlying error happens to carry a detail.
	handler.observeDuplicateNaturalKeyCollision(claim, FeatureDisabledCategory, err)

	var output bytes.Buffer
	if writeErr := metrics.WritePrometheus(&output); writeErr != nil {
		t.Fatal(writeErr)
	}
	rendered := output.String()
	want := `dev_health_cicd_duplicate_natural_key_total{provider="github",dataset="cicd",table="test_suite_results"} 1`
	if !strings.Contains(rendered, want) {
		t.Fatalf("missing %q in:\n%s", want, rendered)
	}
}
