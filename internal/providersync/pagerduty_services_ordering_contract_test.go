package providersync

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

func TestPagerDutyServicesReadbackDoesNotClaimV2ColumnsFromActiveLegacySchema(t *testing.T) {
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "1")
	if err := os.Unsetenv("OPERATIONAL_ORDERING_CONTRACT"); err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("pagerduty", "services")
	now := time.Date(2026, 8, 12, 3, 0, 0, 0, time.UTC)
	service, err := normalizePagerDutyService(
		claim, "acme", pagerDutyServicePayload{ID: "PS1", Name: "Payments"}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	mapping, err := pagerDutyServiceMappingFromReference(
		service,
		pagerDutyServiceRepositoryReference{Provider: "github", FullName: "full-chaos/payments"},
		pagerDutyMappingMetadata,
		now,
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	repoID := uuid.New()
	mapping.RepoID = &repoID
	mapping.ID, mapping.SourceConflictKey = "", ""
	mapping.SourceRevision, mapping.IngestRevision = nil, nil
	mapping.OrderingContract = 0
	if err := fillPagerDutyServiceMappingOrdering(&mapping); err != nil {
		t.Fatal(err)
	}
	serviceEffect, err := effectBatchFromValues(
		"operational_services", EffectReadbackRequired, []pagerDutyServiceRow{service},
	)
	if err != nil {
		t.Fatal(err)
	}
	mappingEffect, err := effectBatchFromValues(
		"operational_service_repository_mappings",
		EffectReadbackRequired,
		[]pagerDutyServiceRepositoryMappingRow{mapping},
	)
	if err != nil {
		t.Fatal(err)
	}
	conn := &pagerDutyLegacySchemaProbeConn{}
	sink := PagerDutyServicesClickHouseEffects{
		Conn: conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
			return nil
		}),
		ProviderInstanceID: "acme",
	}
	for name, effect := range map[string]EffectBatch{
		"services": serviceEffect,
		"mappings": mappingEffect,
	} {
		inspection, inspectErr := sink.InspectEffect(context.Background(), claim, effect)
		if inspectErr != nil || inspection != EffectAbsent {
			t.Fatalf("%s inspection=%s error=%v", name, inspection, inspectErr)
		}
	}
	if len(conn.queries) != 4 {
		t.Fatalf("queries=%d want 4: %v", len(conn.queries), conn.queries)
	}
}

func TestPagerDutyServicesStorageContractDefaultsToLegacySchema(t *testing.T) {
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "1")
	if err := os.Unsetenv("OPERATIONAL_ORDERING_CONTRACT"); err != nil {
		t.Fatal(err)
	}
	contract, err := configuredPagerDutyServicesStorageContract()
	if err != nil {
		t.Fatal(err)
	}
	if contract != pagerDutyServicesLegacyContract {
		t.Fatalf("contract=%d want legacy", contract)
	}
	for name, query := range map[string]string{
		"active services": contract.loadActiveServicesQuery(),
		"active mappings": contract.loadActiveMappingsQuery(),
		"service":         contract.loadServiceQuery(),
		"mapping":         contract.loadMappingQuery(),
	} {
		if !strings.Contains(query, " FINAL ") {
			t.Errorf("%s query does not select legacy current rows with FINAL: %s", name, query)
		}
		for _, column := range []string{"source_revision", "source_conflict_key", "ingest_revision", "ordering_contract"} {
			if strings.Contains(query, column) {
				t.Errorf("%s query claims unavailable legacy column %s: %s", name, column, query)
			}
		}
	}
	if got, want := len(pagerDutyServiceValuesForContract(pagerDutyServiceRow{}, contract)), columnCount(contract.serviceColumns()); got != want {
		t.Fatalf("legacy service values=%d columns=%d", got, want)
	}
	if got, want := len(pagerDutyServiceMappingValuesForContract(pagerDutyServiceRepositoryMappingRow{}, contract)), columnCount(contract.mappingColumns()); got != want {
		t.Fatalf("legacy mapping values=%d columns=%d", got, want)
	}
}

func TestPagerDutyServicesStorageContractPreservesExplicitV2(t *testing.T) {
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "2")
	contract, err := configuredPagerDutyServicesStorageContract()
	if err != nil {
		t.Fatal(err)
	}
	if contract != pagerDutyServicesCurrentContract {
		t.Fatalf("contract=%d want current", contract)
	}
	for name, query := range map[string]string{
		"active services": contract.loadActiveServicesQuery(),
		"active mappings": contract.loadActiveMappingsQuery(),
		"service":         contract.loadServiceQuery(),
		"mapping":         contract.loadMappingQuery(),
	} {
		if strings.Contains(query, " FINAL ") {
			t.Errorf("%s current query unexpectedly uses legacy FINAL: %s", name, query)
		}
		if !strings.Contains(query, "source_revision") || !strings.Contains(query, "ingest_revision") {
			t.Errorf("%s current query omits v2 ordering columns: %s", name, query)
		}
	}
	if got, want := len(pagerDutyServiceValuesForContract(pagerDutyServiceRow{}, contract)), columnCount(contract.serviceColumns()); got != want {
		t.Fatalf("current service values=%d columns=%d", got, want)
	}
	if got, want := len(pagerDutyServiceMappingValuesForContract(pagerDutyServiceRepositoryMappingRow{}, contract)), columnCount(contract.mappingColumns()); got != want {
		t.Fatalf("current mapping values=%d columns=%d", got, want)
	}
}

func TestPagerDutyServicesStorageContractRejectsUnknownValue(t *testing.T) {
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "3")
	if _, err := configuredPagerDutyServicesStorageContract(); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("error=%v want invalid configuration", err)
	}
}

func columnCount(columns string) int {
	if columns == "" {
		return 0
	}
	return strings.Count(columns, ",") + 1
}

type pagerDutyLegacySchemaProbeConn struct {
	driver.Conn
	queries []string
}

func (conn *pagerDutyLegacySchemaProbeConn) Query(
	_ context.Context, query string, _ ...any,
) (driver.Rows, error) {
	conn.queries = append(conn.queries, query)
	for _, unavailable := range []string{
		"source_revision", "source_conflict_key", "ingest_revision", "ordering_contract",
	} {
		if strings.Contains(query, unavailable) {
			return nil, errors.New("Code: 47. DB::Exception: Unknown identifier " + unavailable)
		}
	}
	return pagerDutyEmptyRows{}, nil
}

type pagerDutyEmptyRows struct{}

func (pagerDutyEmptyRows) Next() bool                       { return false }
func (pagerDutyEmptyRows) Scan(...any) error                { return nil }
func (pagerDutyEmptyRows) ScanStruct(any) error             { return nil }
func (pagerDutyEmptyRows) ColumnTypes() []driver.ColumnType { return nil }
func (pagerDutyEmptyRows) Totals(...any) error              { return nil }
func (pagerDutyEmptyRows) Columns() []string                { return nil }
func (pagerDutyEmptyRows) Close() error                     { return nil }
func (pagerDutyEmptyRows) Err() error                       { return nil }
func (pagerDutyEmptyRows) HasData() bool                    { return false }
