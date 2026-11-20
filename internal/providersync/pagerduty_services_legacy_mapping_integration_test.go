//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/operationalordering"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A mapping snapshot reconciles only the sources the writer produces. Stored
// legacy heuristic rows are read-only: a sync neither tombstones them nor
// counts them as extra rows during recovery inspection.
func TestPagerDutyServicesMappingSnapshotKeepsStoredLegacyHeuristicRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "2")
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	seededAt := time.Date(2026, 8, 9, 19, 0, 0, 123456000, time.UTC)
	syncedAt := seededAt.Add(time.Hour)

	type seed struct {
		name       string
		entityType string
		source     pagerDutyServiceMappingSource
		legacy     bool
		serviceID  string
		repo       string
	}
	cases := []struct {
		name          string
		org           string
		seeds         []seed
		inspectBefore EffectInspection
		wantTombstone []string
		wantRowCount  uint64
	}{
		{
			name: "legacy rows only",
			org:  "org-legacy",
			seeds: []seed{
				{name: "legacy-source", entityType: legacyHeuristicMappingSource, legacy: true, serviceID: "PS2", repo: "full-chaos/legacy-a"},
				{name: "legacy-mapping", entityType: "mapping", legacy: true, serviceID: "PS3", repo: "full-chaos/legacy-b"},
			},
			inspectBefore: EffectAbsent,
			wantRowCount:  3,
		},
		{
			name: "non-legacy control",
			org:  "org-control",
			seeds: []seed{
				{name: "stale-admin", source: pagerDutyMappingAdmin, serviceID: "PS2", repo: "full-chaos/stale"},
				{name: "kept-metadata", source: pagerDutyMappingMetadata, serviceID: "PS1", repo: "full-chaos/payments"},
			},
			inspectBefore: EffectConflict,
			wantTombstone: []string{"stale-admin"},
			wantRowCount:  4,
		},
		{
			name: "legacy and non-legacy mixed",
			org:  "org-mixed",
			seeds: []seed{
				{name: "legacy-source", entityType: legacyHeuristicMappingSource, legacy: true, serviceID: "PS2", repo: "full-chaos/legacy-a"},
				{name: "stale-compass", source: pagerDutyMappingCompass, serviceID: "PS3", repo: "full-chaos/stale"},
			},
			inspectBefore: EffectConflict,
			wantTombstone: []string{"stale-compass"},
			wantRowCount:  4,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claim := nativeTestClaim("pagerduty", "services")
			claim.OrgID = tc.org
			service := func(id string, at time.Time) pagerDutyServiceRow {
				row, err := normalizePagerDutyService(claim, "acme", pagerDutyServicePayload{ID: id, Name: id}, at)
				if err != nil {
					t.Fatal(err)
				}
				return row
			}
			seeded := map[string]pagerDutyServiceRepositoryMappingRow{}
			var seedRows []pagerDutyServiceRepositoryMappingRow
			for _, s := range tc.seeds {
				source := s.source
				if s.legacy {
					source = pagerDutyMappingAdmin
				}
				row, err := pagerDutyServiceMappingFromReference(service(s.serviceID, seededAt),
					pagerDutyServiceRepositoryReference{Provider: "github", FullName: s.repo}, source, seededAt)
				if err != nil {
					t.Fatal(err)
				}
				if s.legacy {
					row.SourceEntityType = s.entityType
					row.RelationshipProvenance = pagerDutyStringPtr(legacyHeuristicMappingSource)
					row.MappingKind = pagerDutyStringPtr(legacyHeuristicMappingSource)
					row.RuleID = pagerDutyStringPtr("bounded_name_match.v1")
					confidence := 0.4
					row.RelationshipConfidence = &confidence
					row.ID, row.SourceConflictKey, row.SourceRevision, row.IngestRevision, row.OrderingContract = "", "", nil, nil, 0
					if err := fillPagerDutyServiceMappingOrdering(&row); err != nil {
						t.Fatal(err)
					}
				}
				seeded[s.name] = row
				seedRows = append(seedRows, row)
			}
			insertPagerDutyMappingSeedRows(ctx, t, conn, seedRows)

			snapshotRow, err := pagerDutyServiceMappingFromReference(service("PS1", syncedAt),
				pagerDutyServiceRepositoryReference{Provider: "github", FullName: "full-chaos/payments"},
				pagerDutyMappingMetadata, syncedAt)
			if err != nil {
				t.Fatal(err)
			}
			effect, err := effectBatchFromValues("operational_service_repository_mappings", EffectReadbackRequired,
				[]pagerDutyServiceRepositoryMappingRow{snapshotRow})
			if err != nil {
				t.Fatal(err)
			}
			sink := PagerDutyServicesClickHouseEffects{
				Entitlement: allowIncidentEntitlement,
				Conn:        conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
				ProviderInstanceID: "acme", Now: func() time.Time { return syncedAt },
			}
			if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != tc.inspectBefore {
				t.Errorf("inspection before write=%s error=%v want %s", inspection, err, tc.inspectBefore)
			}
			for attempt := 0; attempt < 2; attempt++ {
				if err := sink.WriteEffect(ctx, claim, effect); err != nil {
					t.Fatalf("write attempt %d: %v", attempt, err)
				}
			}
			if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
				t.Errorf("inspection after write=%s error=%v want exact", inspection, err)
			}

			tombstoned := map[string]bool{}
			for _, name := range tc.wantTombstone {
				tombstoned[name] = true
			}
			for _, s := range tc.seeds {
				row := seeded[s.name]
				active, validTo := pagerDutyMappingLatestState(ctx, t, conn, tc.org, row.ID)
				switch {
				case tombstoned[s.name]:
					if active || validTo == nil || !validTo.Equal(syncedAt) {
						t.Errorf("%s: active=%v valid_to=%v want tombstone at %s", s.name, active, validTo, syncedAt)
					}
				default:
					if !active || validTo != nil {
						t.Errorf("%s: active=%v valid_to=%v want unchanged active row", s.name, active, validTo)
					}
				}
			}
			// Versions, not raw rows: the replayed write may store a byte-equal duplicate.
			var legacyRows, allRows uint64
			if err := conn.QueryRow(ctx, `
SELECT
  uniqExactIf((id, source_revision, source_conflict_key, ingest_revision), source_entity_type = ? OR relationship_provenance = ?),
  uniqExact((id, source_revision, source_conflict_key, ingest_revision))
FROM operational_service_repository_mappings
WHERE org_id = ? AND provider = 'pagerduty' AND provider_instance_id = 'acme'`,
				legacyHeuristicMappingSource, legacyHeuristicMappingSource, tc.org).Scan(&legacyRows, &allRows); err != nil {
				t.Fatal(err)
			}
			wantLegacy := uint64(0)
			for _, s := range tc.seeds {
				if s.legacy {
					wantLegacy++
				}
			}
			if legacyRows != wantLegacy || allRows != tc.wantRowCount {
				t.Errorf("stored rows: legacy=%d all=%d want legacy=%d all=%d", legacyRows, allRows, wantLegacy, tc.wantRowCount)
			}
			t.Logf("org=%s legacy_rows=%d all_rows=%d", tc.org, legacyRows, allRows)
		})
	}
}

func insertPagerDutyMappingSeedRows(
	ctx context.Context, t *testing.T, conn driver.Conn, rows []pagerDutyServiceRepositoryMappingRow,
) {
	t.Helper()
	contract, err := newOperationalTableContracts().resolve(ctx, conn, "operational_service_repository_mappings")
	if err != nil {
		t.Fatal(err)
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO operational_service_repository_mappings ("+
		contract.columns(pagerDutyServicesLegacyMappingColumns)+")")
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(pagerDutyServiceMappingValuesForContract(row, contract)...); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func pagerDutyMappingLatestState(
	ctx context.Context, t *testing.T, conn driver.Conn, org, id string,
) (bool, *time.Time) {
	t.Helper()
	var active bool
	var validTo *time.Time
	if err := conn.QueryRow(ctx, `
SELECT is_active, valid_to
FROM operational_service_repository_mappings
WHERE org_id = ? AND id = ?
ORDER BY `+operationalordering.RevisionOrder+`
LIMIT 1`, org, id).Scan(&active, &validTo); err != nil {
		t.Fatal(err)
	}
	return active, validTo
}
