package streamhandlers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/google/uuid"
)

type statementVariants struct {
	FixedNow string `json:"fixed_now"`
	OrgID    string `json:"org_id"`
	SourceID string `json:"source_id"`
	External []struct {
		Kind, System, Instance, Field, Statement, Table string
		Payload                                         map[string]any
		Rejected                                        bool
		Columns                                         []string
		Values                                          []any
	}
}

func loadStatementVariants(t *testing.T) statementVariants {
	t.Helper()
	_, filename, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("locate test")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "..", "..", "tests", "fixtures", "stored_version_statement_variants_python_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var variants statementVariants
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&variants); err != nil {
		t.Fatal(err)
	}
	if len(variants.External) == 0 {
		t.Fatal("no statement variants")
	}
	return variants
}

// externalUnreferencedColumns are columns this writer inserts that the
// Python reference writer does not; the reference defines no value for them.
var externalUnreferencedColumns = map[string][]string{
	"repos":      {"provider_repo_id", "identity_path"},
	"teams":      {"created_at", "provider_team_id", "provider_name", "gone_at"},
	"work_items": {"description", "due_at", "priority_raw", "service_class"},
}

// For every declared optional field of every in-scope external kind, a record
// stating it null or leaving it absent is refused exactly when the Python
// reference refuses it, and otherwise translates to the reference's value in
// every column the reference writes, before the contract's carry applies.
func TestExternalStatementVariantsMatchThePythonReference(t *testing.T) {
	variants := loadStatementVariants(t)
	now, err := time.Parse(time.RFC3339Nano, variants.FixedNow)
	if err != nil {
		t.Fatal(err)
	}
	sourceID := uuid.MustParse(variants.SourceID)
	unreferenced := map[string]bool{}
	divergences := teamIDDivergenceUse{}
	referenceOnly := referenceOnlyUse{}
	for _, variant := range variants.External {
		name := variant.Kind + " " + variant.System + " " + variant.Field + " " + variant.Statement
		err := validateExternalRecord(variant.Kind, variant.Payload)
		if variant.Rejected {
			if err == nil {
				t.Errorf("%s: the reference refuses the record, Go accepts it", name)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: the reference accepts the record, Go refuses it: %v", name, err)
			continue
		}
		query, _ := externalInsertQuery(variant.Kind)
		extended := query
		values, err := externalRecordValues(externalSinkBatch{
			Pointer: externalPointer{
				OrgID: variants.OrgID, SourceSystem: variant.System, SourceInstance: variant.Instance,
				IngestionID: uuid.MustParse("11111111-2222-4333-8444-555555555555"),
			},
			SourceID: sourceID,
		}, externalSinkRecord{Kind: variant.Kind, ExternalID: "golden", Payload: variant.Payload}, now, &ExternalRecomputeScope{}, nil)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if variant.Kind == "team.v1" {
			values = withTeamCreatedAt(values, nil)
		}
		if contract, ok := externalContract(variant.Kind, variant.System); ok {
			var appended []string
			extended, appended, _ = withContractColumns(query, contract)
			values = append(values, appendedValues(contract, appended, variant.Payload)...)
		}
		columns := strings.Split(extended[strings.IndexByte(extended, '(')+1:strings.LastIndexByte(extended, ')')], ",")
		reference := map[string]any{}
		referenceColumns, referenceValues := referenceOnly.drop(variant.Table, variant.Columns, variant.Values)
		for i, column := range referenceColumns {
			reference[column] = goldenComparableValue(referenceValues[i])
		}
		for i, column := range columns {
			want, ok := reference[column]
			if !ok {
				unreferenced[variant.Table+"."+column] = true
				continue
			}
			want = divergences.expected(variant.Table, column, variant.System, variant.Payload, want)
			if got := goldenComparableValue(values[i]); !reflect.DeepEqual(got, want) {
				t.Errorf("%s: %s = %#v, reference %#v", name, column, got, want)
			}
		}
	}
	divergences.checkReached(t)
	referenceOnly.checkReached(t)
	var got []string
	for column := range unreferenced {
		got = append(got, column)
	}
	sort.Strings(got)
	var want []string
	for table, columns := range externalUnreferencedColumns {
		for _, column := range columns {
			want = append(want, table+"."+column)
		}
	}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("columns without a reference value = %v, want %v", got, want)
	}
}

// referenceValue reads a persist-path value: the ingest API hands timestamps
// to the persist path as RFC 3339 strings.
func referenceValue(value any) any {
	if number, ok := value.(json.Number); ok {
		if parsed, err := number.Float64(); err == nil && strings.ContainsAny(number.String(), ".eE") {
			return goldenComparableValue(parsed)
		}
	}
	if text, ok := value.(string); ok {
		if parsed, err := time.Parse(time.RFC3339, text); err == nil {
			return goldenComparableValue(parsed)
		}
	}
	return goldenComparableValue(value)
}
