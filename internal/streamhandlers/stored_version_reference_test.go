package streamhandlers

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/google/uuid"
)

type statementVariants struct {
	FixedNow        string `json:"fixed_now"`
	OrgID           string `json:"org_id"`
	SourceID        string `json:"source_id"`
	InternalRepoURL string `json:"internal_repo_url"`
	External        []struct {
		Kind, System, Instance, Field, Statement, Table string
		Payload                                         map[string]any
		Rejected                                        bool
		Columns                                         []string
		Values                                          []any
	}
	Internal []struct {
		Entity, Field, Statement, Table string
		Item                            map[string]any
		Rejected                        bool
		Columns                         []string
		Values                          []any
	}
}

func loadStatementVariants(t *testing.T) statementVariants {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
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
	if len(variants.External) == 0 || len(variants.Internal) == 0 {
		t.Fatal("no statement variants")
	}
	return variants
}

// externalUnreferencedColumns are columns this writer inserts that the
// Python reference writer does not; the reference defines no value for them.
var externalUnreferencedColumns = map[string][]string{
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
		if contract, ok := externalContract(variant.Kind, variant.System); ok {
			var appended []string
			extended, appended, _ = withContractColumns(query, contract)
			values = append(values, appendedValues(contract, appended, variant.Payload)...)
		}
		columns := strings.Split(extended[strings.IndexByte(extended, '(')+1:strings.LastIndexByte(extended, ')')], ",")
		reference := map[string]any{}
		for i, column := range variant.Columns {
			reference[column] = goldenComparableValue(variant.Values[i])
		}
		for i, column := range columns {
			want, ok := reference[column]
			if !ok {
				unreferenced[variant.Table+"."+column] = true
				continue
			}
			if got := goldenComparableValue(values[i]); !reflect.DeepEqual(got, want) {
				t.Errorf("%s: %s = %#v, reference %#v", name, column, got, want)
			}
		}
	}
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

// internalUnreferencedColumns are columns the internal ingest writer inserts
// that the Python persist path does not.
var internalUnreferencedColumns = map[string][]string{}

// internalUndefinedReferences are cells where the persist path hands None to
// a non-Nullable column, so the reference defines no value; the writer's own
// value stands there, taken from the named column.
var internalUndefinedReferences = map[string]string{
	"commits committer_when null: committer_when": "author_when",
	"work-items updated_at null: updated_at":      "created_at",
}

// For every declared field of every internal ingest entity, a null that the
// ingest API admits reaches every column the Python persist path writes with
// the persist path's value, before the contract's carry applies.
func TestInternalStatementVariantsMatchThePythonReference(t *testing.T) {
	variants := loadStatementVariants(t)
	now, err := time.Parse(time.RFC3339Nano, variants.FixedNow)
	if err != nil {
		t.Fatal(err)
	}
	unreferenced := map[string]bool{}
	var undefined []string
	for _, variant := range variants.Internal {
		if variant.Rejected {
			continue
		}
		name := variant.Entity + " " + variant.Field + " " + variant.Statement
		raw, _ := json.Marshal(map[string]any{"org_id": variants.OrgID, "repo_url": variants.InternalRepoURL, "items": []any{variant.Item}})
		batch := &productBatch{}
		handler, _ := NewInternalIngestHandler(&productSink{batch: batch})
		handler.now = func() time.Time { return now }
		if err := handler.Handle(context.Background(), streamrunner.Message{
			Stream: "ingest:" + variants.OrgID + ":" + variant.Entity, Fields: map[string]string{"payload": string(raw)},
		}); err != nil {
			t.Errorf("%s: the ingest API admits the item, Go refuses it: %v", name, err)
			continue
		}
		insert := map[string]string{"commits": internalCommitInsert, "pull-requests": internalPullRequestInsert, "deployments": internalDeploymentInsert, "work-items": internalWorkItemInsert}[variant.Entity]
		columns := strings.Split(insert[strings.IndexByte(insert, '(')+1:strings.LastIndexByte(insert, ')')], ",")
		reference := map[string]any{}
		for i, column := range variant.Columns {
			reference[column] = referenceValue(variant.Values[i])
		}
		row := batch.rows[0]
		for i, column := range columns {
			want, ok := reference[column]
			if !ok {
				unreferenced[variant.Table+"."+column] = true
				continue
			}
			if want == nil && row[i] != nil {
				cell := name + ": " + column
				undefined = append(undefined, cell)
				source := internalUndefinedReferences[cell]
				for j, other := range columns {
					if other == source && !reflect.DeepEqual(goldenComparableValue(row[i]), goldenComparableValue(row[j])) {
						t.Errorf("%s = %#v, want the writer's %s %#v", cell, row[i], source, row[j])
					}
				}
				continue
			}
			if got := goldenComparableValue(row[i]); !reflect.DeepEqual(got, want) {
				t.Errorf("%s: %s = %#v, reference %#v", name, column, got, want)
			}
		}
	}
	var got []string
	for column := range unreferenced {
		got = append(got, column)
	}
	sort.Strings(got)
	var want []string
	for table, columns := range internalUnreferencedColumns {
		for _, column := range columns {
			want = append(want, table+"."+column)
		}
	}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("columns without a reference value = %v, want %v", got, want)
	}
	sort.Strings(undefined)
	var pinned []string
	for cell := range internalUndefinedReferences {
		pinned = append(pinned, cell)
	}
	sort.Strings(pinned)
	if !reflect.DeepEqual(undefined, pinned) {
		t.Errorf("cells without a defined reference value = %v, want %v", undefined, pinned)
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
