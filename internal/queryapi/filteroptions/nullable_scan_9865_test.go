package filteroptions

import (
	"context"
	"reflect"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type nullScannerClient struct {
	values   []string
	nullRows map[int]bool
	pairs    [][2]string
	nullKey  map[int]bool
	nullName map[int]bool
}

func (c nullScannerClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if c.pairs != nil {
		return &namePairScanner{rows: c.pairs, nullKey: c.nullKey, nullName: c.nullName}, nil
	}
	return &fixtureRowScanner{rows: c.values, nullRows: c.nullRows}, nil
}

// CHAOS-9865: a filter value is typed Nullable(String) by the server. A NULL is no data: it is not an
// option, and it does not end the scan.
func TestDistinctValuesSkipsANull(t *testing.T) {
	client := nullScannerClient{values: []string{"a", "", "c"}, nullRows: map[int]bool{2: true}}
	got, err := distinctValues(context.Background(), client, "q", "teams", nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
}

// CHAOS-9865: developer id and display name are Nullable(String). A NULL on either side gives no name entry.
func TestNamePairsSkipsANullKeyOrName(t *testing.T) {
	client := nullScannerClient{
		pairs:    [][2]string{{"a@x", "Ann"}, {"b@x", "Bob"}, {"c@x", "Cy"}},
		nullKey:  map[int]bool{1: true},
		nullName: map[int]bool{2: true},
	}
	got, err := namePairs(context.Background(), client, "q", "developers", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]string{"c@x": "Cy"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("names = %v, want %v", got, want)
	}
}
