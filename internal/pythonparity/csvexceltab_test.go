package pythonparity_test

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// csvGoldenString is a golden string: plain, or {"runs": [[char, count], ...]} for a long one.
type csvGoldenString string

func (s *csvGoldenString) UnmarshalJSON(raw []byte) error {
	var plain string
	if err := json.Unmarshal(raw, &plain); err == nil {
		*s = csvGoldenString(plain)
		return nil
	}
	var packed struct {
		Runs [][2]json.RawMessage `json:"runs"`
	}
	if err := json.Unmarshal(raw, &packed); err != nil {
		return err
	}
	var b strings.Builder
	for _, run := range packed.Runs {
		var ch string
		var n int
		if err := json.Unmarshal(run[0], &ch); err != nil {
			return err
		}
		if err := json.Unmarshal(run[1], &n); err != nil {
			return err
		}
		b.WriteString(strings.Repeat(ch, n))
	}
	*s = csvGoldenString(b.String())
	return nil
}

type csvGolden struct {
	Python string `json:"python"`
	Reader []struct {
		In    csvGoldenString     `json:"in"`
		Rows  [][]csvGoldenString `json:"rows"`
		Error *string             `json:"error"`
	} `json:"reader"`
	Writer []struct {
		Fields []string `json:"fields"`
		Out    string   `json:"out"`
		Error  *string  `json:"error"`
	} `json:"writer"`
}

func loadCSVGolden(t *testing.T) csvGolden {
	t.Helper()
	raw, err := os.ReadFile("testdata/csvexceltab.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var g csvGolden
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(g.Python, "3.12.3 ") || len(g.Reader) < 300 || len(g.Writer) < 100 {
		t.Fatalf("golden is not the 3.12.3 recording: python %q, %d reader rows, %d writer rows", g.Python, len(g.Reader), len(g.Writer))
	}
	return g
}

func TestReadCSVTabMatchesTheRecordedPython(t *testing.T) {
	for i, row := range loadCSVGolden(t).Reader {
		got, err := pythonparity.ReadCSVTab(string(row.In))
		if row.Error != nil {
			if err == nil || err.Error() != *row.Error {
				t.Errorf("row %d %q: error %v, Python %q", i, csvShort(string(row.In)), err, *row.Error)
			}
			continue
		}
		if err != nil {
			t.Errorf("row %d %q: error %v, Python accepted", i, csvShort(string(row.In)), err)
			continue
		}
		want := make([][]string, len(row.Rows))
		for j, r := range row.Rows {
			want[j] = make([]string, len(r))
			for k, f := range r {
				want[j][k] = string(f)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("row %d %q: got %q, Python %q", i, csvShort(string(row.In)), got, want)
		}
	}
}

func TestWriteCSVTabRowMatchesTheRecordedPython(t *testing.T) {
	for i, row := range loadCSVGolden(t).Writer {
		got, err := pythonparity.WriteCSVTabRow(row.Fields)
		if row.Error != nil {
			if err == nil || err.Error() != *row.Error {
				t.Errorf("row %d %q: error %v, Python %q", i, row.Fields, err, *row.Error)
			}
			continue
		}
		if err != nil || got != row.Out {
			t.Errorf("row %d %q: got %q (%v), Python %q", i, row.Fields, got, err, row.Out)
		}
	}
}

func csvShort(s string) string {
	if len(s) > 80 {
		return s[:80] + "..."
	}
	return s
}
