package pythonparity

import (
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
)

type jsonLoadsRow struct {
	In            string          `json:"in"`
	Out           json.RawMessage `json:"out"`
	Err           string          `json:"err"`
	Msg           string          `json:"msg"`
	Pos           int             `json:"pos"`
	Lineno        int             `json:"lineno"`
	Colno         int             `json:"colno"`
	Str           string          `json:"str"`
	LoneSurrogate bool            `json:"lone_surrogate"`
	TooDeep       bool            `json:"too_deep"`
}

type jsonTagged struct {
	T string          `json:"t"`
	V json.RawMessage `json:"v"`
}

// jsonLoadsTag renders a LoadsJSON value in the golden's type-tagged form.
func jsonLoadsTag(t *testing.T, v any) any {
	t.Helper()
	switch x := v.(type) {
	case nil:
		return map[string]any{"t": "null"}
	case bool:
		return map[string]any{"t": "bool", "v": x}
	case *big.Int:
		return map[string]any{"t": "int", "v": x.String()}
	case float64:
		return map[string]any{"t": "float", "v": Repr(x)}
	case string:
		return map[string]any{"t": "str", "v": x}
	case []any:
		out := []any{}
		for _, e := range x {
			out = append(out, jsonLoadsTag(t, e))
		}
		return map[string]any{"t": "list", "v": out}
	case OrderedObject:
		out := []any{}
		for _, m := range x {
			out = append(out, []any{m.Key, jsonLoadsTag(t, m.Value)})
		}
		return map[string]any{"t": "dict", "v": out}
	}
	t.Fatalf("unexpected value type %T", v)
	return nil
}

func TestLoadsJSONMatchesRecordedPython(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "jsonloads.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden struct{ Rows []jsonLoadsRow }
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if len(golden.Rows) < 300 {
		t.Fatalf("only %d golden rows", len(golden.Rows))
	}
	for i, row := range golden.Rows {
		got, err := LoadsJSON(row.In)
		switch {
		case row.TooDeep:
			if !errors.Is(err, ErrJSONTooDeep) {
				t.Errorf("row %d %.40q: want ErrJSONTooDeep, got %v", i, row.In, err)
			}
		case row.LoneSurrogate:
			if !errors.Is(err, ErrLoneSurrogate) {
				t.Errorf("row %d %q: want ErrLoneSurrogate, got %v", i, row.In, err)
			}
		case row.Err == "JSONDecodeError":
			var de *JSONDecodeError
			if !errors.As(err, &de) || *de != (JSONDecodeError{Msg: row.Msg, Pos: row.Pos, Lineno: row.Lineno, Colno: row.Colno}) || de.Error() != row.Str {
				t.Errorf("row %d %q: want JSONDecodeError %q, got %v", i, row.In, row.Str, err)
			}
		case row.Err == "ValueError":
			var ve *PyValueError
			if !errors.As(err, &ve) || ve.Msg != row.Msg {
				t.Errorf("row %d %.40q: want ValueError %q, got %v", i, row.In, row.Msg, err)
			}
		case row.Err != "":
			t.Fatalf("row %d: unknown recorded error %q", i, row.Err)
		default:
			if err != nil {
				t.Errorf("row %d %q: Python accepted, Go refused: %v", i, row.In, err)
				continue
			}
			want, gotJSON := map[string]any{}, map[string]any{}
			if err := json.Unmarshal(row.Out, &want); err != nil {
				t.Fatal(err)
			}
			enc, _ := json.Marshal(jsonLoadsTag(t, got))
			if err := json.Unmarshal(enc, &gotJSON); err != nil {
				t.Fatal(err)
			}
			if a, b := mustCanon(t, want), mustCanon(t, gotJSON); a != b {
				t.Errorf("row %d %q:\n got %s\nwant %s", i, row.In, b, a)
			}
		}
	}
}

func mustCanon(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
