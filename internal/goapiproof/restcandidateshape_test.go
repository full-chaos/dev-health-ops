package goapiproof

import "testing"

func TestAssertRESTCandidateShape_Cells(t *testing.T) {
	for _, cell := range []struct {
		name        string
		data        any
		arrayShaped bool
		wantErr     bool
	}{
		{"object-shaped, non-empty: live", map[string]any{"a": 1}, false, false},
		{"object-shaped, empty: not live", map[string]any{}, false, true},
		{"object-shaped, null root: not live", nil, false, true},
		{"object-shaped, array root: wrong kind", []any{1}, false, true},
		{"object-shaped, scalar root: wrong kind", "x", false, true},
		{"array-shaped, non-empty: live", []any{"a"}, true, false},
		{"array-shaped, empty: live -- a scope with zero rows is still a real answer", []any{}, true, false},
		{"array-shaped, null root: not live", nil, true, true},
		{"array-shaped, object root: wrong kind", map[string]any{"a": 1}, true, true},
	} {
		t.Run(cell.name, func(t *testing.T) {
			err := AssertRESTCandidateShape(cell.data, cell.arrayShaped)
			if (err != nil) != cell.wantErr {
				t.Fatalf("AssertRESTCandidateShape(%#v, %v) = %v, want err=%v", cell.data, cell.arrayShaped, err, cell.wantErr)
			}
		})
	}
}
