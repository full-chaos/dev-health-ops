package audit

import "testing"

// TestZeroRowsNeverPanics pins the claim the comment makes, for EVERY method.
// The previous version guarded three and claimed eight; nothing failed, because
// no test called the other five.
func TestZeroRowsNeverPanics(t *testing.T) {
	var zero Rows
	for _, c := range []struct {
		name string
		call func()
	}{
		{"Close", func() { zero.Close() }},
		{"Err", func() { _ = zero.Err() }},
		{"Next", func() { _ = zero.Next() }},
		{"CommandTag", func() { _ = zero.CommandTag() }},
		{"FieldDescriptions", func() { _ = zero.FieldDescriptions() }},
		{"Values", func() { _, _ = zero.Values() }},
		{"RawValues", func() { _ = zero.RawValues() }},
		{"Scan", func() { _ = zero.Scan() }},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("zero Rows.%s panicked: %v", c.name, r)
				}
			}()
			c.call()
		})
	}
}
