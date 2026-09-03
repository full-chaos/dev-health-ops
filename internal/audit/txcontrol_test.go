package audit

import "testing"

// TestTransactionControlIsRefusedBeforeItIsSent covers the lexer directly,
// because the integration proof needs a container and this is the layer where
// the boundary actually lives.
//
// The refusing rows are the point; the ACCEPTING rows are what stop the fix
// from being "refuse everything", which would pass every refusal test and make
// the helper unable to write anything at all.
func TestTransactionControlIsRefusedBeforeItIsSent(t *testing.T) {
	for _, c := range []struct {
		name, sql string
		refuse    bool
	}{
		{"bare COMMIT", `COMMIT`, true},
		{"lowercase with semicolon", `commit;`, true},
		{"leading block comment", `/*x*/ COMMIT`, true},
		{"leading newline and END", "\n END", true},
		{"ROLLBACK", `ROLLBACK`, true},
		{"ABORT", `  abort  `, true},
		{"BEGIN", `BEGIN`, true},
		{"START TRANSACTION", `START TRANSACTION`, true},
		{"SAVEPOINT", `SAVEPOINT s1`, true},
		{"RELEASE", `RELEASE s1`, true},
		{"PREPARE TRANSACTION", `PREPARE TRANSACTION 'x'`, true},
		{"DISCARD ALL", `DISCARD ALL`, true},
		{"line comment then COMMIT", "-- harmless\nCOMMIT", true},
		{"nested block comments then COMMIT", `/* a /* b */ c */ COMMIT`, true},

		// Accepting rows. A mutation must still be able to do its job.
		{"plain INSERT", `INSERT INTO auth.organizations (id) VALUES (1)`, false},
		{"SELECT", `SELECT 1`, false},
		{"UPDATE", `UPDATE auth.organizations SET name = 'x'`, false},
		{"comment then INSERT", `/* audited */ INSERT INTO auth.organizations (id) VALUES (1)`, false},
		{"a column named commit is not the first token", `SELECT commit FROM t`, false},
		{"COMMITTED as a longer word", `SELECT 'READ COMMITTED'`, false},
		{"empty", ``, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := refuseTransactionControl(c.sql)
			if c.refuse && err == nil {
				t.Errorf("%q was sent; it ends or reshapes the transaction", c.sql)
			}
			if !c.refuse && err != nil {
				t.Errorf("%q was refused, so a mutation cannot do its work: %v", c.sql, err)
			}
		})
	}
}
