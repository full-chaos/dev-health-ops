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

// TestMultipleStatementsAreRefused covers the semicolon rule at the layer where
// it lives.
//
// The ACCEPTING rows are the ones that matter here. A semicolon inside a
// literal or a dollar-quoted body is data, not a separator, and a rule that
// cannot tell the difference would refuse ordinary INSERTs whose values happen
// to contain "; COMMIT" -- which is exactly the kind of over-correction this
// package has shipped four times.
func TestMultipleStatementsAreRefused(t *testing.T) {
	for _, c := range []struct {
		name, sql string
		refuse    bool
	}{
		{"the measured attack: no bind args", `INSERT INTO auth.organizations (id) VALUES (gen_random_uuid()); COMMIT;`, true},
		{"block comment between", `INSERT INTO t (a) VALUES (1);/*x*/COMMIT`, true},
		{"line comment between", "INSERT INTO t (a) VALUES (1); -- c\nCOMMIT", true},
		{"three statements", `SELECT 1; SELECT 2; SELECT 3`, true},
		{"second statement is not control", `INSERT INTO t (a) VALUES (1); DROP TABLE t`, true},

		{"trailing semicolon is legal", `INSERT INTO t (a) VALUES (1);`, false},
		{"trailing semicolon with whitespace", "INSERT INTO t (a) VALUES (1);  \n ", false},
		{"trailing semicolon then a comment", `INSERT INTO t (a) VALUES (1); -- done`, false},
		{"semicolon inside a literal", `INSERT INTO t (a) VALUES ('; COMMIT;')`, false},
		{"escaped quote then semicolon in a literal", `INSERT INTO t (a) VALUES ('it''s; COMMIT')`, false},
		// E'a\\' is an escaped BACKSLASH, so the literal ends at that quote and
		// "; COMMIT;" really is a second statement. This row was written the
		// other way round first -- the test was wrong about SQL, not the code.
		{"E-string ending on an escaped backslash: the rest IS a second statement",
			`INSERT INTO t (a) VALUES (E'a\\'); COMMIT;`, true},
		// E'a\' is an escaped QUOTE, so the literal continues and swallows the
		// semicolon.
		{"E-string with an escaped quote keeps the semicolon inside",
			`INSERT INTO t (a) VALUES (E'a\'; COMMIT;')`, false},
		{"dollar-quoted body", `INSERT INTO t (a) VALUES ($$; COMMIT;$$)`, false},
		{"tagged dollar-quoted body", `INSERT INTO t (a) VALUES ($tag$; COMMIT;$tag$)`, false},
		{"no semicolon at all", `INSERT INTO t (a) VALUES (1)`, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := refuseMultipleStatements(c.sql)
			if c.refuse && err == nil {
				t.Errorf("%q carried a second statement and was sent", c.sql)
			}
			if !c.refuse && err != nil {
				t.Errorf("%q is one statement and was refused: %v", c.sql, err)
			}
		})
	}
}
