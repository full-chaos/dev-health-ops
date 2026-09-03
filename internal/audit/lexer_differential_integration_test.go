//go:build integration

package audit

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestLexerAgreesWithPostgresOnMultiStatement stops choosing cases.
//
// Every previous defect in this lexer was found by someone constructing an
// input the author had not thought of -- a comment between INSERT and INTO, a
// semicolon inside a quoted identifier, an ordinary literal ending in a
// backslash, a keyword ending in E adjacent to a quote. A green suite, a clean
// container run and a completed review round all missed the last of those,
// because all three examined the cases I chose. A corpus assembled from the
// cases the author thought of cannot catch the case they did not.
//
// So the SERVER is the oracle. Each generated statement is sent with one bind
// argument through the extended protocol, where PostgreSQL refuses a
// multi-command string with 42601 "cannot insert multiple commands into a
// prepared statement". That is ground truth for the only property this lexer
// claims, and it is not a property I get to define.
//
// Statements the server rejects for ANY OTHER reason are skipped, not counted:
// a syntax error means the generator produced something invalid, and this lexer
// does not claim to validate SQL. The skip count is reported, because a run
// where most cases were skipped proves much less than its pass suggests.
func TestLexerAgreesWithPostgresOnMultiStatement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	conn, err := env.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()

	const seed = 20260903
	rng := rand.New(rand.NewSource(seed))
	t.Logf("seed %d — rerun with the same seed to reproduce any disagreement", seed)

	var checked, skipped, multi int
	skipReasons := map[string]int{}
	var firstDisagreement string

	for i := 0; i < 3000; i++ {
		stmt := generateStatement(rng)

		lexerRefuses := refuseMultipleStatements(stmt) != nil

		_, execErr := conn.Exec(ctx, stmt, pgx.QueryExecModeExec, 1)

		// Only a rejection BY THE SERVER is a verdict. A transport or context
		// failure is not a skip -- if the container dies, every case would
		// "skip" and the run would report a pass over an empty comparison.
		var serverRefuses bool
		if execErr != nil {
			var pgErr *pgconn.PgError
			if !errors.As(execErr, &pgErr) {
				t.Fatalf("case %d: not a server rejection, so the oracle is not answering: %v\nstatement:\n%s",
					i, execErr, stmt)
			}
			if pgErr.Code == "42601" && strings.Contains(pgErr.Message, "multiple commands") {
				serverRefuses = true
			} else {
				skipped++
				skipReasons[pgErr.Code]++
				continue
			}
		}
		checked++
		if serverRefuses {
			multi++
		}
		if lexerRefuses != serverRefuses && firstDisagreement == "" {
			firstDisagreement = fmt.Sprintf(
				"lexer=%v server=%v for:\n%s", lexerRefuses, serverRefuses, stmt)
		}
	}

	t.Logf("checked %d, skipped %d (invalid SQL, by SQLSTATE: %v), server called %d multi-statement",
		checked, skipped, skipReasons, multi)
	if firstDisagreement != "" {
		t.Fatalf("lexer and PostgreSQL disagree:\n%s", firstDisagreement)
	}
	// Both halves must be represented or the run proves one direction only.
	if multi == 0 {
		t.Error("no generated statement was multi-statement: the generator is not exercising the rule")
	}
	if checked-multi == 0 {
		t.Error("every checked statement was multi-statement: no accepting cases were exercised")
	}
	if checked < 500 {
		t.Errorf("only %d cases reached the oracle; too many were skipped to conclude anything", checked)
	}
}

// generateStatement builds `SELECT <payload>, $1::int [alias] [; COMMIT] [;]`.
//
// The payload fragments are the shapes every past defect lived in. $1 is always
// present because the extended protocol only refuses a multi-command string
// when the query actually carries a bind argument -- the parameterless path
// never builds a prepared statement, which is the measurement that moved this
// rule out of the protocol and into the lexer.
func generateStatement(rng *rand.Rand) string {
	payloads := []string{
		`'a'`,
		`'it''s'`,
		`'n\'`,
		`E'a\\'`,
		`e'a\\'`,
		`E'a\'; COMMIT;'`,
		`$$body;$$`,
		`$tag$a;$tag$`,
		`$$a$b;$$`,
		`'a' /* c */`,
		`/* a /* b */ c */ 'x'`,
		`'a' -- t
		`,
		`CASE WHEN true THEN 'a' ELSE'n\' END`,
		`CASE WHEN 'a' LIKE'a\' THEN 'y' ELSE 'n' END`,
		`CASE WHEN true THEN E'a\\' ELSE 'b' END`,
		`';'`,
		`';COMMIT;'`,
		`'a' || ';' || 'b'`,
	}
	aliases := []string{
		``,
		` AS "x;y"`,
		` AS "a""b;c"`,
		` AS "ünïcode;id"`,
		` AS plain`,
	}
	tails := []string{
		``,
		`;`,
		`; COMMIT`,
		`; COMMIT;`,
		`;/*x*/COMMIT`,
		`; -- c
		COMMIT`,
		`  ;   `,
	}
	return fmt.Sprintf("SELECT %s, $1::int%s%s",
		payloads[rng.Intn(len(payloads))],
		aliases[rng.Intn(len(aliases))],
		tails[rng.Intn(len(tails))])
}
