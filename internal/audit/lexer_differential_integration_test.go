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

	var checked, skipped, multi, boundary, dollarIdents int
	skipReasons := map[string]int{}
	var skipExamples []string
	var firstDisagreement string

	for i := 0; i < 3000; i++ {
		stmt, dollarIdent := generateStatement(rng)
		if dollarIdent {
			dollarIdents++
		}

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
				if len(skipExamples) < 6 {
					skipExamples = append(skipExamples,
						fmt.Sprintf("%s %s | %q", pgErr.Code, pgErr.Message, stmt))
				}
				continue
			}
		}
		checked++
		if serverRefuses {
			multi++
		} else if strings.Contains(stmt, ";") {
			// A single statement that CONTAINS a semicolon is where this lexer
			// can actually be wrong. A case count says nothing about how many
			// of those there were: a corpus of 3000 obviously-multi statements
			// and 3000 semicolon-free ones would report the same 3000 and
			// exercise none of the boundary.
			boundary++
		}
		if lexerRefuses != serverRefuses && firstDisagreement == "" {
			firstDisagreement = fmt.Sprintf(
				"lexer=%v server=%v for:\n%s", lexerRefuses, serverRefuses, stmt)
		}
	}

	t.Logf("checked %d, skipped %d (invalid SQL, by SQLSTATE: %v), server called %d multi-statement, %d single-with-semicolon",
		checked, skipped, skipReasons, multi, boundary)
	t.Logf("of those, %d carried a $-bearing identifier", dollarIdents)
	for _, ex := range skipExamples {
		t.Logf("SKIPPED: %s", ex)
	}
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
	// The population that matters. Without this the run can drift into all-easy
	// cases and still report three thousand.
	if boundary < 100 {
		t.Errorf("only %d single statements contained a semicolon; the corpus is not exercising the boundary, whatever its size", boundary)
	}
	// EVERY generated statement must reach the oracle. A skip is not neutral:
	// it is a case that was generated, looked plausible, and produced no
	// verdict at all. The examples logged above name the shapes when this
	// fires, because the useful question is always WHICH generator row is
	// emitting invalid SQL rather than how many did.
	if skipped != 0 {
		t.Errorf("%d generated statements never reached the oracle (%v); the corpus is smaller than its case count", skipped, skipReasons)
	}

	// The class contracts asked for must actually be present, or a green run
	// says nothing about it.
	if dollarIdents < 100 {
		t.Errorf("only %d statements carried a $-bearing identifier; that axis is not being exercised", dollarIdents)
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
func generateStatement(rng *rand.Rand) (string, bool) {
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

		// CONTAINERS WITHIN CONTAINERS. Everything above is ONE container with
		// a semicolon in it. lane-auth-contracts pointed out that a lexer can
		// track a single container correctly and still lose the depth, and that
		// every shape here read as a flat set. These nest two deep.
		`$$ "a;b" $$`,
		`$$/* ; */$$`,
		`$$E'a\;'$$`,
		`$tag$ $$ ; $$ $tag$`,
		`/* $$;$$ */ 'x'`,
		`/* 'a;b' */ 'x'`,
		`/* a /* 'x;' */ b */ 'y'`,
		`E'$$;$$'`,
		`'$$' || ';' || '$$'`,
	}
	aliases := []string{
		``,
		` AS "x;y"`,
		` AS "a""b;c"`,
		` AS "ünïcode;id"`,
		` AS "$$;$$"`,
		` AS "/* ; */"`,

		// $ IS A LEGAL IDENTIFIER CONTINUATION CHARACTER. a$$ is the identifier
		// a-dollar-dollar, not an alias followed by a dollar-quote opener.
		// lane-auth-contracts found this axis missing entirely: every alias
		// above is either bare or double-quoted, so a $ never appeared where it
		// could be mistaken for a container opener.
		` AS a$$`,
		` AS ab$$`,
		` AS a$1`,
		` AS a$b`,
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

		// Tails ending in $$ can close a body that should never have been
		// opened. Harmless after a normal alias; the whole point after a
		// $-bearing one.
		`; COMMIT; --$$`,
		`; COMMIT; /*$$*/`,
		// A dollar-quote in the tail must be a COMPLETE one. `; COMMIT;$$`
		// was here first and left an unterminated body, so PostgreSQL rejected
		// it as malformed before it could ever rule on how many commands it
		// was: 282 of 3000 cases, a tenth of the corpus, producing no verdict
		// while the run still reported PASS.
		`; SELECT $$x$$`,
		`; COMMIT; SELECT $$;$$`,
	}
	alias := aliases[rng.Intn(len(aliases))]
	return fmt.Sprintf("SELECT %s, $1::int%s%s",
		payloads[rng.Intn(len(payloads))],
		alias,
		tails[rng.Intn(len(tails))]), strings.Contains(alias, "$")
}
