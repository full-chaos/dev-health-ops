//go:build integration

package audit

import (
	"context"
	"errors"
	"fmt"
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

	// ENUMERATED, NOT SAMPLED. This used to draw 3000 statements at random,
	// WITH REPLACEMENT, from a grammar whose cross-product is 4455. Measured
	// over 200 simulated runs, that covered 2180 of 4455 combinations -- 48.9%
	// -- so slightly over HALF the grammar went untested in any given run, and
	// WHICH half depended on the seed. Every population figure this test
	// reported before now described a random ~49% sample of a space small
	// enough to cover exactly.
	//
	// Enumeration needs no seed, is reproducible without one, and lets the class
	// counts be EXACT rather than floors. It is not free -- 4455 cases is 48%
	// MORE work than the 3000 draws it replaces -- but the run is a few seconds
	// and the alternative is a coverage figure nobody can state.
	//
	// Where an axis is enumerable, enumerate it: lane-auth-contracts' rule.
	//
	// A correction worth keeping, because it is the same defect twice in one
	// night: I first reported this space as 3267 and the loss as 40%. Both were
	// wrong. I counted the alias list with a script that matched lines ending in
	// a comma, and the four non-ASCII aliases carry trailing comments, so they
	// were silently dropped -- 11 counted where there were 15. I read a number
	// off a tool without checking what the tool counts, which is exactly how I
	// had just miscounted a diff by including its `+++` header. The figures
	// above come from the enumeration itself, which reports its own dimensions.
	payloads, aliases, tails := grammar()
	total := len(payloads) * len(aliases) * len(tails)
	t.Logf("enumerating the whole grammar: %d payloads x %d aliases x %d tails = %d statements",
		len(payloads), len(aliases), len(tails), total)

	var checked, skipped, multi, boundary, dollarIdents, bareNonASCII int
	skipReasons := map[string]int{}
	var skipExamples []string
	var firstDisagreement string
	generated := 0

	for _, payload := range payloads {
		for _, alias := range aliases {
			for _, tail := range tails {
				g := buildStatement(payload, alias, tail)
				stmt := g.sql
				generated++

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
							generated, execErr, stmt)
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
				// COUNTED HERE, NOT AT GENERATION. lane-auth-contracts found these two
				// increments sitting above the skip `continue`, which made
				// "bareNonASCII >= 100" mean 100 were EMITTED, not 100 were COMPARED
				// against PostgreSQL. Those coincide only while skipped is zero, so the
				// class assertions were silently borrowing their validity from the skip
				// assertion -- the 282-skip failure one layer in, and the axis it would
				// have hidden is the one just added. Below the continue, a class count
				// means the oracle answered for that many.
				if g.dollarIdent {
					dollarIdents++
				}
				if g.bareNonASCII {
					bareNonASCII++
				}
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
		}
	}

	t.Logf("checked %d, skipped %d (invalid SQL, by SQLSTATE: %v), server called %d multi-statement, %d single-with-semicolon",
		checked, skipped, skipReasons, multi, boundary)
	t.Logf("of those, %d carried a $-bearing identifier, %d a BARE non-ASCII identifier",
		dollarIdents, bareNonASCII)
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
	// EXHAUSTIVENESS, which is what enumeration buys and sampling could not.
	// Not "at least N of a class appeared" but "every combination in the
	// grammar was built AND the oracle answered for it".
	if generated != total {
		t.Errorf("built %d statements, expected the full cross-product of %d", generated, total)
	}
	if checked != total {
		t.Errorf("only %d of %d enumerated statements reached the oracle", checked, total)
	}

	// EXACT class counts, derived from the grammar rather than asserted as
	// floors. Because the counters now sit below the skip, an exact match also
	// proves every member of the class was COMPARED, not merely emitted.
	expectDollar, expectBareNonASCII := 0, 0
	for _, a := range aliases {
		if strings.Contains(a, "$") {
			expectDollar++
		}
		if !strings.Contains(a, `"`) && hasNonASCII(a) {
			expectBareNonASCII++
		}
	}
	per := len(payloads) * len(tails)
	if want := expectDollar * per; dollarIdents != want {
		t.Errorf("%d statements carried a $-bearing identifier, expected exactly %d", dollarIdents, want)
	}
	if want := expectBareNonASCII * per; bareNonASCII != want {
		t.Errorf("%d statements carried a bare non-ASCII identifier, expected exactly %d; the "+
			">=0x80 branches in identChar and continuesIdentifier are not being exercised as expected",
			bareNonASCII, want)
	}

	// The population that matters. Without this the run can drift into all-easy
	// cases and still report the full count.
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

}

// generateStatement builds `SELECT <payload>, $1::int [alias] [; COMMIT] [;]`.
//
// The payload fragments are the shapes every past defect lived in. $1 is always
// present because the extended protocol only refuses a multi-command string
// when the query actually carries a bind argument -- the parameterless path
// never builds a prepared statement, which is the measurement that moved this
// rule out of the protocol and into the lexer.
func grammar() (payloads, aliases, tails []string) {
	payloads = []string{
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
	aliases = []string{
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

		// NON-ASCII BARE IDENTIFIERS. PostgreSQL's lexer is byte-based: any
		// byte >= 0x80 is an identifier character, start or continuation,
		// regardless of Unicode class. txcontrol.go relies on that in two
		// places -- identChar's `b >= 0x80` and continuesIdentifier's
		// `first >= 0x80` -- and until now the generator emitted no non-ASCII
		// identifier byte at all: the only non-ASCII alias was DOUBLE-QUOTED,
		// so the bare path never saw one and neither branch was ever exercised
		// by the oracle. Correct by construction, unverified against the
		// runtime, invisible to this harness.
		//
		// Written as \u escapes, not literals. lane-auth-contracts lost a
		// U+212A in transit and the row silently tested ASCII twice, printing a
		// clean agree; escapes survive a trip that literals do not.
		" AS \u00fc$$",          // non-ASCII FIRST byte, then the $$ shape
		" AS \u00fc$b",          // non-ASCII first byte, single $
		" AS \u00fcn\u00efcode", // non-ASCII, no $ at all
		" AS a\u00fc$$",         // ASCII start, non-ASCII CONTINUATION byte
	}
	tails = []string{
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
	return payloads, aliases, tails
}

// buildStatement assembles one point of the cross-product.
func buildStatement(payload, alias, tail string) generated {
	return generated{
		sql:          fmt.Sprintf("SELECT %s, $1::int%s%s", payload, alias, tail),
		dollarIdent:  strings.Contains(alias, "$"),
		bareNonASCII: !strings.Contains(alias, `"`) && hasNonASCII(alias),
	}
}

type generated struct {
	sql          string
	dollarIdent  bool
	bareNonASCII bool
}

func hasNonASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return true
		}
	}
	return false
}
