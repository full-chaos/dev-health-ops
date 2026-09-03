package audit

import (
	"fmt"
	"strings"
)

// transactionControl are the statements that end, start or reshape the
// transaction this package owns. A mutation that issues one has taken the
// helper's job away from it, and the outbox event and audit row can no longer
// be atomic with the state the mutation wrote.
//
// PREPARE (two-phase commit) and DISCARD are here for the same reason as the
// obvious ones: PREPARE ALL hands the transaction to an external coordinator,
// and DISCARD ALL resets session state including an open transaction.
var transactionControl = map[string]struct{}{
	"COMMIT": {}, "ROLLBACK": {}, "END": {}, "ABORT": {},
	"BEGIN": {}, "START": {}, "SAVEPOINT": {}, "RELEASE": {},
	"PREPARE": {}, "DISCARD": {},
}

// refuseTransactionControl rejects a statement whose FIRST token is transaction
// control, before it is sent.
//
// WHY THE FIRST TOKEN IS THE WHOLE SURFACE. PostgreSQL cannot commit inside a
// function or a DO block, and CALL of a procedure that commits errors when it
// runs inside an explicit transaction -- which this always is. So transaction
// control reaches the server only as a statement in its own right, and the
// first token identifies it. That is a property of the server, not an
// assumption about callers.
//
// WHAT IT DOES NOT SEE, which is why Commit still checks the connection's
// status afterwards: anything that ends a transaction without saying so in its
// first token. The lexer is the cheap early refusal; the status check is the
// authority.
func refuseTransactionControl(sql string) error {
	token := firstToken(sql)
	if _, controls := transactionControl[token]; !controls {
		return nil
	}
	return fmt.Errorf(
		"%w: the mutation issued %s, which ends or reshapes the transaction this "+
			"helper owns; the outbox event and audit row could not then commit with "+
			"the state it wrote", ErrMutationFailed, token)
}

// firstToken returns the first SQL keyword, upper-cased, skipping leading
// whitespace, -- line comments and /* */ block comments.
//
// Block comments NEST in PostgreSQL, so the depth counter is not decoration: a
// scanner that stopped at the first `*/` would resume inside a comment and read
// a token that is not a token.
func firstToken(sql string) string {
	i := 0
	for i < len(sql) {
		switch {
		case sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r':
			i++
		case strings.HasPrefix(sql[i:], "--"):
			end := strings.IndexByte(sql[i:], '\n')
			if end < 0 {
				return ""
			}
			i += end + 1
		case strings.HasPrefix(sql[i:], "/*"):
			depth, j := 1, i+2
			for j < len(sql) && depth > 0 {
				switch {
				case strings.HasPrefix(sql[j:], "/*"):
					depth, j = depth+1, j+2
				case strings.HasPrefix(sql[j:], "*/"):
					depth, j = depth-1, j+2
				default:
					j++
				}
			}
			if depth != 0 {
				return ""
			}
			i = j
		default:
			start := i
			for i < len(sql) && (sql[i] == '_' ||
				(sql[i] >= 'a' && sql[i] <= 'z') ||
				(sql[i] >= 'A' && sql[i] <= 'Z')) {
				i++
			}
			return strings.ToUpper(sql[start:i])
		}
	}
	return ""
}
