package health

import "github.com/jackc/pgx/v5"

// schemaSatisfiedByRows reads every alembic_version row and reports whether
// one of them proves the schema, with the revisions it read for the log. A
// scan or iteration error is never a pass.
func schemaSatisfiedByRows(rows pgx.Rows) (bool, []string) {
	var recorded []string
	for rows.Next() {
		var revision string
		if err := rows.Scan(&revision); err != nil {
			return false, recorded
		}
		recorded = append(recorded, revision)
	}
	return rows.Err() == nil && anySchemaRevisionSatisfies(recorded), recorded
}

// schemaRevisionSatisfies reports whether an alembic_version row proves the
// schema the api needs. A revision in the generated chain does. A revision
// the chain does not list (a rollback left the database ahead of this build)
// does when it is exactly four ASCII digits and numbered above the minimum;
// anything else, including an unparseable value, does not.
func schemaRevisionSatisfies(revision string) bool {
	if satisfyingRevisions[revision] {
		return true
	}
	number, ok := revisionNumber(revision)
	minimum, _ := revisionNumber(minimumSchemaRevision)
	return ok && number > minimum
}

// anySchemaRevisionSatisfies is true when at least one recorded row proves the
// schema: a database that carries the cutover branch head beside a satisfying
// head passes.
func anySchemaRevisionSatisfies(recorded []string) bool {
	for _, revision := range recorded {
		if schemaRevisionSatisfies(revision) {
			return true
		}
	}
	return false
}

// revisionNumber parses a revision of exactly four ASCII digits: the shape of
// every revision in the Alembic chain, where the number is the chain order.
func revisionNumber(revision string) (int, bool) {
	if len(revision) != 4 {
		return 0, false
	}
	number := 0
	for index := 0; index < len(revision); index++ {
		digit := revision[index]
		if digit < '0' || digit > '9' {
			return 0, false
		}
		number = number*10 + int(digit-'0')
	}
	return number, true
}
