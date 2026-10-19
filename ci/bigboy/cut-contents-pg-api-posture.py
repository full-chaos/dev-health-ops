"""cut-contents.sh's PG api posture delta computation, extracted per CHAOS-3362/CHAOS-6964: this
body is over the 400-byte here-document pipe budget (bash writes a here-document into a pipe and
only then forks the reader; a document that does not fit the pipe buffer hangs the writer forever --
see tests/tooling/test_local_validate_heredocs.py's own doc comment for the full forensics). Read
from disk instead of piped.

Usage: cut-contents-pg-api-posture.py <old-sha> <new-sha> <pg-expect-out-file>
Computed from the manifest itself (RequiredTables {table, insert, update, delete}, SELECT implied, and
ColumnScoped {table, column, privilege}): every privilege ADDED between old and new for the api role
becomes an expectation `devhealth_api <table> <PRIV>` (has_table_privilege read-back) or
`devhealth_api <table>.<column> <PRIV>` (has_column_privilege read-back). Replaces by-name item
matching for PG api grants. An added-privileges read-back, not an exactness check: see
pg_posture_expect.py (CHAOS-10113).
"""

from pg_posture_expect import delta

delta(
    "api",
    "api_authorization.go",
    "apiPosture",
    "PG_POSTURE_DELTA unparsable (manifest missing at a sha)",
)
