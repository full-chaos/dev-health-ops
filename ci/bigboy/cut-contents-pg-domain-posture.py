"""cut-contents.sh's PG domain posture delta computation, extracted per CHAOS-3362/CHAOS-6964 (see
cut-contents-pg-api-posture.py's own doc comment for the full heredoc-pipe-budget rationale).

Usage: cut-contents-pg-domain-posture.py <old-sha> <new-sha> <pg-expect-out-file>
Same struct shapes as the api posture (RequiredTables {table, insert, update, delete}, SELECT
implied, and ColumnScoped {table, column, privilege}): expectations for devhealth_domain, computed
by the shared pg_posture_expect.py (CHAOS-10113).
"""

from pg_posture_expect import delta

delta(
    "domain",
    "domain_authorization.go",
    "domainPosture",
    "PG_DOMAIN_POSTURE_DELTA unparsable",
)
