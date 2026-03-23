EXPORT SCHEMA USAGE AUDIT (dev-health-ops)

Context
- The api/graphql/export_schema.py tool is used to export GraphQL schema in CI for drift checks.
- The --out option writes to a file for comparison; stdout is used for local inspection.

Findings
- The --out export path is CI-oriented and stable; no code in this area requires removal.
- SQLite adapters and their wiring were deprecated in Phase 2; no active utilities left behind that require cleanup beyond what was already removed.

Recommendations
- Keep the --out behavior documented and ensure CI scripts rely on it for drift checks.
- Maintain a lightweight scan for leftover dead code; if future cleanup reveals an orphaned symbol, apply patch with tests to verify.
