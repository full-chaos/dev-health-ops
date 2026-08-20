# Dead code cleanup plan (dev-health-ops)

Goal
- Remove dead code, unused exports, and deprecated utilities with minimal risk and validated via tests.

Scope
- Target: Python backend repo (dev-health-ops) only for Phase 5 cleanup sweep.
- Focus areas:
  - Dead or deprecated SQLite utilities and adapters remaining in code path
  - Functions/classes defined but never imported or used
  - Ensure no regression in test suites and API surfaces

Approach
- Step 1: Inventory
  - Use static analysis to list all function definitions and exported symbols across the codebase.
  - Build a candidate list of symbols not found in any import/use sites within the repo.
- Step 2: Validation
  - For each candidate, Grep for references across the repo to verify usage.
  - If no references found (except tests that may mock), mark as candidate for removal.
- Step 3: Patch & Verify
  - Remove or deprecate candidate code in small, isolated patches.
  - Run unit tests and storage tests; fix breakages.
- Step 4: CI parity
  - Document changes and ensure CI script (ci/run_tests.sh) exercises affected areas.
- Step 5: Commit discipline
  - Create one commit per logical change with a clear rationale in the message.

Risks & Mitigations
- Risk: Removing code that is imported indirectly or used in hidden paths (e.g., via dynamic imports).
  - Mitigation: Rely on static analysis first; manual review for any suspicious symbols; run tests after each patch.
- Risk: Tests rely on deprecated utilities.
  - Mitigation: Update tests if necessary or mark as test-coverage-waiver with justification in PR body.

Acceptance Criteria
- No dead code remains (as per the plan), tests pass, and frontend/backends continue to build.
