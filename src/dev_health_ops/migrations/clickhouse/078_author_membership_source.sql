-- CHAOS-4244: give a PR/MR author its OWN precedence rank, below linked_issue
-- (rank 5) and above manual_fallback (rank 7) -- a person-shaped signal must
-- never beat a real linked-issue donor's team. Previously the reporter
-- candidate was stamped `assignee_membership` (rank 4); this migration only
-- widens the enum (additive, matching CS1's own migrate-before-emit rule,
-- team-attribution.md s0) so the resolver (a following PR) can emit the new
-- value without an insert failure. Storage codes are insertion order, NOT
-- precedence -- precedence lives in compute_work_items._SOURCE_ORDER /
-- github_work_items_derivation_context.go's `order` slice / this repo's
-- _SOURCE_RANK_SQL, all updated alongside this migration.
ALTER TABLE work_item_team_attributions
    MODIFY COLUMN source Enum8('native_team' = 1, 'linked_issue' = 2, 'project_ownership' = 3, 'repo_ownership' = 4, 'assignee_membership' = 5, 'unassigned' = 6, 'issue_project' = 7, 'manual_fallback' = 8, 'author_membership' = 9);
