-- The reference keys of a work unit as their own columns (CHAOS-9539, CHAOS-10726).
--
-- An investment reach check reads the issue and pull request references of every row of an organization. Today it
-- parses them out of structural_evidence_json (the whole evidence text, edges included) with JSONExtract. These
-- two columns hold only the references:
--
--   issue_refs   JSONExtract(structural_evidence_json, 'issues', 'Array(String)')
--   pr_refs      JSONExtract(structural_evidence_json, 'prs', 'Array(String)')
--
-- the same expressions the readers use. They are DEFAULT expressions of the row's own evidence, so:
--   - the ALTER changes metadata only: no part is rewritten and no mutation is queued. A part written before this
--     migration has no data for the columns; a read computes them from the part's structural_evidence_json, so an
--     old row reads its correct keys before any backfill. The bytes a reader saves come only after a part holds
--     the columns (a merge, or the backfill MATERIALIZE COLUMN, CHAOS-10727).
--   - the writer does not name them: ClickHouse computes and stores them on every insert, so the writer works on a
--     database with or without this migration, and a stored value can never differ from its evidence.
-- Malformed or empty evidence ('' or '{}') reads []. Readers switch to the columns only after a verified backfill
-- (CHAOS-10728). Every statement can run again.
ALTER TABLE work_unit_investments ADD COLUMN IF NOT EXISTS issue_refs Array(String) DEFAULT JSONExtract(structural_evidence_json, 'issues', 'Array(String)');
ALTER TABLE work_unit_investments ADD COLUMN IF NOT EXISTS pr_refs Array(String) DEFAULT JSONExtract(structural_evidence_json, 'prs', 'Array(String)');
