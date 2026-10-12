-- team_repo_ownership.share: the owning team's linked share of the repository.
--
-- The inferred derivation writes one row per owning team of a repository: the
-- team with the largest linked share is the main owner (is_primary = 1) and
-- every other owning team is a co-owner (is_primary = 2), each row with its own
-- share (link counts of the deciding tier: native > explicit_text > heuristic).
-- NULL = no share measured (a provider_access, native or manual row, or a row
-- written before this migration); NULL is not a share of zero.
ALTER TABLE team_repo_ownership ADD COLUMN IF NOT EXISTS share Nullable(Float64);
