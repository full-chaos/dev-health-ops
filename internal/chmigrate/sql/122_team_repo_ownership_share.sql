-- team_repo_ownership.share: the owning team's linked share of the repository.
--
-- The inferred derivation writes one row per owning team of a repository, each
-- with its share: the team's issue<->PR links (every tier: native,
-- explicit_text, heuristic) over all links of the repository, so the shares of
-- a repository add up to 1. The main owner (is_primary = 1) is the top team by
-- the native-first rank; every other owning team is a co-owner (is_primary = 2).
-- NULL = no share measured (a provider_access, native or manual row, or a row
-- written before this migration); NULL is not a share of zero.
ALTER TABLE team_repo_ownership ADD COLUMN IF NOT EXISTS share Nullable(Float64);
