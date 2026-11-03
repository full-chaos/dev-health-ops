-- CHAOS-9432: a team's own stable identity at its provider, and when the
-- provider stopped listing it.
--
--   provider_team_id  the provider's immutable id of the team as text (GitHub
--                     team id, GitLab group id, Linear team uuid, the Atlassian
--                     team uuid); '' until a sync that lists the team writes it.
--                     The row key (teams.id) is minted once from the first slug,
--                     path or key the sync saw and is never changed; a rename at
--                     the provider changes native_team_key, not the id, and this
--                     column is what tells the sync it is the same team.
--   provider_name     the provider's own current name of the team. name can be
--                     edited by an admin under the team's sync policy; this is
--                     the name the provider reports.
--   gone_at           the time a run proved the team gone at the provider (its
--                     stable id absent). NULL = not gone. is_active is NOT
--                     touched: a gone team keeps its past reads and attribution.
--
-- Additive, no data mutation, no backfill: the first sync that lists a team
-- writes its provider_team_id and provider_name. Every writer of teams carries
-- the three columns from the version it replaces (internal/teamprovider, held
-- by its census), as it does created_at (CHAOS-9033).
ALTER TABLE teams ADD COLUMN IF NOT EXISTS provider_team_id String DEFAULT '';
ALTER TABLE teams ADD COLUMN IF NOT EXISTS provider_name String DEFAULT '';
ALTER TABLE teams ADD COLUMN IF NOT EXISTS gone_at Nullable(DateTime64(6));
