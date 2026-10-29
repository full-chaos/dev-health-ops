-- teams.members: dropped (CHAOS-9087). The column was a roster copy of the team
-- sync's team_memberships rows: no validity window, no source, one team per
-- person. A person's team comes from team_memberships; no reader and no writer
-- of the column is left in the code. manual_members (the admin roster) stays.
ALTER TABLE teams DROP COLUMN IF EXISTS members;
