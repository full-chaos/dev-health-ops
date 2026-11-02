-- The team split of a person's work_item_user_metrics_daily row (CHAOS-12535).
--
-- A row holds one person's items of one work scope and day under ONE team label
-- (the team with the most of the items). The four arrays are parallel, one
-- element for each team of the items:
--
--   team_split_team_ids          the team id (unassigned for items with no team)
--   team_split_is_primary        1: the items whose primary team it is; 2: the
--                                items it co-owns (work_item_team_attributions)
--   team_split_items_completed   the items of that entry completed on the day
--   team_split_wip_end_of_day    the items of that entry open at the end of the day
--
-- Over the elements with is_primary 1 the counts add up to items_completed and
-- wip_count_end_of_day. A row written before this migration holds empty arrays;
-- the landscape reads its counts as unassigned.
--
-- Additive, no data mutation, no backfill. No key change.
ALTER TABLE work_item_user_metrics_daily ADD COLUMN IF NOT EXISTS team_split_team_ids Array(String) DEFAULT [];
ALTER TABLE work_item_user_metrics_daily ADD COLUMN IF NOT EXISTS team_split_is_primary Array(UInt8) DEFAULT [];
ALTER TABLE work_item_user_metrics_daily ADD COLUMN IF NOT EXISTS team_split_items_completed Array(UInt32) DEFAULT [];
ALTER TABLE work_item_user_metrics_daily ADD COLUMN IF NOT EXISTS team_split_wip_end_of_day Array(UInt32) DEFAULT [];
