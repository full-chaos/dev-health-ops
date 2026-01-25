ALTER TABLE work_items
ADD COLUMN IF NOT EXISTS description Nullable(String);

ALTER TABLE git_pull_requests
ADD COLUMN IF NOT EXISTS body Nullable(String);
