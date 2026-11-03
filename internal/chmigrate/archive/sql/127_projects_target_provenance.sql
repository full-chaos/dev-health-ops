-- CHAOS-9613: state where a project's target_date came from, in the shape of
-- state_source (123): provider-qualified text, no JSON. The columns are the same
-- for every provider, so a reader never branches by provider.
--   target_state       'stated'     the run read the project's versions/milestones and
--                                   one open dated entry supplied target_date
--                      'none_dated' the run read them and no open entry carries a date
--                                   (a measured absence, target_date is NULL)
--                      'held'       the run did not read them (the read failed, or none was
--                                   made): every target column is the value of the last run
--                                   that did, and target_fetched_at says when
--                      'unknown'    no run ever read them: no target value
--   target_kind        what supplied the date: project | version | milestone; '' when none
--   target_source      <provider>.<kind>.<field>: linear.project.targetDate,
--                      jira.version.releaseDate, gitlab.milestone.due_date; '' when none
--   target_ref         the supplying version's or milestone's stable provider id (GitLab:
--                      the sprints row key gitlab:<path>:milestone:<id>); '' for Linear
--   target_ref_name    its display name
--   target_other_open  the number of OTHER open dated versions/milestones of the project;
--                      NULL unless target_state is 'stated' or 'held' on a stated value
--   target_fetched_at  the time of the last successful read of the project's target
-- Additive, no data mutation, no backfill. The first three columns have DEFAULT
-- expressions that depend on provider and target_date: a row written before this
-- migration (and a row an older writer inserts without these columns) reads, for a
-- Linear project that carries a date, 'stated' / 'project' / 'linear.project.targetDate'
-- (Linear's own date; target_fetched_at stays NULL to show it was not read under this
-- rule), and for every other row 'unknown' with empty text and NULL until the next sync
-- of its provider rewrites it. A later DROP of provider or target_date needs these
-- defaults removed first (ALTER TABLE projects MODIFY COLUMN ... DEFAULT '').
ALTER TABLE projects ADD COLUMN IF NOT EXISTS target_state LowCardinality(String) DEFAULT if(provider = 'linear' AND target_date IS NOT NULL, 'stated', 'unknown') AFTER target_date;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS target_kind LowCardinality(String) DEFAULT if(provider = 'linear' AND target_date IS NOT NULL, 'project', '') AFTER target_state;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS target_source LowCardinality(String) DEFAULT if(provider = 'linear' AND target_date IS NOT NULL, 'linear.project.targetDate', '') AFTER target_kind;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS target_ref String DEFAULT '' AFTER target_source;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS target_ref_name String DEFAULT '' AFTER target_ref;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS target_other_open Nullable(UInt32) AFTER target_ref_name;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS target_fetched_at Nullable(DateTime64(3, 'UTC')) AFTER target_other_open;
