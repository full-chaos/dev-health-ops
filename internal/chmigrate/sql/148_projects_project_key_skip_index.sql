-- A data-skipping index on projects' project key.
--
-- A reader that resolves a page of (provider, project key) pairs to projects counts every project sharing each
-- key (key_resolution_count). project_key is not in the sort key (org_id, provider, id), so without an index that
-- count reads every granule of the organization. A bloom filter on ifNull(project_key, '') (the expression the
-- readers compare) lets such a read select only the granules that may hold its keys. MATERIALIZE builds the index
-- for the rows already stored. Every statement can run again.
ALTER TABLE projects ADD INDEX IF NOT EXISTS project_key_bf ifNull(project_key, '') TYPE bloom_filter GRANULARITY 1;
ALTER TABLE projects MATERIALIZE INDEX project_key_bf SETTINGS mutations_sync = 2;
