(
        SELECT
            d.work_unit_id AS work_unit_id,
            d.repo_id AS repo_id,
            d.effort_metric AS effort_metric,
            d.repo_effort_value AS repo_effort_value,
            d.allocation_source AS allocation_source,
            d.repo_source AS repo_source,
            d.org_id AS org_id,
            d.latest_repo_effort_computed_at AS latest_repo_effort_computed_at,
            1 AS has_allocation
        FROM (
            SELECT
                work_unit_id,
                repo_id,
                org_id,
                argMax(effort_metric, computed_at) AS effort_metric,
                argMax(effort_value, computed_at) AS repo_effort_value,
                argMax(allocation_source, computed_at) AS allocation_source,
                (argMax(tuple(repo_source), computed_at)).1 AS repo_source,
                max(computed_at) AS latest_repo_effort_computed_at
            FROM work_unit_repo_effort
            WHERE org_id = {org_id:String}
            GROUP BY org_id, work_unit_id, repo_id
        ) AS d
        INNER JOIN (
            SELECT
                org_id,
                work_unit_id,
                max(computed_at) AS unit_generation_at
            FROM work_unit_repo_effort
            WHERE org_id = {org_id:String}
            GROUP BY org_id, work_unit_id
        ) AS g
            ON g.org_id = d.org_id
            AND g.work_unit_id = d.work_unit_id
        WHERE d.latest_repo_effort_computed_at = g.unit_generation_at
    )