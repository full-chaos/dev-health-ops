(
        SELECT
            work_unit_id,
            (argMax(tuple(work_unit_type), computed_at)).1 AS work_unit_type,
            (argMax(tuple(work_unit_name), computed_at)).1 AS work_unit_name,
            argMax(from_ts, computed_at) AS from_ts,
            argMax(to_ts, computed_at) AS to_ts,
            (argMax(tuple(repo_id), computed_at)).1 AS repo_id,
            (argMax(tuple(provider), computed_at)).1 AS provider,
            argMax(effort_metric, computed_at) AS effort_metric,
            argMax(effort_value, computed_at) AS effort_value,
            argMax(theme_distribution_json, computed_at) AS theme_distribution_json,
            argMax(subcategory_distribution_json, computed_at) AS subcategory_distribution_json,
            argMax(structural_evidence_json, computed_at) AS structural_evidence_json,
            argMax(evidence_quality, computed_at) AS evidence_quality,
            argMax(evidence_quality_band, computed_at) AS evidence_quality_band,
            argMax(categorization_status, computed_at) AS categorization_status,
            argMax(categorization_model_version, computed_at) AS categorization_model_version,
            argMax(categorization_run_id, computed_at) AS categorization_run_id,
            org_id,
            max(computed_at) AS latest_computed_at
        FROM work_unit_investments
        WHERE org_id = {org_id:String}
              AND work_unit_id NOT IN (
                  SELECT superseded_work_unit_id
                  FROM work_unit_supersessions
                  WHERE org_id = {org_id:String}
              )
              AND (
                  (SELECT scope_enabled FROM (
        SELECT
            if(
                marker_count > 0
                AND latest_run_id != ''
                AND (
                    latest_investment_computed_at IS NULL
                    OR latest_investment_computed_at <= latest_run_completed_at
                ),
                1,
                0
            ) AS scope_enabled,
            multiIf(
                marker_count = 0 OR latest_run_id = '', 'unscoped_no_marker',
                latest_investment_computed_at IS NOT NULL
                AND latest_investment_computed_at > latest_run_completed_at,
                'unscoped_fallback',
                'scoped'
            ) AS scope_mode,
            toInt64(greatest(
                0,
                if(
                    latest_investment_computed_at IS NULL,
                    0,
                    dateDiff('second', latest_run_completed_at, latest_investment_computed_at)
                )
            )) AS lag_seconds
        FROM (
        SELECT
            argMax(run_id, completed_at) AS latest_run_id,
            max(completed_at) AS latest_run_completed_at,
            count() AS marker_count
        FROM work_unit_membership_runs
        WHERE org_id = {org_id:String}
    ) AS lcmr
        CROSS JOIN (
        SELECT max(computed_at) AS latest_investment_computed_at
        FROM work_unit_investments
        WHERE org_id = {org_id:String}
    ) AS lic
    )) = 0
                  OR work_unit_id IN (
                      SELECT work_unit_id FROM (
        SELECT DISTINCT m.work_unit_id AS work_unit_id
        FROM work_unit_membership AS m
        INNER JOIN (
        SELECT
            argMax(run_id, completed_at) AS latest_run_id,
            max(completed_at) AS latest_run_completed_at,
            count() AS marker_count
        FROM work_unit_membership_runs
        WHERE org_id = {org_id:String}
    ) AS latest_run ON 1 = 1
        
            LEFT JOIN (
                SELECT
                    org_id,
                    node_type,
                    node_id,
                    max(computed_at) AS legacy_max_computed_at
                FROM work_unit_membership
                WHERE org_id = {org_id:String} AND run_id = ''
                GROUP BY org_id, node_type, node_id
            ) AS lnm
                ON lnm.org_id = m.org_id
                AND lnm.node_type = m.node_type
                AND lnm.node_id = m.node_id
        WHERE m.org_id = {org_id:String}
          AND latest_run.latest_run_id != ''
          AND ((latest_run.latest_run_id != '__legacy__' AND m.run_id = latest_run.latest_run_id) OR (latest_run.latest_run_id = '__legacy__' AND m.run_id = '' AND m.computed_at = lnm.legacy_max_computed_at))
    )
                  )
              )
        GROUP BY org_id, work_unit_id
    )