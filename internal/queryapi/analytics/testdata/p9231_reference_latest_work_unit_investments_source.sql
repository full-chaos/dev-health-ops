(
        SELECT
            work_unit_id,
            (argMax(tuple(work_unit_type), (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset))).1 AS work_unit_type,
            (argMax(tuple(work_unit_name), (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset))).1 AS work_unit_name,
            argMax(from_ts, (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)) AS from_ts,
            argMax(to_ts, (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)) AS to_ts,
            (argMax(tuple(repo_id), (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset))).1 AS repo_id,
            (argMax(tuple(provider), (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset))).1 AS provider,
            argMax(effort_metric, (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)) AS effort_metric,
            argMax(effort_value, (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)) AS effort_value,
            argMax(theme_distribution_json, (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)) AS theme_distribution_json,
            argMax(subcategory_distribution_json, (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)) AS subcategory_distribution_json,
            argMax(structural_evidence_json, (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)) AS structural_evidence_json,
            argMax(evidence_quality, (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)) AS evidence_quality,
            argMax(evidence_quality_band, (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)) AS evidence_quality_band,
            argMax(categorization_status, (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)) AS categorization_status,
            argMax(categorization_model_version, (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)) AS categorization_model_version,
            argMax(categorization_run_id, (work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)) AS categorization_run_id,
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
                  (SELECT argMax(run_id, completed_at) FROM work_unit_membership_runs WHERE org_id = {org_id:String}) = ''
                  OR work_unit_id IN (
                      SELECT work_unit_id FROM (
        SELECT DISTINCT m.work_unit_id AS work_unit_id
        FROM work_unit_membership AS m
        WHERE m.org_id = {org_id:String}
          AND (SELECT argMax(run_id, completed_at) FROM work_unit_membership_runs WHERE org_id = {org_id:String}) != ''
          AND (SELECT argMax(run_id, completed_at) FROM work_unit_membership_runs WHERE org_id = {org_id:String}) != '__legacy__'
          AND m.run_id = (SELECT argMax(run_id, completed_at) FROM work_unit_membership_runs WHERE org_id = {org_id:String})
        UNION ALL
        SELECT DISTINCT m.work_unit_id AS work_unit_id
        FROM work_unit_membership AS m
        
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
          AND (SELECT argMax(run_id, completed_at) FROM work_unit_membership_runs WHERE org_id = {org_id:String}) = '__legacy__'
          AND m.run_id = ''
          AND m.computed_at = lnm.legacy_max_computed_at
    )
                  )
              )
        GROUP BY org_id, work_unit_id
    )