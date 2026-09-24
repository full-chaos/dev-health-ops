
            SELECT
                work_unit_id,
                (argMax(tuple(resolved_team), (cnt, resolved_team_id))).1 AS team
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    ifNull(nullIf(t.team_id, ''), '') AS resolved_team_id,
                    max(ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, ''))) AS resolved_team,
                    uniqExactIf(multiIf(
                        NOT match(evidence_ref, '^[0-9a-fA-F-]{36}#pr[0-9]+$'),
                        evidence_ref,
                        evidence_repo.repo = '' OR evidence_repo.provider = '',
                        '',
                        concat(
                            if(evidence_repo.provider = 'gitlab', 'gitlab:', 'ghpr:'),
                            evidence_repo.repo,
                            if(evidence_repo.provider = 'gitlab', '!', '#'),
                            splitByString('#pr', evidence_ref)[2]
                        )
                    ), ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) IS NOT NULL) AS cnt
                FROM latest_work_unit_investments AS work_unit_investments
                ARRAY JOIN arrayDistinct(arrayConcat(
                    JSONExtract(structural_evidence_json, 'issues', 'Array(String)'),
                    JSONExtract(structural_evidence_json, 'prs', 'Array(String)')
                )) AS evidence_ref
                LEFT JOIN (
    SELECT
        org_id,
        toString(id) AS repo_uuid,
        argMax(repo, last_synced) AS repo,
        if(uniqExact(provider) = 1, argMax(provider, last_synced), '') AS provider
    FROM repos
    WHERE org_id = {org_id:String}
    GROUP BY org_id, id
) AS evidence_repo
                    ON evidence_repo.org_id = work_unit_investments.org_id
                    AND evidence_repo.repo_uuid = splitByString('#pr', evidence_ref)[1]
                LEFT JOIN (
    SELECT
        work_item_id,
        team_id,
        team_name
    FROM work_item_team_attributions FINAL
    WHERE org_id = {org_id:String}
      AND is_primary = 1
      AND (work_item_id, computed_at) IN (
          SELECT work_item_id, max(computed_at)
          FROM work_item_team_attributions
          WHERE org_id = {org_id:String}
          GROUP BY work_item_id
      )
) AS t
                    ON t.work_item_id = multiIf(
                        NOT match(evidence_ref, '^[0-9a-fA-F-]{36}#pr[0-9]+$'),
                        evidence_ref,
                        evidence_repo.repo = '' OR evidence_repo.provider = '',
                        '',
                        concat(
                            if(evidence_repo.provider = 'gitlab', 'gitlab:', 'ghpr:'),
                            evidence_repo.repo,
                            if(evidence_repo.provider = 'gitlab', '!', '#'),
                            splitByString('#pr', evidence_ref)[2]
                        )
                    )
                WHERE work_unit_investments.from_ts < {end_ts:DateTime64(3, 'UTC')}
                  AND work_unit_investments.to_ts >= {start_ts:DateTime64(3, 'UTC')}
                  AND work_unit_investments.org_id = {org_id:String}
                GROUP BY work_unit_id, resolved_team_id
            )
            GROUP BY work_unit_id
