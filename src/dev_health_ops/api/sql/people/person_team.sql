SELECT
    team_id
FROM (
    SELECT
        arrayJoin(team_ids) AS team_id,
        updated_at
    FROM identities FINAL
    WHERE (
        canonical_id IN %(identities)s
        OR email IN %(identities)s
        OR arrayExists(
            identity -> (
                (
                    startsWith(identity, 'github:')
                    AND has(
                        JSONExtract(provider_identities, 'github', 'Array(String)'),
                        replaceRegexpOne(identity, '^github:', '')
                    )
                )
                OR (
                    startsWith(identity, 'gitlab:')
                    AND has(
                        JSONExtract(provider_identities, 'gitlab', 'Array(String)'),
                        replaceRegexpOne(identity, '^gitlab:', '')
                    )
                )
                OR (
                    startsWith(identity, 'linear:')
                    AND has(
                        JSONExtract(provider_identities, 'linear', 'Array(String)'),
                        replaceRegexpOne(identity, '^linear:', '')
                    )
                )
                OR (
                    startsWith(identity, 'jira:accountid:')
                    AND has(
                        JSONExtract(provider_identities, 'jira', 'Array(String)'),
                        replaceRegexpOne(identity, '^jira:accountid:', '')
                    )
                )
                OR (
                    startsWith(identity, 'accountid:')
                    AND has(
                        JSONExtract(provider_identities, 'jira', 'Array(String)'),
                        replaceRegexpOne(identity, '^accountid:', '')
                    )
                )
            ),
            %(identities)s
        )
    )
      AND org_id = %(org_id)s
      AND is_active = 1
)
WHERE team_id != ''
ORDER BY updated_at DESC, team_id
LIMIT 1
