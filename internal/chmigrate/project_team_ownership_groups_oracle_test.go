//go:build integration

package chmigrate_test

// acrProjectTeamGroupsSQL is the context-fabric reader's per-organization
// project -> team ownership group statement (cursor space: ingest stamps), the
// oracle the stored groups of 147 are compared with. Rendered from
// github.com/full-chaos/dev-health-acr at 1b2f760f1a91b181148dea60bd8846062a9211ff by
// projectTeamsGroupSQL(true) (internal/contextfabric/devhealthsource), with the
// reader's own readers.ProjectIdentityCatalogSQL (dev-health-go v0.42.0).
// Byte for byte; {org_id:String} is the organization.
const acrProjectTeamGroupsSQL = `SELECT o.project_id, o.team_id, o.source_name,
       minIf(o.first_valid_from, o.unassertable = 0) AS first_valid_from,
       argMaxIf(tuple(o.valid_to), (o.valid_to IS NULL, if(o.valid_to IS NULL, -toUnixTimestamp64Micro(o.valid_from), toUnixTimestamp64Micro(o.valid_to)), toUnixTimestamp64Micro(o.valid_from)), o.unassertable = 0).1 IS NULL AS latest_is_open,
       ifNull(argMaxIf(tuple(o.valid_to), (o.valid_to IS NULL, if(o.valid_to IS NULL, -toUnixTimestamp64Micro(o.valid_from), toUnixTimestamp64Micro(o.valid_to)), toUnixTimestamp64Micro(o.valid_from)), o.unassertable = 0).1, toDateTime64(0, 3, 'UTC')) AS latest_valid_to,
       max(o.row_watermark) AS observed_at, o.provider,
       toUInt8(countIf(o.unassertable = 0) = 0) AS edge_suppressed,
       groupUniqArrayIf(concat(o.ownership_ref, '\0', o.ownership_key, '\0', o.team_id, '\0', o.source_name), o.unassertable = 1 AND o.retraction_only = 0) AS conflict_identities,
       toUInt8(countIf(o.unassertable = 1 AND o.retraction_only = 0) > 0) AS conflicting_identity_present, max(o.row_ingest) AS ingest_at
FROM (
	SELECT project_id, provider, ownership_ref, ownership_key, team_id, source_name, first_valid_from, valid_from, valid_to, retraction_only,
	       greatest(updated_at, max(project_updated_at) OVER (PARTITION BY provider, ownership_ref, ownership_key, retraction_only)) AS row_watermark, greatest(ingest_at, max(project_ingest_at) OVER (PARTITION BY provider, ownership_ref, ownership_key, retraction_only), max(provider_ingest_at) OVER (PARTITION BY provider, ownership_ref, ownership_key, retraction_only)) AS row_ingest,
	       toUInt8(retraction_only = 1 OR min(project_id) OVER (PARTITION BY provider, ownership_ref, ownership_key, retraction_only) != max(project_id) OVER (PARTITION BY provider, ownership_ref, ownership_key, retraction_only)) AS unassertable
	FROM (
		SELECT p.id AS project_id, p.provider AS provider,
		       o.project_ref AS ownership_ref, o.project_key AS ownership_key,
		       o.team_id AS team_id, o.source_name AS source_name, o.first_valid_from AS first_valid_from, o.valid_from AS valid_from, o.valid_to AS valid_to, o.updated_at AS updated_at, o.ingest_at AS ingest_at,
		       p.project_updated_at AS project_updated_at, p.provider_updated_at AS provider_updated_at, p.project_ingest_at AS project_ingest_at, p.provider_ingest_at AS provider_ingest_at, toUInt8(0) AS retraction_only
		FROM (
	SELECT pi.*, w.project_updated_at AS project_updated_at, w.provider_updated_at AS provider_updated_at , w.project_ingest_at AS project_ingest_at, w.provider_ingest_at AS provider_ingest_at
	FROM (SELECT * FROM (
	SELECT provider, id, project_key, key_resolution_count, scope, max(scope_kind) AS scope_kind
	FROM (
		SELECT provider, id, project_key,
			key_resolution_count AS project_key_resolution_count,
			(project_key != '' AND key_resolution_count = 1) AS key_scope_emitted
		FROM (
		SELECT id, provider, project_key, key_resolution_count
		FROM (
			SELECT id, provider, ifNull(project_key, '') AS project_key,
				countIf(ifNull(project_key, '') != '') OVER (PARTITION BY provider, ifNull(project_key, '')) AS key_resolution_count
			FROM projects FINAL
			WHERE org_id = {org_id:String}
		))
	)
	ARRAY JOIN
		if(key_scope_emitted, [id, project_key], [id]) AS scope,
		if(key_scope_emitted, ['id', 'key'], ['id']) AS scope_kind,
		if(key_scope_emitted, [toUInt64(1), project_key_resolution_count], [toUInt64(1)]) AS key_resolution_count
	GROUP BY provider, id, project_key, key_resolution_count, scope
) AS p) AS pi
	INNER JOIN (
		SELECT provider, id, updated_at AS project_updated_at, max(updated_at) OVER (PARTITION BY provider) AS provider_updated_at, last_synced AS project_ingest_at, max(last_synced) OVER (PARTITION BY provider) AS provider_ingest_at
		FROM projects FINAL
		WHERE org_id = {org_id:String}
	) AS w ON w.provider = pi.provider AND w.id = pi.id
) AS p
		INNER JOIN (
		SELECT provider, project_ref, project_key, team_id, source_name, first_valid_from, valid_from, valid_to, updated_at, ingest_at,
		       scope_match.1 AS scope_value, scope_match.2 AS required_scope_kind
		FROM (
		SELECT provider, project_ref, project_key, team_id, source_name,
		       min(vf) AS first_valid_from,
		       argMax(tuple(vf, vt), ord) AS latest_assertion,
		       latest_assertion.1 AS valid_from, latest_assertion.2 AS valid_to,
		       max(upd) AS updated_at, max(ing) AS ingest_at
		FROM (
			SELECT provider, project_id AS project_ref, ifNull(project_key, '') AS project_key, team_id, toString(source) AS source_name,
			       valid_from AS vf, valid_to AS vt, updated_at AS upd, last_synced AS ing,
			       (valid_to IS NULL, if(valid_to IS NULL, -toUnixTimestamp64Micro(valid_from), toUnixTimestamp64Micro(valid_to)), toUnixTimestamp64Micro(valid_from)) AS ord
			FROM team_project_ownership FINAL
			WHERE org_id = {org_id:String}
		)
		GROUP BY provider, project_ref, project_key, team_id, source_name
	)
		ARRAY JOIN [(project_ref, ''), (project_key, 'key')] AS scope_match
	) AS o ON o.provider = p.provider AND o.scope_value = p.scope
		INNER JOIN (SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1) AS t ON t.id = o.team_id
		WHERE o.required_scope_kind = '' OR p.scope_kind = o.required_scope_kind

		UNION ALL

		SELECT p.id AS project_id, p.provider AS provider,
		       o.project_ref AS ownership_ref, o.project_key AS ownership_key,
		       o.team_id AS team_id, o.source_name AS source_name, o.first_valid_from AS first_valid_from, o.valid_from AS valid_from, o.valid_to AS valid_to, o.updated_at AS updated_at, o.ingest_at AS ingest_at,
		       p.project_updated_at AS project_updated_at, p.provider_updated_at AS provider_updated_at, p.project_ingest_at AS project_ingest_at, p.provider_ingest_at AS provider_ingest_at, toUInt8(1) AS retraction_only
		FROM (
	SELECT pa.provider AS provider, pa.id AS id, pa.project_key AS project_key, pa.project_updated_at AS project_updated_at, pa.provider_updated_at AS provider_updated_at, pa.project_ingest_at AS project_ingest_at, pa.provider_ingest_at AS provider_ingest_at,
	       count() OVER (PARTITION BY pa.provider, pa.project_key) AS key_project_count
	FROM (SELECT * FROM (
	SELECT pi.*, w.project_updated_at AS project_updated_at, w.provider_updated_at AS provider_updated_at , w.project_ingest_at AS project_ingest_at, w.provider_ingest_at AS provider_ingest_at
	FROM (SELECT * FROM (
	SELECT provider, id, project_key, key_resolution_count, scope, max(scope_kind) AS scope_kind
	FROM (
		SELECT provider, id, project_key,
			key_resolution_count AS project_key_resolution_count,
			(project_key != '' AND key_resolution_count = 1) AS key_scope_emitted
		FROM (
		SELECT id, provider, project_key, key_resolution_count
		FROM (
			SELECT id, provider, ifNull(project_key, '') AS project_key,
				countIf(ifNull(project_key, '') != '') OVER (PARTITION BY provider, ifNull(project_key, '')) AS key_resolution_count
			FROM projects FINAL
			WHERE org_id = {org_id:String}
		))
	)
	ARRAY JOIN
		if(key_scope_emitted, [id, project_key], [id]) AS scope,
		if(key_scope_emitted, ['id', 'key'], ['id']) AS scope_kind,
		if(key_scope_emitted, [toUInt64(1), project_key_resolution_count], [toUInt64(1)]) AS key_resolution_count
	GROUP BY provider, id, project_key, key_resolution_count, scope
) AS p) AS pi
	INNER JOIN (
		SELECT provider, id, updated_at AS project_updated_at, max(updated_at) OVER (PARTITION BY provider) AS provider_updated_at, last_synced AS project_ingest_at, max(last_synced) OVER (PARTITION BY provider) AS provider_ingest_at
		FROM projects FINAL
		WHERE org_id = {org_id:String}
	) AS w ON w.provider = pi.provider AND w.id = pi.id
) AS p) AS pa
	WHERE pa.scope_kind = 'id' AND pa.project_key != ''
) AS p
		INNER JOIN (
		SELECT provider, project_ref, project_key, team_id, source_name, first_valid_from, valid_from, valid_to, updated_at, ingest_at,
		       if(project_key != '', project_key, project_ref) AS match_value
		FROM (
		SELECT provider, project_ref, project_key, team_id, source_name,
		       min(vf) AS first_valid_from,
		       argMax(tuple(vf, vt), ord) AS latest_assertion,
		       latest_assertion.1 AS valid_from, latest_assertion.2 AS valid_to,
		       max(upd) AS updated_at, max(ing) AS ingest_at
		FROM (
			SELECT provider, project_id AS project_ref, ifNull(project_key, '') AS project_key, team_id, toString(source) AS source_name,
			       valid_from AS vf, valid_to AS vt, updated_at AS upd, last_synced AS ing,
			       (valid_to IS NULL, if(valid_to IS NULL, -toUnixTimestamp64Micro(valid_from), toUnixTimestamp64Micro(valid_to)), toUnixTimestamp64Micro(valid_from)) AS ord
			FROM team_project_ownership FINAL
			WHERE org_id = {org_id:String} AND (SELECT count() FROM (
	SELECT pa.provider AS provider, pa.id AS id, pa.project_key AS project_key, pa.project_updated_at AS project_updated_at, pa.provider_updated_at AS provider_updated_at, pa.project_ingest_at AS project_ingest_at, pa.provider_ingest_at AS provider_ingest_at,
	       count() OVER (PARTITION BY pa.provider, pa.project_key) AS key_project_count
	FROM (SELECT * FROM (
	SELECT pi.*, w.project_updated_at AS project_updated_at, w.provider_updated_at AS provider_updated_at , w.project_ingest_at AS project_ingest_at, w.provider_ingest_at AS provider_ingest_at
	FROM (SELECT * FROM (
	SELECT provider, id, project_key, key_resolution_count, scope, max(scope_kind) AS scope_kind
	FROM (
		SELECT provider, id, project_key,
			key_resolution_count AS project_key_resolution_count,
			(project_key != '' AND key_resolution_count = 1) AS key_scope_emitted
		FROM (
		SELECT id, provider, project_key, key_resolution_count
		FROM (
			SELECT id, provider, ifNull(project_key, '') AS project_key,
				countIf(ifNull(project_key, '') != '') OVER (PARTITION BY provider, ifNull(project_key, '')) AS key_resolution_count
			FROM projects FINAL
			WHERE org_id = {org_id:String}
		))
	)
	ARRAY JOIN
		if(key_scope_emitted, [id, project_key], [id]) AS scope,
		if(key_scope_emitted, ['id', 'key'], ['id']) AS scope_kind,
		if(key_scope_emitted, [toUInt64(1), project_key_resolution_count], [toUInt64(1)]) AS key_resolution_count
	GROUP BY provider, id, project_key, key_resolution_count, scope
) AS p) AS pi
	INNER JOIN (
		SELECT provider, id, updated_at AS project_updated_at, max(updated_at) OVER (PARTITION BY provider) AS provider_updated_at, last_synced AS project_ingest_at, max(last_synced) OVER (PARTITION BY provider) AS provider_ingest_at
		FROM projects FINAL
		WHERE org_id = {org_id:String}
	) AS w ON w.provider = pi.provider AND w.id = pi.id
) AS p) AS pa
	WHERE pa.scope_kind = 'id' AND pa.project_key != ''
) AS p WHERE p.key_project_count > 1) > 0
		)
		GROUP BY provider, project_ref, project_key, team_id, source_name
	)
	) AS o ON o.provider = p.provider AND o.match_value = p.project_key
		INNER JOIN (SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1) AS t ON t.id = o.team_id
		WHERE p.key_project_count > 1
	)
) AS o
GROUP BY o.project_id, o.provider, o.team_id, o.source_name`
