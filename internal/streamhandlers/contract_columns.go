package streamhandlers

import "github.com/full-chaos/dev-health-ops/internal/storedversion"

const operationalBaseColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,source_revision,source_conflict_key,ingest_revision,ordering_contract,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence"

func stated(columns ...string) []storedversion.Column {
	out := make([]storedversion.Column, 0, len(columns))
	for _, column := range columns {
		out = append(out, storedversion.Column{Name: column, Rule: storedversion.Stated, Fields: []string{column}})
	}
	return out
}

func contractColumns(groups ...[]storedversion.Column) []storedversion.Column {
	var out []storedversion.Column
	for _, group := range groups {
		out = append(out, group...)
	}
	return out
}

var (
	idOrg        = storedversion.Column{Name: "org_id", Rule: storedversion.Writer}
	idLastSynced = storedversion.Column{Name: "last_synced", Rule: storedversion.Writer}
	idSource     = storedversion.Column{Name: "source_id", Rule: storedversion.Writer}
)

func noField(columns ...string) []storedversion.Column {
	out := make([]storedversion.Column, 0, len(columns))
	for _, column := range columns {
		out = append(out, storedversion.Column{Name: column, Rule: storedversion.NoField})
	}
	return out
}
