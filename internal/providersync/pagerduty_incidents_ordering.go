package providersync

import "github.com/full-chaos/dev-health-ops/internal/providerfoundation"

func fillPagerDutyIncidentOrdering(row *pagerDutyIncidentRow) error {
	if row == nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	fields := gitLabOperationalBaseFields(
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.RawStatus, row.RawSeverity,
		row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity,
		row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence,
	)
	fields = append(fields,
		jiraOperationalField{"service_id", jiraStringValue(row.ServiceID)},
		jiraOperationalField{"service_external_id", jiraStringValue(row.ServiceExternalID)},
		jiraOperationalField{"escalation_policy_id", jiraStringValue(row.EscalationPolicyID)},
		jiraOperationalField{"title", row.Title},
		jiraOperationalField{"description", jiraStringValue(row.Description)},
		jiraOperationalField{"started_at", jiraTimeValue(row.StartedAt)},
		jiraOperationalField{"resolved_at", jiraTimeValue(row.ResolvedAt)},
		jiraOperationalField{"is_deleted", row.IsDeleted},
		jiraOperationalField{"deleted_at", jiraTimeValue(row.DeletedAt)},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_incident", row.OrgID, row.Provider, row.ProviderInstanceID,
		row.ExternalID, row.SourceVersionAt, row.ObservedAt, row.LastSynced, fields,
	)
	if err != nil {
		return err
	}
	row.ID, row.SourceConflictKey = id, conflict
	row.SourceRevision, row.IngestRevision = sourceRevision, ingestRevision
	row.OrderingContract = 2
	return nil
}

func fillPagerDutyAlertOrdering(row *pagerDutyAlertRow) error {
	if row == nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	fields := gitLabOperationalBaseFields(
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.RawStatus, row.RawSeverity,
		row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity,
		row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence,
	)
	fields = append(fields,
		jiraOperationalField{"service_id", jiraStringValue(row.ServiceID)},
		jiraOperationalField{"incident_id", jiraStringValue(row.IncidentID)},
		jiraOperationalField{"title", row.Title},
		jiraOperationalField{"description", jiraStringValue(row.Description)},
		jiraOperationalField{"triggered_at", jiraTimeValue(row.TriggeredAt)},
		jiraOperationalField{"acknowledged_at", jiraTimeValue(row.AcknowledgedAt)},
		jiraOperationalField{"resolved_at", jiraTimeValue(row.ResolvedAt)},
		jiraOperationalField{"is_deleted", row.IsDeleted},
		jiraOperationalField{"deleted_at", jiraTimeValue(row.DeletedAt)},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_alert", row.OrgID, row.Provider, row.ProviderInstanceID,
		row.ExternalID, row.SourceVersionAt, row.ObservedAt, row.LastSynced, fields,
	)
	if err != nil {
		return err
	}
	row.ID, row.SourceConflictKey = id, conflict
	row.SourceRevision, row.IngestRevision = sourceRevision, ingestRevision
	row.OrderingContract = 2
	return nil
}

func fillPagerDutyLogEntryOrdering(row *pagerDutyLogEntryRow) error {
	if row == nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	fields := gitLabOperationalBaseFields(
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.RawStatus, row.RawSeverity,
		row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity,
		row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence,
	)
	fields = append(fields,
		jiraOperationalField{"incident_id", row.IncidentID},
		jiraOperationalField{"event_type", row.EventType},
		jiraOperationalField{"body", jiraStringValue(row.Body)},
		jiraOperationalField{"actor_type", jiraStringValue(row.ActorType)},
		jiraOperationalField{"actor_id", jiraStringValue(row.ActorID)},
		jiraOperationalField{"occurred_at", jiraTimeValue(row.OccurredAt)},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_incident_timeline_event", row.OrgID, row.Provider,
		row.ProviderInstanceID, row.ExternalID, row.SourceVersionAt,
		row.ObservedAt, row.LastSynced, fields,
	)
	if err != nil {
		return err
	}
	row.ID, row.SourceConflictKey = id, conflict
	row.SourceRevision, row.IngestRevision = sourceRevision, ingestRevision
	row.OrderingContract = 2
	return nil
}

func fillPagerDutyNoteOrdering(row *pagerDutyNoteRow) error {
	if row == nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	fields := gitLabOperationalBaseFields(
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.RawStatus, row.RawSeverity,
		row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity,
		row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence,
	)
	fields = append(fields,
		jiraOperationalField{"incident_id", row.IncidentID},
		jiraOperationalField{"body", row.Body},
		jiraOperationalField{"author_user_id", jiraStringValue(row.AuthorUserID)},
		jiraOperationalField{"created_at", jiraTimeValue(row.CreatedAt)},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_incident_note", row.OrgID, row.Provider, row.ProviderInstanceID,
		row.ExternalID, row.SourceVersionAt, row.ObservedAt, row.LastSynced, fields,
	)
	if err != nil {
		return err
	}
	row.ID, row.SourceConflictKey = id, conflict
	row.SourceRevision, row.IngestRevision = sourceRevision, ingestRevision
	row.OrderingContract = 2
	return nil
}
