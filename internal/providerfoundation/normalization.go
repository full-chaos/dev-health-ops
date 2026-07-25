package providerfoundation

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
	"unicode/utf8"
)

// NormalizationContext carries sink metadata that is not owned by a provider
// entity. Dataset adapters obtain it from the claimed sync unit.
type NormalizationContext struct {
	IntegrationID string     `json:"integration_id"`
	Provenance    Provenance `json:"provenance"`
}

// WorkItemRecord is the shared subset of Python's normalized WorkItem needed
// to construct a provider-independent sink envelope.
type WorkItemRecord struct {
	Provider      string    `json:"provider"`
	OrgID         string    `json:"org_id"`
	WorkItemID    string    `json:"work_item_id"`
	Title         string    `json:"title"`
	Type          string    `json:"type"`
	Status        string    `json:"status"`
	StatusRaw     *string   `json:"status_raw"`
	ProjectKey    *string   `json:"project_key"`
	ProjectID     *string   `json:"project_id"`
	ProjectName   *string   `json:"project_name"`
	NativeTeamKey *string   `json:"native_team_key"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// SourceRecord carries provider records that do not have a dedicated semantic
// model yet but still require a provider-independent, sink-ready identity.
// EntityType is constrained to the reference/work-item support surfaces used
// by the dormant GitHub/GitLab executor.
type SourceRecord struct {
	Provider   string            `json:"provider"`
	OrgID      string            `json:"org_id"`
	EntityType string            `json:"entity_type"`
	SourceID   string            `json:"source_id"`
	ObservedAt time.Time         `json:"observed_at"`
	Attributes map[string]string `json:"attributes"`
}

// FeatureFlagRecord is the shared subset of Python's FeatureFlagRecord needed
// by the sink envelope.
type FeatureFlagRecord struct {
	Provider    string    `json:"provider"`
	OrgID       string    `json:"org_id"`
	FlagKey     string    `json:"flag_key"`
	ProjectKey  *string   `json:"project_key"`
	Environment string    `json:"environment"`
	FlagType    *string   `json:"flag_type"`
	LastSynced  time.Time `json:"last_synced"`
}

type operationalRecord struct {
	Provider           string
	OrgID              string
	ProviderInstanceID string
	SourceEntityType   string
	ExternalID         string
	SourceVersionAt    time.Time `json:"source_version_at"`
	ObservedAt         time.Time `json:"observed_at"`
	NormalizedStatus   *string
}

// OperationalServiceRecord is the canonical operational-service normalization
// input shared with Python.
type OperationalServiceRecord struct {
	Provider           string    `json:"provider"`
	OrgID              string    `json:"org_id"`
	ProviderInstanceID string    `json:"provider_instance_id"`
	SourceEntityType   string    `json:"source_entity_type"`
	ExternalID         string    `json:"external_id"`
	SourceVersionAt    time.Time `json:"source_version_at"`
	ObservedAt         time.Time `json:"observed_at"`
	Name               string    `json:"name"`
	NormalizedStatus   *string   `json:"normalized_status"`
}

// OperationalIncidentRecord is the canonical operational-incident
// normalization input shared with Python.
type OperationalIncidentRecord struct {
	Provider           string    `json:"provider"`
	OrgID              string    `json:"org_id"`
	ProviderInstanceID string    `json:"provider_instance_id"`
	SourceEntityType   string    `json:"source_entity_type"`
	ExternalID         string    `json:"external_id"`
	SourceVersionAt    time.Time `json:"source_version_at"`
	ObservedAt         time.Time `json:"observed_at"`
	Title              string    `json:"title"`
	NormalizedStatus   *string   `json:"normalized_status"`
}

var workItemProviders = map[string]struct{}{
	"github": {}, "gitlab": {}, "jira": {}, "linear": {},
}

var workItemTypes = map[string]struct{}{
	"story": {}, "task": {}, "bug": {}, "epic": {}, "pr": {},
	"merge_request": {}, "issue": {}, "incident": {}, "chore": {}, "unknown": {},
}

var workItemStatuses = map[string]struct{}{
	"backlog": {}, "todo": {}, "in_progress": {}, "in_review": {},
	"blocked": {}, "done": {}, "canceled": {}, "unknown": {},
}

var canonicalOperationalStatuses = map[string]struct{}{
	"active": {}, "open": {}, "acknowledged": {}, "resolved": {},
	"closed": {}, "suppressed": {},
}

var sourceRecordEntityTypes = map[string]struct{}{
	"repository":           {},
	"work_item_label":      {},
	"work_item_project":    {},
	"work_item_transition": {},
	"work_item_comment":    {},
}

func NormalizeWorkItem(context NormalizationContext, item WorkItemRecord) (NormalizedEnvelope, error) {
	if _, ok := workItemProviders[item.Provider]; !ok {
		return NormalizedEnvelope{}, ErrNormalizationInvalid
	}
	if _, ok := workItemTypes[item.Type]; !ok {
		return NormalizedEnvelope{}, ErrNormalizationInvalid
	}
	if _, ok := workItemStatuses[item.Status]; !ok {
		return NormalizedEnvelope{}, ErrNormalizationInvalid
	}
	if item.WorkItemID == "" || item.Title == "" || item.UpdatedAt.IsZero() {
		return NormalizedEnvelope{}, ErrNormalizationInvalid
	}
	attributes := map[string]string{
		"status":        item.Status,
		"title":         item.Title,
		"type":          item.Type,
		"work_scope_id": workItemScope(item),
	}
	addOptionalAttribute(attributes, "status_raw", item.StatusRaw)
	return normalizedEnvelope(context, item.Provider, item.OrgID, "work_item", item.WorkItemID, item.UpdatedAt, attributes)
}

func NormalizeFeatureFlag(context NormalizationContext, flag FeatureFlagRecord) (NormalizedEnvelope, error) {
	if (flag.Provider != "launchdarkly" && flag.Provider != "gitlab") || flag.FlagKey == "" || flag.Environment == "" || flag.LastSynced.IsZero() {
		return NormalizedEnvelope{}, ErrNormalizationInvalid
	}
	attributes := map[string]string{"environment": flag.Environment}
	addOptionalAttribute(attributes, "flag_type", flag.FlagType)
	addOptionalAttribute(attributes, "project_key", flag.ProjectKey)
	return normalizedEnvelope(context, flag.Provider, flag.OrgID, "feature_flag", flag.FlagKey, flag.LastSynced, attributes)
}

func NormalizeSourceRecord(context NormalizationContext, record SourceRecord) (NormalizedEnvelope, error) {
	if _, ok := workItemProviders[record.Provider]; !ok {
		return NormalizedEnvelope{}, ErrNormalizationInvalid
	}
	if _, ok := sourceRecordEntityTypes[record.EntityType]; !ok ||
		record.SourceID == "" || record.ObservedAt.IsZero() {
		return NormalizedEnvelope{}, ErrNormalizationInvalid
	}
	attributes := make(map[string]string, len(record.Attributes))
	for key, value := range record.Attributes {
		if strings.TrimSpace(key) == "" {
			return NormalizedEnvelope{}, ErrNormalizationInvalid
		}
		attributes[key] = value
	}
	return normalizedEnvelope(
		context,
		record.Provider,
		record.OrgID,
		record.EntityType,
		record.SourceID,
		record.ObservedAt,
		attributes,
	)
}

func NormalizeOperationalService(context NormalizationContext, service OperationalServiceRecord) (NormalizedEnvelope, error) {
	common := operationalRecord{
		Provider: service.Provider, OrgID: service.OrgID,
		ProviderInstanceID: service.ProviderInstanceID, SourceEntityType: service.SourceEntityType,
		ExternalID: service.ExternalID, SourceVersionAt: service.SourceVersionAt,
		ObservedAt: service.ObservedAt, NormalizedStatus: service.NormalizedStatus,
	}
	sourceID, err := validateOperationalRecord(common, "operational_service")
	if err != nil || service.Name == "" {
		return NormalizedEnvelope{}, ErrNormalizationInvalid
	}
	attributes := map[string]string{
		"external_id":          service.ExternalID,
		"name":                 service.Name,
		"provider_instance_id": service.ProviderInstanceID,
	}
	addOptionalAttribute(attributes, "normalized_status", service.NormalizedStatus)
	return normalizedEnvelope(context, service.Provider, service.OrgID, "operational_service", sourceID, service.ObservedAt, attributes)
}

func NormalizeOperationalIncident(context NormalizationContext, incident OperationalIncidentRecord) (NormalizedEnvelope, error) {
	common := operationalRecord{
		Provider: incident.Provider, OrgID: incident.OrgID,
		ProviderInstanceID: incident.ProviderInstanceID, SourceEntityType: incident.SourceEntityType,
		ExternalID: incident.ExternalID, SourceVersionAt: incident.SourceVersionAt,
		ObservedAt: incident.ObservedAt, NormalizedStatus: incident.NormalizedStatus,
	}
	sourceID, err := validateOperationalRecord(common, "operational_incident")
	if err != nil || incident.Title == "" {
		return NormalizedEnvelope{}, ErrNormalizationInvalid
	}
	attributes := map[string]string{
		"external_id":          incident.ExternalID,
		"provider_instance_id": incident.ProviderInstanceID,
		"title":                incident.Title,
	}
	addOptionalAttribute(attributes, "normalized_status", incident.NormalizedStatus)
	return normalizedEnvelope(context, incident.Provider, incident.OrgID, "operational_incident", sourceID, incident.ObservedAt, attributes)
}

func normalizedEnvelope(
	context NormalizationContext,
	provider, orgID, entityType, sourceID string,
	observedAt time.Time,
	attributes map[string]string,
) (NormalizedEnvelope, error) {
	envelope := NormalizedEnvelope{
		SchemaVersion: "v1",
		Provider:      provider,
		OrgID:         orgID,
		IntegrationID: context.IntegrationID,
		EntityType:    entityType,
		SourceID:      sourceID,
		DedupeKey:     strings.Join([]string{provider, entityType, sourceID}, ":"),
		ObservedAt:    observedAt,
		Provenance:    context.Provenance,
		Attributes:    attributes,
	}
	if err := envelope.Validate(); err != nil {
		return NormalizedEnvelope{}, ErrNormalizationInvalid
	}
	return envelope, nil
}

func workItemScope(item WorkItemRecord) string {
	if item.Provider == "jira" && optionalString(item.ProjectKey) != "" {
		return optionalString(item.ProjectKey)
	}
	for _, value := range []*string{item.ProjectID, item.ProjectName, item.NativeTeamKey, item.ProjectKey} {
		if result := optionalString(value); result != "" {
			return result
		}
	}
	return ""
}

func validateOperationalRecord(record operationalRecord, family string) (string, error) {
	if record.SourceEntityType == "" || record.SourceVersionAt.IsZero() || record.ObservedAt.IsZero() {
		return "", ErrNormalizationInvalid
	}
	if record.NormalizedStatus != nil {
		if _, ok := canonicalOperationalStatuses[*record.NormalizedStatus]; !ok {
			return "", ErrNormalizationInvalid
		}
	}
	return CanonicalOperationalID(record.OrgID, record.Provider, record.ProviderInstanceID, family, record.ExternalID)
}

func addOptionalAttribute(attributes map[string]string, name string, value *string) {
	if value != nil {
		attributes[name] = *value
	}
}

func optionalString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// CanonicalOperationalID mirrors Python canonical_operational_id, including
// ensure_ascii JSON escaping, so both runtimes derive the same immutable ID.
func CanonicalOperationalID(orgID, provider, providerInstanceID, entityFamily, externalID string) (string, error) {
	components := []string{orgID, provider, providerInstanceID, entityFamily, externalID}
	var seed strings.Builder
	seed.WriteByte('[')
	for index, component := range components {
		if component == "" || !utf8.ValidString(component) {
			return "", ErrNormalizationInvalid
		}
		if index > 0 {
			seed.WriteByte(',')
		}
		appendASCIIJSONString(&seed, component)
	}
	seed.WriteByte(']')
	digest := sha256.Sum256([]byte(seed.String()))
	return hex.EncodeToString(digest[:]), nil
}

func appendASCIIJSONString(target *strings.Builder, value string) {
	const hexDigits = "0123456789abcdef"
	target.WriteByte('"')
	for _, char := range value {
		switch char {
		case '"', '\\':
			target.WriteByte('\\')
			target.WriteRune(char)
		case '\b':
			target.WriteString(`\b`)
		case '\f':
			target.WriteString(`\f`)
		case '\n':
			target.WriteString(`\n`)
		case '\r':
			target.WriteString(`\r`)
		case '\t':
			target.WriteString(`\t`)
		default:
			switch {
			case char >= 0x20 && char <= 0x7e:
				target.WriteRune(char)
			case char <= 0xffff:
				target.WriteString(`\u`)
				for shift := 12; shift >= 0; shift -= 4 {
					target.WriteByte(hexDigits[(char>>shift)&0xf])
				}
			default:
				codepoint := char - 0x10000
				for _, surrogate := range []rune{0xd800 + codepoint>>10, 0xdc00 + codepoint&0x3ff} {
					target.WriteString(`\u`)
					for shift := 12; shift >= 0; shift -= 4 {
						target.WriteByte(hexDigits[(surrogate>>shift)&0xf])
					}
				}
			}
		}
	}
	target.WriteByte('"')
}
