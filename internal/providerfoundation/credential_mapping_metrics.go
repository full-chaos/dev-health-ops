package providerfoundation

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// credentialMappingRejectedName is the Python api's
// CREDENTIAL_MAPPING_REJECTED_TOTAL (metrics/prometheus.py): stored
// credentials a provider resolver refused to build, by the field it could not
// find. The dho api exports it through internal/api/apimetrics, like every
// counter its Python api served on /metrics.
const credentialMappingRejectedName = "credential_mapping_rejected_total"

var credentialMappingRejected = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/providerfoundation").Int64Counter(
		credentialMappingRejectedName,
		metric.WithDescription("Stored credentials a provider resolver refused to build, by the field it could not find."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(credentialMappingRejectedName)
	}
	return counter
}()

// MappingField is one required field of a credential mapping, in the order
// the resolver checks them, and whether the mapping resolved a non-empty
// value for it. Presence only: the value itself never reaches this package.
type MappingField struct {
	Name    string
	Present bool
}

// RecordCredentialMappingRejected is resolver.py's _record_mapping_rejected:
// it counts one rejected mapping against the first required field that is
// empty ("unknown" when none is). The field name is the caller's own fixed
// vocabulary, never credential contents, so the label cannot carry secret
// material or grow without bound. It is the one implementation every Go
// resolver of a provider mapping calls, the API's connection probe included.
func RecordCredentialMappingRejected(ctx context.Context, provider string, fields ...MappingField) {
	missing := "unknown"
	for _, field := range fields {
		if !field.Present {
			missing = field.Name
			break
		}
	}
	credentialMappingRejected.Add(ctx, 1, metric.WithAttributes(
		attribute.String("provider", provider), attribute.String("missing_field", missing)))
}

// jiraMappingFields are jira_credentials_from_mapping's three required
// fields, in its order, as the credential resolves them: the aliased API
// token, the email and the aliased base URL.
func jiraMappingFields(credential Credential) []MappingField {
	token := firstConfiguredSecret(credential, jiraAPITokenAliases)
	email, _ := credential.Secret("email")
	return []MappingField{
		{Name: "api_token", Present: token.Configured()},
		{Name: "email", Present: email.Configured()},
		{Name: "base_url", Present: jiraCredentialBaseURL(credential) != ""},
	}
}

// mappingComplete reports whether every required field is present.
func mappingComplete(fields []MappingField) bool {
	for _, field := range fields {
		if !field.Present {
			return false
		}
	}
	return true
}
