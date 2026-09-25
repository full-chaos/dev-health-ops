package integrationsadmin

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// errStoredJSON is a stored JSON column that Python's dict(value or {}) does
// not turn into a dict the way a JSON object does (a non-empty list or
// scalar): the Python route answers its unhandled 500 for most such values.
// A row like that is only writable outside the api (CHAOS-6286 is the same
// class in the sync admin).
var errStoredJSON = errors.New("stored JSON column is not an object")

// storedDict is dict(getattr(row, column) or {}) over the column's JSON
// text: an object is itself, a falsy value is {}.
func storedDict(text string) (*pyjson.Object, error) {
	value, err := pyjson.DecodeString(text)
	if err != nil {
		return nil, err
	}
	if object, ok := value.(*pyjson.Object); ok {
		return object, nil
	}
	if !pyjson.Truthy(value) {
		return pyjson.NewObject(), nil
	}
	return nil, errStoredJSON
}

func timeText(at time.Time) string { return pytime.Pydantic(pytime.UTC(at)) }

func timeOrNull(at *time.Time) pyjson.Value {
	if at == nil {
		return nil
	}
	return timeText(*at)
}

func textOrNull(text *string) pyjson.Value {
	if text == nil {
		return nil
	}
	return *text
}

func uuidOrNull(id *uuid.UUID) pyjson.Value {
	if id == nil {
		return nil
	}
	return id.String()
}

// integrationObject is IntegrationResponse in field order.
func integrationObject(row integration) (*pyjson.Object, error) {
	return integrationObjectWith(row, nil)
}

// integrationObjectWith renders row with config in place of its stored one
// when config is not nil (an update's in-memory value).
func integrationObjectWith(row integration, override *pyjson.Object) (*pyjson.Object, error) {
	config := override
	if config == nil {
		stored, err := storedDict(row.Config)
		if err != nil {
			return nil, err
		}
		config = stored
	}
	out := pyjson.NewObject()
	out.Set("id", row.ID.String())
	out.Set("org_id", row.OrgID)
	out.Set("provider", row.Provider)
	out.Set("credential_id", uuidOrNull(row.CredentialID))
	out.Set("name", row.Name)
	out.Set("config", config)
	out.Set("is_active", row.IsActive)
	out.Set("schedule_cron", textOrNull(row.ScheduleCron))
	out.Set("timezone", textOrNull(row.Timezone))
	out.Set("created_at", timeText(row.CreatedAt))
	out.Set("updated_at", timeText(row.UpdatedAt))
	return out, nil
}

// sourceObject is IntegrationSourceResponse in field order (metadata is the
// field's alias, which the response writes).
func sourceObject(row source) (*pyjson.Object, error) {
	metadata, err := storedDict(row.Metadata)
	if err != nil {
		return nil, err
	}
	out := pyjson.NewObject()
	out.Set("id", row.ID.String())
	out.Set("org_id", row.OrgID)
	out.Set("integration_id", row.IntegrationID.String())
	out.Set("provider", row.Provider)
	out.Set("source_type", row.SourceType)
	out.Set("external_id", row.ExternalID)
	out.Set("name", row.Name)
	out.Set("full_name", row.FullName)
	out.Set("metadata", metadata)
	out.Set("is_enabled", row.IsEnabled)
	out.Set("discovered_at", timeText(row.DiscoveredAt))
	out.Set("last_seen_at", timeText(row.LastSeenAt))
	out.Set("last_sync_at", timeOrNull(row.LastSyncAt))
	if row.LastSyncSuccess == nil {
		out.Set("last_sync_success", nil)
	} else {
		out.Set("last_sync_success", *row.LastSyncSuccess)
	}
	out.Set("last_sync_error", textOrNull(row.LastSyncError))
	return out, nil
}

// datasetObject is IntegrationDatasetResponse in field order.
func datasetObject(row dataset) (*pyjson.Object, error) {
	options, err := storedDict(row.Options)
	if err != nil {
		return nil, err
	}
	out := pyjson.NewObject()
	out.Set("id", row.ID.String())
	out.Set("org_id", row.OrgID)
	out.Set("integration_id", row.IntegrationID.String())
	out.Set("dataset_key", row.DatasetKey)
	out.Set("is_enabled", row.IsEnabled)
	out.Set("options", options)
	out.Set("unavailable_reason", textOrNull(row.UnavailableReason))
	out.Set("unavailable_since", timeOrNull(row.UnavailableSince))
	out.Set("unavailable_last_seen_at", timeOrNull(row.UnavailableLastSeenAt))
	return out, nil
}
