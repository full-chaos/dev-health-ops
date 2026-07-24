package providersync

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// ClickHouseRepositoryShadowSource projects the Python-owned repos table into
// the normalized envelope used by the native repo-metadata shadow. The Python
// sink intentionally does not persist every provider response field (for
// example archived), so this adapter and the native envelope compare only the
// shared persisted contract: name, URL, and default branch.
type ClickHouseRepositoryShadowSource struct {
	Conn driver.Conn
}

func (source ClickHouseRepositoryShadowSource) Load(
	ctx context.Context,
	claim Claim,
) ([]providerfoundation.NormalizedEnvelope, error) {
	if ctx == nil || source.Conn == nil || claim.Validate() != nil ||
		claim.Dataset != "repo-metadata" {
		return nil, ErrInvalidConfiguration
	}
	repositoryName := claim.SourceExternalID
	if claim.Provider == "gitlab" {
		repositoryName = claim.SourceName
	}
	if repositoryName == "" {
		return nil, ErrInvalidConfiguration
	}
	rows, err := source.Conn.Query(ctx, `
SELECT repo,
       coalesce(argMax(settings, last_synced), '{}') AS settings_json,
       max(last_synced) AS observed_at
FROM repos
WHERE org_id = ? AND provider = ? AND repo = ?
GROUP BY repo
LIMIT 2`,
		claim.OrgID, claim.Provider, repositoryName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var envelopes []providerfoundation.NormalizedEnvelope
	for rows.Next() {
		var name, settingsJSON string
		var observedAt time.Time
		if err := rows.Scan(&name, &settingsJSON, &observedAt); err != nil {
			return nil, err
		}
		settings := map[string]any{}
		if json.Unmarshal([]byte(settingsJSON), &settings) != nil {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		attributes := map[string]string{"name": name, "default_branch": ""}
		for sourceName, targetName := range map[string]string{
			"default_branch": "default_branch",
			"url":            "url",
		} {
			if value, ok := settings[sourceName].(string); ok && value != "" {
				attributes[targetName] = value
			}
		}
		sourceID := claim.Provider + ":repo:" + claim.SourceExternalID
		if claim.Provider == "gitlab" {
			sourceID = "gitlab:project:" + claim.SourceExternalID
		}
		envelope, err := providerfoundation.NormalizeSourceRecord(
			normalizationContext(claim),
			providerfoundation.SourceRecord{
				Provider: claim.Provider, OrgID: claim.OrgID,
				EntityType: "repository", SourceID: sourceID,
				ObservedAt: observedAt.UTC(), Attributes: attributes,
			},
		)
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, envelope)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(envelopes) != 1 {
		return nil, ErrShadowMismatch
	}
	return envelopes, nil
}

var _ ShadowSource = ClickHouseRepositoryShadowSource{}
