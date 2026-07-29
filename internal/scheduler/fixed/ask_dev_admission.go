package fixed

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const askDevFeatureKey = "ask_dev"

// AskDevRetentionState separates product availability from lifecycle duty.
// Disabling Ask Dev stops new use, but it must never strand content that was
// persisted while the feature was enabled.
type AskDevRetentionState struct {
	FeatureEnabled    bool
	HasPersistedState bool
}

// Eligible reports whether the Ask Dev cleanup schedule still has a lifecycle
// obligation. Existing state deliberately wins over a disabled feature.
func (state AskDevRetentionState) Eligible() bool {
	return state.FeatureEnabled || state.HasPersistedState
}

// AskDevRetentionAdmission reads the two server-owned facts that admit the
// native Ask Dev retention schedule. It runs inside the occurrence transaction
// so admission and publication observe one PostgreSQL snapshot.
type AskDevRetentionAdmission interface {
	State(context.Context, pgx.Tx) (AskDevRetentionState, error)
}

type postgresAskDevRetentionAdmission struct{}

// NewPostgresAskDevRetentionAdmission constructs the production admission
// reader. The engine supplies its coordinator transaction per occurrence.
func NewPostgresAskDevRetentionAdmission() AskDevRetentionAdmission {
	return postgresAskDevRetentionAdmission{}
}

// askDevRetentionAdmissionSQL mirrors the canonical Ask Dev feature decision
// for this explicit-enable feature only: global availability and valid storage
// are required; an active organization override wins; otherwise an explicit
// license override may enable it. Tier membership alone never enables Ask Dev.
//
// The second EXISTS is intentionally independent. Once any conversation is
// stored, retention continues even if the feature row is disabled, removed, or
// overridden off during rollback.
const askDevRetentionAdmissionSQL = `
WITH ask_dev_feature AS (
	SELECT id
	FROM feature_flags
	WHERE key = $1
	  AND is_enabled = TRUE
	  AND min_tier IN ('community', 'team', 'enterprise')
), enabled_organization AS (
	SELECT 1
	FROM ask_dev_feature AS feature
	JOIN organizations AS organization ON TRUE
	LEFT JOIN org_licenses AS license
	  ON license.org_id = organization.id
	LEFT JOIN org_feature_overrides AS feature_override
	  ON feature_override.org_id = organization.id
	 AND feature_override.feature_id = feature.id
	WHERE CASE
		WHEN feature_override.id IS NOT NULL
		 AND (feature_override.expires_at IS NULL OR feature_override.expires_at > CURRENT_TIMESTAMP)
		THEN feature_override.is_enabled
		WHEN jsonb_typeof((license.features_override::jsonb) -> $1) = 'boolean'
		THEN ((license.features_override::jsonb) ->> $1)::boolean
		ELSE FALSE
	END
	LIMIT 1
)
SELECT EXISTS (SELECT 1 FROM enabled_organization),
	EXISTS (SELECT 1 FROM dev_conversations LIMIT 1)
`

func (postgresAskDevRetentionAdmission) State(
	ctx context.Context,
	tx pgx.Tx,
) (AskDevRetentionState, error) {
	if ctx == nil || tx == nil {
		return AskDevRetentionState{}, ErrProducerUnavailable
	}
	var state AskDevRetentionState
	if err := tx.QueryRow(ctx, askDevRetentionAdmissionSQL, askDevFeatureKey).Scan(
		&state.FeatureEnabled,
		&state.HasPersistedState,
	); err != nil {
		return AskDevRetentionState{}, fmt.Errorf("load Ask Dev retention admission: %w", err)
	}
	return state, nil
}

type disabledAskDevRetentionAdmission struct{}

func (disabledAskDevRetentionAdmission) State(
	context.Context,
	pgx.Tx,
) (AskDevRetentionState, error) {
	return AskDevRetentionState{}, nil
}
