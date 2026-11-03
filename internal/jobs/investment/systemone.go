package investment

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
)

// SystemOne is the one interface the categorization call path of a run uses
// for a backend that answers on the SystemOne (decisions) call shape. jev, the
// served decision backend, is its only implementation.
type SystemOne interface {
	categorize(ctx context.Context, cfg Config, entry preprocessed) (categorize.CategorizationOutcome, error)
	// Stamp is the model-version stamp of the rows, and their skip-existing key.
	Stamp() string
	// Identity is the provider and model of the llm_token_usage row.
	Identity() (provider, model string)
	// CredentialSource is chwrite.CredentialSourceOrg when the backend calls the org's own model, else platform.
	CredentialSource() string
	keepsLastRow(index int) bool
	isLowQuality(index int) bool
	noteSkippedTerminal(count int)
	finish(ctx context.Context, writer *chwrite.Writer, cfg Config)
	SetObserver(observer ServedObserver)
	Close() error
}

var _ SystemOne = (*ServedDecision)(nil)
