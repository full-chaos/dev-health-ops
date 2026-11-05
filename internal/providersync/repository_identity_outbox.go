package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/repokey"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// EnvRepositoryIdentityOutbox switches the repository identity producer: on, the
// commit of a `repos` effect appends one repository_identity.observed event per
// repository with a provider id to the auth outbox, in the ledger's transaction.
const EnvRepositoryIdentityOutbox = "WORKER_REPO_IDENTITY_OUTBOX"

// repositoryIdentityDestination is the one effect destination that carries
// repository identities.
const repositoryIdentityDestination = "repos"

// ParseRepositoryIdentityOutbox reads EnvRepositoryIdentityOutbox: unset or
// empty is off; any other value must parse as a bool.
func ParseRepositoryIdentityOutbox(lookup func(string) (string, bool)) (bool, error) {
	raw, ok := lookup(EnvRepositoryIdentityOutbox)
	if !ok || strings.TrimSpace(raw) == "" {
		return false, nil
	}
	on, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", EnvRepositoryIdentityOutbox)
	}
	return on, nil
}

// RepositoryIdentityOutbox is the auth schema whose append function receives
// the events. A PostgresRepository without one appends nothing.
type RepositoryIdentityOutbox struct {
	Schema authschema.ValidatedIdentifier
}

func (outbox *RepositoryIdentityOutbox) append(ctx context.Context, tx pgx.Tx, events []audit.RepositoryIdentityEvent) error {
	if len(events) == 0 {
		return nil
	}
	return audit.AppendRepositoryIdentity(ctx, tx, outbox.Schema, events)
}

// repositoryIdentityPath is the canonical repository key of a provider path:
// spaces trimmed and lower case, as the backfill reads ClickHouse
// (lowerUTF8(trimBoth(repo))). Only ASCII is accepted, so Go and ClickHouse
// case mapping cannot disagree. A path that is not a repository path is "",
// which the event check refuses.
func repositoryIdentityPath(repo string) string {
	trimmed := strings.Trim(repo, " ")
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] >= 0x80 {
			return ""
		}
	}
	path := strings.ToLower(trimmed)
	if !repokey.Valid(path) {
		return ""
	}
	return path
}

type repositoryIdentityRow struct {
	OrgID          string    `json:"org_id"`
	Repo           string    `json:"repo"`
	Provider       string    `json:"provider"`
	LastSynced     time.Time `json:"last_synced"`
	ProviderRepoID string    `json:"provider_repo_id"`
}

// repositoryIdentityEvents builds the events of one effect batch. Only the
// `repos` destination carries identities. A row without a provider id (local
// git, External Push) gives no event; a row whose organization, provider, id or
// path is malformed is skipped and counted, as the backfill skips it.
func repositoryIdentityEvents(batch EffectBatch) ([]audit.RepositoryIdentityEvent, int, error) {
	if batch.Destination != repositoryIdentityDestination {
		return nil, 0, nil
	}
	events := make([]audit.RepositoryIdentityEvent, 0, len(batch.Rows))
	skipped := 0
	for _, raw := range batch.Rows {
		var row repositoryIdentityRow
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, 0, ErrEffectRecoveryUnsafe
		}
		if row.ProviderRepoID == "" {
			continue
		}
		// A malformed organization parses to uuid.Nil, which Validate refuses.
		org, _ := uuid.Parse(row.OrgID)
		event := audit.RepositoryIdentityEvent{
			OrganizationID: org, Provider: row.Provider, ProviderRepoID: row.ProviderRepoID,
			Path: repositoryIdentityPath(row.Repo), ObservedAt: row.LastSynced.UTC(),
		}
		if event.Validate() != nil {
			skipped++
			continue
		}
		events = append(events, event)
	}
	return events, skipped, nil
}

// appendRepositoryIdentities runs in the transaction that flips entry to
// Committed: it appends the identity events of batch, which must be the batch
// of that effect (same content digest). It appends nothing when the repository
// has no outbox or the batch is not a `repos` batch.
func (repository *PostgresRepository) appendRepositoryIdentities(
	ctx context.Context,
	tx pgx.Tx,
	claim Claim,
	entry EffectLedgerEntry,
	batch EffectBatch,
) error {
	if repository.RepositoryIdentities == nil {
		return nil
	}
	if batch.ContentDigest != entry.ContentDigest {
		return ErrEffectLedgerConflict
	}
	events, skipped, err := repositoryIdentityEvents(batch)
	if err != nil {
		return err
	}
	if skipped > 0 {
		slog.Default().WarnContext(ctx, "repository_identity_rows_skipped",
			"provider", claim.Provider, "dataset", claim.Dataset, "skipped", skipped, "events", len(events))
	}
	return repository.RepositoryIdentities.append(ctx, tx, events)
}
