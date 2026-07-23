package providersync

import (
	"context"
	"encoding/json"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type ShadowSource interface {
	Load(context.Context, Claim) ([]providerfoundation.NormalizedEnvelope, error)
}

type ShadowComparison struct {
	Match           bool
	NativeRecords   int
	PythonRecords   int
	MissingNative   int
	MissingPython   int
	ContentMismatch int
}

type ShadowComparator interface {
	Compare(context.Context, Claim, []providerfoundation.NormalizedEnvelope) (ShadowComparison, error)
}

type NormalizedShadowComparator struct{ Python ShadowSource }

func (comparator NormalizedShadowComparator) Compare(
	ctx context.Context,
	claim Claim,
	native []providerfoundation.NormalizedEnvelope,
) (ShadowComparison, error) {
	if comparator.Python == nil {
		return ShadowComparison{}, ErrInvalidConfiguration
	}
	python, err := comparator.Python.Load(ctx, claim)
	if err != nil {
		return ShadowComparison{}, err
	}
	nativeRecords, err := comparableEnvelopes(native)
	if err != nil {
		return ShadowComparison{}, err
	}
	pythonRecords, err := comparableEnvelopes(python)
	if err != nil {
		return ShadowComparison{}, err
	}
	result := ShadowComparison{NativeRecords: len(nativeRecords), PythonRecords: len(pythonRecords)}
	for key, nativeValue := range nativeRecords {
		pythonValue, ok := pythonRecords[key]
		if !ok {
			result.MissingPython++
			continue
		}
		if nativeValue != pythonValue {
			result.ContentMismatch++
		}
	}
	for key := range pythonRecords {
		if _, ok := nativeRecords[key]; !ok {
			result.MissingNative++
		}
	}
	result.Match = result.MissingNative == 0 && result.MissingPython == 0 && result.ContentMismatch == 0
	return result, nil
}

type comparableEnvelope struct {
	SchemaVersion string                        `json:"schema_version"`
	Provider      string                        `json:"provider"`
	OrgID         string                        `json:"org_id"`
	IntegrationID string                        `json:"integration_id"`
	EntityType    string                        `json:"entity_type"`
	SourceID      string                        `json:"source_id"`
	DedupeKey     string                        `json:"dedupe_key"`
	Provenance    providerfoundation.Provenance `json:"provenance"`
	Attributes    map[string]string             `json:"attributes"`
}

func comparableEnvelopes(input []providerfoundation.NormalizedEnvelope) (map[string]string, error) {
	records := make(map[string]string, len(input))
	for _, envelope := range input {
		if err := envelope.Validate(); err != nil {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		// observed_at is an ingestion observation, not provider entity content.
		// Python and Go may observe the same immutable record at different
		// instants, so exact comparison would report false semantic drift.
		encoded, err := json.Marshal(comparableEnvelope{
			SchemaVersion: envelope.SchemaVersion,
			Provider:      envelope.Provider,
			OrgID:         envelope.OrgID,
			IntegrationID: envelope.IntegrationID,
			EntityType:    envelope.EntityType,
			SourceID:      envelope.SourceID,
			DedupeKey:     envelope.DedupeKey,
			Provenance:    envelope.Provenance,
			Attributes:    envelope.Attributes,
		})
		if err != nil {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		if prior, exists := records[envelope.DedupeKey]; exists && prior != string(encoded) {
			return nil, providerfoundation.ErrSinkDuplicate
		}
		records[envelope.DedupeKey] = string(encoded)
	}
	return records, nil
}

type BackoffGateFactory func(Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate
type GenerationSinkFactory func(providerfoundation.LeaseGuard) providerfoundation.GenerationSink

type Executor struct {
	Credentials       providerfoundation.CredentialResolver
	Doer              providerfoundation.HTTPDoer
	Retry             providerfoundation.RetryPolicy
	Budget            providerfoundation.BudgetStore
	BudgetLimits      map[CostClass]int
	BudgetTTL         time.Duration
	Gate              BackoffGateFactory
	Metrics           *providerfoundation.Metrics
	Handler           DatasetHandler
	Comparator        ShadowComparator
	Journal           GenerationJournal
	Sink              GenerationSinkFactory
	Destination       string
	HeartbeatInterval time.Duration
	Now               func() time.Time
}

type ExecutionResult struct {
	Fetch         FetchEvidence
	Comparison    ShadowComparison
	BlocksWritten int
	BlocksSkipped int
	ShadowOnly    bool
}

func (executor Executor) now() time.Time {
	if executor.Now != nil {
		return executor.Now().UTC()
	}
	return time.Now().UTC()
}

func (executor Executor) Execute(
	ctx context.Context,
	session *LeaseSession,
	descriptor ExecutionDescriptor,
) (ExecutionResult, error) {
	if ctx == nil || session == nil || !session.valid() ||
		descriptor.Provider != session.Claim.Provider ||
		descriptor.Dataset != session.Claim.Dataset ||
		!descriptor.NativeShadow ||
		executor.Doer == nil || executor.Handler == nil || executor.Comparator == nil ||
		executor.HeartbeatInterval <= 0 {
		return ExecutionResult{}, ErrInvalidConfiguration
	}
	if descriptor.RouteEnabled &&
		(executor.Budget == nil || executor.Gate == nil || executor.Journal == nil ||
			executor.Sink == nil || executor.Destination == "") {
		return ExecutionResult{}, ErrInvalidConfiguration
	}
	var result ExecutionResult
	err := session.Run(ctx, executor.HeartbeatInterval, func(
		workContext context.Context,
		guard providerfoundation.LeaseGuard,
	) error {
		credential, err := executor.Credentials.Resolve(
			workContext,
			guard,
			session.Claim.TenantScope(),
		)
		if err != nil {
			return err
		}
		client, err := executor.newClient(credential, guard)
		if err != nil {
			return err
		}
		if descriptor.RouteEnabled {
			limit := executor.BudgetLimits[session.Claim.CostClass]
			if limit < 1 || executor.BudgetTTL <= 0 {
				return ErrInvalidConfiguration
			}
			client.Budget = executor.Budget
			client.BudgetKey = providerfoundation.BudgetKey{
				Provider: session.Claim.Provider, OrgID: session.Claim.OrgID,
				Host: client.BaseURL.Hostname(), CostClass: string(session.Claim.CostClass),
				Limit: limit, TTL: executor.BudgetTTL,
			}
			client.Gate = executor.Gate(session.Claim, client)
			if client.Gate == nil {
				return ErrInvalidConfiguration
			}
		}
		client.Metrics = executor.Metrics
		fetched, err := executor.Handler.Fetch(workContext, session.Claim, client)
		if err != nil {
			return err
		}
		result.Fetch = fetched.Evidence
		comparison, err := executor.Comparator.Compare(workContext, session.Claim, fetched.Envelopes)
		if err != nil {
			return err
		}
		result.Comparison = comparison
		if !comparison.Match {
			return ErrShadowMismatch
		}
		if !descriptor.RouteEnabled {
			result.ShadowOnly = true
			return nil
		}
		if len(fetched.Envelopes) == 0 {
			return nil
		}
		blocks, err := providerfoundation.BuildGenerationBlocks(
			session.Claim.GenerationKey(), executor.Destination, fetched.Envelopes,
		)
		if err != nil {
			return err
		}
		desired, err := NewGenerationJournalState(blocks, executor.now())
		if err != nil {
			return err
		}
		persisted, err := executor.Journal.Prepare(workContext, session.Claim, desired, executor.now())
		if err != nil {
			return err
		}
		sink := executor.Sink(guard)
		if sink == nil {
			return ErrInvalidConfiguration
		}
		for index, block := range blocks {
			switch persisted.Blocks[index].Status {
			case GenerationBlockCommitted:
				result.BlocksSkipped++
				continue
			case GenerationBlockWriting:
				// A process may have died after ClickHouse accepted this block.
				// Never trust the finite dedupe window for a late retry.
				return ErrGenerationBlockAmbiguous
			case GenerationBlockPending:
			default:
				return ErrGenerationJournalConflict
			}
			if err := executor.Journal.BeginBlock(
				workContext, session.Claim, block.Index(), block.ContentDigest(), executor.now(),
			); err != nil {
				return err
			}
			if err := sink.WriteGenerationBlock(workContext, block); err != nil {
				return err
			}
			if err := executor.Journal.CommitBlock(
				workContext, session.Claim, block.Index(), block.ContentDigest(), executor.now(),
			); err != nil {
				return err
			}
			result.BlocksWritten++
		}
		return nil
	})
	return result, err
}

func (executor Executor) newClient(
	credential providerfoundation.Credential,
	guard providerfoundation.LeaseGuard,
) (*providerfoundation.HTTPClient, error) {
	retry := executor.Retry
	switch credential.Provider {
	case "github":
		return providerfoundation.NewGitHubClient(credential, executor.Doer, retry, guard)
	case "gitlab":
		return providerfoundation.NewGitLabClient(credential, executor.Doer, retry, guard)
	default:
		return nil, ErrInvalidConfiguration
	}
}

var _ ShadowComparator = NormalizedShadowComparator{}
