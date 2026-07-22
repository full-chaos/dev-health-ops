package joboutbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

var schemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

type riverClient interface {
	InsertTx(context.Context, pgx.Tx, river.JobArgs, *river.InsertOpts) (*rivertype.JobInsertResult, error)
}

// RiverInserter validates an immutable outbox row against the checked-in
// registry, then uses River's supported transactional API.
type RiverInserter struct {
	client   riverClient
	registry PolicyRegistry
}

func NewRiverInserter(pool *pgxpool.Pool, schema string, registry PolicyRegistry) (*RiverInserter, error) {
	if pool == nil || registry == nil || !schemaPattern.MatchString(schema) {
		return nil, ErrInvalidConfiguration
	}
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{Schema: schema})
	if err != nil {
		return nil, ErrInvalidConfiguration
	}
	return &RiverInserter{client: client, registry: registry}, nil
}

func (inserter *RiverInserter) Insert(ctx context.Context, tx pgx.Tx, row Row) (int64, error) {
	if inserter == nil || inserter.client == nil || inserter.registry == nil || tx == nil {
		return 0, ErrInvalidConfiguration
	}
	descriptor, args, err := prepareRow(inserter.registry, row)
	if err != nil {
		return 0, err
	}
	metadata, err := json.Marshal(relayMetadata{
		WorkerOutboxID:  row.ID,
		PayloadHash:     row.PayloadHash,
		ContractVersion: row.ContractVersion,
	})
	if err != nil {
		return 0, ErrContractRejected
	}
	result, err := inserter.client.InsertTx(ctx, tx, args, &river.InsertOpts{
		Queue:       descriptor.Queue,
		Priority:    descriptor.Priority,
		MaxAttempts: descriptor.MaxAttempts,
		ScheduledAt: row.ScheduledAt.UTC(),
		Metadata:    metadata,
		UniqueOpts: river.UniqueOpts{
			ByArgs:  true,
			ByState: rivertype.JobStates(),
		},
	})
	if err != nil {
		return 0, ErrRiverInsert
	}
	if err := verifyInsertResult(result, row, descriptor); err != nil {
		return 0, err
	}
	return result.Job.ID, nil
}

type relayArgs struct {
	ContractVersion int                    `json:"contract_version"`
	OrganizationID  *string                `json:"organization_id,omitempty"`
	CorrelationID   string                 `json:"correlation_id"`
	IdempotencyKey  string                 `json:"idempotency_key" river:"unique"`
	Domain          jobcontract.DomainLink `json:"domain"`
	Payload         json.RawMessage        `json:"payload"`
	kind            string
}

func (args relayArgs) Kind() string { return args.kind }

type relayMetadata struct {
	WorkerOutboxID  string `json:"worker_outbox_id"`
	PayloadHash     string `json:"payload_hash"`
	ContractVersion int    `json:"contract_version"`
}

func prepareRow(registry PolicyRegistry, row Row) (jobruntime.Descriptor, relayArgs, error) {
	descriptor, ok := registry.Descriptor(row.JobKind)
	if !ok {
		return jobruntime.Descriptor{}, relayArgs{}, ErrContractRejected
	}
	if row.ContractVersion < 1 || !containsVersion(descriptor.SupportedVersions, row.ContractVersion) {
		return jobruntime.Descriptor{}, relayArgs{}, ErrContractRejected
	}
	if row.Queue != descriptor.Queue || row.Priority != descriptor.Priority || row.MaxAttempts != descriptor.MaxAttempts {
		return jobruntime.Descriptor{}, relayArgs{}, ErrPolicyRejected
	}
	if !uuidPattern.MatchString(row.ID) || !hashPattern.MatchString(row.PayloadHash) || len(row.Args) > jobcontract.MaxEnvelopeBytes {
		return jobruntime.Descriptor{}, relayArgs{}, ErrContractRejected
	}
	envelope, err := jobcontract.Decode(row.JobKind, row.Args)
	if err != nil || envelope.ContractVersion != row.ContractVersion || envelope.IdempotencyKey != row.DedupeKey {
		return jobruntime.Descriptor{}, relayArgs{}, ErrContractRejected
	}
	canonical, err := jobcontract.MarshalCanonical(envelope)
	if err != nil || canonicalHash(canonical) != row.PayloadHash {
		return jobruntime.Descriptor{}, relayArgs{}, ErrContractRejected
	}
	var args relayArgs
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&args); err != nil {
		return jobruntime.Descriptor{}, relayArgs{}, ErrContractRejected
	}
	args.kind = row.JobKind
	return descriptor, args, nil
}

func verifyInsertResult(result *rivertype.JobInsertResult, row Row, descriptor jobruntime.Descriptor) error {
	if result == nil || result.Job == nil || result.Job.ID <= 0 || result.Job.Kind != row.JobKind ||
		result.Job.Queue != descriptor.Queue || result.Job.Priority != descriptor.Priority ||
		result.Job.MaxAttempts != descriptor.MaxAttempts {
		return ErrPolicyRejected
	}
	envelope, err := jobcontract.Decode(result.Job.Kind, result.Job.EncodedArgs)
	if err != nil || envelope.ContractVersion != row.ContractVersion || envelope.IdempotencyKey != row.DedupeKey {
		return ErrContractRejected
	}
	canonical, err := jobcontract.MarshalCanonical(envelope)
	if err != nil || canonicalHash(canonical) != row.PayloadHash {
		return ErrContractRejected
	}
	var metadata relayMetadata
	decoder := json.NewDecoder(bytes.NewReader(result.Job.Metadata))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil || metadata.WorkerOutboxID != row.ID ||
		metadata.PayloadHash != row.PayloadHash || metadata.ContractVersion != row.ContractVersion {
		return ErrContractRejected
	}
	return nil
}

func canonicalHash(value []byte) string {
	hash := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(hash[:])
}

func containsVersion(versions []int, candidate int) bool {
	for _, version := range versions {
		if version == candidate {
			return true
		}
	}
	return false
}
