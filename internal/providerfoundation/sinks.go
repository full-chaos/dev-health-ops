package providerfoundation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5"
)

// Sink is the only persistence boundary for normalized provider output. A
// caller validates its lease immediately before WriteBatch and acknowledges a
// sync unit only after it returns successfully.
type Sink interface {
	WriteBatch(context.Context, []NormalizedEnvelope) error
}

// PostgresSink runs the supplied writer in one real pgx transaction. It does
// not invent a parallel source-record table: dataset-specific writers in
// CHAOS-3045/3046 bind to existing Python-owned semantic tables and use the
// canonical dedupe/provenance fields below.
type PostgresSink struct {
	Pool interface {
		BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	}
	Write func(context.Context, pgx.Tx, []NormalizedEnvelope) error
	Lease LeaseGuard
}

func (s PostgresSink) WriteBatch(ctx context.Context, batch []NormalizedEnvelope) error {
	if s.Pool == nil || s.Write == nil || s.Lease == nil {
		return ErrCredentialInvalid
	}
	if err := s.Lease.Assert(ctx); err != nil {
		return err
	}
	if err := validateBatch(batch); err != nil {
		return err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err := s.Lease.Assert(ctx); err != nil {
		return err
	}
	if err := s.Write(ctx, tx, batch); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ClickHouseSink inserts a batch with a durable dedupe key and provenance.
// The destination is selected by the dataset adapter, but rows and columns are
// fixed here so adapter code cannot drop tenant/provenance evidence.
type ClickHouseSink struct {
	Conn  driver.Conn
	Table string
	Lease LeaseGuard
}

func (s ClickHouseSink) WriteBatch(ctx context.Context, batch []NormalizedEnvelope) error {
	if s.Conn == nil || s.Table == "" || s.Lease == nil {
		return ErrCredentialInvalid
	}
	if err := s.Lease.Assert(ctx); err != nil {
		return err
	}
	if !validClickHouseTable(s.Table) {
		return ErrCredentialInvalid
	}
	if err := s.Lease.Assert(ctx); err != nil {
		return err
	}
	return WriteClickHouseBatch(ctx, s.Conn, s.Table, batch, DefaultClickHouseBatch)
}

// BatchAppender is implemented by clickhouse-go's prepared batch and is kept
// separate to make the insert protocol testable without a live ClickHouse.
type BatchAppender interface {
	Append(...any) error
	Send() error
}
type ClickHouseBatchFactory func(context.Context, driver.Conn, string, []string) (BatchAppender, error)

func DefaultClickHouseBatch(ctx context.Context, conn driver.Conn, table string, columns []string) (BatchAppender, error) {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table+" (schema_version, provider, org_id, integration_id, entity_type, source_id, dedupe_key, observed_at, provenance_source, provenance_confidence, provenance_evidence_id, attributes_json)")
	if err != nil {
		return nil, err
	}
	return batch, nil
}
func WriteClickHouseBatch(ctx context.Context, conn driver.Conn, table string, batch []NormalizedEnvelope, factory ClickHouseBatchFactory) error {
	if conn == nil || !validClickHouseTable(table) || factory == nil {
		return ErrCredentialInvalid
	}
	if err := validateBatch(batch); err != nil {
		return err
	}
	writer, err := factory(ctx, conn, table, nil)
	if err != nil {
		return err
	}
	for _, e := range batch {
		attributes, err := canonicalAttributes(e.Attributes)
		if err != nil {
			return err
		}
		if err := writer.Append(e.SchemaVersion, e.Provider, e.OrgID, e.IntegrationID, e.EntityType, e.SourceID, e.DedupeKey, e.ObservedAt.UTC(), e.Provenance.Source, e.Provenance.Confidence, e.Provenance.EvidenceID, attributes); err != nil {
			return err
		}
	}
	return writer.Send()
}

func validClickHouseTable(value string) bool {
	for _, part := range strings.Split(value, ".") {
		if part == "" {
			return false
		}
		for index, char := range part {
			if (char >= 'a' && char <= 'z') || char == '_' || (index > 0 && char >= '0' && char <= '9') {
				continue
			}
			return false
		}
	}
	return true
}

func validateBatch(batch []NormalizedEnvelope) error {
	if len(batch) == 0 {
		return nil
	}
	seen := map[string]string{}
	for _, e := range batch {
		if err := e.Validate(); err != nil {
			return err
		}
		digest, err := envelopeDigest(e)
		if err != nil {
			return err
		}
		if prior, ok := seen[e.DedupeKey]; ok && prior != digest {
			return ErrSinkDuplicate
		}
		seen[e.DedupeKey] = digest
	}
	return nil
}
func envelopeDigest(e NormalizedEnvelope) (string, error) {
	attributes, err := canonicalAttributes(e.Attributes)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(e.SchemaVersion + "\x00" + e.Provider + "\x00" + e.OrgID + "\x00" + e.IntegrationID + "\x00" + e.EntityType + "\x00" + e.SourceID + "\x00" + e.Provenance.Source + "\x00" + e.Provenance.Confidence + "\x00" + e.Provenance.EvidenceID + "\x00" + attributes))
	return hex.EncodeToString(sum[:]), nil
}
func canonicalAttributes(input map[string]string) (string, error) {
	if len(input) == 0 {
		return "{}", nil
	}
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make(map[string]string, len(keys))
	for _, key := range keys {
		ordered[key] = input[key]
	}
	encoded, err := json.Marshal(ordered)
	return string(encoded), err
}
