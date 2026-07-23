package providerfoundation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5"
)

const (
	maxGenerationBlockRows     = 500
	maxGenerationBlockBytes    = 4 << 20
	defaultReplayGuardCapacity = 4096
	maxReplayGuardCapacity     = 100_000
)

// Sink is the only persistence boundary for normalized provider output. A
// caller validates its lease immediately before WriteBatch and acknowledges a
// sync unit only after it returns successfully.
type Sink interface {
	WriteBatch(context.Context, []NormalizedEnvelope) error
}

// GenerationSink is the only sink accepted by a recoverable sync-unit
// executor. Its generation key must remain stable across attempts so a worker
// killed after ClickHouse accepted a block cannot produce a second logical
// generation when its lease is recovered.
type GenerationSink interface {
	WriteGenerationBlock(context.Context, GenerationBlock) error
}

type GenerationBlockInspection string

const (
	GenerationBlockAbsent   GenerationBlockInspection = "absent"
	GenerationBlockExact    GenerationBlockInspection = "exact"
	GenerationBlockConflict GenerationBlockInspection = "conflict"
)

// GenerationBlockReadback is the operator recovery boundary for a block whose
// ClickHouse write may have succeeded before its Postgres journal commit.
type GenerationBlockReadback interface {
	InspectGenerationBlock(context.Context, GenerationBlock) (GenerationBlockInspection, error)
}

// GenerationBlock is produced only by BuildGenerationBlocks. It binds one
// bounded, deterministic block to a stable generation, destination, index,
// and content digest. The opaque fields prevent callers from accidentally
// reusing a unit-wide token for multiple blocks.
type GenerationBlock struct {
	generation, destination, contentDigest string
	index                                  int
	batch                                  []NormalizedEnvelope
}

func (block GenerationBlock) Generation() string  { return block.generation }
func (block GenerationBlock) Destination() string { return block.destination }
func (block GenerationBlock) Index() int          { return block.index }
func (block GenerationBlock) ContentDigest() string {
	return block.contentDigest
}
func (block GenerationBlock) Batch() []NormalizedEnvelope {
	return append([]NormalizedEnvelope(nil), block.batch...)
}

func BuildGenerationBlocks(
	generation string,
	destination string,
	batch []NormalizedEnvelope,
) ([]GenerationBlock, error) {
	return buildGenerationBlocks(generation, destination, batch, maxGenerationBlockRows, maxGenerationBlockBytes)
}

func buildGenerationBlocks(
	generation string,
	destination string,
	batch []NormalizedEnvelope,
	maxRows int,
	maxBytes int,
) ([]GenerationBlock, error) {
	generation = strings.TrimSpace(generation)
	if generation == "" || len(generation) > 256 || !validClickHouseTable(destination) ||
		maxRows < 1 || maxRows > maxGenerationBlockRows ||
		maxBytes < 1 || maxBytes > maxGenerationBlockBytes {
		return nil, ErrSinkGenerationUnsafe
	}
	if err := validateBatch(batch); err != nil {
		return nil, err
	}
	ordered := append([]NormalizedEnvelope(nil), batch...)
	sort.Slice(ordered, func(left, right int) bool {
		if ordered[left].DedupeKey != ordered[right].DedupeKey {
			return ordered[left].DedupeKey < ordered[right].DedupeKey
		}
		return ordered[left].SourceID < ordered[right].SourceID
	})
	var blocks []GenerationBlock
	for offset := 0; offset < len(ordered); {
		end, size := offset, 0
		for end < len(ordered) && end-offset < maxRows {
			encoded, err := json.Marshal(ordered[end])
			if err != nil || len(encoded) > maxBytes {
				return nil, ErrSinkGenerationUnsafe
			}
			if end > offset && size+len(encoded) > maxBytes {
				break
			}
			size += len(encoded)
			end++
		}
		contentDigest, err := generationContentDigest(ordered[offset:end])
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, GenerationBlock{
			generation: generation, destination: destination, index: len(blocks),
			contentDigest: contentDigest,
			batch:         append([]NormalizedEnvelope(nil), ordered[offset:end]...),
		})
		offset = end
	}
	return blocks, nil
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
	Conn        driver.Conn
	Table       string
	Lease       LeaseGuard
	ReplayGuard *GenerationReplayGuard
	Verifier    ClickHouseDeduplicationVerifier
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

// WriteGenerationBlock sends one bounded block with a token stable for its
// generation, destination, and deterministic block index. It refuses tables
// whose actual engine/settings do not guarantee insert deduplication.
func (s ClickHouseSink) WriteGenerationBlock(ctx context.Context, block GenerationBlock) error {
	if s.Conn == nil || s.Table == "" || s.Lease == nil || s.ReplayGuard == nil {
		return ErrSinkGenerationUnsafe
	}
	if err := s.Lease.Assert(ctx); err != nil {
		return err
	}
	if err := block.validate(s.Table); err != nil {
		return err
	}
	if err := s.ReplayGuard.Remember(block); err != nil {
		return err
	}
	verifier := s.Verifier
	if verifier == nil {
		verifier = SystemTableDeduplicationVerifier{}
	}
	if err := verifier.Verify(ctx, s.Conn, s.Table); err != nil {
		return err
	}
	generationContext, err := clickHouseGenerationContext(ctx, block)
	if err != nil {
		return err
	}
	if err := s.Lease.Assert(generationContext); err != nil {
		return err
	}
	return WriteClickHouseBatch(generationContext, s.Conn, s.Table, block.batch, DefaultClickHouseBatch)
}

// InspectGenerationBlock determines whether every exact row in a deterministic
// block is present, every row is absent, or the destination is mixed/conflicted.
// Only the first two states are safe for an operator to resolve automatically.
func (s ClickHouseSink) InspectGenerationBlock(
	ctx context.Context,
	block GenerationBlock,
) (GenerationBlockInspection, error) {
	if ctx == nil || s.Conn == nil || s.Lease == nil {
		return "", ErrSinkGenerationUnsafe
	}
	if err := s.Lease.Assert(ctx); err != nil {
		return "", err
	}
	if err := block.validate(s.Table); err != nil {
		return "", err
	}
	exact, absent := 0, 0
	for _, expected := range block.batch {
		matched, found, err := inspectClickHouseEnvelope(ctx, s.Conn, s.Table, expected)
		if err != nil {
			return "", err
		}
		switch {
		case matched:
			exact++
		case !found:
			absent++
		default:
			return GenerationBlockConflict, nil
		}
	}
	switch {
	case exact == len(block.batch):
		return GenerationBlockExact, nil
	case absent == len(block.batch):
		return GenerationBlockAbsent, nil
	default:
		return GenerationBlockConflict, nil
	}
}

func inspectClickHouseEnvelope(
	ctx context.Context,
	conn driver.Conn,
	table string,
	expected NormalizedEnvelope,
) (matched bool, found bool, err error) {
	if !validClickHouseTable(table) {
		return false, false, ErrSinkGenerationUnsafe
	}
	rows, err := conn.Query(ctx, `
SELECT schema_version, provider, org_id, integration_id, entity_type, source_id,
       dedupe_key, observed_at, provenance_source, provenance_confidence,
       provenance_evidence_id, attributes_json
FROM `+table+`
WHERE org_id = ? AND integration_id = ? AND dedupe_key = ?`,
		expected.OrgID, expected.IntegrationID, expected.DedupeKey,
	)
	if err != nil {
		return false, false, fmt.Errorf("%w: ClickHouse generation readback", ErrSinkGenerationUnsafe)
	}
	defer rows.Close()
	expectedDigest, err := envelopeDigest(expected)
	if err != nil {
		return false, false, err
	}
	for rows.Next() {
		var (
			schemaVersion, provider, orgID, integrationID string
			entityType, sourceID, dedupeKey               string
			observedAt                                    time.Time
			provenanceSource, provenanceConfidence        string
			provenanceEvidenceID, attributesJSON          string
		)
		if err := rows.Scan(
			&schemaVersion, &provider, &orgID, &integrationID, &entityType,
			&sourceID, &dedupeKey, &observedAt, &provenanceSource,
			&provenanceConfidence, &provenanceEvidenceID, &attributesJSON,
		); err != nil {
			return false, false, fmt.Errorf("%w: ClickHouse generation scan", ErrSinkGenerationUnsafe)
		}
		found = true
		attributes := map[string]string{}
		if json.Unmarshal([]byte(attributesJSON), &attributes) != nil {
			return false, false, fmt.Errorf("%w: ClickHouse attributes readback", ErrSinkGenerationUnsafe)
		}
		actualDigest, err := envelopeDigest(NormalizedEnvelope{
			SchemaVersion: schemaVersion,
			Provider:      provider,
			OrgID:         orgID,
			IntegrationID: integrationID,
			EntityType:    entityType,
			SourceID:      sourceID,
			DedupeKey:     dedupeKey,
			ObservedAt:    observedAt.UTC(),
			Provenance: Provenance{
				Source: provenanceSource, Confidence: provenanceConfidence,
				EvidenceID: provenanceEvidenceID,
			},
			Attributes: attributes,
		})
		if err != nil {
			return false, false, err
		}
		if actualDigest == expectedDigest {
			matched = true
		}
	}
	if err := rows.Err(); err != nil {
		return false, false, fmt.Errorf("%w: ClickHouse generation rows", ErrSinkGenerationUnsafe)
	}
	return matched, found, nil
}

type clickHouseGenerationContextKey struct{}

func clickHouseGenerationContext(ctx context.Context, block GenerationBlock) (context.Context, error) {
	if ctx == nil {
		return nil, ErrSinkGenerationUnsafe
	}
	if err := block.validate(block.destination); err != nil {
		return nil, err
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{
		block.generation, block.destination, strconv.Itoa(block.index),
	}, "\x00")))
	token := hex.EncodeToString(digest[:])
	ctx = context.WithValue(ctx, clickHouseGenerationContextKey{}, token)
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"insert_deduplication_token": token,
		"insert_deduplicate":         1,
		// Reconciliation may classify an absent block as safe to retry. Force
		// synchronous insertion even when a server/user profile enables async
		// inserts so an accepted-but-not-yet-visible queue entry cannot look
		// absent during operator readback.
		"async_insert":          0,
		"wait_for_async_insert": 1,
	})), nil
}

func clickHouseGenerationToken(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	token, _ := ctx.Value(clickHouseGenerationContextKey{}).(string)
	return token
}

func (block GenerationBlock) validate(destination string) error {
	if block.generation == "" || len(block.generation) > 256 ||
		block.destination != destination || !validClickHouseTable(block.destination) ||
		block.index < 0 || block.index > 1_000_000 ||
		len(block.batch) < 1 || len(block.batch) > maxGenerationBlockRows {
		return ErrSinkGenerationUnsafe
	}
	digest, err := generationContentDigest(block.batch)
	if err != nil || digest != block.contentDigest {
		return ErrSinkGenerationUnsafe
	}
	encodedBytes := 0
	for _, envelope := range block.batch {
		encoded, encodeErr := json.Marshal(envelope)
		if encodeErr != nil {
			return ErrSinkGenerationUnsafe
		}
		encodedBytes += len(encoded)
		if encodedBytes > maxGenerationBlockBytes {
			return ErrSinkGenerationUnsafe
		}
	}
	return validateBatch(block.batch)
}

func generationContentDigest(batch []NormalizedEnvelope) (string, error) {
	digest := sha256.New()
	for _, envelope := range batch {
		encoded, err := json.Marshal(envelope)
		if err != nil {
			return "", err
		}
		_, _ = digest.Write(encoded)
		_, _ = digest.Write([]byte{'\n'})
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

type GenerationReplayGuard struct {
	mu       sync.Mutex
	digests  map[string]string
	order    []string
	next     int
	capacity int
}

func NewGenerationReplayGuard() *GenerationReplayGuard {
	return &GenerationReplayGuard{
		digests:  make(map[string]string, defaultReplayGuardCapacity),
		order:    make([]string, 0, defaultReplayGuardCapacity),
		capacity: defaultReplayGuardCapacity,
	}
}

func NewGenerationReplayGuardWithCapacity(capacity int) (*GenerationReplayGuard, error) {
	if capacity < 1 || capacity > maxReplayGuardCapacity {
		return nil, ErrSinkGenerationUnsafe
	}
	return &GenerationReplayGuard{
		digests:  make(map[string]string, capacity),
		order:    make([]string, 0, capacity),
		capacity: capacity,
	}, nil
}

func (guard *GenerationReplayGuard) Remember(block GenerationBlock) error {
	if guard == nil || guard.capacity < 1 || guard.capacity > maxReplayGuardCapacity {
		return ErrSinkGenerationUnsafe
	}
	key := strings.Join([]string{block.generation, block.destination, strconv.Itoa(block.index)}, "\x00")
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if existing, ok := guard.digests[key]; ok {
		if existing != block.contentDigest {
			return ErrSinkReplayConflict
		}
		return nil
	}
	if len(guard.order) < guard.capacity {
		guard.order = append(guard.order, key)
	} else {
		evicted := guard.order[guard.next]
		delete(guard.digests, evicted)
		guard.order[guard.next] = key
		guard.next = (guard.next + 1) % guard.capacity
	}
	guard.digests[key] = block.contentDigest
	return nil
}

func (guard *GenerationReplayGuard) Size() int {
	if guard == nil {
		return 0
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	return len(guard.digests)
}

type ClickHouseDeduplicationVerifier interface {
	Verify(context.Context, driver.Conn, string) error
}

type SystemTableDeduplicationVerifier struct{}

var nonReplicatedDedupWindow = regexp.MustCompile(`(?i)\bnon_replicated_deduplication_window\s*=\s*([1-9][0-9]*)\b`)

func (SystemTableDeduplicationVerifier) Verify(ctx context.Context, conn driver.Conn, table string) error {
	if ctx == nil || conn == nil || !validClickHouseTable(table) {
		return ErrSinkGenerationUnsafe
	}
	parts := strings.Split(table, ".")
	query := "SELECT engine, create_table_query FROM system.tables WHERE database = currentDatabase() AND name = ?"
	args := []any{parts[0]}
	if len(parts) == 2 {
		query = "SELECT engine, create_table_query FROM system.tables WHERE database = ? AND name = ?"
		args = []any{parts[0], parts[1]}
	} else if len(parts) != 1 {
		return ErrSinkGenerationUnsafe
	}
	var engine, createQuery string
	if err := conn.QueryRow(ctx, query, args...).Scan(&engine, &createQuery); err != nil {
		return ErrSinkGenerationUnsafe
	}
	switch {
	case strings.HasPrefix(engine, "Replicated"), strings.HasPrefix(engine, "Shared"):
		return nil
	case strings.Contains(engine, "MergeTree") && nonReplicatedDedupWindow.MatchString(createQuery):
		return nil
	default:
		return fmt.Errorf("%w: ClickHouse table engine/settings", ErrSinkGenerationUnsafe)
	}
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
	sum := sha256.Sum256([]byte(e.SchemaVersion + "\x00" + e.Provider + "\x00" + e.OrgID + "\x00" + e.IntegrationID + "\x00" + e.EntityType + "\x00" + e.SourceID + "\x00" + e.DedupeKey + "\x00" + e.ObservedAt.UTC().Format(time.RFC3339Nano) + "\x00" + e.Provenance.Source + "\x00" + e.Provenance.Confidence + "\x00" + e.Provenance.EvidenceID + "\x00" + attributes))
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

var _ GenerationSink = ClickHouseSink{}
var _ GenerationBlockReadback = ClickHouseSink{}
