package streamhandlers

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	operationalConflictDomain = "operational-conflict-v1"
	operationalRevisionDomain = "operational-source-revision-v1"
)

type operationalField struct {
	name  string
	value any
}

type operationalBase struct {
	orgID, provider, providerInstanceID, sourceEntityType, externalID string
	sourceVersionAt                                                   time.Time
	sourceID                                                          *uuid.UUID
	sourceURL                                                         *string
	sourceEventAt                                                     *time.Time
	sourceEventID                                                     *string
	observedAt, lastSynced                                            time.Time
	rawStatus, rawSeverity, rawPriority                               *string
	normalizedStatus, normalizedSeverity, normalizedPriority          *string
	relationshipProvenance                                            *string
	relationshipConfidence                                            *float64
}

func operationalValues(family string, base operationalBase, entity []operationalField) ([]any, error) {
	id, err := canonicalOperationalID(base.orgID, base.provider, base.providerInstanceID, family, base.externalID)
	if err != nil {
		return nil, err
	}
	conflictFields := []operationalField{
		{"org_id", base.orgID},
		{"provider", base.provider},
		{"provider_instance_id", base.providerInstanceID},
		{"source_entity_type", base.sourceEntityType},
		{"external_id", base.externalID},
		{"source_version_at", base.sourceVersionAt},
		{"source_id", nullableUUID(base.sourceID)},
		{"source_url", nullableString(base.sourceURL)},
		{"source_event_at", nullableTimePointer(base.sourceEventAt)},
		{"source_event_id", nullableString(base.sourceEventID)},
		{"raw_status", nullableString(base.rawStatus)},
		{"raw_severity", nullableString(base.rawSeverity)},
		{"raw_priority", nullableString(base.rawPriority)},
		{"normalized_status", nullableString(base.normalizedStatus)},
		{"normalized_severity", nullableString(base.normalizedSeverity)},
		{"normalized_priority", nullableString(base.normalizedPriority)},
		{"relationship_provenance", nullableString(base.relationshipProvenance)},
		{"relationship_confidence", nullableFloatPointer(base.relationshipConfidence)},
	}
	conflictFields = append(conflictFields, entity...)
	conflictKey, err := encodeOperationalConflict(family, conflictFields)
	if err != nil {
		return nil, err
	}
	sourceRevision, err := operationalSourceRevision(base.sourceVersionAt, 1, conflictKey)
	if err != nil {
		return nil, err
	}
	ingestRevision, err := operationalIngestRevision(base.lastSynced, base.observedAt)
	if err != nil {
		return nil, err
	}
	values := []any{
		base.orgID, base.provider, base.providerInstanceID, base.sourceEntityType,
		base.externalID, base.sourceVersionAt, sourceRevision, conflictKey,
		ingestRevision, uint8(2), id, nullableUUID(base.sourceID), nullableString(base.sourceURL),
		nullableTimePointer(base.sourceEventAt), nullableString(base.sourceEventID), base.observedAt, base.lastSynced,
		nullableString(base.rawStatus), nullableString(base.rawSeverity), nullableString(base.rawPriority), nullableString(base.normalizedStatus),
		nullableString(base.normalizedSeverity), nullableString(base.normalizedPriority),
		nullableString(base.relationshipProvenance), nullableFloatPointer(base.relationshipConfidence),
	}
	for _, field := range entity {
		values = append(values, field.value)
	}
	return values, nil
}

func canonicalOperationalID(orgID, provider, providerInstanceID, family, externalID string) (string, error) {
	parts := []string{orgID, provider, providerInstanceID, family, externalID}
	for _, part := range parts {
		if part == "" {
			return "", fmt.Errorf("canonical operational identity must be non-empty")
		}
	}
	quoted := make([]string, len(parts))
	for index, part := range parts {
		quoted[index] = strconv.QuoteToASCII(part)
	}
	digest := sha256.Sum256([]byte("[" + strings.Join(quoted, ",") + "]"))
	return hex.EncodeToString(digest[:]), nil
}

func encodeOperationalConflict(family string, fields []operationalField) (string, error) {
	var encoded bytes.Buffer
	encoded.WriteString(operationalConflictDomain)
	if err := encodeOperationalField(&encoded, "entity_family", family); err != nil {
		return "", err
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if _, duplicate := seen[field.name]; duplicate {
			return "", fmt.Errorf("duplicate operational conflict field %q", field.name)
		}
		seen[field.name] = struct{}{}
		if err := encodeOperationalField(&encoded, field.name, field.value); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(encoded.Bytes()), nil
}

func encodeOperationalField(out *bytes.Buffer, name string, value any) error {
	if name == "" {
		return fmt.Errorf("operational conflict field name is empty")
	}
	valueType, marker, encoded, err := encodeOperationalValue(name, value)
	if err != nil {
		return err
	}
	writeLength(out, []byte(name), 4)
	writeLength(out, []byte(valueType), 2)
	out.WriteByte(marker)
	writeLength(out, encoded, 8)
	return nil
}

func encodeOperationalValue(name string, value any) (string, byte, []byte, error) {
	switch typed := value.(type) {
	case nil:
		return "null", 0, nil, nil
	case bool:
		if typed {
			return "bool", 1, []byte{1}, nil
		}
		return "bool", 1, []byte{0}, nil
	case string:
		return "string", 1, []byte(typed), nil
	case time.Time:
		if err := validateOperationalTime(typed); err != nil {
			return "", 0, nil, fmt.Errorf("%s: %w", name, err)
		}
		return "datetime", 1, []byte(typed.UTC().Format("2006-01-02T15:04:05.000000Z")), nil
	case uuid.UUID:
		return "uuid", 1, []byte(strings.ToLower(typed.String())), nil
	case int:
		return "integer", 1, []byte(strconv.Itoa(typed)), nil
	case int32:
		return "integer", 1, []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int64:
		return "integer", 1, []byte(strconv.FormatInt(typed, 10)), nil
	case uint8:
		return "integer", 1, []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint32:
		return "integer", 1, []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint64:
		return "integer", 1, []byte(strconv.FormatUint(typed, 10)), nil
	case float64:
		encoded := make([]byte, 8)
		binary.BigEndian.PutUint64(encoded, math.Float64bits(typed))
		return "float64", 1, encoded, nil
	case []any:
		var encoded bytes.Buffer
		_ = binary.Write(&encoded, binary.BigEndian, uint64(len(typed)))
		for index, item := range typed {
			if err := encodeOperationalField(&encoded, strconv.Itoa(index), item); err != nil {
				return "", 0, nil, err
			}
		}
		return "list", 1, encoded.Bytes(), nil
	case map[string]any:
		var encoded bytes.Buffer
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		_ = binary.Write(&encoded, binary.BigEndian, uint64(len(keys)))
		for _, key := range keys {
			if err := encodeOperationalField(&encoded, key, typed[key]); err != nil {
				return "", 0, nil, err
			}
		}
		return "map", 1, encoded.Bytes(), nil
	default:
		return "", 0, nil, fmt.Errorf("%s: unsupported operational conflict type %T", name, value)
	}
}

func writeLength(out *bytes.Buffer, value []byte, width int) {
	length := uint64(len(value))
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], length)
	out.Write(encoded[8-width:])
	out.Write(value)
}

func operationalSourceRevision(at time.Time, rank uint8, conflictKey string) (*big.Int, error) {
	micros, err := operationalMicros(at)
	if err != nil {
		return nil, err
	}
	conflictBytes, err := hex.DecodeString(conflictKey)
	if err != nil || !bytes.HasPrefix(conflictBytes, []byte(operationalConflictDomain)) {
		return nil, fmt.Errorf("invalid operational conflict key")
	}
	digest := sha256.Sum256(append([]byte(operationalRevisionDomain), conflictBytes...))
	tie := new(big.Int).SetBytes(digest[:7])
	revision := new(big.Int).Lsh(new(big.Int).SetUint64(micros), 64)
	revision.Or(revision, new(big.Int).Lsh(new(big.Int).SetUint64(uint64(rank)), 56))
	revision.Or(revision, tie)
	return revision, nil
}

func operationalIngestRevision(lastSynced, observedAt time.Time) (*big.Int, error) {
	lastSyncedMicros, err := operationalMicros(lastSynced)
	if err != nil {
		return nil, err
	}
	observedMicros, err := operationalMicros(observedAt)
	if err != nil {
		return nil, err
	}
	revision := new(big.Int).Lsh(new(big.Int).SetUint64(lastSyncedMicros), 64)
	return revision.Or(revision, new(big.Int).SetUint64(observedMicros)), nil
}

func operationalMicros(value time.Time) (uint64, error) {
	if err := validateOperationalTime(value); err != nil {
		return 0, err
	}
	return uint64(value.UTC().UnixMicro()), nil
}

func validateOperationalTime(value time.Time) error {
	_, offset := value.Zone()
	maximum := time.Date(2299, 12, 31, 23, 59, 59, 999999000, time.UTC)
	if value.IsZero() || offset != 0 || value.Before(time.Unix(0, 0).UTC()) || value.After(maximum) {
		return fmt.Errorf("UTC ClickHouse DateTime64(6) required")
	}
	return nil
}

func nullableUUID(value *uuid.UUID) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableTimePointer(value *time.Time) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableFloatPointer(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}
