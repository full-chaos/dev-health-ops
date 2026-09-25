package operationalbackfill

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// This file is the canonical operational entity contract of the Python
// producer (models/operational.py, operational_ordering.py and
// operational_ordering_codec.py): the id, the conflict key and the two
// revision numbers. Every value here is compared with the real Python
// producer by the venue oracle.

const (
	conflictDomain = "operational-conflict-v1"
	revisionDomain = "operational-source-revision-v1"

	// orderingContract is the value every current-shape row carries.
	orderingContract = 2
	// rankActiveUpdate is the operation rank of a live (not deleted) entity.
	rankActiveUpdate = 1
)

var (
	unixEpoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	// clickHouseDateTime64Max is Python's CLICKHOUSE_DATETIME64_MAX.
	clickHouseDateTime64Max = time.Date(2299, 12, 31, 23, 59, 59, 999999000, time.UTC)
)

// field is one named value of an entity, in the dataclass declaration order.
// A value is nil, bool, string or time.Time.
type field struct {
	name  string
	value any
}

// pyJSONASCII encodes text as Python's json.dumps(text, ensure_ascii=True)
// does: the short escapes, \u00XX for the other control characters, \uXXXX
// (with surrogate pairs) for everything outside ASCII.
func pyJSONASCII(text string) string {
	var out strings.Builder
	out.WriteByte('"')
	for _, r := range text {
		switch {
		case r == '"':
			out.WriteString(`\"`)
		case r == '\\':
			out.WriteString(`\\`)
		case r == '\n':
			out.WriteString(`\n`)
		case r == '\r':
			out.WriteString(`\r`)
		case r == '\t':
			out.WriteString(`\t`)
		case r == '\b':
			out.WriteString(`\b`)
		case r == '\f':
			out.WriteString(`\f`)
		case r >= 0x20 && r <= 0x7e:
			out.WriteRune(r)
		case r < 0x10000:
			fmt.Fprintf(&out, `\u%04x`, r)
		default:
			r -= 0x10000
			fmt.Fprintf(&out, `\u%04x\u%04x`, 0xD800+(r>>10), 0xDC00+(r&0x3FF))
		}
	}
	out.WriteByte('"')
	return out.String()
}

// canonicalID is canonical_operational_id: the sha256 of the JSON seed.
func canonicalID(orgID, provider, instance, family, externalID string) (string, error) {
	for _, component := range []struct{ name, value string }{
		{"org_id", orgID}, {"provider", provider}, {"provider_instance_id", instance},
		{"entity_family", family}, {"external_id", externalID},
	} {
		if component.value == "" {
			return "", fmt.Errorf("%s must be non-empty, got ''", component.name)
		}
		if !utf8.ValidString(component.value) {
			return "", fmt.Errorf("%s is not valid UTF-8", component.name)
		}
	}
	seed := "[" + pyJSONASCII(orgID) + "," + pyJSONASCII(provider) + "," + pyJSONASCII(instance) +
		"," + pyJSONASCII(family) + "," + pyJSONASCII(externalID) + "]"
	digest := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(digest[:]), nil
}

// utcMicroseconds is utc_microseconds: the value must be UTC and inside the
// ClickHouse DateTime64(6) range.
func utcMicroseconds(value time.Time, name string) (uint64, error) {
	if _, offset := value.Zone(); offset != 0 {
		return 0, fmt.Errorf("invalid operational ordering field %s: UTC datetime with fold=0 required", name)
	}
	value = value.UTC()
	if value.Before(unixEpoch) || value.After(clickHouseDateTime64Max) {
		return 0, fmt.Errorf("invalid operational ordering field %s: ClickHouse DateTime64(6) range required", name)
	}
	// Seconds and microseconds separately: time.Time.Sub saturates a Duration
	// (int64 nanoseconds) at the year 2262, well before the DateTime64 maximum.
	return uint64(value.Unix())*1_000_000 + uint64(value.Nanosecond()/1000), nil
}

func lengthPrefixed(out *bytes.Buffer, value []byte, width int) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	out.Write(size[8-width:])
	out.Write(value)
}

func encodeField(out *bytes.Buffer, name string, value any) error {
	if name == "" {
		return errors.New("invalid operational ordering field field_name: non-empty name required")
	}
	var valueType string
	var marker byte = 1
	var encoded []byte
	switch typed := value.(type) {
	case nil:
		valueType, marker = "null", 0
	case bool:
		valueType = "bool"
		if typed {
			encoded = []byte{1}
		} else {
			encoded = []byte{0}
		}
	case string:
		if !utf8.ValidString(typed) {
			return fmt.Errorf("invalid operational ordering field %s: invalid UTF-8 text", name)
		}
		valueType, encoded = "string", []byte(typed)
	case time.Time:
		if _, err := utcMicroseconds(typed, name); err != nil {
			return err
		}
		valueType, encoded = "datetime", []byte(typed.UTC().Format("2006-01-02T15:04:05.000000Z"))
	default:
		return fmt.Errorf("invalid operational ordering field %s: unsupported value type %T", name, value)
	}
	lengthPrefixed(out, []byte(name), 4)
	lengthPrefixed(out, []byte(valueType), 2)
	out.WriteByte(marker)
	lengthPrefixed(out, encoded, 8)
	return nil
}

// conflictKey is encode_conflict_fields: the domain tag, the entity family and
// each field as a length-prefixed name/type/marker/value record, in hex.
func conflictKey(family string, fields []field) (string, error) {
	var out bytes.Buffer
	out.WriteString(conflictDomain)
	if err := encodeField(&out, "entity_family", family); err != nil {
		return "", err
	}
	for _, item := range fields {
		if err := encodeField(&out, item.name, item.value); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(out.Bytes()), nil
}

// sourceRevision is build_source_revision: microseconds, then the operation
// rank, then 56 bits of the sha256 of the conflict key.
func sourceRevision(at time.Time, rank uint8, key string) (*big.Int, error) {
	micros, err := utcMicroseconds(at, "source_version_at")
	if err != nil {
		return nil, err
	}
	decoded, err := hex.DecodeString(key)
	if err != nil {
		return nil, fmt.Errorf("invalid operational ordering field source_conflict_key: valid hex required")
	}
	digest := sha256.Sum256(append([]byte(revisionDomain), decoded...))
	revision := new(big.Int).Lsh(new(big.Int).SetUint64(micros), 64)
	revision.Or(revision, new(big.Int).Lsh(big.NewInt(int64(rank)), 56))
	revision.Or(revision, new(big.Int).SetBytes(digest[:7]))
	return revision, nil
}

// ingestRevision is build_ingest_revision: last_synced microseconds, then
// observed_at microseconds.
func ingestRevision(lastSynced, observedAt time.Time) (*big.Int, error) {
	lastSyncedMicros, err := utcMicroseconds(lastSynced, "last_synced")
	if err != nil {
		return nil, err
	}
	observedMicros, err := utcMicroseconds(observedAt, "observed_at")
	if err != nil {
		return nil, err
	}
	revision := new(big.Int).Lsh(new(big.Int).SetUint64(lastSyncedMicros), 64)
	return revision.Or(revision, new(big.Int).SetUint64(observedMicros)), nil
}

// pyStrip is Python's str.strip(): it also strips U+001C..U+001F.
func pyStrip(text string) string {
	return strings.TrimFunc(text, func(r rune) bool {
		return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f)
	})
}

// pyLower is Python's str.lower() for the characters that differ from Go's:
// U+0130 lowers to "i" plus a combining dot.
func pyLower(text string) string {
	return strings.ToLower(strings.ReplaceAll(text, "İ", "i̇"))
}
