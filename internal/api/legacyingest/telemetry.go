package legacyingest

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// maxSignalItems is the `items` bound of IngestTelemetryRequest.
const maxSignalItems = 5000

// signalBucket is one validated IngestTelemetrySignalBucket, in the shape
// write_telemetry_signal_buckets stores it: an empty endpoint_group,
// release_ref or repo_id is "" (the Python route maps them to None and the
// sink writes "" for None).
type signalBucket struct {
	SignalType, EndpointGroup, Environment, RepoID, ReleaseRef, SchemaVersion, DedupeKey string
	SignalCount, SessionCount                                                            *big.Int
	UniquePseudonymous                                                                   *big.Int // nil = NULL
	BucketStart, BucketEnd                                                               pytime.DateTime
	IsSampled                                                                            bool
}

// valueError is the FastAPI rendering of a ValueError raised in a
// field_validator: ctx.error is the exception, which jsonable_encoder
// renders as {}.
func valueError(loc []pyjson.Value, input pyjson.Value, message string) pybody.Error {
	ctx := pyjson.NewObject()
	ctx.Set("error", pyjson.NewObject())
	return pybody.Error{Type: "value_error", Loc: loc, Msg: "Value error, " + message, Input: input, Ctx: ctx}
}

// repoID is `repo_id: UUID | str = ""` with its mode="before" validator: a
// falsy value is "", anything else must be uuid.UUID(str(value)) (Python's
// own constructor, which is laxer than pydantic's UUID parse). The stored
// text is the canonical lower-case form.
func repoID(e *pybody.Errors, raw pyjson.Value, loc []pyjson.Value) (string, bool) {
	if !pyjson.Truthy(raw) {
		return "", true
	}
	parsed, err := pythonparity.ParseUUID(pyjson.Str(raw))
	if err != nil {
		*e = append(*e, valueError(loc, raw, "repo_id must be a valid UUID, got: "+pyjson.Repr(raw)))
		return "", false
	}
	return uuid.UUID(parsed).String(), true
}

// parseSignalBucket is IngestTelemetrySignalBucket.
func parseSignalBucket(m pybody.Model) (signalBucket, bool) {
	signalType := pybody.Get(m, "signal_type", pybody.Required, pybody.Str)
	signalCount := pybody.Get(m, "signal_count", pybody.Required, pybody.Int)
	sessionCount := pybody.Get(m, "session_count", pybody.Required, pybody.Int)
	unique := pybody.Get(m, "unique_pseudonymous_count", pybody.Nullable, pybody.Int)
	endpointGroup := pybody.Get(m, "endpoint_group", pybody.Defaulted, pybody.Str)
	environment := pybody.Get(m, "environment", pybody.Required, pybody.Str)
	repo := pybody.Get(m, "repo_id", pybody.Defaulted, repoID)
	releaseRef := pybody.Get(m, "release_ref", pybody.Defaulted, pybody.Str)
	bucketStart := pybody.Get(m, "bucket_start", pybody.Required, pybody.Datetime)
	bucketEnd := pybody.Get(m, "bucket_end", pybody.Required, pybody.Datetime)
	sampled := pybody.Get(m, "is_sampled", pybody.Defaulted, pybody.Bool)
	schemaVersion := pybody.Get(m, "schema_version", pybody.Defaulted, pybody.Str)
	dedupeKey := pybody.Get(m, "dedupe_key", pybody.Required, pybody.Str)
	ok := signalType.OK && signalCount.OK && sessionCount.OK && unique.OK && endpointGroup.OK && environment.OK &&
		repo.OK && releaseRef.OK && bucketStart.OK && bucketEnd.OK && sampled.OK && schemaVersion.OK && dedupeKey.OK
	if !ok {
		return signalBucket{}, false
	}
	out := signalBucket{
		SignalType: signalType.Value, SignalCount: signalCount.Value, SessionCount: sessionCount.Value,
		EndpointGroup: endpointGroup.Value, Environment: environment.Value, RepoID: repo.Value, ReleaseRef: releaseRef.Value,
		BucketStart: bucketStart.Value, BucketEnd: bucketEnd.Value, IsSampled: sampled.Value,
		SchemaVersion: "1.0", DedupeKey: dedupeKey.Value,
	}
	if schemaVersion.Set {
		out.SchemaVersion = schemaVersion.Value
	}
	if unique.Set && !unique.Null {
		out.UniquePseudonymous = unique.Value
	}
	return out, true
}

// parseTelemetry is IngestTelemetryRequest: org_id, then items (1..5000).
func parseTelemetry(body pybody.Body) (batch, pybody.Errors) {
	var errs pybody.Errors
	object, ok := errs.Object(body)
	if !ok {
		return batch{}, errs
	}
	m := pybody.Model{Errors: &errs, Object: object, Loc: []pyjson.Value{"body"}}
	orgID := pybody.Get(m, "org_id", pybody.Required, pybody.Str)
	items := pybody.Get(m, "items", pybody.Required, pybody.SizedModelList(1, maxSignalItems, func(item pybody.Model) (signalBucket, bool) {
		return parseSignalBucket(item)
	}))
	if !(orgID.OK && items.OK) || len(errs) > 0 {
		return batch{}, errs
	}
	return batch{OrgID: orgID.Value, Items: len(items.Value), Signals: items.Value}, nil
}

// insertQuery is write_telemetry_signal_buckets' column list.
const insertQuery = "INSERT INTO telemetry_signal_bucket (org_id, signal_type, signal_count, session_count, unique_pseudonymous_count, " +
	"endpoint_group, environment, repo_id, release_ref, bucket_start, bucket_end, ingested_at, is_sampled, schema_version, dedupe_key)"

// ClickHouse is the connection the telemetry route writes with.
type ClickHouse interface {
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// errUnstorable is a value the ClickHouse client refuses (the Python route's
// unhandled exception, an unhandled 500 here too).
var errUnstorable = errors.New("legacyingest: a telemetry value does not fit its column")

var maxUInt64 = new(big.Int).SetUint64(^uint64(0))

// storable reports whether a count fits its UInt64 column.
func storable(value *big.Int) bool { return value.Sign() >= 0 && value.Cmp(maxUInt64) <= 0 }

// insertSignals is _persist_telemetry's write: every row in one insert, so a
// value the column cannot hold (a negative or oversized count, a string the
// client cannot encode) fails the whole request and stores nothing.
func insertSignals(ctx context.Context, conn ClickHouse, orgID string, rows []signalBucket, ingestedAt time.Time) error {
	for _, row := range rows {
		if !storable(row.SignalCount) || !storable(row.SessionCount) || (row.UniquePseudonymous != nil && !storable(row.UniquePseudonymous)) {
			return errUnstorable
		}
		for _, text := range []string{orgID, row.SignalType, row.EndpointGroup, row.Environment, row.RepoID, row.ReleaseRef, row.SchemaVersion, row.DedupeKey} {
			if pyjson.HasSurrogate(text) {
				return errUnstorable
			}
		}
	}
	batch, err := conn.PrepareBatch(ctx, insertQuery)
	if err != nil {
		return err
	}
	for _, row := range rows {
		var unique any
		if row.UniquePseudonymous != nil {
			value := row.UniquePseudonymous.Uint64()
			unique = &value
		}
		sampled := uint8(0)
		if row.IsSampled {
			sampled = 1
		}
		if err := batch.Append(orgID, row.SignalType, row.SignalCount.Uint64(), row.SessionCount.Uint64(), unique,
			row.EndpointGroup, row.Environment, row.RepoID, row.ReleaseRef, row.BucketStart.Time.UTC(), row.BucketEnd.Time.UTC(),
			ingestedAt, sampled, row.SchemaVersion, row.DedupeKey); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}
