// Package syncreconciler observes the legacy sync-dispatch outbox during the
// phased worker migration. It is deliberately read-only: execution, claims,
// and transport delivery remain owned by the existing Celery reconciler.
package syncreconciler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	minimumStepLimit = 1
	maximumStepLimit = 100

	// PredicateVersion names the exact due-row predicate and ordering shared
	// with Python claim_due_outbox_rows.
	PredicateVersion = "sync_dispatch_due_v1"
	// DigestVersion names the canonical metadata and candidate framing used by
	// CandidateDigest.
	DigestVersion = "sync_dispatch_candidate_digest_v1"
)

var (
	// ErrInvalidConfiguration is returned before an observer can make an
	// unbounded or ambiguous database observation.
	ErrInvalidConfiguration = errors.New("invalid sync dispatch observer configuration")
	// ErrUnavailable deliberately hides database details from lifecycle and
	// metrics paths while still closing readiness.
	ErrUnavailable = errors.New("sync dispatch observer database unavailable")
	// ErrUnknownKind makes contract drift inside the bounded candidate window
	// a fail-closed condition. The returned Observation retains bounded
	// diagnostic counts and the parity digest.
	ErrUnknownKind = errors.New("sync dispatch observer found an unknown kind")
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// Registry is the intentionally small route-policy seam used by the
// observer. syncdispatchcontract.Registry satisfies it without coupling this
// package to artifact loading or route execution.
type Registry interface {
	Lookup(string) (syncdispatchcontract.Descriptor, bool)
}

// KindObservation is a bounded claim-order snapshot for one fixed v1 kind.
// Route is copied from the checked-in contract and is never inferred from a
// stored outbox row.
type KindObservation struct {
	Kind          string
	Route         string
	DuePending    int64
	ExpiredClaims int64
}

// Observation describes only the first Limit due rows in Python claim order.
// ObservedAt is always the UTC cutoff supplied to the SQL predicate.
// CandidateDigest is sha256:<lowercase hex> over canonical metadata followed
// by the sampled candidates in order.
//
// Every field uses this cross-language byte framing:
//
//	decimal UTF-8 byte length of field name, ":", field-name bytes,
//	decimal UTF-8 byte length of value, ":", value bytes, "\n"
//
// Fields occur in this exact order:
//
//	digest_version, predicate_version, observed_at, limit,
//	then candidate_kind, candidate_id for every sampled row.
//
// observed_at is UTC with exactly nine fractional digits and a trailing Z;
// limit is base-10 ASCII; UUIDs are canonical lowercase text. Length prefixes
// make arbitrary UTF-8 kinds unambiguous. Metrics deliberately omit all string
// parity fields.
type Observation struct {
	Kinds             []KindObservation
	UnknownKindCount  int64
	CeleryDuePending  int64
	RiverDuePending   int64
	SampledCandidates int64
	Truncated         bool
	ObservedAt        time.Time
	Limit             int
	PredicateVersion  string
	DigestVersion     string
	CandidateDigest   string
}

type candidateRow struct {
	id             string
	kind           string
	claimExpiresAt *time.Time
}

type readFunc func(context.Context, time.Time, int) ([]candidateRow, error)

// Observer performs a single bounded, ordered SELECT using pgxpool.
type Observer struct {
	descriptors []syncdispatchcontract.Descriptor
	byKind      map[string]syncdispatchcontract.Descriptor
	read        readFunc
}

var frozenKinds = []string{
	syncdispatchcontract.KindDispatchSyncRun,
	syncdispatchcontract.KindFinalizeSyncRun,
	syncdispatchcontract.KindPostSync,
	syncdispatchcontract.KindReferenceDiscovery,
}

// NewObserver constructs a database-backed, observe-only reconciler. No
// transaction is opened because this component must never claim, lock, or
// mutate a sync-dispatch row.
func NewObserver(pool *pgxpool.Pool, registry Registry) (*Observer, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	return newObserver(registry, func(ctx context.Context, now time.Time, limit int) ([]candidateRow, error) {
		return readObservation(ctx, pool, now, limit)
	})
}

func newObserver(registry Registry, read readFunc) (*Observer, error) {
	descriptors, err := fixedDescriptors(registry)
	if err != nil || read == nil {
		return nil, ErrInvalidConfiguration
	}
	byKind := make(map[string]syncdispatchcontract.Descriptor, len(descriptors))
	for _, descriptor := range descriptors {
		byKind[descriptor.Kind] = descriptor
	}
	return &Observer{descriptors: descriptors, byKind: byKind, read: read}, nil
}

// Step takes exactly one bounded database snapshot. It requests one extra row
// solely to prove truncation, then aggregates and digests only the first limit
// rows.
func (observer *Observer) Step(ctx context.Context, now time.Time, limit int) (Observation, error) {
	if observer == nil || observer.read == nil || ctx == nil || now.IsZero() ||
		limit < minimumStepLimit || limit > maximumStepLimit {
		return Observation{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return Observation{}, err
	}

	now = now.UTC()
	rows, err := observer.read(ctx, now, limit+1)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return Observation{}, contextErr
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return Observation{}, err
		}
		return Observation{}, ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return Observation{}, err
	}
	return observer.buildObservation(rows, now, limit)
}

func fixedDescriptors(registry Registry) ([]syncdispatchcontract.Descriptor, error) {
	if registry == nil {
		return nil, ErrInvalidConfiguration
	}
	descriptors := make([]syncdispatchcontract.Descriptor, 0, len(frozenKinds))
	for _, kind := range frozenKinds {
		descriptor, ok := registry.Lookup(kind)
		expectedDelivery := syncdispatchcontract.DeliveryAtLeastOnce
		if kind == syncdispatchcontract.KindPostSync {
			expectedDelivery = syncdispatchcontract.DeliveryAtMostOnceMarkBefore
		}
		if !ok || descriptor.Kind != kind || descriptor.Delivery != expectedDelivery ||
			descriptor.RollbackRoute != syncdispatchcontract.RouteCelery ||
			(descriptor.Route != syncdispatchcontract.RouteCelery && descriptor.Route != syncdispatchcontract.RouteRiver) {
			return nil, ErrInvalidConfiguration
		}
		descriptors = append(descriptors, descriptor)
	}
	sort.Slice(descriptors, func(left, right int) bool { return descriptors[left].Kind < descriptors[right].Kind })
	return descriptors, nil
}

func (observer *Observer) buildObservation(rows []candidateRow, now time.Time, limit int) (Observation, error) {
	if len(rows) > limit+1 {
		return Observation{}, ErrUnavailable
	}
	sampled := rows
	truncated := len(rows) > limit
	if truncated {
		sampled = rows[:limit]
	}

	result := Observation{
		Kinds:             make([]KindObservation, 0, len(observer.descriptors)),
		SampledCandidates: int64(len(sampled)),
		Truncated:         truncated,
		ObservedAt:        now,
		Limit:             limit,
		PredicateVersion:  PredicateVersion,
		DigestVersion:     DigestVersion,
	}
	kindIndexes := make(map[string]int, len(observer.descriptors))
	for _, descriptor := range observer.descriptors {
		kindIndexes[descriptor.Kind] = len(result.Kinds)
		result.Kinds = append(result.Kinds, KindObservation{Kind: descriptor.Kind, Route: descriptor.Route})
	}

	hasher := sha256.New()
	writeDigestField(hasher, "digest_version", result.DigestVersion)
	writeDigestField(hasher, "predicate_version", result.PredicateVersion)
	writeDigestField(hasher, "observed_at", canonicalObservedAt(result.ObservedAt))
	writeDigestField(hasher, "limit", strconv.Itoa(result.Limit))
	seenIDs := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		if !uuidPattern.MatchString(row.id) {
			return Observation{}, ErrUnavailable
		}
		if _, duplicate := seenIDs[row.id]; duplicate {
			return Observation{}, ErrUnavailable
		}
		seenIDs[row.id] = struct{}{}
		if row.claimExpiresAt != nil && row.claimExpiresAt.After(now) {
			return Observation{}, ErrUnavailable
		}
		if index >= limit {
			continue
		}

		writeDigestField(hasher, "candidate_kind", row.kind)
		writeDigestField(hasher, "candidate_id", row.id)
		kindIndex, known := kindIndexes[row.kind]
		if !known {
			result.UnknownKindCount++
			continue
		}
		result.Kinds[kindIndex].DuePending++
		if row.claimExpiresAt != nil {
			result.Kinds[kindIndex].ExpiredClaims++
		}
		switch observer.byKind[row.kind].Route {
		case syncdispatchcontract.RouteCelery:
			result.CeleryDuePending++
		case syncdispatchcontract.RouteRiver:
			result.RiverDuePending++
		default:
			return Observation{}, ErrUnavailable
		}
	}
	result.CandidateDigest = "sha256:" + hex.EncodeToString(hasher.Sum(nil))

	knownDue := result.CeleryDuePending + result.RiverDuePending
	if knownDue < 0 || result.UnknownKindCount < 0 ||
		knownDue > result.SampledCandidates ||
		knownDue+result.UnknownKindCount != result.SampledCandidates ||
		result.SampledCandidates > int64(limit) ||
		(result.Truncated && result.SampledCandidates != int64(limit)) {
		return Observation{}, ErrUnavailable
	}
	if result.UnknownKindCount != 0 {
		return result, ErrUnknownKind
	}
	return result, nil
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeDigestField(output digestWriter, name, value string) {
	_, _ = output.Write([]byte(strconv.Itoa(len(name))))
	_, _ = output.Write([]byte(":"))
	_, _ = output.Write([]byte(name))
	_, _ = output.Write([]byte(strconv.Itoa(len(value))))
	_, _ = output.Write([]byte(":"))
	_, _ = output.Write([]byte(value))
	_, _ = output.Write([]byte("\n"))
}

func canonicalObservedAt(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func readObservation(
	ctx context.Context,
	pool *pgxpool.Pool,
	now time.Time,
	limit int,
) ([]candidateRow, error) {
	rows, err := pool.Query(ctx, observationSQL, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]candidateRow, 0, limit)
	for rows.Next() {
		var row candidateRow
		if err := rows.Scan(&row.id, &row.kind, &row.claimExpiresAt); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// observationSQL mirrors Python claim_due_outbox_rows exactly. The caller
// passes limit+1 so the read remains bounded while one extra row can prove the
// sampled claim-order window was truncated.
const observationSQL = `
SELECT outbox.id::text, outbox.kind, outbox.claim_expires_at
FROM public.sync_dispatch_outbox AS outbox
WHERE outbox.status = 'pending'
    AND outbox.available_at <= $1
    AND (outbox.claim_expires_at IS NULL OR outbox.claim_expires_at <= $1)
ORDER BY outbox.available_at, outbox.id
LIMIT $2
`
