package sync

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strconv"
	"time"
)

const (
	minimumSnapshotLimit = 1
	maximumSnapshotLimit = 100
)

// BuildSnapshot evaluates at most limit candidates plus one truncation proof.
// Ordering is fixed by config ID so equivalent PostgreSQL snapshots compare
// deterministically even when their source scan order changes.
func BuildSnapshot(observedAt time.Time, limit int, candidates []Candidate) (Snapshot, error) {
	return buildSnapshotContext(context.Background(), observedAt, limit, candidates)
}

func buildSnapshotContext(
	ctx context.Context,
	observedAt time.Time,
	limit int,
	candidates []Candidate,
) (Snapshot, error) {
	if ctx == nil {
		return Snapshot{}, fmt.Errorf("snapshot context is required")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if limit < minimumSnapshotLimit || limit > maximumSnapshotLimit {
		return Snapshot{}, fmt.Errorf("snapshot limit must be between %d and %d", minimumSnapshotLimit, maximumSnapshotLimit)
	}
	if len(candidates) > limit+1 {
		return Snapshot{}, fmt.Errorf("snapshot exceeds bounded candidate window")
	}
	ordered := append([]Candidate(nil), candidates...)
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].ConfigID < ordered[right].ConfigID })
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	truncated := len(ordered) > limit
	if truncated {
		ordered = ordered[:limit]
	}
	observedAt = observedAt.UTC()
	result := Snapshot{
		ObservedAt:        observedAt,
		Limit:             limit,
		Truncated:         truncated,
		DigestVersion:     TimingDigestVersion,
		EvaluationVersion: EvaluationVersion,
		EligibilityScope:  ScheduleMarkerEvaluationScope,
		Candidates:        make([]EvaluatedCandidate, 0, len(ordered)),
	}
	for _, candidate := range ordered {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, err
		}
		evaluation, err := evaluateContext(ctx, candidate, observedAt)
		if err != nil {
			return Snapshot{}, err
		}
		result.Candidates = append(result.Candidates, EvaluatedCandidate{
			Candidate: candidate, Evaluation: evaluation,
		})
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	result.CandidateDigest = candidateDigest(result)
	return result, nil
}

func candidateDigest(snapshot Snapshot) string {
	hasher := sha256.New()
	writeDigestField(hasher, "digest_version", snapshot.DigestVersion)
	writeDigestField(hasher, "evaluation_version", snapshot.EvaluationVersion)
	writeDigestField(hasher, "eligibility_scope", snapshot.EligibilityScope)
	writeDigestField(hasher, "observed_at", canonicalTime(snapshot.ObservedAt))
	writeDigestField(hasher, "limit", strconv.Itoa(snapshot.Limit))
	writeDigestField(hasher, "truncated", strconv.FormatBool(snapshot.Truncated))
	for _, evaluated := range snapshot.Candidates {
		writeDigestField(hasher, "config_id", evaluated.Candidate.ConfigID)
		writeDigestField(hasher, "decision", string(evaluated.Evaluation.Decision))
		writeDigestField(hasher, "due", strconv.FormatBool(evaluated.Evaluation.Due))
		writeDigestField(hasher, "timing_eligible", strconv.FormatBool(evaluated.Evaluation.TimingEligible))
		writeDigestField(hasher, "running_marker", string(evaluated.Evaluation.RunningMarker))
		writeDigestField(hasher, "timezone", evaluated.Evaluation.Timezone)
		writeDigestField(hasher, "timezone_fallback", strconv.FormatBool(evaluated.Evaluation.TimezoneFallback))
		next := ""
		if evaluated.Evaluation.NextOccurrence != nil {
			next = canonicalTime(*evaluated.Evaluation.NextOccurrence)
		}
		writeDigestField(hasher, "next_occurrence", next)
	}
	return "sha256:" + fmt.Sprintf("%x", hasher.Sum(nil))
}

type digestWriter interface{ Write([]byte) (int, error) }

func writeDigestField(output digestWriter, name, value string) {
	_, _ = output.Write([]byte(strconv.Itoa(len([]byte(name)))))
	_, _ = output.Write([]byte(":"))
	_, _ = output.Write([]byte(name))
	_, _ = output.Write([]byte(strconv.Itoa(len([]byte(value)))))
	_, _ = output.Write([]byte(":"))
	_, _ = output.Write([]byte(value))
	_, _ = output.Write([]byte("\n"))
}

func canonicalTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}
