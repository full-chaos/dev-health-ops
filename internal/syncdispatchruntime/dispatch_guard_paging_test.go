package syncdispatchruntime

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
)

// guardEnvRecord is what a decision wrote to its environment: the bucket locks, the per-bucket log inputs.
type guardEnvRecord struct {
	locked  []dispatchBucket
	emitted []string
}

func recordingGuardEnv(totalCap int, active map[dispatchBucket]int) (guardEnv, *guardEnvRecord) {
	record := &guardEnvRecord{}
	return guardEnv{
		lockBuckets: func(buckets []dispatchBucket) error {
			record.locked = append([]dispatchBucket(nil), buckets...)
			return nil
		},
		totalCap:    func() int { return totalCap },
		activeCount: func(bucket dispatchBucket) (int, error) { return active[bucket], nil },
		emit: func(bucket dispatchBucket, activeCount, reclaimedStale, cappedNew, cappedStale, headroom int) {
			record.emitted = append(record.emitted, fmt.Sprintf("%v %d %d %d %d %d", bucket, activeCount, reclaimedStale, cappedNew, cappedStale, headroom))
		},
	}, record
}

// referenceGuardDecision is the unpaged authorizeRun decision (every unit in memory), kept as the oracle.
func referenceGuardDecision(units []dispatchGuardUnit, env guardEnv, orgID string, now time.Time) (guardDecision, error) {
	staleCutoff := now.Add(-staleDispatchSeconds())
	lockBucketsByUnit := map[dispatchBucket][]dispatchGuardUnit{}
	candidatesByBucket := map[dispatchBucket][]dispatchGuardUnit{}
	deferredBuckets := map[dispatchBucket]bool{}
	for _, unit := range units {
		bucket := dispatchBucket{orgID: orgID, provider: unit.provider, costClass: unit.costClass}
		switch unit.status {
		case syncRunUnitStatusPlanned:
			candidatesByBucket[bucket] = append(candidatesByBucket[bucket], unit)
			lockBucketsByUnit[bucket] = append(lockBucketsByUnit[bucket], unit)
		case syncRunUnitStatusDispatching:
			if !unit.updatedAt.After(staleCutoff) {
				candidatesByBucket[bucket] = append(candidatesByBucket[bucket], unit)
				lockBucketsByUnit[bucket] = append(lockBucketsByUnit[bucket], unit)
			}
		case syncRunUnitStatusRetrying:
			lockBucketsByUnit[bucket] = append(lockBucketsByUnit[bucket], unit)
			if unit.availableAt != nil {
				if !unit.availableAt.After(now) {
					candidatesByBucket[bucket] = append(candidatesByBucket[bucket], unit)
				} else {
					deferredBuckets[bucket] = true
				}
			}
		}
	}
	keys := func(a map[dispatchBucket][]dispatchGuardUnit, b map[dispatchBucket]bool) []dispatchBucket {
		seen := map[dispatchBucket]bool{}
		for bucket := range a {
			seen[bucket] = true
		}
		for bucket := range b {
			seen[bucket] = true
		}
		return sortBucketList(seen)
	}
	if err := env.lockBuckets(keys(lockBucketsByUnit, nil)); err != nil {
		return guardDecision{}, err
	}
	allBuckets := keys(candidatesByBucket, deferredBuckets)
	totalCap := env.totalCap()
	if len(units) > totalCap {
		capped := make([]string, 0, len(units)-totalCap)
		for _, unit := range units[totalCap:] {
			capped = append(capped, unit.id)
		}
		return guardDecision{allowed: false, reason: fmt.Sprintf("sync run unit cap exceeded: %d/%d", len(units), totalCap), cappedUnitIDs: capped}, nil
	}
	authorizedUnitIDs := make(map[string]bool)
	for _, bucketUnits := range candidatesByBucket {
		for _, unit := range bucketUnits {
			authorizedUnitIDs[unit.id] = true
		}
	}
	var cappedUnitIDs []string
	slotHeadroom := make(map[dispatchBucket]int, len(allBuckets))
	for _, bucket := range allBuckets {
		activeCount, err := env.activeCount(bucket)
		if err != nil {
			return guardDecision{}, err
		}
		var reclaims, newWork []dispatchGuardUnit
		for _, unit := range candidatesByBucket[bucket] {
			if unit.status == syncRunUnitStatusDispatching {
				reclaims = append(reclaims, unit)
			} else {
				newWork = append(newWork, unit)
			}
		}
		sort.SliceStable(reclaims, func(i, j int) bool {
			if !reclaims[i].updatedAt.Equal(reclaims[j].updatedAt) {
				return reclaims[i].updatedAt.Before(reclaims[j].updatedAt)
			}
			return reclaims[i].id < reclaims[j].id
		})
		allowedSlots := concurrencyCapForCostClass(bucket.costClass) - activeCount
		if allowedSlots < 0 {
			allowedSlots = 0
		}
		prioritized := append(append([]dispatchGuardUnit{}, reclaims...), newWork...)
		var cappedNew, cappedStale int
		if len(prioritized) > allowedSlots {
			for _, unit := range prioritized[allowedSlots:] {
				cappedUnitIDs = append(cappedUnitIDs, unit.id)
				if unit.status == syncRunUnitStatusDispatching {
					cappedStale++
				} else {
					cappedNew++
				}
			}
		}
		headroom := allowedSlots - len(prioritized)
		if headroom < 0 {
			headroom = 0
		}
		slotHeadroom[bucket] = headroom
		reclaimedStale := len(reclaims)
		if reclaimedStale > allowedSlots {
			reclaimedStale = allowedSlots
		}
		env.emit(bucket, activeCount, reclaimedStale, cappedNew, cappedStale, headroom)
	}
	if len(cappedUnitIDs) > 0 {
		return guardDecision{allowed: true, reason: fmt.Sprintf("sync unit concurrency cap exceeded: %d capped", len(cappedUnitIDs)), cappedUnitIDs: cappedUnitIDs, concurrencyCapped: true, slotHeadroom: slotHeadroom, authorizedUnitIDs: authorizedUnitIDs}, nil
	}
	return guardDecision{allowed: true, slotHeadroom: slotHeadroom, authorizedUnitIDs: authorizedUnitIDs}, nil
}

func randomGuardUnits(random *rand.Rand, n int, now time.Time) []dispatchGuardUnit {
	providers := []string{"github", "gitlab", "jira"}
	classes := []string{"light", "medium", "heavy"}
	statuses := []string{syncRunUnitStatusPlanned, syncRunUnitStatusDispatching, syncRunUnitStatusRunning, syncRunUnitStatusRetrying, "success", "failed"}
	units := make([]dispatchGuardUnit, 0, n)
	for index := 0; index < n; index++ {
		unit := dispatchGuardUnit{
			id:        fmt.Sprintf("%08d", index),
			status:    statuses[random.Intn(len(statuses))],
			provider:  providers[random.Intn(len(providers))],
			costClass: classes[random.Intn(len(classes))],
			updatedAt: now.Add(-time.Duration(random.Intn(3600)) * time.Second),
		}
		switch random.Intn(3) {
		case 0:
		case 1:
			at := now.Add(-time.Duration(random.Intn(600)) * time.Second)
			unit.availableAt = &at
		case 2:
			at := now.Add(time.Duration(random.Intn(600)+1) * time.Second)
			unit.availableAt = &at
		}
		units = append(units, unit)
	}
	return units
}

func pagedGuardSource(units []dispatchGuardUnit, k int, maxRows *int) guardUnitSource {
	return func(fold func(dispatchGuardUnit) error) error {
		return forEachUnitPage(k, func(afterID string, limit int) ([]dispatchGuardUnit, error) {
			var page []dispatchGuardUnit
			for _, unit := range units {
				if afterID != "" && unit.id <= afterID {
					continue
				}
				page = append(page, unit)
				if len(page) == limit {
					break
				}
			}
			if len(page) > *maxRows {
				*maxRows = len(page)
			}
			return page, nil
		}, func(unit dispatchGuardUnit) string { return unit.id }, func(page []dispatchGuardUnit) error {
			for _, unit := range page {
				if err := fold(unit); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

func TestPagedGuardDecisionEqualsTheUnpagedDecisionAndReadsAtMostKRowsPerCall(t *testing.T) {
	const k = unitPageSize
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	var denied, concurrencyCapped, allowedClean int
	defer func() {
		if denied == 0 || concurrencyCapped == 0 || allowedClean == 0 {
			t.Errorf("the cases miss a decision shape: denied=%d concurrencyCapped=%d allowedClean=%d", denied, concurrencyCapped, allowedClean)
		}
	}()
	for _, n := range []int{0, k, k + 1, 10*k + 7} {
		for seed := int64(1); seed <= 6; seed++ {
			for _, capKind := range []string{"above", "below"} {
				name := fmt.Sprintf("N=%d seed=%d cap=%s", n, seed, capKind)
				t.Run(name, func(t *testing.T) {
					random := rand.New(rand.NewSource(seed*1000 + int64(n)))
					units := randomGuardUnits(random, n, now)
					active := map[dispatchBucket]int{}
					for _, provider := range []string{"github", "gitlab", "jira"} {
						for _, class := range []string{"light", "medium", "heavy"} {
							active[dispatchBucket{orgID: "org-1", provider: provider, costClass: class}] = random.Intn(5)
						}
					}
					totalCap := n + 10
					if capKind == "below" {
						totalCap = n / 2
					}
					refEnv, refRecord := recordingGuardEnv(totalCap, active)
					want, err := referenceGuardDecision(units, refEnv, "org-1", now)
					if err != nil {
						t.Fatal(err)
					}
					maxRows := 0
					gotEnv, gotRecord := recordingGuardEnv(totalCap, active)
					got, err := decideDispatchGuard(pagedGuardSource(units, k, &maxRows), gotEnv, "org-1", now)
					if err != nil {
						t.Fatal(err)
					}
					switch {
					case !want.allowed:
						denied++
					case want.concurrencyCapped:
						concurrencyCapped++
					default:
						allowedClean++
					}
					if maxRows > k {
						t.Fatalf("a call returned %d rows, want at most %d", maxRows, k)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("decision differs\n got: %+v\nwant: %+v", summarizeDecision(got), summarizeDecision(want))
					}
					if !reflect.DeepEqual(gotRecord.locked, refRecord.locked) {
						t.Fatalf("locked buckets = %v, want %v", gotRecord.locked, refRecord.locked)
					}
					if !reflect.DeepEqual(gotRecord.emitted, refRecord.emitted) {
						t.Fatalf("emitted = %v, want %v", gotRecord.emitted, refRecord.emitted)
					}
				})
			}
		}
	}
}

func summarizeDecision(decision guardDecision) string {
	return fmt.Sprintf("allowed=%v reason=%q capped=%d concurrencyCapped=%v headroom=%v authorized=%d",
		decision.allowed, decision.reason, len(decision.cappedUnitIDs), decision.concurrencyCapped, decision.slotHeadroom, len(decision.authorizedUnitIDs))
}

// A unit that turns up in a bucket the first pass did not lock is never authorized by the second pass.
func TestPagedGuardIgnoresAUnitInAnUnlockedBucketSeenOnlyByTheSecondPass(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	first := []dispatchGuardUnit{{id: "00000001", status: syncRunUnitStatusPlanned, provider: "github", costClass: "light", updatedAt: now}}
	second := append(append([]dispatchGuardUnit{}, first...),
		dispatchGuardUnit{id: "00000002", status: syncRunUnitStatusPlanned, provider: "gitlab", costClass: "heavy", updatedAt: now})
	pass := 0
	source := func(fold func(dispatchGuardUnit) error) error {
		pass++
		units := first
		if pass > 1 {
			units = second
		}
		for _, unit := range units {
			if err := fold(unit); err != nil {
				return err
			}
		}
		return nil
	}
	env, record := recordingGuardEnv(100, nil)
	decision, err := decideDispatchGuard(source, env, "org-1", now)
	if err != nil {
		t.Fatal(err)
	}
	if decision.authorizedUnitIDs["00000002"] || !decision.authorizedUnitIDs["00000001"] {
		t.Fatalf("authorized = %v, want only the unit of the locked bucket", decision.authorizedUnitIDs)
	}
	if len(record.locked) != 1 {
		t.Fatalf("locked = %v, want the one bucket of the first pass", record.locked)
	}
}
