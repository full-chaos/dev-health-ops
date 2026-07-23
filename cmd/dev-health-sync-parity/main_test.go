package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
)

func TestParseConfigRequiresBoundedLimitAndSecretEnvironment(t *testing.T) {
	lookup := func(values map[string]string) func(string) (string, bool) {
		return func(name string) (string, bool) {
			value, ok := values[name]
			return value, ok
		}
	}
	if _, err := parseConfig([]string{"--limit", "1"}, lookup(nil), &bytes.Buffer{}); err == nil {
		t.Fatal("parseConfig accepted no database environment")
	}
	if _, err := parseConfig([]string{"--limit", "0"}, lookup(map[string]string{"DATABASE_URI": "postgres://safe"}), &bytes.Buffer{}); err == nil {
		t.Fatal("parseConfig accepted an unbounded limit")
	}
	config, err := parseConfig(
		[]string{"--limit", "3", "--cutoff", "2026-07-23T12:00:00.123456Z"},
		lookup(map[string]string{"DATABASE_URI": "postgres://safe"}),
		&bytes.Buffer{},
	)
	if err != nil || config.CutoffSource != "argument" || config.Limit != 3 ||
		canonicalCutoff(config.Cutoff) != "2026-07-23T12:00:00.123456000Z" {
		t.Fatalf("parseConfig() = %#v, %v", config, err)
	}
	if _, err := parseConfig(
		[]string{"--limit", "3", "--cutoff", "2026-07-23T12:00:00.123456789Z"},
		lookup(map[string]string{"DATABASE_URI": "postgres://safe"}),
		&bytes.Buffer{},
	); err == nil {
		t.Fatal("parseConfig accepted cutoff precision Python cannot represent")
	}
	for _, uri := range []string{
		"postgresql+asyncpg://domain:secret@db/app?sslmode=require",
		"postgresql+psycopg://domain:secret@db/app?sslmode=require",
		"postgresql+psycopg2://domain:secret@db/app?sslmode=require",
		"postgres+asyncpg://domain:secret@db/app?sslmode=require",
	} {
		config, err := parseConfig(
			[]string{"--limit", "1"},
			lookup(map[string]string{"POSTGRES_URI": uri}),
			&bytes.Buffer{},
		)
		if err != nil || config.DatabaseURI != "postgresql://domain:secret@db/app?sslmode=require" {
			t.Fatalf("normalized config = %#v, %v", config, err)
		}
	}
}

func TestExecuteHelpNeedsNoDatabaseOrPythonRuntime(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := execute(
		context.Background(),
		[]string{"--help"},
		func(string) (string, bool) { return "", false },
		&stdout,
		&stderr,
		nil,
	)
	if code != 0 || !strings.Contains(stdout.String(), "--limit") || stderr.Len() != 0 ||
		strings.Contains(stdout.String(), "--contract-root") ||
		strings.Contains(stdout.String(), `"status"`) {
		t.Fatalf("help = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestCompareObservationsReportsOnlySafeRedactedFields(t *testing.T) {
	cutoff := time.Date(2026, time.July, 23, 12, 0, 0, 123456000, time.UTC)
	base := parityObservation{
		Event:             "sync_dispatch_parity_observation",
		Runtime:           "celery",
		ObservedAt:        canonicalCutoff(cutoff),
		Limit:             2,
		PredicateVersion:  syncreconciler.PredicateVersion,
		DigestVersion:     syncreconciler.DigestVersion,
		CandidateDigest:   "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SampledCandidates: 2,
		Kinds: []parityKind{
			{Kind: syncdispatchcontract.KindDispatchSyncRun, Route: syncdispatchcontract.RouteCelery, DuePending: 1},
			{Kind: syncdispatchcontract.KindFinalizeSyncRun, Route: syncdispatchcontract.RouteCelery},
			{Kind: syncdispatchcontract.KindPostSync, Route: syncdispatchcontract.RouteCelery},
			{Kind: syncdispatchcontract.KindReferenceDiscovery, Route: syncdispatchcontract.RouteCelery, DuePending: 1},
		},
		CeleryDuePending: 2,
	}
	actual := base
	actual.Runtime = "go_observer"
	actual.CandidateDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	actual.Kinds = append([]parityKind(nil), base.Kinds...)
	actual.Kinds[0].DuePending = 2
	mismatches := compareObservations(base, actual)
	if len(mismatches) != 2 || mismatches[0].Field != "candidate_digest" ||
		mismatches[1].Field != "kinds.dispatch_sync_run.due_pending" {
		t.Fatalf("mismatches = %#v", mismatches)
	}
	encoded, err := json.Marshal(mismatches)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"candidate_id", "org_id", "token", "postgres", "00000000-"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("mismatch output leaked %q: %s", forbidden, encoded)
		}
	}

	actual = base
	actual.Runtime = "go_observer"
	actual.Kinds = append([]parityKind(nil), base.Kinds...)
	actual.Kinds[0].Kind = syncdispatchcontract.KindFinalizeSyncRun
	mismatches = compareObservations(base, actual)
	if len(mismatches) != 1 || mismatches[0].Field != "kinds.dispatch_sync_run.kind" {
		t.Fatalf("kind mismatches = %#v", mismatches)
	}
}

func TestValidPythonObservationRequiresTheExistingRedactedContract(t *testing.T) {
	config := commandConfig{
		Cutoff: time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC),
		Limit:  1,
	}
	observation := parityObservation{
		Event:             "sync_dispatch_parity_observation",
		Runtime:           "celery",
		ObservedAt:        canonicalCutoff(config.Cutoff),
		Limit:             1,
		PredicateVersion:  syncreconciler.PredicateVersion,
		DigestVersion:     syncreconciler.DigestVersion,
		CandidateDigest:   "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SampledCandidates: 1,
		UnknownKindCount:  1,
		Kinds: []parityKind{
			{Kind: syncdispatchcontract.KindDispatchSyncRun, Route: syncdispatchcontract.RouteCelery},
			{Kind: syncdispatchcontract.KindFinalizeSyncRun, Route: syncdispatchcontract.RouteCelery},
			{Kind: syncdispatchcontract.KindPostSync, Route: syncdispatchcontract.RouteCelery},
			{Kind: syncdispatchcontract.KindReferenceDiscovery, Route: syncdispatchcontract.RouteCelery},
		},
	}
	if !validPythonObservation(observation, config) {
		t.Fatal("validPythonObservation rejected a bounded redacted observation")
	}
	observation.Kinds[0].Kind = "unbounded_label"
	if validPythonObservation(observation, config) {
		t.Fatal("validPythonObservation accepted an unbounded kind label")
	}
}

func TestReplacedEnvironmentDoesNotDuplicateSecretsOrSnapshotTokens(t *testing.T) {
	environment := replacedEnvironment(
		[]string{"PATH=/bin", "SYNC_DISPATCH_PARITY_SNAPSHOT_ID=old", "SYNC_DISPATCH_PARITY_DATABASE_URI=old"},
		map[string]string{
			"SYNC_DISPATCH_PARITY_SNAPSHOT_ID":  "new",
			"SYNC_DISPATCH_PARITY_DATABASE_URI": "postgres://safe",
		},
	)
	joined := strings.Join(environment, "\n")
	if strings.Count(joined, "SYNC_DISPATCH_PARITY_SNAPSHOT_ID=") != 1 ||
		strings.Count(joined, "SYNC_DISPATCH_PARITY_DATABASE_URI=") != 1 ||
		strings.Contains(joined, "old") {
		t.Fatalf("environment replacement = %q", joined)
	}
}
