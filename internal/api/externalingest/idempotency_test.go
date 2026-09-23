package externalingest

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestClassifyExisting(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := BatchRow{IngestionID: uuid.New(), PayloadHash: "hash-a", Status: "accepted", UpdatedAt: now}

	t.Run("different payload hash is CONFLICT, never overwritten", func(t *testing.T) {
		outcome := classifyExisting(base, "hash-b", now)
		if outcome.Kind != outcomeConflict {
			t.Fatalf("got %s", outcome.Kind)
		}
	})
	t.Run("stream_unavailable is always RETRY", func(t *testing.T) {
		row := base
		row.Status = "stream_unavailable"
		if got := classifyExisting(row, "hash-a", now).Kind; got != outcomeRetry {
			t.Fatalf("got %s", got)
		}
	})
	t.Run("failed is always RETRY", func(t *testing.T) {
		row := base
		row.Status = "failed"
		if got := classifyExisting(row, "hash-a", now).Kind; got != outcomeRetry {
			t.Fatalf("got %s", got)
		}
	})
	t.Run("fresh accepted is REPLAY", func(t *testing.T) {
		if got := classifyExisting(base, "hash-a", now).Kind; got != outcomeReplay {
			t.Fatalf("got %s", got)
		}
	})
	t.Run("accepted older than the stale window is RETRY", func(t *testing.T) {
		row := base
		row.UpdatedAt = now.Add(-time.Duration(acceptedStaleMinutesDefault+1) * time.Minute)
		if got := classifyExisting(row, "hash-a", now).Kind; got != outcomeRetry {
			t.Fatalf("got %s", got)
		}
	})
	t.Run("accepted exactly at the stale boundary is still REPLAY (age must exceed, not equal, the window)", func(t *testing.T) {
		row := base
		row.UpdatedAt = now.Add(-time.Duration(acceptedStaleMinutesDefault) * time.Minute)
		if got := classifyExisting(row, "hash-a", now).Kind; got != outcomeReplay {
			t.Fatalf("got %s", got)
		}
	})
	t.Run("stale processing REPLAYs, deliberately not RETRY (CC13's narrowing)", func(t *testing.T) {
		row := base
		row.Status = "processing"
		row.UpdatedAt = now.Add(-24 * time.Hour)
		if got := classifyExisting(row, "hash-a", now).Kind; got != outcomeReplay {
			t.Fatalf("got %s", got)
		}
	})
	t.Run("terminal completed/partial are REPLAY, not RETRY", func(t *testing.T) {
		for _, status := range []string{"completed", "partial"} {
			row := base
			row.Status = status
			if got := classifyExisting(row, "hash-a", now).Kind; got != outcomeReplay {
				t.Errorf("%s: got %s", status, got)
			}
		}
	})
}

func TestComputePayloadHashIsDeterministicAndContentSensitive(t *testing.T) {
	envelope, err := parseEnvelope([]byte(validEnvelopeJSON()))
	if err != nil {
		t.Fatal(err)
	}
	hashA, err := computePayloadHash(envelope)
	if err != nil {
		t.Fatal(err)
	}
	hashB, err := computePayloadHash(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if hashA != hashB {
		t.Fatal("hashing the same envelope twice must be deterministic")
	}

	other, err := parseEnvelope([]byte(`{
		"schemaVersion": "external-ingest.v1",
		"idempotencyKey": "key-1",
		"source": {"system": "github", "instance": "acme/repo"},
		"records": [{"kind": "repository.v1", "externalId": "acme/repo-different", "payload": {}}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	hashC, err := computePayloadHash(other)
	if err != nil {
		t.Fatal(err)
	}
	if hashA == hashC {
		t.Fatal("a different payload must hash differently")
	}
}

func TestComputePayloadHashIsIndifferentToTheDefaultEntityFamily(t *testing.T) {
	explicit, err := parseEnvelope([]byte(`{
		"schemaVersion": "external-ingest.v1",
		"idempotencyKey": "key-1",
		"source": {"system": "github", "instance": "acme/repo", "entityFamily": "legacy"},
		"records": [{"kind": "repository.v1", "externalId": "acme/repo", "payload": {}}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	implicit, err := parseEnvelope([]byte(validEnvelopeJSON()))
	if err != nil {
		t.Fatal(err)
	}
	hashExplicit, err := computePayloadHash(explicit)
	if err != nil {
		t.Fatal(err)
	}
	hashImplicit, err := computePayloadHash(implicit)
	if err != nil {
		t.Fatal(err)
	}
	if hashExplicit != hashImplicit {
		t.Fatal("an explicit \"legacy\" entityFamily and an omitted one must hash the same, " +
			"matching idempotency.py's explicit pop of the default")
	}
}
