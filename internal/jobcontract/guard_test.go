package jobcontract

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
)

func guardProbes(t testing.TB) []string {
	t.Helper()
	scanner, err := credshape.Default()
	if err != nil {
		t.Fatal(err)
	}
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	return probes
}

func heartbeatDocument(mutate func(document map[string]any)) []byte {
	document := map[string]any{
		"contract_version": 1,
		"correlation_id":   "job-heartbeat-0001",
		"idempotency_key":  "heartbeat:2026-07-21T12:00:00Z",
		"domain":           map[string]any{"type": "schedule_occurrence", "id": "00000000-0000-4000-8000-000000000001"},
		"payload":          map[string]any{"scheduled_for": "2026-07-21T12:00:00Z"},
	}
	if mutate != nil {
		mutate(document)
	}
	data, err := json.Marshal(document)
	if err != nil {
		panic(err)
	}
	return data
}

func wantRefusal(t *testing.T, err error, path string, value string) {
	t.Helper()
	if err == nil {
		t.Fatalf("a credential-shaped value at %s was accepted", path)
	}
	if !errors.Is(err, ErrCredentialShapedValue) {
		t.Fatalf("error %q is not ErrCredentialShapedValue", err)
	}
	var refusal *ValueRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("error %q carries no *ValueRefusal", err)
	}
	if refusal.Path != path {
		t.Errorf("path = %q, want %q", refusal.Path, path)
	}
	if want := path + ": credential-shaped value (rule " + refusal.Rule + ")"; err.Error() != want {
		t.Errorf("error text = %q, want %q", err.Error(), want)
	}
	if refusal.Rule == "" {
		t.Error("refusal names no rule")
	}
	if value != "" && strings.Contains(err.Error(), value) {
		t.Error("refusal echoes the value")
	}
}

func TestDecodeAcceptsTheControlEnvelope(t *testing.T) {
	if _, err := Decode(KindHeartbeat, heartbeatDocument(nil)); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeRefusesACredentialShapedValueInEveryEnvelopeString(t *testing.T) {
	probes := guardProbes(t)
	places := []struct {
		path   string
		mutate func(document map[string]any, value string)
	}{
		{"correlation_id", func(d map[string]any, v string) { d["correlation_id"] = v }},
		{"idempotency_key", func(d map[string]any, v string) { d["idempotency_key"] = v }},
		{"trace_parent", func(d map[string]any, v string) { d["trace_parent"] = v }},
		{"domain.type", func(d map[string]any, v string) { d["domain"].(map[string]any)["type"] = v }},
		{"domain.id", func(d map[string]any, v string) { d["domain"].(map[string]any)["id"] = v }},
		{"payload.scheduled_for", func(d map[string]any, v string) { d["payload"].(map[string]any)["scheduled_for"] = v }},
		{"payload.extra.note", func(d map[string]any, v string) {
			d["payload"].(map[string]any)["extra"] = map[string]any{"note": v}
		}},
		{"payload.items[].note", func(d map[string]any, v string) {
			d["payload"].(map[string]any)["items"] = []any{"fine", map[string]any{"note": v}}
		}},
	}
	for _, place := range places {
		t.Run(place.path, func(t *testing.T) {
			for _, probe := range probes {
				_, err := Decode(KindHeartbeat, heartbeatDocument(func(d map[string]any) { place.mutate(d, probe) }))
				wantRefusal(t, err, place.path, probe)
			}
		})
	}
}

func TestDecodeRefusesAValueWrittenWithJSONEscapes(t *testing.T) {
	probe := guardProbes(t)[0]
	data := heartbeatDocument(func(d map[string]any) { d["correlation_id"] = probe })
	escaped := strings.Replace(string(data), probe, `\u`+"0"+`065`+probe[1:], 1)
	if escaped == string(data) {
		t.Fatal("test did not escape the value")
	}
	_, err := Decode(KindHeartbeat, []byte(escaped))
	wantRefusal(t, err, "correlation_id", probe)
}

func TestRefusalPathNeverCarriesAFreeTextKey(t *testing.T) {
	probe := guardProbes(t)[0]
	data := heartbeatDocument(func(d map[string]any) {
		d["payload"].(map[string]any)[probe] = probe
	})
	_, err := Decode(KindHeartbeat, data)
	wantRefusal(t, err, "payload.*", probe)
}

func TestRefusalPathPrintsAnUnsafeKeyAsAStar(t *testing.T) {
	probe := guardProbes(t)[0]
	for _, key := range []string{"Free Text Key", "UPPER", "with-dash", "9starts_with_digit", strings.Repeat("a", 65)} {
		data := heartbeatDocument(func(d map[string]any) {
			d["payload"].(map[string]any)[key] = map[string]any{"note": probe}
		})
		_, err := Decode(KindHeartbeat, data)
		wantRefusal(t, err, "payload.*.note", key)
	}
}

func TestMarshalCanonicalRefusesATypedFieldBeforeReturningBytes(t *testing.T) {
	probe := guardProbes(t)[0]
	data, err := MarshalCanonical(Envelope{
		ContractVersion: 1, CorrelationID: "job-heartbeat-0001", IdempotencyKey: "heartbeat:2026-07-21T12:00:00Z",
		Domain:  DomainLink{Type: "schedule_occurrence", ID: "00000000-0000-4000-8000-000000000001"},
		Payload: HeartbeatPayload{ScheduledFor: probe},
	})
	if data != nil {
		t.Fatal("MarshalCanonical returned bytes for a credential-shaped value")
	}
	wantRefusal(t, err, "payload.scheduled_for", probe)
}

func TestLenientDecodeOfAMergeBaseIsNotGuarded(t *testing.T) {
	probe := guardProbes(t)[0]
	var target map[string]any
	if err := decodeLenient(heartbeatDocument(func(d map[string]any) { d["correlation_id"] = probe }), MaxEnvelopeBytes, &target); err != nil {
		t.Fatalf("decodeLenient refused a merge-base file: %v", err)
	}
}

func TestEveryRegisteredFixtureDecodesUnderTheGuard(t *testing.T) {
	root := contractRoot(t)
	registry, err := LoadRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(registry.Jobs) < 24 {
		t.Fatalf("registry lists %d kinds", len(registry.Jobs))
	}
	for _, job := range registry.Jobs {
		fixtures := 0
		for _, paths := range job.Fixtures {
			for _, path := range paths {
				fixtures++
				data, err := os.ReadFile(filepath.Join(root, path))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := Decode(job.Kind, data); err != nil {
					t.Errorf("%s fixture %s: %v", job.Kind, path, err)
				}
			}
		}
		if fixtures == 0 {
			t.Errorf("%s has no fixture", job.Kind)
		}
	}
}

func TestGuardAddsNoAllocationsToTheTokenWalk(t *testing.T) {
	scanner, err := credshape.Default()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(contractRoot(t), "examples", "sync.provider_unit.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	plain := testing.AllocsPerRun(200, func() {
		if err := validateJSONTokens(data, nil); err != nil {
			t.Fatal(err)
		}
	})
	guarded := testing.AllocsPerRun(200, func() {
		guard := valueGuard{scanner: scanner}
		if err := validateJSONTokens(data, &guard); err != nil {
			t.Fatal(err)
		}
	})
	if guarded != plain {
		t.Fatalf("token walk allocs: plain %v, guarded %v", plain, guarded)
	}
}

func BenchmarkDecodeGuarded(b *testing.B) {
	data, err := os.ReadFile(filepath.Join(contractRoot(b), "examples", "sync.provider_unit.v1.json"))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Decode(KindSyncProviderUnit, data); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkTokenWalk(b *testing.B, guarded bool) {
	scanner, err := credshape.Default()
	if err != nil {
		b.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(contractRoot(b), "examples", "sync.provider_unit.v1.json"))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var guard *valueGuard
		if guarded {
			guard = &valueGuard{scanner: scanner}
		}
		if err := validateJSONTokens(data, guard); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTokenWalkPlain(b *testing.B)   { benchmarkTokenWalk(b, false) }
func BenchmarkTokenWalkGuarded(b *testing.B) { benchmarkTokenWalk(b, true) }
