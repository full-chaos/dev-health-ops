package policy

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

func TestNewShadowRefusesRowsThatDifferFromTheirCatalogEntry(t *testing.T) {
	ok := funcDecider{func(context.Context, pep.Request) pep.Decision { return pep.Decision{} }}
	catalogRows, err := ParseShadowRows(authv1.GoAPIEndpointProfiles, "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	var key string
	var base ShadowRow
	for k, row := range catalogRows {
		if row.ResourceType == "organization" && !row.HighRisk && row.Namespace == "teams" {
			key, base = k, row
			break
		}
	}
	if key == "" {
		t.Fatal("no low-risk organization teams row in the profiles")
	}
	if _, err := NewShadow(ShadowConfig{Decider: ok, Rows: map[string]ShadowRow{key: base}}); err != nil {
		t.Fatalf("a row equal to its catalog entry was refused: %v", err)
	}
	for name, mutate := range map[string]func(ShadowRow) ShadowRow{
		"unknown action":     func(r ShadowRow) ShadowRow { r.Action = "teams.nonexistent"; return r },
		"wrong namespace":    func(r ShadowRow) ShadowRow { r.Namespace = "billing"; return r },
		"wrong resource":     func(r ShadowRow) ShadowRow { r.ResourceType = "platform"; return r },
		"wrong high-risk":    func(r ShadowRow) ShadowRow { r.HighRisk = true; return r },
		"action of another":  func(r ShadowRow) ShadowRow { r.Action = "billing.read"; return r },
		"namespace of other": func(r ShadowRow) ShadowRow { r.Namespace = "settings"; return r },
	} {
		if _, err := NewShadow(ShadowConfig{Decider: ok, Rows: map[string]ShadowRow{key: mutate(base)}}); err == nil {
			t.Errorf("%s: NewShadow accepted a row that differs from its catalog entry", name)
		}
	}
}

func TestShadowFromEnvNilLookupIsOffWithoutPanic(t *testing.T) {
	state := recordState(t)
	var logs bytes.Buffer
	deps := envDeps(&logs)
	deps.NewDecider = func() (pep.Decider, error) {
		t.Error("NewDecider was called for a nil lookup")
		return nil, nil
	}
	var s *Shadow
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("ShadowFromEnv panicked on a nil lookup: %v", r)
			}
		}()
		s = ShadowFromEnv(nil, deps)
	}()
	if s != nil {
		t.Fatal("a shadow was built from a nil lookup")
	}
	if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "lookup") {
		t.Errorf("log %q, want an ERROR naming the lookup", logs.String())
	}
	if len(*state) != 1 || (*state)[0] != (shadowState{false, true}) {
		t.Errorf("state %v, want one write: off and config-invalid", *state)
	}
}
