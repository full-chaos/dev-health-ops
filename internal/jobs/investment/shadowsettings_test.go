package investment

import (
	"strings"
	"testing"
	"time"
)

// Default OFF: with no provider nothing else is read as "on", whatever the
// other variables hold.
func TestShadowSettingsAreOffByDefault(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"nothing set": {},
		"everything but the provider": {
			EnvShadowOrgIDs: "*", EnvShadowSamplePercent: "100", EnvShadowConcurrency: "8",
			EnvShadowMaxSeconds: "60", EnvShadowMaxUSDPerRun: "1",
		},
		"a provider of white space": {EnvShadowProvider: "   ", EnvShadowOrgIDs: "*"},
	} {
		settings, err := ShadowSettingsFromEnv(envLookup(env))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if settings.Provider != "" || settings.EnabledFor("org-a") || settings.unitSelected("org-a", "unit-1") {
			t.Fatalf("%s: the phase is on: %+v", name, settings)
		}
	}
}

// A provider with no org list is OFF: it fails closed.
func TestAnEmptyOrgListFailsClosed(t *testing.T) {
	for name, orgs := range map[string]string{"unset": "", "white space": "  ", "only separators": " , ,, "} {
		settings, err := ShadowSettingsFromEnv(envLookup(map[string]string{EnvShadowProvider: "typesafe", EnvShadowOrgIDs: orgs}))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if settings.Provider != "typesafe" {
			t.Fatalf("%s: provider = %q", name, settings.Provider)
		}
		if settings.EnabledFor("org-a") || settings.EnabledFor("") || settings.AllOrgs || len(settings.OrgIDs) != 0 {
			t.Fatalf("%s: a provider with no org list is on: %+v", name, settings)
		}
	}
}

func TestShadowSettingsDefaultsAndValues(t *testing.T) {
	settings, err := ShadowSettingsFromEnv(envLookup(map[string]string{EnvShadowProvider: " TypeSafe ", EnvShadowOrgIDs: "org-a, org-b"}))
	if err != nil {
		t.Fatal(err)
	}
	if settings.Provider != "typesafe" || settings.SamplePercent != 100 || settings.Concurrency != 4 ||
		settings.Budget != 60*time.Second || settings.MaxNanoUSD != 500_000_000 {
		t.Fatalf("defaults = %+v", settings)
	}
	if !settings.EnabledFor("org-a") || !settings.EnabledFor("org-b") || settings.EnabledFor("org-c") || settings.EnabledFor("") {
		t.Fatalf("org list = %+v", settings)
	}

	settings, err = ShadowSettingsFromEnv(envLookup(map[string]string{
		EnvShadowProvider: "typesafe", EnvShadowOrgIDs: "*", EnvShadowSamplePercent: "10",
		EnvShadowConcurrency: "8", EnvShadowMaxSeconds: "45", EnvShadowMaxUSDPerRun: "0.25",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !settings.AllOrgs || settings.SamplePercent != 10 || settings.Concurrency != 8 ||
		settings.Budget != 45*time.Second || settings.MaxNanoUSD != 250_000_000 {
		t.Fatalf("values = %+v", settings)
	}
	if !settings.EnabledFor("any-org") || settings.EnabledFor("") {
		t.Fatal(`"*" must allow every org and never the empty org`)
	}
}

// The clamps hold in code, whatever is configured.
func TestShadowSettingsClamps(t *testing.T) {
	for _, tc := range []struct {
		name, variable, value string
		check                 func(ShadowSettings) bool
	}{
		{"seconds 600 -> 120", EnvShadowMaxSeconds, "600", func(s ShadowSettings) bool { return s.Budget == 120*time.Second }},
		{"seconds 121 -> 120", EnvShadowMaxSeconds, "121", func(s ShadowSettings) bool { return s.Budget == 120*time.Second }},
		{"seconds 120 stays", EnvShadowMaxSeconds, "120", func(s ShadowSettings) bool { return s.Budget == 120*time.Second }},
		{"seconds 119 stays", EnvShadowMaxSeconds, "119", func(s ShadowSettings) bool { return s.Budget == 119*time.Second }},
		// 9223372037 s is one second more than a time.Duration can hold: a
		// multiplication before the clamp would wrap to a negative budget.
		{"a number of seconds over the Duration range cannot wrap", EnvShadowMaxSeconds, "9223372037", func(s ShadowSettings) bool { return s.Budget == 120*time.Second }},
		{"a huge number of seconds cannot wrap", EnvShadowMaxSeconds, "99999999999999", func(s ShadowSettings) bool { return s.Budget == 120*time.Second }},
		{"concurrency 99 -> 32", EnvShadowConcurrency, "99", func(s ShadowSettings) bool { return s.Concurrency == 32 }},
		{"concurrency 32 stays", EnvShadowConcurrency, "32", func(s ShadowSettings) bool { return s.Concurrency == 32 }},
		{"usd 1e9 -> 100", EnvShadowMaxUSDPerRun, "1000000000", func(s ShadowSettings) bool { return s.MaxNanoUSD == 100_000_000_000 }},
		{"usd 0 is a cap of zero", EnvShadowMaxUSDPerRun, "0", func(s ShadowSettings) bool { return s.MaxNanoUSD == 0 }},
		{"sample 0 stays", EnvShadowSamplePercent, "0", func(s ShadowSettings) bool { return s.SamplePercent == 0 }},
	} {
		settings, err := ShadowSettingsFromEnv(envLookup(map[string]string{EnvShadowProvider: "typesafe", EnvShadowOrgIDs: "*", tc.variable: tc.value}))
		if err != nil || !tc.check(settings) {
			t.Errorf("%s: %+v (%v)", tc.name, settings, err)
		}
	}
	filled := ShadowSettings{Budget: 600 * time.Second, Concurrency: 500, SamplePercent: 300, MaxNanoUSD: 1 << 60}.clamped()
	if filled.Budget != MaxShadowBudget || filled.Concurrency != maxLLMConcurrency || filled.SamplePercent != 100 || filled.MaxNanoUSD != maxShadowMaxNanoUSD {
		t.Fatalf("a filled struct is not clamped: %+v", filled)
	}
	low := ShadowSettings{Budget: -1, Concurrency: 0, SamplePercent: -5, MaxNanoUSD: -1}.clamped()
	if low.Budget != DefaultShadowBudget || low.Concurrency != 1 || low.SamplePercent != 0 || low.MaxNanoUSD != 0 {
		t.Fatalf("a filled struct is not clamped from below: %+v", low)
	}
}

// A value that cannot be used is an error that names the variable and never
// the value; the settings it returns are off.
func TestAShadowSettingThatCannotBeUsedIsRefusedByName(t *testing.T) {
	const marker = "zzmarkerzz"
	for _, tc := range []struct{ variable, value string }{
		{EnvShadowProvider, "openai" + marker},
		{EnvShadowSamplePercent, "101"}, {EnvShadowSamplePercent, "-1"}, {EnvShadowSamplePercent, marker},
		{EnvShadowConcurrency, "0"}, {EnvShadowConcurrency, marker},
		{EnvShadowMaxSeconds, "0"}, {EnvShadowMaxSeconds, "-5"}, {EnvShadowMaxSeconds, "1.5"}, {EnvShadowMaxSeconds, marker},
		{EnvShadowMaxUSDPerRun, "-0.1"}, {EnvShadowMaxUSDPerRun, "NaN"}, {EnvShadowMaxUSDPerRun, "Inf"}, {EnvShadowMaxUSDPerRun, marker},
	} {
		env := map[string]string{EnvShadowProvider: "typesafe", EnvShadowOrgIDs: "*"}
		env[tc.variable] = tc.value
		settings, err := ShadowSettingsFromEnv(envLookup(env))
		if err == nil {
			t.Errorf("%s=%q was accepted: %+v", tc.variable, tc.value, settings)
			continue
		}
		if !strings.Contains(err.Error(), tc.variable) || strings.Contains(err.Error(), marker) || strings.Contains(err.Error(), tc.value) {
			t.Errorf("%s=%q: the error must name the variable and not the value: %v", tc.variable, tc.value, err)
		}
		if settings.Provider != "" || settings.EnabledFor("org-a") {
			t.Errorf("%s=%q: the refused settings are on", tc.variable, tc.value)
		}
	}
}

// EnabledFor, clause by clause, on a struct that no parser filled: the provider
// and the org list are each needed.
func TestEnabledForNeedsAProviderAndAnAllowedOrg(t *testing.T) {
	for _, tc := range []struct {
		name     string
		settings ShadowSettings
		org      string
		want     bool
	}{
		{"provider and all orgs", ShadowSettings{Provider: "typesafe", AllOrgs: true}, "org-a", true},
		{"all orgs and no provider", ShadowSettings{AllOrgs: true}, "org-a", false},
		{"a listed org and no provider", ShadowSettings{OrgIDs: map[string]struct{}{"org-a": {}}}, "org-a", false},
		{"provider and no org list", ShadowSettings{Provider: "typesafe"}, "org-a", false},
		{"provider and another org", ShadowSettings{Provider: "typesafe", OrgIDs: map[string]struct{}{"org-b": {}}}, "org-a", false},
	} {
		if got := tc.settings.EnabledFor(tc.org); got != tc.want {
			t.Errorf("%s: enabled = %v, want %v", tc.name, got, tc.want)
		}
	}
}
