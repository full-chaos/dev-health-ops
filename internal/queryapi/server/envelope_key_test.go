package server

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestLoadEnvelopeKeyDecidesOnceFromTheThreeValues(t *testing.T) {
	usable, empty := writeAbsentKeyJWKS(t, true), writeAbsentKeyJWKS(t, false)
	missing := filepath.Join(t.TempDir(), "missing.json")
	for name, tc := range map[string]struct {
		path, issuer, audience string
		verifier, unusable     bool
		unset                  []string
	}{
		"all unset":           {"", "", "", false, false, envelopeKeyNames},
		"path unset":          {"", iaIssuer, iaAudience, false, false, envelopeKeyNames[:1]},
		"issuer unset":        {usable, "", iaAudience, false, false, envelopeKeyNames[1:2]},
		"audience unset":      {usable, iaIssuer, "", false, false, envelopeKeyNames[2:]},
		"usable key":          {usable, iaIssuer, iaAudience, true, false, nil},
		"JWKS with no key":    {empty, iaIssuer, iaAudience, false, true, nil},
		"JWKS file missing":   {missing, iaIssuer, iaAudience, false, true, nil},
		"JWKS not a document": {writeEnvelopeKeyFile(t, "this is not a jwks document"), iaIssuer, iaAudience, false, true, nil},
	} {
		t.Run(name, func(t *testing.T) {
			key, err := loadEnvelopeKey(tc.path, tc.issuer, tc.audience)
			if err != nil {
				t.Fatalf("loadEnvelopeKey: %v (the key never fails start)", err)
			}
			if got := key.verifier != nil; got != tc.verifier {
				t.Fatalf("verifier present = %v, want %v", got, tc.verifier)
			}
			if key.unusable != tc.unusable || !slices.Equal(key.unset, tc.unset) {
				t.Fatalf("unusable=%v unset=%v, want unusable=%v unset=%v", key.unusable, key.unset, tc.unusable, tc.unset)
			}
			probe := key.jwksProbe()
			if tc.verifier {
				if probe == nil || probe.CheckJWKS() != nil {
					t.Fatalf("a usable key gives readiness no working JWKS probe")
				}
				return
			}
			if probe != nil {
				t.Fatalf("no usable key, but jwksProbe is a non-nil %T: readinessProbes would keep query_jwks", probe)
			}
			if names := readinessProbeNames(readinessProbes(fakePinger{}, fakePinger{}, probe, nil)); names != "query_clickhouse,query_postgres" {
				t.Fatalf("probes = %s, want no query_jwks", names)
			}
		})
	}
}

func TestEnvelopeVerifierFromEnvIsNilWithoutAUsableKey(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"unset":            nil,
		"partial":          {"GO_API_ENVELOPE_ISSUER": iaIssuer, "GO_API_ENVELOPE_AUDIENCE": iaAudience},
		"JWKS with no key": fullKeyEnv(writeAbsentKeyJWKS(t, false)),
	} {
		t.Run(name, func(t *testing.T) {
			verifier, err := envelopeVerifierFromEnv(func(n string) string { return env[n] })
			if err != nil || verifier != nil {
				t.Fatalf("envelopeVerifierFromEnv = %v, %v; want nil, nil", verifier, err)
			}
		})
	}
	env := fullKeyEnv(writeAbsentKeyJWKS(t, true))
	if verifier, err := envelopeVerifierFromEnv(func(n string) string { return env[n] }); err != nil || verifier == nil {
		t.Fatalf("a usable key: envelopeVerifierFromEnv = %v, %v; want a verifier", verifier, err)
	}
}

func readinessProbeNames(probes []ReadinessProbe) string {
	var out []string
	for _, probe := range probes {
		out = append(out, probe.Name)
	}
	return strings.Join(out, ",")
}

func writeEnvelopeKeyFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
