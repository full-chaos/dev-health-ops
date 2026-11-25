package server

import (
	"strings"
	"testing"
)

// CHAOS-14644 / CHAOS-14645: the four documents that select dataHealth omit the team (no variable, no literal), so a
// web build that sends no team is served org-wide. The texts the web sent before stay registered as legacy texts.
var dataHealthOmitTeam = []struct {
	operation     string
	current       string
	currentDigest string
	legacy        []string
	legacyDigests []string
}{
	{"connectorsDataHealth", registeredConnectorsDataHealthDocument, "d2a289f6419d0f6f9826ca978ce37d6f90e629934d2f49782d4a1ce2c874a136",
		[]string{registeredConnectorsDataHealthV1Document}, []string{"0e43d67d571539081b2006ae8b2148dd4ea31c959be22be1db9e927083060baa"}},
	{"dataHealthIdentity", registeredDataHealthIdentityDocument, "04a822b0357f0a0195867c8eb5343d9bf878d26b674d8bbd849ef9c6d1d6c2b5",
		[]string{registeredDataHealthIdentityV1Document, registeredDataHealthIdentityV2Document},
		[]string{"a31987266aad8c1e9eb7cb1b572cc45aecc417f566773e9b5ba6e1bf10ec1a94", "3a6f9e0676663b447d633872098e14e387f07b95b90b88d6255263d3a339362d"}},
	{"metricLineage", registeredMetricLineageDocument, "717b35c73431903150f79b061642065b33191d8a9472bf76b17f3d4e325b4f95",
		[]string{registeredMetricLineageV1Document}, []string{"4d93b59c335d6b8e5709f60bf4186030aa1b6b454c438e73c9177c675ccd705d"}},
	{"mappingCoverageHealth", registeredMappingCoverageHealthDocument, "08ddf1a9d01ae058ddb7ab25052aadccfe2bf9bc3424bdb0bdb4da9f77207dcc",
		[]string{registeredMappingCoverageHealthV1Document}, []string{"f87ebae2df502280d389cfd3bf6452e2e1ef8ed6965280f5ba809350ac6a0d01"}},
}

func TestDataHealthDocumentsOmitTheTeamAndPinTheirDigests(t *testing.T) {
	for _, c := range dataHealthOmitTeam {
		if got := digestHex(c.current); got != c.currentDigest {
			t.Errorf("%s: current digest = %s, want %s", c.operation, got, c.currentDigest)
		}
		if strings.Contains(c.current, "team") || strings.Contains(c.current, "ID!") && c.operation != "metricLineage" {
			t.Errorf("%s: the current document must carry no team variable or argument:\n%s", c.operation, c.current)
		}
		if !strings.Contains(c.current, "dataHealth {\n") {
			t.Errorf("%s: the current document must select dataHealth with no argument:\n%s", c.operation, c.current)
		}
	}
}

func TestDataHealthLegacyTextsStayRegisteredAndResolveToTheOneOperation(t *testing.T) {
	current := map[string]string{}
	legacy := map[string][]string{}
	for _, c := range dataHealthOmitTeam {
		current[c.operation] = digestHex(c.current)
		legacy[c.operation] = c.legacyDigests
		for i, text := range c.legacy {
			if got := digestHex(text); got != c.legacyDigests[i] {
				t.Errorf("%s: legacy text %d digest = %s, want %s", c.operation, i, got, c.legacyDigests[i])
			}
			if !strings.Contains(text, "dataHealth(team:") {
				t.Errorf("%s: a legacy text is a text that sent a team:\n%s", c.operation, text)
			}
		}
		registered := legacyDigestsByOperation[c.operation]
		for _, want := range c.legacyDigests {
			found := false
			for _, got := range registered {
				found = found || got == want
			}
			if !found {
				t.Errorf("legacyDigestsByOperation[%s] = %v lacks %s", c.operation, registered, want)
			}
		}
	}
	byDigest, err := buildOperationByDigest(current, legacy)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range dataHealthOmitTeam {
		for _, text := range append([]string{c.current}, c.legacy...) {
			if operation, ok := operationForDocument(text, byDigest); !ok || operation != c.operation {
				t.Errorf("%s: a registered text resolves to %q, %v", c.operation, operation, ok)
			}
		}
	}
}

// The identity text with the CHAOS-8954 name fields (V2) is the V1 text plus exactly those selections.
func TestDataHealthIdentityV2IsV1PlusTheNameSelection(t *testing.T) {
	var kept []string
	for _, line := range strings.Split(registeredDataHealthIdentityV2Document, "\n") {
		if strings.TrimSpace(line) != "suggestedCanonicalName" {
			kept = append(kept, line)
		}
	}
	if strings.Join(kept, "\n") != registeredDataHealthIdentityV1Document {
		t.Fatalf("V2 less suggestedCanonicalName is not V1:\n%s", strings.Join(kept, "\n"))
	}
}
