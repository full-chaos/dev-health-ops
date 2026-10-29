package workload

import (
	"crypto/sha256"
	"slices"
	"strings"
	"testing"
)

func TestTheBootstrapShapeIsThePrefixAnd43Base64URLCharacters(t *testing.T) {
	body := strings.Repeat("a", 20) + strings.Repeat("Z", 10) + "0123456789-_A"
	if len(body) != 43 {
		t.Fatalf("test body is %d characters", len(body))
	}
	if !ValidBootstrapShape(BootstrapPrefix + body) {
		t.Fatal("a value of the shape is refused")
	}
	for name, value := range map[string]string{
		"empty":          "",
		"prefix only":    BootstrapPrefix,
		"no prefix":      body,
		"short":          BootstrapPrefix + body[:42],
		"long":           BootstrapPrefix + body + "a",
		"plus":           BootstrapPrefix + body[:42] + "+",
		"slash":          BootstrapPrefix + body[:42] + "/",
		"padding":        BootstrapPrefix + body[:42] + "=",
		"space":          BootstrapPrefix + body[:42] + " ",
		"newline":        BootstrapPrefix + body + "\n",
		"upper prefix":   "FCWLB_" + body,
		"leading space":  " " + BootstrapPrefix + body,
		"another prefix": "svc_authsess_" + body,
	} {
		if ValidBootstrapShape(value) {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestTheBootstrapDigestIsSHA256OfTheWholeValue(t *testing.T) {
	value := BootstrapPrefix + strings.Repeat("b", 43)
	if BootstrapDigest(value) != sha256.Sum256([]byte(value)) {
		t.Fatal("the digest is not SHA-256 of the whole value")
	}
	if BootstrapDigest(value) == sha256.Sum256([]byte(strings.TrimPrefix(value, BootstrapPrefix))) {
		t.Fatal("the digest leaves the prefix out")
	}
}

func TestIntersectKeepsOnlyTheSharedAudiencesInOrder(t *testing.T) {
	for _, tc := range []struct{ a, b, want []string }{
		{[]string{"auth-service", "dev-health-api"}, []string{"dev-health-api"}, []string{"dev-health-api"}},
		{[]string{"auth-service", "dev-health-api"}, []string{"dev-health-api", "auth-service"}, []string{"auth-service", "dev-health-api"}},
		{[]string{"auth-service"}, nil, []string{}},
		{nil, []string{"auth-service"}, []string{}},
		{[]string{"auth-service"}, []string{"acr-api"}, []string{}},
	} {
		if got := intersect(tc.a, tc.b); !slices.Equal(got, tc.want) {
			t.Errorf("intersect(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestBootstrapReasonsAreNamedForLogsOnly(t *testing.T) {
	for internal, want := range map[reason]Reason{
		reasonRevoked: ReasonRevoked, reasonExpired: ReasonExpired, reasonWrongClass: ReasonUnknown,
		reasonNotPlatform: ReasonUnknown, reasonWorkloadDisabled: ReasonDisabled, reasonPrincipalOff: ReasonDisabled,
		reasonNoAudience: ReasonNoAudience, reasonUnknown: ReasonUnknown,
	} {
		err := &RefusalError{Reason: publicReason(internal)}
		if RefusalReason(err) != string(want) || err.Error() != "workload: refused: "+string(want) {
			t.Errorf("%s: reason %q text %q, want %q", internal, RefusalReason(err), err.Error(), want)
		}
	}
}
