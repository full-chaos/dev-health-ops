package workload

import (
	"crypto/sha256"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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

// liveRow is a bootstrap row that decide accepts at now.
func liveRow(now time.Time) bootstrapRow {
	expires := now.Add(time.Hour)
	return bootstrapRow{
		credentialID: uuid.MustParse("1c2d3e4f-5a6b-4c7d-8e9f-0a1b2c3d4e5f"), class: BootstrapClass, owner: "platform",
		expiresAt: &expires, credentialAudiences: []string{"dev-health-api", "acr-api"},
		principalID: uuid.MustParse("2d3e4f5a-6b7c-4d8e-9f0a-1b2c3d4e5f6a"), name: "go-api",
		audiences: []string{"auth-service", "dev-health-api"}, actions: []string{"workloads.read"}, kind: "workload", revision: 7,
	}
}

func TestDecideAcceptsALiveRowAndNarrowsItsAudiences(t *testing.T) {
	now := time.Unix(1_900_000_000, 0)
	row := liveRow(now)
	reg, why := row.decide(now)
	if why != "" {
		t.Fatalf("a live row refused: %s", why)
	}
	if reg.CredentialID != row.credentialID || reg.PrincipalID != row.principalID || reg.Name != "go-api" || reg.Revision != 7 ||
		!reg.ExpiresAt.Equal(*row.expiresAt) || reg.BindingID != uuid.Nil ||
		!slices.Equal(reg.Audiences, []string{"dev-health-api"}) || !slices.Equal(reg.AllowedActions, []string{"workloads.read"}) {
		t.Fatalf("registration %+v", reg)
	}
	row.expiresAt = nil
	if reg, why = row.decide(now); why != "" || !reg.ExpiresAt.IsZero() {
		t.Fatalf("no expiry: %+v %s, want live with a zero expiry", reg, why)
	}
}

func TestDecideRefusesEachDeadRow(t *testing.T) {
	now := time.Unix(1_900_000_000, 0)
	at := func(d time.Duration) *time.Time { v := now.Add(d); return &v }
	for _, tc := range []struct {
		name   string
		mutate func(*bootstrapRow)
		want   reason
	}{
		{"another class", func(r *bootstrapRow) { r.class = "internal_svc_authsess_token" }, reasonWrongClass},
		{"domain owned", func(r *bootstrapRow) { r.owner = "acr" }, reasonNotPlatform},
		{"not a workload", func(r *bootstrapRow) { r.kind = "service_account" }, reasonWrongKind},
		{"revoked", func(r *bootstrapRow) { r.revoked = true }, reasonRevoked},
		{"revoked and unexpired", func(r *bootstrapRow) { r.revoked, r.expiresAt = true, nil }, reasonRevoked},
		{"expired", func(r *bootstrapRow) { r.expiresAt = at(-time.Second) }, reasonExpired},
		{"expires at now", func(r *bootstrapRow) { r.expiresAt = at(0) }, reasonExpired},
		{"workload disabled", func(r *bootstrapRow) { r.workloadDisabled = true }, reasonWorkloadDisabled},
		{"principal disabled", func(r *bootstrapRow) { r.principalDisabled = true }, reasonPrincipalOff},
		{"credential has no audience", func(r *bootstrapRow) { r.credentialAudiences = nil }, reasonNoAudience},
		{"workload has no audience", func(r *bootstrapRow) { r.audiences = nil }, reasonNoAudience},
		{"no shared audience", func(r *bootstrapRow) { r.credentialAudiences = []string{"acr-api"} }, reasonNoAudience},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := liveRow(now)
			tc.mutate(&row)
			reg, why := row.decide(now)
			if why != tc.want || reg.PrincipalID != uuid.Nil || reg.CredentialID != uuid.Nil {
				t.Fatalf("decide = %+v, %q; want refused %q with no registration", reg, why, tc.want)
			}
		})
	}
	live := liveRow(now)
	live.expiresAt = at(time.Nanosecond)
	if _, why := live.decide(now); why != "" {
		t.Fatalf("one nanosecond before its expiry the row is refused: %s", why)
	}
}
