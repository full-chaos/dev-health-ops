package admin

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

var (
	chainActor   = "77777777-7777-4777-8777-777777777777"
	chainSession = &policy.Impersonation{
		ID:           uuid.MustParse("88888888-8888-4888-8888-888888888888"),
		AdminUserID:  uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		TargetUserID: uuid.MustParse("55555555-5555-4555-8555-555555555555"),
		TargetOrgID:  uuid.MustParse("66666666-6666-4666-8666-666666666666"),
	}
)

// chainUsers are the caller classes the audit sites tell apart: no user, no
// claim, an empty claim, and a claim that names the actor.
var chainUsers = []struct {
	name string
	user *policy.User
}{
	{"nil user", nil},
	{"no claim", &policy.User{UserID: "u"}},
	{"empty claim", &policy.User{UserID: "u", ImpersonatedBy: strPtr("")}},
	{"actor claim", &policy.User{UserID: "u", ImpersonatedBy: strPtr(chainActor)}},
}

// TestImpersonationAuditMetadataBytes pins the start/stop audit metadata,
// byte for byte, for every caller class with and without a live go-api row.
func TestImpersonationAuditMetadataBytes(t *testing.T) {
	row := `"impersonation_target":"55555555-5555-4555-8555-555555555555","impersonation_org":"66666666-6666-4666-8666-666666666666"}`
	want := map[string][2]string{
		"nil user":    {"", `{"impersonated_by":"11111111-1111-4111-8111-111111111111",` + row},
		"no claim":    {"", `{"impersonated_by":"11111111-1111-4111-8111-111111111111",` + row},
		"empty claim": {"", `{"impersonated_by":"11111111-1111-4111-8111-111111111111",` + row},
		"actor claim": {`{"impersonated_by":"` + chainActor + `"}`, `{"impersonated_by":"` + chainActor + `",` + row},
	}
	for _, c := range chainUsers {
		for i, ctx := range []context.Context{context.Background(), policy.WithImpersonation(context.Background(), chainSession)} {
			if got := string(impersonationAuditMetadata(ctx, c.user)); got != want[c.name][i] {
				t.Errorf("%s, row %v: %q, want %q", c.name, i == 1, got, want[c.name][i])
			}
		}
	}
}

// TestSetImpersonatedByMatchesTheInlineCheck is the differential cell of the
// four settings audit rows: setImpersonatedBy writes what the inline check it
// replaced wrote, for every caller class.
func TestSetImpersonatedByMatchesTheInlineCheck(t *testing.T) {
	for _, c := range chainUsers {
		if c.user == nil {
			continue
		}
		inline := pyjson.NewObject()
		inline.Set("setting", "x")
		if c.user.ImpersonatedBy != nil && *c.user.ImpersonatedBy != "" {
			inline.Set("impersonated_by", *c.user.ImpersonatedBy)
		}
		shared := pyjson.NewObject()
		shared.Set("setting", "x")
		setImpersonatedBy(shared, c.user)
		a, errA := pyjson.Marshal(inline)
		b, errB := pyjson.Marshal(shared)
		if errA != nil || errB != nil || string(a) != string(b) {
			t.Errorf("%s: shared %s, inline %s (%v %v)", c.name, b, a, errA, errB)
		}
	}
}
