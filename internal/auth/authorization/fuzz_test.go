package authorization

import (
	"regexp"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

var fuzzActionShape = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)

func FuzzNormalizeRequest(f *testing.F) {
	f.Add("org.read", "organization", "org-1")
	f.Add("admin.users", "platform", "")
	f.Add("", "", "")
	f.Add("ORG.READ", "organization", "Org-1")
	f.Add("org.read\n", "organization", "org-1\x00")
	f.Fuzz(func(t *testing.T, action, resType, org string) {
		in := Request{Action: action, ResourceKind: resType, OrganizationID: org}
		out, ok := NormalizeRequest(in)
		if !ok {
			if out != (Request{}) {
				t.Fatalf("rejected request returned a value: %+v", out)
			}
			return
		}
		if out != in {
			t.Fatalf("normalisation changed the request: %+v -> %+v", in, out)
		}
		if !fuzzActionShape.MatchString(out.Action) {
			t.Fatalf("accepted action %q", out.Action)
		}
		if out.ResourceKind != "organization" && out.ResourceKind != "platform" {
			t.Fatalf("accepted resource type %q", out.ResourceKind)
		}
		if (out.ResourceKind == "platform") != (out.OrganizationID == "") {
			t.Fatalf("organization id presence wrong: %+v", out)
		}
	})
}

func FuzzEvaluateRequest(f *testing.F) {
	f.Add("org.read", "organization", "org-1", "owner")
	f.Add("admin.users", "platform", "", "platform_admin")
	f.Add("org.fly", "organization", "org-1", "owner")
	f.Fuzz(func(t *testing.T, action, resType, org, role string) {
		s := fakeSnapshot{
			principal:    StatusActive,
			platformRole: role,
			orgStatus:    map[string]Status{org: StatusActive},
			memberships:  map[string]Membership{org: {OrgID: org, Role: role, Status: StatusActive}},
		}
		d := NewEvaluator().Evaluate(Request{Action: action, ResourceKind: resType, OrganizationID: org}, s)
		if !d.Allowed {
			return
		}
		a, err := policycatalog.Default().Action(action)
		if err != nil {
			t.Fatalf("allowed an action outside the catalog: %q", action)
		}
		if !policycatalog.Default().Allows(role, a.ID) {
			t.Fatalf("allowed %q for role %q that does not hold it", action, role)
		}
		r, err := policycatalog.Default().Role(role)
		if err != nil || r.Scope != a.Scope {
			t.Fatalf("allowed across scopes: role %q action %q", role, action)
		}
	})
}
