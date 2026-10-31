package authorization

import "testing"

func FuzzEvaluateRepositoryGrant(f *testing.F) {
	f.Add("acme/api", "acme/api", "org-1", "org-1", "viewer", int64(0), false, nowNano)
	f.Add("acme/api", "*", "org-1", "org-2", "admin", int64(5), true, nowNano)
	f.Add("a/b", "a/b", "org-1", "org-1", "owner", nowNano, false, int64(0))
	f.Add("A/B", "a/b", "org-1", "org-1", "viewer", int64(-1), false, int64(-1))
	f.Fuzz(func(t *testing.T, reqKey, grantKey, reqOrg, grantOrg, role string, exp int64, revoked bool, now int64) {
		g := Grant{OrgID: grantOrg, Role: role, ResourceKind: "repository", ResourceKey: grantKey, ExpiresAtUnixNano: exp, Revoked: revoked}
		s := grantFake{
			fakeSnapshot: fakeSnapshot{
				principal:   StatusActive,
				orgStatus:   map[string]Status{reqOrg: StatusActive},
				memberships: map[string]Membership{reqOrg: {OrgID: reqOrg, Role: "member", Status: StatusActive}},
			},
			grants: []Grant{g},
			now:    now,
		}
		r := Request{Action: repoAction, ResourceKind: "repository", OrganizationID: reqOrg, ResourceKey: reqKey}
		d := plantedEvaluator().Evaluate(r, s)
		if !d.Allowed {
			return
		}
		if _, ok := NormalizeRequest(r); !ok {
			t.Fatalf("allowed a non-canonical request %+v", r)
		}
		if d.Reason == ReasonAllowedByMembershipDefault {
			if grantOrg == reqOrg {
				t.Fatal("default allow with a row of the pair")
			}
			if now <= 0 || d.CacheClass != CacheDecisionBound || d.ExpiresAtUnixNano != wantExpiry(now) {
				t.Fatalf("default allow without clock or 30 s bound: %+v now %d", d, now)
			}
			return
		}
		switch {
		case grantOrg != reqOrg:
			t.Fatalf("allowed across orgs: %q grant for %q", grantOrg, reqOrg)
		case revoked:
			t.Fatal("allowed a revoked grant")
		case now <= 0:
			t.Fatalf("allowed with no clock: %d", now)
		case exp != 0 && now >= exp:
			t.Fatalf("allowed an expired grant: now %d exp %d", now, exp)
		case grantKey != reqKey && grantKey != "*":
			t.Fatalf("grant %q allowed key %q", grantKey, reqKey)
		case !plantedHolders[role]:
			t.Fatalf("grant role %q does not hold the action", role)
		case d.CacheClass != CacheResourceRevisionBound || d.ExpiresAtUnixNano != wantExpiry(now):
			t.Fatalf("grant allow cache class %q expiry %d for clock %d", d.CacheClass, d.ExpiresAtUnixNano, now)
		}
	})
}

func wantExpiry(now int64) int64 {
	if now > 1<<63-1-testBound {
		return 1<<63 - 1
	}
	return now + testBound
}
