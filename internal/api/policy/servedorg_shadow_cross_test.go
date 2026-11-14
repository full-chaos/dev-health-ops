package policy

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// crossCaller is one caller class of the served-org census.
type crossCaller struct {
	name   string
	who    persona
	store  func() *accessStore
	claims func() *RoutedResult
}

func crossCallers() []crossCaller {
	member := func(role string) func() *accessStore {
		return func() *accessStore {
			store := personaStore(personaMember)
			store.roles[[2]uuid.UUID{userID, ownOrgID}] = role
			return store
		}
	}
	twoOrg := func() *accessStore {
		store := personaStore(personaAdmin)
		store.roles[[2]uuid.UUID{userID, uuid.MustParse(memberOrg)}] = "member"
		store.members[[2]uuid.UUID{userID, uuid.MustParse(memberOrg)}] = true
		return store
	}
	orgless := func() *RoutedResult { return userClaims(func(c *AccessTokenClaims) { c.OrgID = nil }) }
	delegatedOrgless := func() *RoutedResult {
		wire := "prn_" + wireID(actorID)
		return userClaims(func(c *AccessTokenClaims) { c.OrgID = nil; c.Actor = &wire })
	}
	return []crossCaller{
		{"admin of the token org", personaAdmin, nil, nil},
		{"member", personaMember, member("member"), nil},
		{"viewer", personaMember, member("viewer"), nil},
		{"platform admin with no membership row", personaSuperuser, func() *accessStore { return legacyStore(true, "") }, nil},
		{"two-org caller", personaAdmin, twoOrg, nil},
		{"superuser with an org-less token", personaSuperuser, nil, orgless},
		{"delegated session with no organization", personaSuperuser, nil, delegatedOrgless},
	}
}

// crossAnswer is what one side did: the questions it asked and, when it asked, its allow.
type crossAnswer struct {
	questions []string
	asked     bool
	allowed   bool
}

func crossQuestions(calls []pdpSeen) []string {
	var out []string
	for _, c := range calls {
		out = append(out, fmt.Sprintf("%s %s org=%v:%s key=%s", c.Action, c.Type, c.HasOrg, c.Org, c.Key))
	}
	slices.Sort(out)
	return out
}

// CHAOS-13675: the shadow asks what the enforcer asks and gets its answer. For every served-org census row (5 families),
// every caller class and every served value, at the row's level and, for the org-less callers, on an AdminOrg row, with
// a decision point that allows and one that denies: the shadow's questions equal the enforcer's, and when they asked,
// the shadow's allow (legacy answer, flipped on a mismatch) equals the enforcer's (served or refused).
func TestServedOrgShadowAsksWhatTheEnforcerAsks(t *testing.T) {
	verdicts := map[string]func(string) (bool, string){"allow": allowAll, "deny": denyWith("grant_absent")}
	tally := map[string]int{}
	for _, row := range servedOrgCensus {
		levels := []Authz{row.level}
		if row.level != AdminOrg {
			levels = append(levels, AdminOrg)
		}
		for _, level := range levels {
			at := row
			at.level = level
			t.Run(fmt.Sprintf("%s level %d", row.pattern, level), func(t *testing.T) {
				namespace := servedOrgNamespace(t, row.pattern)
				values := []string{ownOrg, strangerOrg, "acme"}
				if row.query {
					values = append(values, "")
				}
				for _, caller := range crossCallers() {
					orgless := caller.claims != nil
					if level == AdminOrg && level != row.level && !orgless {
						continue
					}
					for _, value := range values {
						for verdictName, verdict := range verdicts {
							setup := servedOrgSetup{who: caller.who, store: caller.store}
							if caller.claims != nil {
								setup.claims = caller.claims()
							}
							enforcerPDP := newTestPDP(t, verdict)
							enforced := setup
							enforced.enforcer = servedOrgEnforcer(t, enforcerPDP, namespace)
							got := servedOrgServe(t, at, enforced, value)
							enf := crossAnswer{questions: crossQuestions(enforcerPDP.calls())}
							enf.asked = len(enf.questions) > 0
							enf.allowed = got.status == http.StatusOK

							rec := recordShadow(t)
							shadowPDP := newTestPDP(t, verdict)
							sh, _ := newShadowFor(t, shadowPDP.client(t), func(c *ShadowConfig) { c.Rows = servedOrgRows(t); c.Classes = nil })
							shadowed := setup
							shadowed.shadow = sh
							legacy := servedOrgServe(t, at, shadowed, value)
							_, results := rec.snapshot()
							sha := crossAnswer{questions: crossQuestions(shadowPDP.calls())}
							sha.asked = len(sha.questions) > 0
							legacyAllowed := legacy.status == http.StatusOK
							if len(results) == 1 {
								switch results[0] {
								case "match":
									sha.allowed = legacyAllowed
								case "mismatch":
									sha.allowed = !legacyAllowed
								}
							}
							name := fmt.Sprintf("%s served=%q pdp=%s", caller.name, value, verdictName)
							tally[fmt.Sprintf("asked=%v allowed=%v platform=%v", enf.asked, enf.asked && enf.allowed,
								slices.ContainsFunc(enf.questions, func(q string) bool { return strings.Contains(q, " platform ") }))]++
							if !enf.asked {
								tally["unasked: "+caller.name+fmt.Sprintf(" level %d", level)]++
							}
							if !slices.Equal(sha.questions, enf.questions) {
								t.Errorf("%s: shadow asked %v, enforcer asked %v", name, sha.questions, enf.questions)
								continue
							}
							if enf.asked && sha.allowed != enf.allowed {
								t.Errorf("%s: shadow allow %v (results %v, legacy %d), enforcer allow %v (%d %q)", name, sha.allowed, results,
									legacy.status, enf.allowed, got.status, strings.TrimSpace(got.body))
							}
							// Asked nothing: a delegated session's bearer keeps the legacy answer (skipped); an AdminOrg row refuses
							// a token with no organization unasked (decided_no_question), and that refusal is the legacy one, byte
							// for byte, on both sides.
							if !enf.asked {
								want := "skipped"
								if orgless && level == AdminOrg && caller.name != "delegated session with no organization" {
									want = "decided_no_question"
									status, body := EnforcedOrglessRefusal()
									if got.status != status || got.body != string(body) || legacy.status != status || legacy.body != string(body) {
										t.Errorf("%s: enforced %d %q, legacy %d %q, want both %d %q", name, got.status, got.body, legacy.status, legacy.body, status, body)
									}
								}
								// A delegated bearer the row does not take is refused by authentication first: no shadow line.
								if !slices.Equal(results, []string{want}) && !(want == "skipped" && len(results) == 0) {
									t.Errorf("%s: the enforcer asked nothing, the shadow recorded %v, want [%s]", name, results, want)
								}
							}
						}
					}
				}
			})
		}
	}
	t.Logf("CROSS-TALLY %v", tally)
	for _, want := range []string{"asked=true allowed=true platform=false", "asked=true allowed=false platform=false",
		"asked=true allowed=true platform=true", "asked=true allowed=false platform=true", "asked=false allowed=false platform=false"} {
		if tally[want] == 0 {
			t.Errorf("no case %q: the cell does not reach every path (%v)", want, tally)
		}
	}
}
