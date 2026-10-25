package authrows

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
)

func regexpMust(expr string) *regexp.Regexp { return regexp.MustCompile(expr) }

// goldenBody is the frozen, projected body of one step of the auth-flow venue golden.
func goldenBody(t *testing.T, step string) string {
	t.Helper()
	raw, err := os.ReadFile(repoFile(t, "internal", "apiservice", "authflowvenue", "testdata", "golden", "auth-flow-venue-oracle.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Requests []struct {
			Name string `json:"name"`
			Body string `json:"body"`
		} `json:"requests"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	for _, r := range golden.Requests {
		if r.Name == step {
			return r.Body
		}
	}
	t.Fatalf("no golden step %q", step)
	return ""
}

const (
	aliceLogin1 = "login: alice (a refresh token the reset revokes)"
	aliceLogin2 = "login: alice, new password"
	opaque      = "dhr1_i0N6Nj8HjP5KO2ocv9zhhQURDbq0_B322SXiiZTXj68"
	sidItem     = "|sid=string:ses_gCStSnYMRLCcpk3FDgGkng"
)

var goldenRefresh = regexpMust(`"refresh_token":"<jwt\|(?:[^"<>]|<id>)*>"`)

// targetOf writes what auth-service answers for a golden body: the opaque refresh credential in place of the
// refresh JWT, the sid claim before the sub claim, and (for alice's two sign-ins) organization A as owner.
func targetOf(t *testing.T, golden string, alice bool) string {
	t.Helper()
	body := goldenRefresh.ReplaceAllString(golden, `"refresh_token":"`+opaque+`"`)
	at := strings.Index(body, "|sub=string:")
	if at < 0 {
		t.Fatalf("no sub claim in %q", body)
	}
	body = body[:at] + sidItem + body[at:]
	if alice {
		body = strings.Replace(body, "org_id=string:b0000000-0000-4000-8000-00000000000b|role=string:member", "org_id=string:a0000000-0000-4000-8000-00000000000a|role=string:owner", 1)
		body = strings.Replace(body, `"org_id":"b0000000-0000-4000-8000-00000000000b","role":"member"`, `"org_id":"a0000000-0000-4000-8000-00000000000a","role":"owner"`, 1)
	}
	return body
}

var sixSteps = []struct {
	step  string
	alice bool
}{
	{aliceLogin1, true}, {"accept: ivan joins org A", false}, {"onboard: olive creates an org", false},
	{"onboard: oscar joins org D", false}, {aliceLogin2, true}, {"login: pia, new password", false},
}

// The six sign-in answers of the corpus compare equal once each ruled difference is classified.
func TestProjectedSignInAnswersMatchTheGoldens(t *testing.T) {
	for _, s := range sixSteps {
		golden := goldenBody(t, s.step)
		target := targetOf(t, golden, s.alice)
		if target == golden {
			t.Fatalf("%s: the target body equals the golden's: nothing to classify", s.step)
		}
		if got, want := TargetResponseBody(target), GoldenResponseBody(s.step, golden); got != want {
			t.Errorf("%s: projected target and golden differ:\n target %s\n golden %s", s.step, got, want)
		}
	}
}

// Each ruled difference is exactly that: any other change in the answer is still a difference.
func TestProjectionKeepsEveryOtherDifference(t *testing.T) {
	golden := goldenBody(t, "accept: ivan joins org A")
	target := targetOf(t, golden, false)
	want := GoldenResponseBody("accept: ivan joins org A", golden)
	if TargetResponseBody(target) != want {
		t.Fatal("the clean pair does not match")
	}
	for name, mutate := range map[string]func(string) string{
		"a refresh JWT on the target": func(s string) string {
			return strings.Replace(s, `"refresh_token":"`+opaque+`"`, `"refresh_token":"<jwt|type=string:refresh>"`, 1)
		},
		"a refresh credential one character short":  func(s string) string { return strings.Replace(s, opaque, opaque[:len(opaque)-1], 1) },
		"a refresh credential one character long":   func(s string) string { return strings.Replace(s, opaque, opaque+"A", 1) },
		"a refresh credential with another prefix":  func(s string) string { return strings.Replace(s, "dhr1_", "dhr2_", 1) },
		"a refresh credential outside the alphabet": func(s string) string { return strings.Replace(s, opaque, opaque[:10]+"+"+opaque[11:], 1) },
		"no refresh token":                          func(s string) string { return strings.Replace(s, `"refresh_token":"`+opaque+`",`, "", 1) },
		"another extra claim":                       func(s string) string { return strings.Replace(s, sidItem, sidItem+"|extra=string:x", 1) },
		"a second sid":                              func(s string) string { return strings.Replace(s, sidItem, sidItem+sidItem, 1) },
		"a changed claim":                           func(s string) string { return strings.Replace(s, "type=string:access", "type=string:other", 1) },
		"a changed role":                            func(s string) string { return strings.Replace(s, `"role":"admin"`, `"role":"owner"`, 1) },
		"a sid not in the session form": func(s string) string {
			return strings.Replace(s, "ses_gCStSnYMRLCcpk3FDgGkng", "gCStSnYMRLCcpk3FDgGkng", 1)
		},
		"a sid of another length": func(s string) string {
			return strings.Replace(s, "ses_gCStSnYMRLCcpk3FDgGkng", "ses_gCStSnYMRLCcpk3FDgGkn", 1)
		},
		"a changed organization name": func(s string) string { return strings.Replace(s, `"org_name":"Org A"`, `"org_name":"Org Z"`, 1) },
	} {
		mutated := mutate(target)
		if mutated == target {
			t.Fatalf("%s: the mutation changed nothing", name)
		}
		if TargetResponseBody(mutated) == want {
			t.Errorf("%s was classified away", name)
		}
	}
	// the golden side: a refresh credential that is not a JWT stays a difference.
	opaqueGolden := goldenRefresh.ReplaceAllString(golden, `"refresh_token":"`+opaque+`"`)
	if GoldenResponseBody("accept: ivan joins org A", opaqueGolden) == want {
		t.Error("an opaque refresh credential on the golden side was treated as a JWT")
	}
	// a sid outside the access token is not the ignored claim.
	elsewhere := strings.Replace(target, `"token_type":"bearer"`, `"token_type":"bearer","sid":"ses_gCStSnYMRLCcpk3FDgGkng"`, 1)
	if TargetResponseBody(elsewhere) == want {
		t.Error("a sid field outside the access token was ignored")
	}
}

// Alice's organization and role are classified on her two sign-in steps and nowhere else.
func TestOrganizationChoiceIsClassifiedOnlyForAlicesTwoSignIns(t *testing.T) {
	for _, step := range []string{aliceLogin1, aliceLogin2} {
		golden := goldenBody(t, step)
		if GoldenResponseBody(step, golden) == GoldenResponseBody("login: pia, new password", goldenBody(t, "login: pia, new password")) {
			t.Fatal("distinct steps projected alike")
		}
		target := targetOf(t, golden, true)
		if TargetResponseBody(target) != GoldenResponseBody(step, golden) {
			t.Errorf("%s: alice's organization choice was not classified", step)
		}
		// the same difference with another organization or role is not the ruled one.
		for name, mutate := range map[string]func(string) string{
			"organization C": func(s string) string {
				return strings.ReplaceAll(s, "a0000000-0000-4000-8000-00000000000a", "c0000000-0000-4000-8000-00000000000c")
			},
			"role admin":     func(s string) string { return strings.ReplaceAll(s, "role=string:owner", "role=string:admin") },
			"user role only": func(s string) string { return strings.Replace(s, `"role":"owner"`, `"role":"admin"`, 1) },
		} {
			if mutated := mutate(target); mutated == target || TargetResponseBody(mutated) == GoldenResponseBody(step, golden) {
				t.Errorf("%s, %s: classified away (or the mutation changed nothing)", step, name)
			}
		}
	}
	// another step with the same difference, and another user in the alice step, are differences.
	pia := goldenBody(t, "login: pia, new password")
	piaTarget := strings.Replace(targetOf(t, pia, false), `"org_id":null,"role":"member"`, `"org_id":"a0000000-0000-4000-8000-00000000000a","role":"owner"`, 1)
	if piaTarget == targetOf(t, pia, false) || TargetResponseBody(piaTarget) == GoldenResponseBody("login: pia, new password", pia) {
		t.Error("another user's organization difference was classified")
	}
	aliceGolden := goldenBody(t, aliceLogin1)
	for _, other := range []string{"login: alice, old password", "refresh: alice's token from before the reset", aliceLogin1 + " "} {
		if got := GoldenResponseBody(other, aliceGolden); !strings.Contains(got, orgBID) || strings.Contains(got, orgAID) {
			t.Errorf("step %q was rewritten", other)
		}
	}
	otherUser := strings.ReplaceAll(aliceGolden, "10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000009")
	if got := GoldenResponseBody(aliceLogin1, otherUser); !strings.Contains(got, orgBID) || strings.Contains(got, orgAID) {
		t.Error("a different user in alice's step was rewritten")
	}
	// the user object and the access token must both name alice: a body with only one of them is left alone.
	onlyToken := strings.Replace(aliceGolden, `"user":{"id":"10000000-0000-4000-8000-000000000001"`, `"user":{"id":"10000000-0000-4000-8000-000000000009"`, 1)
	if got := GoldenResponseBody(aliceLogin1, onlyToken); !strings.Contains(got, orgBID) {
		t.Error("a body whose user object is not alice's was rewritten")
	}
	onlySub := strings.Replace(aliceGolden, "|sub=string:10000000-0000-4000-8000-000000000001|", "|sub=string:10000000-0000-4000-8000-000000000009|", 1)
	if got := GoldenResponseBody(aliceLogin1, onlySub); !strings.Contains(got, orgBID) {
		t.Error("a body whose access token is not alice's was rewritten")
	}
}
