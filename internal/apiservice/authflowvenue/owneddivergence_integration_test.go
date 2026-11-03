//go:build integration

package authflowvenue_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/authrows"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// stepDivergence is one declared difference of the users-owned leg's HTTP answers: a step where auth-service's
// credentials routes answer differently from the golden. It is exact: the step, both statuses, a marker of each body and
// the names of the headers that differ, and it must fire exactly once in a run. Any other difference still fails.
type stepDivergence struct {
	Step                           string
	PythonStatus, GoStatus         int
	PythonBodyMarker, GoBodyMarker string
	Headers                        []string
	Reason                         string
}

// credentialsNoStore is DiffOptions.CredentialsNoStore for a run: only an external run that forwards the credentials
// aggregate.
func credentialsNoStore(external bool, forwarded authrows.Forwarded) bool {
	return external && forwarded.Has(authrows.Credentials)
}

// checkStepDivergences is every way a table of declared divergences is malformed.
func checkStepDivergences(table []stepDivergence) []string {
	var problems []string
	seen := map[string]bool{}
	for _, d := range table {
		switch {
		case d.Step == "":
			problems = append(problems, "an entry has no step")
			continue
		case seen[d.Step]:
			problems = append(problems, d.Step+": step repeated")
		}
		seen[d.Step] = true
		if d.GoStatus < 400 || d.GoStatus >= 500 {
			problems = append(problems, fmt.Sprintf("%s: go status %d is not a 4xx: only a stricter go answer may be declared", d.Step, d.GoStatus))
		}
		if d.PythonStatus < 400 {
			problems = append(problems, fmt.Sprintf("%s: python status %d is not an error answer", d.Step, d.PythonStatus))
		}
		if d.PythonBodyMarker == "" || d.GoBodyMarker == "" {
			problems = append(problems, d.Step+": both body markers are named")
		}
		if len(d.Headers) == 0 {
			problems = append(problems, d.Step+": the differing headers are named")
		}
		names := map[string]bool{}
		for _, name := range d.Headers {
			if names[name] {
				problems = append(problems, d.Step+": header "+name+" repeated")
			}
			names[name] = true
		}
		if !strings.Contains(d.Reason, "CHAOS-11398") {
			problems = append(problems, d.Step+": the reason cites the CHAOS-11398 family")
		}
	}
	return problems
}

// covers is true when this is the divergence: the step, the statuses, both body markers, and exactly the headers it
// names differ among the compared ones.
func (d stepDivergence) covers(step string, python, goResponse venueoracle.Response, compared []string) bool {
	if step != d.Step || python.Status != d.PythonStatus || goResponse.Status != d.GoStatus ||
		!strings.Contains(python.Body, d.PythonBodyMarker) || !strings.Contains(goResponse.Body, d.GoBodyMarker) {
		return false
	}
	var differing []string
	for _, name := range compared {
		pv, pok := python.Headers[name]
		gv, gok := goResponse.Headers[name]
		if pv != gv || pok != gok {
			differing = append(differing, name)
		}
	}
	want := append([]string(nil), d.Headers...)
	sort.Strings(differing)
	sort.Strings(want)
	return strings.Join(differing, ",") == strings.Join(want, ",")
}

// divergenceVerdict is every entry of table that did not fire exactly once.
func divergenceVerdict(table []stepDivergence, fired map[string]int) []string {
	var problems []string
	for _, d := range table {
		if fired[d.Step] != 1 {
			problems = append(problems, fmt.Sprintf("%s: the declared difference fired %d times, want exactly 1", d.Step, fired[d.Step]))
		}
	}
	return problems
}

const credentialsFamily = "CHAOS-11398 family: auth-service's credentials and refresh routes answer where the Python plane and go-api did not"

var securityHeaders = []string{"content-length", "content-security-policy", "referrer-policy", "strict-transport-security", "vary",
	"x-content-type-options", "x-frame-options"}

var ownedStepDivergences = []stepDivergence{
	{Step: "verify: non-ascii signature", PythonStatus: 500, GoStatus: 400, PythonBodyMarker: "Internal Server Error",
		GoBodyMarker: "Invalid or expired verification token", Headers: securityHeaders,
		Reason: "Python answers its generic 500 for a signature with a non-ASCII character; auth-service refuses it as an invalid token. Go is stricter. " + credentialsFamily},
	{Step: "reset: non-ascii signature", PythonStatus: 500, GoStatus: 400, PythonBodyMarker: "Internal Server Error",
		GoBodyMarker: "Invalid or expired token", Headers: securityHeaders,
		Reason: "the same for a reset link. " + credentialsFamily},
	{Step: "register: password over 72 bytes", PythonStatus: 500, GoStatus: 422, PythonBodyMarker: "Internal Server Error",
		GoBodyMarker: "Password must be no more than 72 bytes long",
		Headers:      append(append([]string{}, securityHeaders...), "access-control-allow-credentials", "access-control-allow-origin", "access-control-expose-headers"),
		Reason:       "Python's bcrypt raises on a password over 72 bytes (500); auth-service refuses it by the password policy. Go is stricter. " + credentialsFamily},
	{Step: "refresh: alice's token from before the reset", PythonStatus: 401, GoStatus: 401, PythonBodyMarker: "Refresh token reuse detected",
		GoBodyMarker: "Invalid or expired refresh token", Headers: []string{"content-length"},
		Reason: "a password reset revokes alice's refresh credentials; auth-service answers the revoked one as invalid, Python as a reuse. " + credentialsFamily},
	{Step: "limit: verify, another e-mail key", PythonStatus: 400, GoStatus: 429, PythonBodyMarker: "Invalid or expired verification token",
		GoBodyMarker: "Rate limit exceeded", Headers: []string{"content-length"},
		Reason: "auth-service counts the verify limit by one key where Python counted per e-mail, so the twelfth request is limited. " + credentialsFamily},
}

// goAPIStepDivergences are the steps where go-api's own answer is stricter than the frozen Python answer; exact, as
// ownedStepDivergences is.
var goAPIStepDivergences = []stepDivergence{
	{Step: "register: password over 72 bytes", PythonStatus: 500, GoStatus: 422, PythonBodyMarker: "Internal Server Error",
		GoBodyMarker: "Password must be no more than 72 bytes long",
		Headers: []string{"content-length", "content-security-policy", "referrer-policy", "strict-transport-security", "vary",
			"x-content-type-options", "x-frame-options", "access-control-allow-credentials", "access-control-allow-origin", "access-control-expose-headers"},
		Reason: "Python's bcrypt raises on a password over 72 bytes (500); go-api refuses it with passwordpolicy.ValidateForHash before it hashes. Go is stricter. " + credentialsFamily},
}

func TestCredentialsNoStoreNeedsTheCredentialsAggregateForwarded(t *testing.T) {
	parse := func(text string) authrows.Forwarded {
		f, err := authrows.ParseForwarded(text)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	for name, c := range map[string]struct {
		external  bool
		forwarded string
		want      bool
	}{
		"credentials forwarded, external":     {true, "sessions,local_login,credentials", true},
		"credentials not forwarded, external": {true, "sessions,local_login", false},
		"nothing forwarded, external":         {true, "", false},
		"credentials forwarded, not external": {false, "sessions,local_login,credentials", false},
	} {
		if got := credentialsNoStore(c.external, parse(c.forwarded)); got != c.want {
			t.Errorf("%s: %v, want %v", name, got, c.want)
		}
	}
}

func TestStepDivergenceTableIsWellFormedAndRefusesAGoFiveHundred(t *testing.T) {
	if problems := append(checkStepDivergences(ownedStepDivergences), checkStepDivergences(goAPIStepDivergences)...); len(problems) != 0 {
		t.Fatalf("the declared table: %v", problems)
	}
	base := ownedStepDivergences[0]
	for name, mutate := range map[string]func(d *stepDivergence){
		"go 500 where python answers 400": func(d *stepDivergence) { d.PythonStatus, d.GoStatus = 400, 500 },
		"go 503 where python answers 500": func(d *stepDivergence) { d.GoStatus = 503 },
		"go 200":                          func(d *stepDivergence) { d.GoStatus = 200 },
		"no go marker":                    func(d *stepDivergence) { d.GoBodyMarker = "" },
		"no python marker":                func(d *stepDivergence) { d.PythonBodyMarker = "" },
		"no headers":                      func(d *stepDivergence) { d.Headers = nil },
		"no step":                         func(d *stepDivergence) { d.Step = "" },
		"no ticket in the reason":         func(d *stepDivergence) { d.Reason = "because" },
		"a repeated header":               func(d *stepDivergence) { d.Headers = []string{"content-length", "content-length"} },
	} {
		entry := base
		mutate(&entry)
		if problems := checkStepDivergences([]stepDivergence{entry}); len(problems) == 0 {
			t.Errorf("%s was accepted", name)
		}
	}
	if problems := checkStepDivergences([]stepDivergence{base, base}); len(problems) == 0 {
		t.Error("a repeated step was accepted")
	}
}

// responsesOf builds the two answers an entry declares: the headers it names differ, content-type is the same on both.
func responsesOf(d stepDivergence) (venueoracle.Response, venueoracle.Response, []string) {
	python := venueoracle.Response{Status: d.PythonStatus, Headers: map[string]string{"content-type": "application/json"}, Body: `{"detail":"` + d.PythonBodyMarker + `"}`}
	goResponse := venueoracle.Response{Status: d.GoStatus, Headers: map[string]string{"content-type": "application/json"}, Body: `{"detail":{"message":"` + d.GoBodyMarker + `"}}`}
	compared := []string{"content-type"}
	for _, name := range d.Headers {
		goResponse.Headers[name] = "v"
		compared = append(compared, name)
	}
	sort.Strings(compared)
	return python, goResponse, compared
}

func TestStepDivergenceCoversOnlyTheExactDifference(t *testing.T) {
	for _, d := range ownedStepDivergences {
		python, goResponse, compared := responsesOf(d)
		if !d.covers(d.Step, python, goResponse, compared) {
			t.Errorf("%s: the declared difference was not covered", d.Step)
			continue
		}
		wrong := map[string]func() bool{
			"another step":          func() bool { return d.covers(d.Step+" x", python, goResponse, compared) },
			"another python status": func() bool { p := python; p.Status++; return d.covers(d.Step, p, goResponse, compared) },
			"another go status":     func() bool { g := goResponse; g.Status++; return d.covers(d.Step, python, g, compared) },
			"another go body": func() bool {
				g := goResponse
				g.Body = `{"detail":"other"}`
				return d.covers(d.Step, python, g, compared)
			},
			"another python body": func() bool {
				p := python
				p.Body = `{"detail":"other"}`
				return d.covers(d.Step, p, goResponse, compared)
			},
			"an extra differing header": func() bool {
				g := goResponse
				g.Headers = map[string]string{"x-extra": "1"}
				for k, v := range goResponse.Headers {
					g.Headers[k] = v
				}
				return d.covers(d.Step, python, g, append(append([]string{}, compared...), "x-extra"))
			},
			"a missing differing header": func() bool {
				g := goResponse
				g.Headers = map[string]string{}
				for k, v := range goResponse.Headers {
					if k != d.Headers[0] {
						g.Headers[k] = v
					}
				}
				return d.covers(d.Step, python, g, compared)
			},
			"a differing value of a shared header": func() bool {
				g := goResponse
				g.Headers = map[string]string{}
				for k, v := range goResponse.Headers {
					g.Headers[k] = v
				}
				g.Headers["content-type"] = "text/plain"
				return d.covers(d.Step, python, g, compared)
			},
		}
		for name, run := range wrong {
			if run() {
				t.Errorf("%s: %s was covered", d.Step, name)
			}
		}
	}
}

func TestStepDivergenceMustFireExactlyOnce(t *testing.T) {
	fired := map[string]int{}
	for _, d := range ownedStepDivergences {
		fired[d.Step] = 1
	}
	if problems := divergenceVerdict(ownedStepDivergences, fired); len(problems) != 0 {
		t.Fatalf("every entry fired once: %v", problems)
	}
	first := ownedStepDivergences[0].Step
	for name, count := range map[string]int{"never": 0, "twice": 2} {
		fired[first] = count
		problems := divergenceVerdict(ownedStepDivergences, fired)
		if len(problems) != 1 || !strings.Contains(problems[0], first) || !strings.Contains(problems[0], fmt.Sprint(count)) {
			t.Errorf("%s: problems %v", name, problems)
		}
	}
}

// The steps run 2 of the owned leg showed answering differently, each declared once and no other.
func TestOwnedStepDivergenceTableHoldsExactlyTheFiveObservedSteps(t *testing.T) {
	var got []string
	for _, d := range ownedStepDivergences {
		got = append(got, d.Step)
	}
	want := []string{"verify: non-ascii signature", "reset: non-ascii signature", "register: password over 72 bytes",
		"refresh: alice's token from before the reset", "limit: verify, another e-mail key"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("declared steps = %q, want %q", got, want)
	}
}
