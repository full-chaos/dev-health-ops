package goapiproof

import (
	"encoding/base64"
	"os"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func fakeJWT(payload string) string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"HS256"}`)) + "." + enc([]byte(payload)) + "." + enc([]byte("sig"))
}

// The gate admits exactly one cell of the whole product: an allowlisted
// venue, its own acknowledgement, and one of its own hosts. Expected values
// are GENERATED from the allowlist, so a new venue or host is enumerated.
func TestCheckVenueAdmitsOnlyTheAllowlistedCell(t *testing.T) {
	venues := append([]string{"", "production", "PRODUCTION", " production ", "Production", "unknown", "bigboy"}, Venues()...)
	hosts := []string{"prod.example.com", "localhost", "127.0.0.1", "::1", "bigboy", "api", "evil-api", "api.evil.com", "localhost.evil.com", ""}
	admitted := 0
	for _, venue := range venues {
		for _, ack := range []string{"", "other", "production", venue} {
			for _, host := range hosts {
				edge := "https://" + host + ":8000/graphql"
				if strings.Contains(host, ":") {
					edge = "https://[" + host + "]:8000/graphql"
				}
				if host == "" {
					edge = "not a url"
				}
				want := false
				for _, allowed := range venueEdgeHosts[venue] {
					if allowed == host && ack == venue && venue != "" {
						want = true
					}
				}
				got := CheckVenue(venue, ack, edge) == nil
				if got != want {
					t.Errorf("CheckVenue(%q, %q, %q) admitted=%t, want %t", venue, ack, edge, got, want)
				}
				if got {
					admitted++
				}
			}
		}
	}
	wantAdmitted := 0
	for _, hosts := range venueEdgeHosts {
		wantAdmitted += len(hosts)
	}
	if admitted != wantAdmitted {
		t.Fatalf("admitted %d cells, want %d (one per allowlisted host)", admitted, wantAdmitted)
	}
}

func TestProductionIsNeverAVenueKey(t *testing.T) {
	for name := range venueEdgeHosts {
		if strings.EqualFold(strings.TrimSpace(name), VenueProduction) || name == "" {
			t.Fatalf("venue allowlist carries %q", name)
		}
	}
}

func TestPeekPrincipalDomain(t *testing.T) {
	cases := []struct {
		name, token, role string
		superuser, ok     bool
	}{
		{"admin", fakeJWT(`{"role":"admin","is_superuser":false}`), "admin", false, true},
		{"superuser", fakeJWT(`{"role":"member","is_superuser":true}`), "member", true, true},
		{"bearer prefix", "Bearer " + fakeJWT(`{"role":"owner","is_superuser":false}`), "owner", false, true},
		{"absent role", fakeJWT(`{"is_superuser":true}`), "", false, false},
		{"absent flag", fakeJWT(`{"role":"admin"}`), "", false, false},
		{"null role", fakeJWT(`{"role":null,"is_superuser":false}`), "", false, false},
		{"wrong type role", fakeJWT(`{"role":1,"is_superuser":false}`), "", false, false},
		{"wrong type flag", fakeJWT(`{"role":"admin","is_superuser":"true"}`), "", false, false},
		{"empty", "", "", false, false},
		{"two segments", "a.b", "", false, false},
		{"four segments", "a.b.c.d", "", false, false},
		{"bad base64", "a.!!!.c", "", false, false},
		{"not json", "a." + base64.RawURLEncoding.EncodeToString([]byte("nope")) + ".c", "", false, false},
		{"array payload", fakeJWT(`[]`), "", false, false},
	}
	for _, tc := range cases {
		role, superuser, err := PeekPrincipal(tc.token)
		if (err == nil) != tc.ok || role != tc.role || superuser != tc.superuser {
			t.Errorf("%s: got (%q,%t,%v)", tc.name, role, superuser, err)
		}
	}
}

func TestAuthzRequirementSatisfied(t *testing.T) {
	r := AuthzRequirement{Roles: []string{"admin", "owner", "operator"}}
	for _, tc := range []struct {
		role      string
		superuser bool
		want      bool
	}{
		{"admin", false, true}, {"ADMIN", false, true}, {"owner", false, true}, {"operator", false, true},
		{"viewer", false, false}, {"member", false, false}, {"", false, false}, {"administrator", false, false},
		{"member", true, true}, {"", true, true}, {"viewer", true, true},
	} {
		if got := r.Satisfied(tc.role, tc.superuser); got != tc.want {
			t.Errorf("Satisfied(%q,%t)=%t want %t", tc.role, tc.superuser, got, tc.want)
		}
	}
}

// The eligible set is COMPUTED from the minter's vocabulary. Pinned against
// the list of every operation the prover covers: a new operation with an
// authz declaration, or a widened minter, turns this red.
func TestVenueEligibleSetIsPinnedOverEveryOperation(t *testing.T) {
	want := []string{"connectorsDataHealth", "dataHealthIdentity", "mappingCoverageHealth", "metricLineage"}
	if got := VenueEligibleOperations(KnownOperations()); !reflect.DeepEqual(got, want) {
		t.Fatalf("eligible = %v, want %v", got, want)
	}
	for operation := range operationAuthz {
		if _, err := SpecFor(operation); err != nil {
			t.Errorf("authz table names %s, which the prover cannot prove: %v", operation, err)
		}
		if !VenueEligible(operation) {
			t.Errorf("%s declares a requirement a viewer satisfies: its entry is dead", operation)
		}
	}
	for _, operation := range KnownOperations() {
		if _, declared := AuthzFor(operation); !declared && VenueEligible(operation) {
			t.Errorf("%s is eligible with no declared requirement", operation)
		}
	}
}

func TestAViewerProvableOperationIsNeverEligible(t *testing.T) {
	for _, roles := range [][]string{{"viewer"}, {"member"}, {"viewer", "admin"}, {"MEMBER"}} {
		operationAuthz["zz_synthetic"] = AuthzRequirement{Roles: roles}
		if VenueEligible("zz_synthetic") {
			t.Errorf("requirement %v is satisfiable by a viewer and must not be eligible", roles)
		}
	}
	operationAuthz["zz_synthetic"] = AuthzRequirement{Roles: []string{"admin"}}
	if !VenueEligible("zz_synthetic") {
		t.Error("an admin-only requirement must be eligible")
	}
	delete(operationAuthz, "zz_synthetic")
	if VenueEligible("zz_synthetic") || VenueEligible("featureFlags") {
		t.Error("an operation with no declared requirement is never eligible")
	}
}

const (
	testSchema = "sha256:schema"
	testBuild  = "b18e56fa7b18e56fa7b18e56fa7b18e56fa7b18e"
	testDoc    = "sha256:doc"
)

func goodReceipt(t *testing.T, operation, mode string) *VenueReceipt {
	t.Helper()
	spec, err := SpecFor(operation)
	if err != nil {
		t.Fatal(err)
	}
	zero := 0
	route := RouteEdge
	variants := []string{""}
	for _, variant := range spec.Variants {
		variants = append(variants, variant.Name)
	}
	receipt := &VenueReceipt{
		SchemaDigest: testSchema, CandidateBuild: testBuild, Stage: Stage, ExitCause: "completed",
		Venue: &VenueStamp{Name: "bigboy-compose", Role: "admin"}, Digest: "d1",
	}
	for _, variant := range variants {
		receipt.Outcomes = append(receipt.Outcomes, VenueOutcome{
			Operation: operation, Variant: variant, DocumentDigest: testDoc, Executed: true, Admitted: true,
			Route: route, EdgeBuildBinding: EdgeBuildPresent, TerminalState: TerminalStateMatch, Outside: &zero,
		})
	}
	return receipt
}

func TestVenueAdmitDomain(t *testing.T) {
	const op = "metricLineage"
	admit := func(r *VenueReceipt, operation, schema, doc, build, mode string) error {
		return VenueAdmit(r, operation, schema, doc, build, mode)
	}
	if err := admit(goodReceipt(t, op, "primary"), op, testSchema, testDoc, testBuild, "primary"); err != nil {
		t.Fatalf("the canonical receipt must admit: %v", err)
	}
	if err := admit(goodReceipt(t, op, "canary"), op, testSchema, testDoc, testBuild, "canary"); err != nil {
		t.Fatalf("canary: %v", err)
	}
	type mutation struct {
		name  string
		apply func(*VenueReceipt)
	}
	mutations := []mutation{
		{"venue absent", func(r *VenueReceipt) { r.Venue = nil }},
		{"venue production", func(r *VenueReceipt) { r.Venue.Name = "production" }},
		{"venue Production spaced", func(r *VenueReceipt) { r.Venue.Name = " Production " }},
		{"venue unknown", func(r *VenueReceipt) { r.Venue.Name = "staging" }},
		{"venue empty", func(r *VenueReceipt) { r.Venue.Name = "" }},
		{"role viewer", func(r *VenueReceipt) { r.Venue.Role = "viewer" }},
		{"role member", func(r *VenueReceipt) { r.Venue.Role = "member" }},
		{"role empty", func(r *VenueReceipt) { r.Venue.Role = "" }},
		{"stage shadow", func(r *VenueReceipt) { r.Stage = "shadow" }},
		{"exit stopped", func(r *VenueReceipt) { r.ExitCause = "stopped_by_signal" }},
		{"exit run error", func(r *VenueReceipt) { r.ExitCause = "completed_with_run_error" }},
		{"exit empty", func(r *VenueReceipt) { r.ExitCause = "" }},
		{"build other", func(r *VenueReceipt) { r.CandidateBuild = strings.Repeat("a", 40) }},
		{"build empty", func(r *VenueReceipt) { r.CandidateBuild = "" }},
		{"build prefix", func(r *VenueReceipt) { r.CandidateBuild = testBuild[:12] }},
		{"schema other", func(r *VenueReceipt) { r.SchemaDigest = "sha256:other" }},
		{"schema empty", func(r *VenueReceipt) { r.SchemaDigest = "" }},
		{"no outcomes", func(r *VenueReceipt) { r.Outcomes = nil }},
		{"other operation only", func(r *VenueReceipt) {
			for i := range r.Outcomes {
				r.Outcomes[i].Operation = "connectorsDataHealth"
			}
		}},
		{"doc digest other", func(r *VenueReceipt) { r.Outcomes[0].DocumentDigest = "sha256:other" }},
		{"not executed", func(r *VenueReceipt) { r.Outcomes[0].Executed = false }},
		{"not admitted", func(r *VenueReceipt) { r.Outcomes[0].Admitted = false }},
		{"binding absent", func(r *VenueReceipt) { r.Outcomes[0].EdgeBuildBinding = EdgeBuildAbsent }},
		{"binding empty", func(r *VenueReceipt) { r.Outcomes[0].EdgeBuildBinding = "" }},
		{"terminal unsupported", func(r *VenueReceipt) { r.Outcomes[0].TerminalState = "unsupported" }},
		{"terminal empty", func(r *VenueReceipt) { r.Outcomes[0].TerminalState = "" }},
		{"mismatch uncited", func(r *VenueReceipt) { r.Outcomes[0].TerminalState = TerminalStateMismatch }},
		{"mismatch outside 1", func(r *VenueReceipt) {
			one := 1
			r.Outcomes[0].TerminalState, r.Outcomes[0].Outside, r.Outcomes[0].BaselineDefects = TerminalStateMismatch, &one, []string{"CHAOS-1"}
		}},
		{"mismatch outside absent", func(r *VenueReceipt) {
			r.Outcomes[0].TerminalState, r.Outcomes[0].Outside, r.Outcomes[0].BaselineDefects = TerminalStateMismatch, nil, []string{"CHAOS-1"}
		}},
		{"mismatch blank citation", func(r *VenueReceipt) {
			r.Outcomes[0].TerminalState, r.Outcomes[0].BaselineDefects = TerminalStateMismatch, []string{"CHAOS-1", " \t"}
		}},
		{"mismatch go-only citation", func(r *VenueReceipt) {
			r.Outcomes[0].TerminalState, r.Outcomes[0].BaselineDefects = TerminalStateMismatch, []string{"go-only: x"}
		}},
		{"variant undeclared", func(r *VenueReceipt) { r.Outcomes = append(r.Outcomes, VenueOutcome{Operation: op, Variant: "NOPE"}) }},
		{"one variant failing", func(r *VenueReceipt) { r.Outcomes[len(r.Outcomes)-1].Executed = false }},
		{"one variant missing", func(r *VenueReceipt) { r.Outcomes = r.Outcomes[:len(r.Outcomes)-1] }},
		{"base missing", func(r *VenueReceipt) { r.Outcomes = r.Outcomes[1:] }},
		{"route empty", func(r *VenueReceipt) { r.Outcomes[0].Route = "" }},
	}
	for _, m := range mutations {
		receipt := goodReceipt(t, op, "primary")
		m.apply(receipt)
		if err := admit(receipt, op, testSchema, testDoc, testBuild, "primary"); err == nil {
			t.Errorf("%s: admitted", m.name)
		}
	}
	// Live side: another build, schema, document, mode, empty digests.
	good := goodReceipt(t, op, "primary")
	for name, err := range map[string]error{
		"live build other": admit(good, op, testSchema, testDoc, strings.Repeat("c", 40), "primary"),
		"live build empty": admit(good, op, testSchema, testDoc, "", "primary"),
		"live schema":      admit(good, op, "sha256:x", testDoc, testBuild, "primary"),
		"live doc":         admit(good, op, testSchema, "sha256:x", testBuild, "primary"),
		"live doc empty":   admit(good, op, testSchema, "", testBuild, "primary"),
		"mode unknown":     admit(good, op, testSchema, testDoc, testBuild, "shadow"),
		"nil receipt":      admit(nil, op, testSchema, testDoc, testBuild, "primary"),
		"viewer-provable":  admit(good, "featureFlags", testSchema, testDoc, testBuild, "primary"),
		"undeclared op":    admit(good, "zzNoSuchOp", testSchema, testDoc, testBuild, "primary"),
	} {
		if err == nil {
			t.Errorf("%s: admitted", name)
		}
	}
	// Primary needs the edge route; canary takes any recorded route.
	proofRoute := goodReceipt(t, op, "primary")
	for i := range proofRoute.Outcomes {
		proofRoute.Outcomes[i].Route = RouteProof
	}
	if admit(proofRoute, op, testSchema, testDoc, testBuild, "primary") == nil {
		t.Error("a proof-route receipt admitted for primary")
	}
	if err := admit(proofRoute, op, testSchema, testDoc, testBuild, "canary"); err != nil {
		t.Errorf("a proof-route receipt must admit for canary: %v", err)
	}
	// Cited mismatch is the second admitted arm.
	cited := goodReceipt(t, op, "primary")
	for i := range cited.Outcomes {
		cited.Outcomes[i].TerminalState = TerminalStateMismatch
		cited.Outcomes[i].BaselineDefects = []string{"CHAOS-6090"}
	}
	if err := admit(cited, op, testSchema, testDoc, testBuild, "primary"); err != nil {
		t.Errorf("a fully cited mismatch must admit: %v", err)
	}
	// An admin superuser member also satisfies.
	super := goodReceipt(t, op, "primary")
	super.Venue.Role, super.Venue.Superuser = "member", true
	if err := admit(super, op, testSchema, testDoc, testBuild, "primary"); err != nil {
		t.Errorf("superuser: %v", err)
	}
}

func TestParseVenueReceipt(t *testing.T) {
	good := `{"schema_digest":"s","candidate_build":"b","stage":"deployed_executed","exit_cause":"completed","venue":{"name":"bigboy-compose","principal_role":"admin","principal_superuser":false},"outcomes":[]}`
	r, err := ParseVenueReceipt([]byte(good))
	if err != nil || r.Venue == nil || r.Venue.Role != "admin" || len(r.Digest) != 64 {
		t.Fatalf("good: %+v %v", r, err)
	}
	for name, raw := range map[string]string{
		"empty": "", "not json": "x", "trailing": good + " {}", "trailing garbage": good + "x", "array": "[]",
	} {
		if _, err := ParseVenueReceipt([]byte(raw)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
	other, _ := ParseVenueReceipt([]byte(good + "\n"))
	if other.Digest == r.Digest {
		t.Error("the digest must cover the exact bytes")
	}
}

func TestApplyVenueReceiptOnlyAddsAdmissions(t *testing.T) {
	const op = "connectorsDataHealth"
	docs := map[string]string{op: testDoc, "featureFlags": testDoc}
	request := EnableRequest{SchemaDigest: testSchema, RunningBuild: testBuild, Mode: "primary", VenueReceipt: goodReceipt(t, op, "primary")}
	admitted, refused, still := applyVenueReceipt(request, docs, []string{op, "featureFlags"})
	if admitted[op] != "d1" || len(admitted) != 1 || refused["featureFlags"] == "" || !reflect.DeepEqual(still, []string{"featureFlags"}) {
		t.Fatalf("admitted=%v refused=%v still=%v", admitted, refused, still)
	}
	request.VenueReceipt = nil
	admitted, refused, still = applyVenueReceipt(request, docs, []string{op})
	if len(admitted) != 0 || len(refused) != 0 || !reflect.DeepEqual(still, []string{op}) {
		t.Fatalf("no receipt must change nothing: %v %v %v", admitted, refused, still)
	}
	request.VenueReceipt = goodReceipt(t, op, "primary")
	request.RunningBuild = strings.Repeat("d", 40)
	if admitted, _, still = applyVenueReceipt(request, docs, []string{op}); len(admitted) != 0 || len(still) != 1 {
		t.Fatalf("another build must not admit: %v %v", admitted, still)
	}
}

func TestRequestIdentityFoldsVenueOnlyWhenSet(t *testing.T) {
	base := AuthContext{PrincipalKind: "stored_account", Audience: "query-api", KeyID: "k"}
	vars := map[string]any{"a": 1}
	plain, _ := RequestIdentity("org", base, vars)
	withZero := base
	withZero.PrincipalRole, withZero.PrincipalSuperuser = "admin", true // no Venue: not folded
	if got, _ := RequestIdentity("org", withZero, vars); got != plain {
		t.Fatal("a run without a venue must keep its identity")
	}
	seen := map[string]string{plain: "plain"}
	for name, auth := range map[string]AuthContext{
		"admin":     {PrincipalKind: base.PrincipalKind, Audience: base.Audience, KeyID: "k", Venue: "bigboy-compose", PrincipalRole: "admin"},
		"viewer":    {PrincipalKind: base.PrincipalKind, Audience: base.Audience, KeyID: "k", Venue: "bigboy-compose", PrincipalRole: "viewer"},
		"superuser": {PrincipalKind: base.PrincipalKind, Audience: base.Audience, KeyID: "k", Venue: "bigboy-compose", PrincipalRole: "admin", PrincipalSuperuser: true},
		"venue":     {PrincipalKind: base.PrincipalKind, Audience: base.Audience, KeyID: "k", Venue: "other", PrincipalRole: "admin"},
	} {
		id, _ := RequestIdentity("org", auth, vars)
		if prior, dup := seen[id]; dup {
			t.Errorf("%s shares an identity with %s", name, prior)
		}
		seen[id] = name
	}
}

func TestVenueEvidenceClassDomain(t *testing.T) {
	h1, h2 := strings.Repeat("a", 64), strings.Repeat("b", 64)
	for name, tc := range map[string]struct{ evidence, want string }{
		"class 1 written":       {VenueEvidence(h1, "", "op note"), VenueClassAdmin},
		"class 2 written":       {VenueEvidence(h1, h2, "op note"), VenueClassNoData},
		"class 1 empty note":    {VenueEvidence(h1, "", ""), VenueClassAdmin},
		"bare class 1":          {VenueEvidencePrefix + h1, VenueClassAdmin},
		"waiver":                {UnprovenEvidencePrefix + VenueEvidence(h1, "", "x"), ""},
		"plain text":            {"chris ruled it", ""},
		"empty":                 {"", ""},
		"short digest":          {"VENUE-PROOF:abc note", ""},
		"upper digest":          {"VENUE-PROOF:" + strings.ToUpper(h1) + " x", ""},
		"digest run-on":         {"VENUE-PROOF:" + h1 + "z", ""},
		"lowercase prefix":      {"venue-proof:" + h1, ""},
		"leading space":         {" " + VenueEvidence(h1, "", "x"), ""},
		"no-data without venue": {NoProdDataEvidencePrefix + h2 + " note", ""},
		"venue then no-data":    {VenueEvidence(h1, "", "") + " NO-PROD-DATA:" + h2, VenueClassAdmin},
	} {
		if got := VenueEvidenceClass(tc.evidence); got != tc.want {
			t.Errorf("%s: %q want %q", name, got, tc.want)
		}
	}
}

// A login token must never be accepted at a host name production itself
// defines. The allowlist is pinned to exactly the names below, and none of
// them may be a service name in the production compose file (the `api`
// service there is the production edge on :8000, reachable by that name
// from inside production's network).
func TestVenueHostsAreNeverProductionServiceNames(t *testing.T) {
	if got, want := venueEdgeHosts["bigboy-compose"], []string{"localhost", "127.0.0.1", "::1", "bigboy"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("allowlist = %v, want exactly %v", got, want)
	}
	raw, err := os.ReadFile("../../deploy/docker-compose/compose.production.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]yaml.Node `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatal(err)
	}
	if _, ok := compose.Services["api"]; !ok || len(compose.Services) < 10 {
		t.Fatalf("production compose parse looks wrong: %d services", len(compose.Services))
	}
	for name, hosts := range venueEdgeHosts {
		for _, host := range hosts {
			if _, prod := compose.Services[host]; prod {
				t.Errorf("venue %s allows %q, a production compose service name", name, host)
			}
		}
	}
	for _, edge := range []string{"http://api:8000/graphql", "http://query-api:8090/graphql", "http://dev-health-api:8000/graphql"} {
		if err := CheckVenue("bigboy-compose", "bigboy-compose", edge); err == nil {
			t.Errorf("login token accepted at %s", edge)
		}
	}
}
