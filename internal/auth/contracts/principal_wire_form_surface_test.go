package contracts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// CHAOS-12402 (pm 11-03): one prn_ wire form per surface, no join rule.
//
//	form S, session.WirePrincipalID: "prn_" + base64url (no padding) of the 16 bytes. Tokens (sub, act.sub), sessions,
//	config subjects, authorize decisions, credential bodies and the read API.
//	form U, provisioning.WireID: "prn_" + the canonical UUID text. Provisioning events and the audit wire, the same as
//	org_ (D6624).
//
// The cell holds each producer (file and the line that builds the id) to its form. The producer table is the analysis
// table B of CHAOS-11800/12402 (B1 to B12).
const (
	formS = "S"
	formU = "U"
)

type wireProducer struct {
	surface string
	file    string // repo relative
	needle  string // the line that builds the id
	form    string
}

var wireProducers = []wireProducer{
	{"B1 access token sub", "internal/auth/authapi/issuer_eddsa.go", "Sub:           session.WirePrincipalID(grant.PrincipalID)", formS},
	{"B1 access token act.sub", "internal/auth/authapi/issuer_eddsa.go", "contracts.AccessTokenActor{Sub: session.WirePrincipalID(*grant.Actor)}", formS},
	{"B1 acr mint sub", "internal/auth/acrmint/acrmint.go", "Sub:           session.WirePrincipalID(r.PrincipalID)", formS},
	{"B1 edge token sub", "internal/edgetokenmint/eddsa.go", `"sub": "prn_" + base64.RawURLEncoding.EncodeToString(userID[:])`, formS},
	{"B2 workload token sub", "internal/auth/workloadexchange/mint.go", "Sub:           session.WirePrincipalID(reg.PrincipalID)", formS},
	{"B4 session principal_id", "internal/auth/session/store.go", "PrincipalID:   WirePrincipalID(r.PrincipalID)", formS},
	{"B4 session actor_principal_id", "internal/auth/session/store.go", "ActorPrincipalID: WirePrincipalID(r.Delegation.Actor)", formS},
	{"B5 authorize decision", "internal/auth/authapi/authorize.go", "PrincipalID:    session.WirePrincipalID(item.principal.ID)", formS},
	{"B6 credential principal_id", "internal/auth/authapi/provisioning_serviceaccount_keys.go", "PrincipalID: session.WirePrincipalID(k.Principal)", formS},
	{"B6 credential created_by", "internal/auth/authapi/provisioning_serviceaccount_keys.go", "body.CreatedBy = session.WirePrincipalID(*k.CreatedBy)", formS},

	{"B7 principal event", "internal/auth/copyproject/copyproject.go", `AggregateID: provisioning.WireID("principal", principal)`, formU},
	{"B7 user event", "internal/auth/copyproject/copyproject.go", `AggregateID: provisioning.WireID("user", principal)`, formU},
	{"B7 principal dependency", "internal/auth/decisionstate/decisionstate.go", `AggregateID: provisioning.WireID("principal", principal.String())`, formU},
	{"B8 service account event", "internal/auth/serviceaccount/store.go", `func wire(id uuid.UUID) string { return provisioning.WireID("service_account", id.String()) }`, formU},
	{"B8 owner principal", "internal/auth/serviceaccount/store.go", `func ownerWire(id uuid.UUID) string { return provisioning.WireID("principal", id.String()) }`, formU},
	{"B8 workload event", "internal/auth/workload/store.go", `func wire(id uuid.UUID) string { return provisioning.WireID("workload", id.String()) }`, formU},
	{"B8 workload enable event", "internal/auth/workload/enable.go", `AggregateID: provisioning.WireID("principal", plan.PrincipalID.String())`, formU},
	{"B8 external identity change", "internal/auth/identity/identities.go", `"principal_id": provisioning.WireID("principal", subject)`, formU},
	{"B9 event payload actor", "internal/audit/audit.go", `payload["actor_principal_id"] = provisioning.WireID("principal", *m.Audit.ActorPrincipalID)`, formU},
	{"B9 audit wire actor", "internal/testsupport/auditwire/auditwire.go", `set("actor_principal_id", row.ActorPrincipalID, "principal")`, formU},
	{"B9 audit wire subject", "internal/testsupport/auditwire/auditwire.go", `set("subject_principal_id", row.SubjectPrincipalID, "principal")`, formU},
	{"B10 workload revision join", "internal/auth/workload/store.go", `r.aggregate_type = 'workload' AND r.aggregate_id = 'prn_' || w.principal_id::text`, formU},
	{"B10 workload bootstrap join", "internal/auth/workload/bootstrap.go", `r.aggregate_type = 'workload' AND r.aggregate_id = 'prn_' || w.principal_id::text`, formU},
	{"B10 service account key join", "internal/auth/serviceaccount/keys.go", `r.aggregate_type = 'service_account' AND r.aggregate_id = 'prn_' || c.principal_id::text`, formU},
}

func wireRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(wireRepoRoot(t), filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(b)
}

func TestPrincipalWireFormPerSurface(t *testing.T) {
	id := uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	if got, want := session.WirePrincipalID(id), "prn_ABEiM0RVZneImaq7zN3u_w"; got != want {
		t.Fatalf("form S = %q, want %q", got, want)
	}
	if got, want := provisioning.WireID("principal", id.String()), "prn_00112233-4455-6677-8899-aabbccddeeff"; got != want {
		t.Fatalf("form U = %q, want %q", got, want)
	}
	if len(session.WirePrincipalID(id)) != 26 || len(provisioning.WireID("principal", id.String())) != 40 {
		t.Fatal("the two forms must differ in length: 26 and 40")
	}
	for _, p := range wireProducers {
		src := readRepoFile(t, p.file)
		line := ""
		for _, l := range strings.Split(src, "\n") {
			if strings.Contains(l, p.needle) {
				line = l
				break
			}
		}
		if line == "" {
			t.Errorf("%s: %s no longer builds the id as %q (form %s)", p.surface, p.file, p.needle, p.form)
			continue
		}
		switch p.form {
		case formS:
			if strings.Contains(line, "provisioning.WireID") || strings.Contains(line, "'prn_' ||") {
				t.Errorf("%s: the producer line uses form U, want form S: %s", p.surface, line)
			}
		case formU:
			if strings.Contains(line, "WirePrincipalID") || strings.Contains(line, "RawURLEncoding") {
				t.Errorf("%s: the producer line uses form S, want form U: %s", p.surface, line)
			}
		}
	}
}

// No file builds one id from both forms: a compare or join of S with U is a bug, and no join rule exists.
func TestNoPrincipalProducerFileMixesTheTwoForms(t *testing.T) {
	seen := map[string]bool{}
	for _, p := range wireProducers {
		if seen[p.file] {
			continue
		}
		seen[p.file] = true
		src := readRepoFile(t, p.file)
		hasS := strings.Contains(src, "WirePrincipalID(") || strings.Contains(src, `"prn_" + base64`)
		hasU := strings.Contains(src, `provisioning.WireID("principal"`) || strings.Contains(src, "'prn_' ||")
		if hasS && hasU {
			t.Errorf("%s builds both prn_ forms; one surface, one form", p.file)
		}
	}
}

// The wire-form rule is written where the contracts name the id forms.
func TestPrincipalWireFormRuleIsDocumentedPerSurface(t *testing.T) {
	doc := readRepoFile(t, "contracts/auth/v1/README.md")
	for _, want := range []string{
		"prn_ wire form per surface",
		"session.WirePrincipalID",
		"provisioning.WireID",
		"no join rule",
		"Provisioning events and the audit wire",
		"Access and workload token `sub`",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("contracts/auth/v1/README.md does not carry %q", want)
		}
	}
}
