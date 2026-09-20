package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

func loginToken(payload string) string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"HS256"}`)) + "." + enc([]byte(payload)) + "." + enc([]byte("sig"))
}

func writeToken(t *testing.T, mode os.FileMode, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "edge.jwt")
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadTokenFileDomain(t *testing.T) {
	token := loginToken(`{"role":"admin","is_superuser":false}`)
	if got, err := readTokenFile(writeToken(t, 0o600, token+"\n")); err != nil || got != token {
		t.Fatalf("0600 file: %q %v", got, err)
	}
	for name, mode := range map[string]os.FileMode{"0644": 0o644, "0640": 0o640, "0604": 0o604, "0660": 0o660} {
		if _, err := readTokenFile(writeToken(t, mode, token)); err == nil {
			t.Errorf("mode %s accepted", name)
		}
	}
	dir := t.TempDir()
	real := writeToken(t, 0o600, token)
	link := filepath.Join(dir, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readTokenFile(link); err == nil {
		t.Error("symlink accepted")
	}
	if _, err := readTokenFile(dir); err == nil {
		t.Error("directory accepted")
	}
	if _, err := readTokenFile(filepath.Join(dir, "missing")); err == nil {
		t.Error("missing file accepted")
	}
	if _, err := readTokenFile(writeToken(t, 0o600, strings.Repeat("a", 17<<10))); err == nil {
		t.Error("oversize file accepted")
	}
}

func venueFlags(t *testing.T, venue string) flags {
	return flags{
		edgeURL:         "http://localhost:8000/graphql",
		edgeBearerFile:  writeToken(t, 0o600, loginToken(`{"role":"admin","is_superuser":false}`)),
		venue:           venue,
		proofBearerExec: `["/bin/true"]`,
	}
}

// The production prover never accepts a login token: every way of naming
// production, or naming nothing, is refused before any request exists.
func TestLoginTokenIsRefusedUnlessTheVenueGatePasses(t *testing.T) {
	for _, tc := range []struct {
		name, venue, ack, edge string
		ok                     bool
	}{
		{"default venue", "", "", "http://localhost:8000/graphql", false},
		{"production", "production", "production", "http://localhost:8000/graphql", false},
		{"Production", "Production", "Production", "http://localhost:8000/graphql", false},
		{"unknown", "staging", "staging", "http://localhost:8000/graphql", false},
		{"no ack", "bigboy-compose", "", "http://localhost:8000/graphql", false},
		{"wrong ack", "bigboy-compose", "production", "http://localhost:8000/graphql", false},
		{"production host", "bigboy-compose", "bigboy-compose", "https://api.example.com/graphql", false},
		{"the venue", "bigboy-compose", "bigboy-compose", "http://localhost:8000/graphql", true},
	} {
		t.Setenv(goapiproof.VenueAckEnvVar, tc.ack)
		f := venueFlags(t, tc.venue)
		f.edgeURL = tc.edge
		edge, _, err := credentials(f)
		if (err == nil) != tc.ok {
			t.Errorf("%s: err=%v", tc.name, err)
			continue
		}
		if tc.ok {
			stamp, err := venueStamp(f)
			if err != nil || stamp == nil || stamp.Name != "bigboy-compose" || stamp.Role != "admin" || stamp.Superuser {
				t.Errorf("%s: stamp %+v %v", tc.name, stamp, err)
			}
			request, _ := http.NewRequest(http.MethodPost, tc.edge, nil)
			if err := edge.Apply(context.Background(), request); err != nil || !strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ") {
				t.Errorf("%s: credential %v", tc.name, err)
			}
		}
	}
}

func TestVenueFlagCombinations(t *testing.T) {
	t.Setenv(goapiproof.VenueAckEnvVar, "bigboy-compose")
	both := venueFlags(t, "bigboy-compose")
	both.edgeBearerExec = `["/bin/true"]`
	if _, _, err := credentials(both); err == nil {
		t.Error("file and exec together accepted")
	}
	alone := flags{venue: "bigboy-compose", edgeURL: "http://localhost:8000/graphql"}
	if _, err := venueStamp(alone); err == nil {
		t.Error("-venue without -edge-bearer-file accepted")
	}
	if stamp, err := venueStamp(flags{}); stamp != nil || err != nil {
		t.Errorf("no venue flags must be a no-op: %+v %v", stamp, err)
	}
	// A token with no role claim cannot be labelled, so it is refused.
	noRole := venueFlags(t, "bigboy-compose")
	noRole.edgeBearerFile = writeToken(t, 0o600, loginToken(`{"sub":"x"}`))
	if _, err := venueStamp(noRole); err == nil {
		t.Error("an unlabellable token accepted")
	}
}

func TestReportCarriesTheVenueOnlyForAVenueRun(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stamp *goapiproof.VenueStamp
	}{
		{"venue", &goapiproof.VenueStamp{Name: "bigboy-compose", Role: "admin", Superuser: true}},
		{"none", nil},
	} {
		path := filepath.Join(t.TempDir(), "r.json")
		f := flags{orgID: "o", window: goapiproof.DefaultWindow(), reportPath: path, stamp: tc.stamp}
		if err := emitReport(f, goapiproof.RegistryView{SchemaDigest: "s", BuildIdentity: "b"}, goapiproof.ProverBuild{}, nil,
			goapiproof.Summary{ByTerminalState: map[string]int{}, ByRefusalReason: map[string]int{}}, goapiproof.StaticCredential("Authorization", "x", "Bearer a.b.c"), goapiproof.StaticCredential("Authorization", "x", "Bearer a.b.c"), exitCompleted, nil); err != nil {
			t.Fatal(err)
		}
		raw, _ := os.ReadFile(path)
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		venue, present := decoded["venue"].(map[string]any)
		if present != (tc.stamp != nil) {
			t.Fatalf("%s: venue present=%t", tc.name, present)
		}
		if present && (venue["name"] != "bigboy-compose" || venue["principal_role"] != "admin" || venue["principal_superuser"] != true) {
			t.Fatalf("venue block %v", venue)
		}
		// The report a venue run writes is what enable reads back.
		if present {
			receipt, err := goapiproof.ParseVenueReceipt(raw)
			if err != nil || receipt.Venue == nil || receipt.Venue.Role != "admin" || !receipt.Venue.Superuser || receipt.ExitCause != exitCompleted || receipt.Stage != goapiproof.Stage {
				t.Fatalf("round trip: %+v %v", receipt, err)
			}
		}
	}
}

func TestAuthContextForSetsTheVenueOnlyWithAStamp(t *testing.T) {
	f := flags{principalKind: "stored_account", audience: "query-api"}
	if a := authContextFor(f, nil); a.Venue != "" || a.PrincipalRole != "" {
		t.Errorf("%+v", a)
	}
	a := authContextFor(f, &goapiproof.VenueStamp{Name: "bigboy-compose", Role: "admin", Superuser: true})
	if a.Venue != "bigboy-compose" || a.PrincipalRole != "admin" || !a.PrincipalSuperuser {
		t.Errorf("%+v", a)
	}
}

// A venue run writes NO store receipt. A go_api_proof_run row is ordinary
// enablement proof for whichever database the DSN names, so an admin
// principal's row there would satisfy the shared predicate without the
// venue-receipt checks. Driven through run() with a fake store: zero
// statements reach it, the report says zero written, and the venue block
// is still in the report. The control (no venue) writes two.
func TestAVenueRunWritesNoStoreReceipts(t *testing.T) {
	withProverCommit(t, e2eBuildSHA)
	tokenFile := writeToken(t, 0o600, loginToken(`{"sub":"edge","org_id":"70d529e0","role":"admin","is_superuser":false}`))
	t.Setenv(goapiproof.VenueAckEnvVar, "bigboy-compose")
	e2eEdgeArgs = func(*testing.T) []string {
		return []string{"-edge-bearer-file=" + tokenFile, "-venue=bigboy-compose"}
	}
	t.Cleanup(func() { e2eEdgeArgs = nil })

	stdout, runErr, reportPath, pool := runTwoOperationsEndToEnd(t)
	if runErr != nil {
		t.Fatalf("run(): %v\n%s", runErr, stdout)
	}
	if n := len(pool.execs); n != 0 {
		t.Fatalf("a venue run sent %d statements to the store: %+v", n, pool.execs)
	}
	if !strings.Contains(stdout, "no store receipts written") || !strings.Contains(stdout, "receipts_written=0") || !strings.Contains(stdout, "executed=2") {
		t.Fatalf("stdout: %s", stdout)
	}
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Venue   *goapiproof.VenueStamp `json:"venue"`
		Summary struct {
			ReceiptsWritten int `json:"receipts_written"`
		} `json:"summary"`
		Outcomes []struct {
			ReceiptWritten bool `json:"receipt_written"`
		} `json:"outcomes"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Venue == nil || decoded.Venue.Role != "admin" || decoded.Summary.ReceiptsWritten != 0 {
		t.Fatalf("report: %s", raw)
	}
	for _, o := range decoded.Outcomes {
		if o.ReceiptWritten {
			t.Fatalf("an outcome claims a written receipt: %s", raw)
		}
	}
}
