package shadowvenue

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const driveBody = `{"ok":true,"org":"` + testOrgA + `"}`

// healthyTokens are the tokens of testPrincipals: sub of the principal, the org of its principal (none for the superuser).
func healthyTokens(t *testing.T, venue Venue, exp time.Time) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, p := range venue.Principals {
		org := ""
		if p.Org != none {
			org = venue.Orgs[p.Org]
		}
		out[p.Name] = jwtOf(t, "EdDSA", "at+jwt", WirePrincipal(p.UUID), org, exp)
	}
	return out
}

type bodyAPI struct {
	srv  *httptest.Server
	mu   sync.Mutex
	seen int
}

func newBodyAPI(t *testing.T, body string) *bodyAPI {
	t.Helper()
	f := &bodyAPI{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.seen++
		f.mu.Unlock()
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *bodyAPI) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen
}

func TestPlanGivesEachSlotTheSubAndOrgItsTokenMustCarry(t *testing.T) {
	plan, err := planOf(t, "58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	if err != nil {
		t.Fatal(err)
	}
	venue, _ := testPrincipals(t)
	for _, s := range plan.Slots {
		p, _ := venue.Principal(s.Principal)
		wantOrg := venue.Orgs[p.Org]
		if p.Org == none {
			wantOrg = none
		}
		if s.WantSub != WirePrincipal(p.UUID) || s.WantOrg != wantOrg {
			t.Fatalf("slot %d %s: want sub %q org %q, got %q %q", s.Seq, s.Principal, WirePrincipal(p.UUID), wantOrg, s.WantSub, s.WantOrg)
		}
	}
}

func TestDriveRecordsClaimsAndBodyDigestsPerSlotAndNeverTheTokenOrTheBody(t *testing.T) {
	api := newBodyAPI(t, driveBody)
	plan, err := planOf(t, "58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	if err != nil {
		t.Fatal(err)
	}
	venue, _ := testPrincipals(t)
	exp := time.Now().Add(time.Hour)
	tokens := healthyTokens(t, venue, exp)
	d := &Driver{Base: api.srv.URL, Client: api.srv.Client(), Gap: MinGap, Sleep: func(context.Context, time.Duration) error { return nil }}
	var out bytes.Buffer
	if err := d.Drive(context.Background(), plan, tokens, &out); err != nil {
		t.Fatal(err)
	}
	for _, token := range tokens {
		if strings.Contains(out.String(), token) || strings.Contains(out.String(), strings.Split(token, ".")[1]) {
			t.Fatal("the slots file holds a token")
		}
	}
	if strings.Contains(out.String(), "ok") && strings.Contains(out.String(), driveBody) {
		t.Fatal("the slots file holds a body")
	}
	slots, err := ParseSlots(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(driveBody))
	for _, s := range slots {
		if s.ClaimSub != s.WantSub || s.ClaimOrg != s.WantOrg || s.ClaimExp.Unix() != exp.Unix() {
			t.Fatalf("slot %d claims %q %q %v, want %q %q %v", s.Seq, s.ClaimSub, s.ClaimOrg, s.ClaimExp, s.WantSub, s.WantOrg, exp)
		}
		if s.BodyLen != len(driveBody) || s.BodySHA != hex.EncodeToString(sum[:]) || len(s.BodyNormSHA) != 64 {
			t.Fatalf("slot %d body %d %q %q", s.Seq, s.BodyLen, s.BodySHA, s.BodyNormSHA)
		}
	}
}

// A token with less than 120 s left at a slot stops the drive before that slot: the slot is not sent and not skipped.
func TestDriveStopsBeforeASlotWhoseTokenHasLessThanTwoMinutesLeft(t *testing.T) {
	api := newBodyAPI(t, driveBody)
	plan, err := planOf(t, "58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	if err != nil {
		t.Fatal(err)
	}
	venue, _ := testPrincipals(t)
	clock := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	tokens := healthyTokens(t, venue, clock.Add(150*time.Second))
	d := &Driver{Base: api.srv.URL, Client: api.srv.Client(), Gap: MinGap,
		Now:   func() time.Time { return clock },
		Sleep: func(context.Context, time.Duration) error { clock = clock.Add(40 * time.Second); return nil }}
	var out bytes.Buffer
	err = d.Drive(context.Background(), plan, tokens, &out)
	if err == nil || !strings.Contains(err.Error(), "expires in") || !strings.Contains(err.Error(), "slot") {
		t.Fatalf("a token about to expire did not stop the drive: %v", err)
	}
	if n := api.count(); n != 1 {
		t.Fatalf("%d requests sent; the drive must stop before the slot whose token has < 120 s left (1 sent at 150 s, 110 s left at the next)", n)
	}
	if strings.Contains(err.Error(), tokens["member-A"]) {
		t.Fatal("the token is in the error")
	}
}

func TestDriveRefusesAnExpiredTokenBeforeAnyRequest(t *testing.T) {
	api := newBodyAPI(t, driveBody)
	plan, err := planOf(t, "58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	if err != nil {
		t.Fatal(err)
	}
	venue, _ := testPrincipals(t)
	tokens := healthyTokens(t, venue, time.Now().Add(-time.Minute))
	d := &Driver{Base: api.srv.URL, Client: api.srv.Client(), Gap: MinGap, Sleep: func(context.Context, time.Duration) error { return nil }}
	var out bytes.Buffer
	if err := d.Drive(context.Background(), plan, tokens, &out); err == nil || api.count() != 0 || out.Len() != 0 {
		t.Fatalf("an expired token was driven: %v, %d requests, %d bytes of slots file", err, api.count(), out.Len())
	}
}

func TestDriveRefusesABodyOverTheLimit(t *testing.T) {
	api := newBodyAPI(t, strings.Repeat("x", 4<<20+10))
	plan, err := planOf(t, "58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	if err != nil {
		t.Fatal(err)
	}
	venue, _ := testPrincipals(t)
	d := &Driver{Base: api.srv.URL, Client: api.srv.Client(), Gap: MinGap, Sleep: func(context.Context, time.Duration) error { return nil }}
	if err := d.Drive(context.Background(), plan, healthyTokens(t, venue, time.Now().Add(time.Hour)), io.Discard); err == nil || !strings.Contains(err.Error(), "4 MiB") {
		t.Fatalf("a body over the limit was digested: %v", err)
	}
}

func TestParseSlotsRefusesTheV1HeaderAndAShortLine(t *testing.T) {
	v1 := "seq\trow\tcaller\tprincipal\tcross_org\texpect\tk\tmethod\troute\tpath\tt_start\tt_end\tstatus\tnote\n"
	if _, err := ParseSlots([]byte(v1)); err == nil || !strings.Contains(err.Error(), "v1") {
		t.Fatalf("a v1 slots file was read: %v", err)
	}
	if _, err := ParseSlots([]byte(slotsHeader + "\n1\tx\n")); err == nil {
		t.Fatal("a short line was read")
	}
}

// Mint signs principals in and writes one owner-only token file each; no token reaches a result, an error or a log.
func mintServer(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		org := ""
		if i := strings.Index(string(raw), `"org_id":"`); i >= 0 {
			org = string(raw)[i+10 : i+10+36]
		}
		_, _ = io.WriteString(w, `{"access_token":"`+jwtOf(t, "EdDSA", "at+jwt", WirePrincipal(claimUser), org, time.Now().Add(time.Hour))+`"}`)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func superOrgVenue(t *testing.T) Venue {
	t.Helper()
	_, raw := testPrincipals(t)
	pw := filepath.Join(t.TempDir(), "su.pw")
	if err := os.WriteFile(pw, []byte("fake-su\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	v, err := ParsePrincipals([]byte(raw +
		"org\tProof Org E\t-\t" + claimOrgE + "\tE\t-\t-\n" +
		"principal\tsuperuser-E\ts@example.test\t" + claimUser + "\tE\tplatform_admin_org(no_membership)\t" + pw + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestMintWritesOwnerOnlyTokenFilesAndPrintsNoToken(t *testing.T) {
	srv := mintServer(t, http.StatusOK)
	venue := superOrgVenue(t)
	dir := filepath.Join(t.TempDir(), "tokens")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	d := &Driver{Base: srv.URL, Client: srv.Client()}
	got, err := Mint(context.Background(), d, venue, []string{"superuser-E"}, dir)
	if err != nil || len(got) != 1 {
		t.Fatalf("%v %v", got, err)
	}
	if got[0].Name != "superuser-E" || got[0].Sub != WirePrincipal(claimUser) || got[0].Org != claimOrgE || got[0].Exp.IsZero() {
		t.Fatalf("%+v", got[0])
	}
	path := filepath.Join(dir, "superuser-E.token")
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("token file: %v %v", info, err)
	}
	raw, _ := os.ReadFile(path)
	if _, err := ReadTokenFile(path); err != nil || !strings.Contains(string(raw), ".") {
		t.Fatalf("the written file is no token file: %v", err)
	}
	if strings.Contains(MintLine(got[0]), strings.TrimSpace(string(raw))) {
		t.Fatal("the mint line holds the token")
	}
	if _, err := Mint(context.Background(), d, venue, []string{"superuser-E"}, dir); err == nil {
		t.Fatal("an existing token file was overwritten")
	}
}

func TestMintRefusesWhatItCannotMintSafely(t *testing.T) {
	venue := superOrgVenue(t)
	srv := mintServer(t, http.StatusOK)
	d := &Driver{Base: srv.URL, Client: srv.Client()}
	good := t.TempDir()
	if err := os.Chmod(good, 0o700); err != nil {
		t.Fatal(err)
	}
	open := t.TempDir()
	if err := os.Chmod(open, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, run := range map[string]func() error{
		"a name that is no principal": func() error { _, err := Mint(context.Background(), d, venue, []string{"nobody"}, good); return err },
		"a principal with no org":     func() error { _, err := Mint(context.Background(), d, venue, []string{"superuser"}, good); return err },
		"a directory open to the group": func() error {
			_, err := Mint(context.Background(), d, venue, []string{"superuser-E"}, open)
			return err
		},
		"no name": func() error { _, err := Mint(context.Background(), d, venue, nil, good); return err },
		"a name twice": func() error {
			_, err := Mint(context.Background(), d, venue, []string{"superuser-E", "superuser-E"}, good)
			return err
		},
		"a refused sign-in": func() error {
			_, err := Mint(context.Background(), &Driver{Base: mintServer(t, http.StatusUnauthorized).URL, Client: http.DefaultClient}, venue, []string{"superuser-E"}, good)
			return err
		},
		"a directory that does not exist": func() error {
			_, err := Mint(context.Background(), d, venue, []string{"superuser-E"}, filepath.Join(good, "nope"))
			return err
		},
	} {
		if err := run(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if entries, _ := os.ReadDir(good); len(entries) != 0 {
		t.Errorf("a refused mint left %d files", len(entries))
	}
}

func TestMintRefusesATokenThatIsNoUserAccessToken(t *testing.T) {
	venue := superOrgVenue(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"access_token":"tok-not-a-jwt"}`)
	}))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := Mint(context.Background(), &Driver{Base: srv.URL, Client: srv.Client()}, venue, []string{"superuser-E"}, dir)
	if err == nil || strings.Contains(err.Error(), "tok-not-a-jwt") {
		t.Fatalf("%v", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatal("a file was written for a bad token")
	}
}

func TestMintChecksTheOrgClaimOfWhatItGot(t *testing.T) {
	venue := superOrgVenue(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"access_token":"`+jwtOf(t, "EdDSA", "at+jwt", WirePrincipal(claimUser), "", time.Now().Add(time.Hour))+`"}`)
	}))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Mint(context.Background(), &Driver{Base: srv.URL, Client: srv.Client()}, venue, []string{"superuser-E"}, dir); err == nil {
		t.Fatal("a token with no org claim was written for an org principal")
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatal("a file was written for a token with the wrong org")
	}
}
