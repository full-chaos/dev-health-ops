package shadowvenue

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestCheckBaseAcceptsOnlyAPlainLoopbackOrigin(t *testing.T) {
	for _, ok := range []string{"http://127.0.0.1:8093", "http://localhost:8093", "http://[::1]:8093", "http://127.0.0.1:8093/"} {
		if _, err := CheckVenueBase(ok, EgressRead{}, ""); err != nil {
			t.Errorf("%s refused: %v", ok, err)
		}
	}
	for _, bad := range []string{
		"https://api.fullchaos.dev", "http://api.fullchaos.dev", "http://10.0.0.5:8093", "http://go-api:8000",
		"https://127.0.0.1:8093", "http://user:pw@127.0.0.1:8093", "http://127.0.0.1:8093/api", "127.0.0.1:8093",
		"http://127.0.0.1.example.com:8093", "http://0.0.0.0:8093", "http://127.0.0.1:8093?x=1",
	} {
		if _, err := CheckVenueBase(bad, EgressRead{}, ""); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

func TestReadPasswordReadsAnOwnerOnlyFileAndNeverEchoesIt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.pw")
	if err := os.WriteFile(path, []byte("s3cret-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadPassword(path)
	if err != nil || got != "s3cret-value" {
		t.Fatalf("%q %v", got, err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPassword(path); err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("a group-readable file: %v", err)
	}
	empty := filepath.Join(dir, "e.pw")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPassword(empty); err == nil {
		t.Fatal("an empty file accepted")
	}
	link := filepath.Join(dir, "l.pw")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPassword(link); err == nil {
		t.Fatal("a symlink accepted")
	}
	// An owner-only FIFO passes every mode check but the regular-file one; reading it would block the driver.
	fifo := filepath.Join(dir, "f.pw")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := ReadPassword(fifo); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a FIFO accepted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadPassword blocked on a FIFO")
	}
}

type seenRequest struct {
	method, path, auth, body string
	at                       time.Time
}

// fakeAPI is a loopback go-api: login answers a token per email, every other route answers 403 for a member token.
type fakeAPI struct {
	srv  *httptest.Server
	mu   sync.Mutex
	seen []seenRequest
}

func newFakeAPI(t *testing.T) *fakeAPI {
	t.Helper()
	f := &fakeAPI{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.seen = append(f.seen, seenRequest{method: r.Method, path: r.URL.Path, auth: r.Header.Get("Authorization"), body: string(raw), at: time.Now()})
		f.mu.Unlock()
		if r.URL.Path == "/api/v1/auth/login" {
			var in struct{ Email, Password string }
			_ = json.Unmarshal(raw, &in)
			if !strings.HasPrefix(in.Password, "fake-") {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"detail":"Invalid credentials ` + in.Password + `"}`))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "tok-" + in.Email, "token_type": "bearer"})
			return
		}
		if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer tok-m@") {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func TestLoginReadsTheAccessTokenAndKeepsTheBodyOutOfItsError(t *testing.T) {
	api := newFakeAPI(t)
	base, err := api.srv.URL, error(nil)
	if err != nil {
		t.Fatal(err)
	}
	venue, _ := testPrincipals(t)
	d := &Driver{Base: base, Client: api.srv.Client()}
	member, _ := venue.Principal("member-A")
	token, err := d.Login(context.Background(), member, venue.Orgs["A"])
	if err != nil || token != "tok-m@example.test" {
		t.Fatalf("%q %v", token, err)
	}
	if !strings.Contains(api.seen[0].body, `"org_id":"`+testOrgA+`"`) {
		t.Fatalf("login body has no org_id: %s", api.seen[0].body)
	}
	super, _ := venue.Principal("superuser")
	if _, err := d.Login(context.Background(), super, ""); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(api.seen[1].body, "org_id") {
		t.Fatalf("an org-less login sent org_id: %s", api.seen[1].body)
	}
	bad := member
	path := filepath.Join(t.TempDir(), "bad.pw")
	if err := os.WriteFile(path, []byte("wrong-pw\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	bad.PasswordFile = path
	_, err = d.Login(context.Background(), bad, venue.Orgs["A"])
	if err == nil || strings.Contains(err.Error(), "wrong-pw") || !strings.Contains(err.Error(), "401") {
		t.Fatalf("a refused login: %v", err)
	}
}

// Drive sends one request at a time, gap apart, with the cell's token and body, and writes one slot line each; the
// slots file holds no token.
func TestDriveSendsEverySlotInOrderAndWritesTheSlotsFile(t *testing.T) {
	api := newFakeAPI(t)
	base := api.srv.URL
	plan, err := planOf(t,
		"58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n"+
			"59\tPOST\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t{\"name\":\"x\"}\n"+
			"22\tPOST\t/api/v1/billing/plans/pull-stripe\tA\tno\tstripe\t-\tmatch\t/api/v1/billing/plans/pull-stripe\t-\n")
	if err != nil {
		t.Fatal(err)
	}
	var slept []time.Duration
	d := &Driver{Base: base, Client: api.srv.Client(), Gap: 2500 * time.Millisecond,
		Sleep: func(_ context.Context, gap time.Duration) error { slept = append(slept, gap); return nil }}
	tokens := map[string]string{"member-A": "tok-m@example.test", "admin-A": "tok-a@example.test", "superuser": "tok-s@example.test"}
	var out bytes.Buffer
	if err := d.Drive(context.Background(), plan, tokens, &out); err != nil {
		t.Fatal(err)
	}
	if len(api.seen) != 18 {
		t.Fatalf("%d requests, want 18 (the stripe row is a gap)", len(api.seen))
	}
	if len(slept) != 18 {
		t.Fatalf("%d gaps, want one after each request", len(slept))
	}
	if api.seen[0].auth != "Bearer tok-m@example.test" || api.seen[3].auth != "Bearer tok-a@example.test" || api.seen[6].auth != "Bearer tok-s@example.test" {
		t.Fatalf("tokens: %q %q %q", api.seen[0].auth, api.seen[3].auth, api.seen[6].auth)
	}
	if api.seen[0].body != "" || api.seen[9].method != "POST" || api.seen[9].body != `{"name":"x"}` {
		t.Fatalf("bodies: %q %s %q", api.seen[0].body, api.seen[9].method, api.seen[9].body)
	}
	if strings.Contains(out.String(), "tok-") {
		t.Fatal("the slots file holds a token")
	}
	slots, err := ParseSlots(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(slots) != 21 {
		t.Fatalf("%d slot lines, want 21 (18 sent, one gap per stripe cell)", len(slots))
	}
	if slots[0].Status != http.StatusForbidden || slots[3].Status != http.StatusOK || !slots[0].End.After(slots[0].Start) {
		t.Fatalf("slot 1 %+v slot 4 %+v", slots[0], slots[3])
	}
	if !slots[20].Gap() || !strings.Contains(slots[20].Note, "stripe") {
		t.Fatalf("last slot %+v is no gap", slots[20])
	}
	missing := map[string]string{"member-A": "tok-m@example.test"}
	if err := d.Drive(context.Background(), plan, missing, io.Discard); err == nil {
		t.Fatal("a plan with a principal that has no token was driven")
	}
}

func TestDriveRefusesAGapShorterThanTheShadowTimeout(t *testing.T) {
	api := newFakeAPI(t)
	base := api.srv.URL
	plan, _ := planOf(t, "58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	d := &Driver{Base: base, Client: api.srv.Client(), Gap: 2 * time.Second}
	tokens := map[string]string{"member-A": "a", "admin-A": "b", "superuser": "c"}
	if err := d.Drive(context.Background(), plan, tokens, io.Discard); err == nil {
		t.Fatal("a 2 s gap was accepted")
	}
	if len(api.seen) != 0 {
		t.Fatalf("%d requests sent before the refusal", len(api.seen))
	}
}

// The pre-send guard: a destructive slot whose path or body holds an id that is not frozen (here org A's, or no frozen
// ids at all) refuses the whole drive before any request, even when the plan did not come from MakePlan.
func TestDriveRefusesADestructiveSlotOutsideTheFrozenIDsBeforeAnyRequest(t *testing.T) {
	api := newFakeAPI(t)
	base := api.srv.URL
	_, frozen, _, _ := testPoolVenue(t)
	tokens := map[string]string{"member-A": "a", "superuser": "s"}
	first := Slot{Seq: 1, Row: "58", Caller: "member", Principal: "member-A", Expect: "match", K: 1, Method: "GET", Route: "/api/v1/admin/teams", Path: "/api/v1/admin/teams", Body: none}
	del := Slot{Seq: 2, Row: "13", Caller: "superuser", Principal: "superuser", Expect: "match", K: 1, Method: "DELETE", Route: "/api/v1/admin/orgs/{org_id}", Body: none, Destructive: true}
	for name, c := range map[string]struct {
		path   string
		frozen ThrowawayIDs
	}{
		"org A":     {"/api/v1/admin/orgs/" + testOrgA, frozen},
		"no frozen": {"/api/v1/admin/orgs/" + poolID(1, "org"), ThrowawayIDs{}},
	} {
		slot := del
		slot.Path = c.path
		d := &Driver{Base: base, Client: api.srv.Client(), Gap: MinGap, Frozen: c.frozen, Sleep: func(context.Context, time.Duration) error { return nil }}
		if err := d.Drive(context.Background(), Plan{Slots: []Slot{first, slot}}, tokens, io.Discard); err == nil || !strings.Contains(err.Error(), "refused before any request") {
			t.Errorf("%s: %v", name, err)
		}
		if len(api.seen) != 0 {
			t.Fatalf("%s: %d requests sent before the refusal", name, len(api.seen))
		}
	}
	slot := del
	slot.Path = "/api/v1/admin/orgs/" + poolID(1, "org")
	d := &Driver{Base: base, Client: api.srv.Client(), Gap: MinGap, Frozen: frozen, Sleep: func(context.Context, time.Duration) error { return nil }}
	if err := d.Drive(context.Background(), Plan{Slots: []Slot{first, slot}}, tokens, io.Discard); err != nil {
		t.Fatalf("a pool org delete was refused: %v", err)
	}
	if len(api.seen) != 2 {
		t.Fatalf("%d requests, want 2", len(api.seen))
	}
}

// A non-loopback base is go-api's own venue address only: the private IP named as goapi_ip by a readable egress record
// that shows an internal network with no default route.
func TestCheckVenueBaseAcceptsOnlyTheRecordedGoAPIAddress(t *testing.T) {
	good := ParseEgress("before.txt", []byte(egressText("before")))
	if u, err := CheckVenueBase("http://"+testGoAPIIP+":8000", good, testRunID); err != nil || u.Host != testGoAPIIP+":8000" {
		t.Fatalf("the recorded address: %v %v", u, err)
	}
	if _, err := CheckVenueBase("http://127.0.0.1:8093", EgressRead{}, ""); err != nil {
		t.Fatalf("loopback with no record: %v", err)
	}
	set := func(key, value string) EgressRead {
		lines := strings.Split(egressText("before"), "\n")
		for i, l := range lines {
			if strings.HasPrefix(l, key+"=") {
				lines[i] = key + "=" + value
			}
		}
		return ParseEgress("before.txt", []byte(strings.Join(lines, "\n")))
	}
	cases := map[string]struct {
		base   string
		record EgressRead
	}{
		"no record":                 {"http://" + testGoAPIIP + ":8000", EgressRead{}},
		"unreadable record":         {"http://" + testGoAPIIP + ":8000", EgressRead{Name: "x", Err: os.ErrNotExist}},
		"a malformed record":        {"http://" + testGoAPIIP + ":8000", ParseEgress("before.txt", []byte(egressText("before")+"goapi_ip="+testGoAPIIP+"\n"))},
		"another private ip":        {"http://172.30.0.9:8000", good},
		"a public ip":               {"http://8.8.8.8:8000", set("goapi_ip", "8.8.8.8")},
		"a host name":               {"http://go-api:8000", set("goapi_ip", "go-api")},
		"network not internal":      {"http://" + testGoAPIIP + ":8000", set("network_internal", "false")},
		"a default route":           {"http://" + testGoAPIIP + ":8000", set("default_route", "via 172.30.0.1")},
		"a path":                    {"http://" + testGoAPIIP + ":8000/api", good},
		"https":                     {"https://" + testGoAPIIP + ":8000", good},
		"a record of another drive": {"http://" + testGoAPIIP + ":8000", set("run_id", "13267-4b")},
		"the after read":            {"http://" + testGoAPIIP + ":8000", set("run_id", "none")},
	}
	for name, c := range cases {
		if _, err := CheckVenueBase(c.base, c.record, testRunID); err == nil {
			t.Errorf("%s: %s accepted", name, c.base)
		}
	}
}
