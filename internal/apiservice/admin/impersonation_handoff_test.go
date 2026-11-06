package admin

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// wantConsumeLua is the consume script as the fake runs it: any other script text is an unknown script.
const wantConsumeLua = `if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`

// fakeValkey is a loopback RESP server with the commands of the handoff record: SET (NX, EX), EVAL of the consume
// script only, GET, DEL. One mutex runs each command whole, as Valkey's one thread does. Its clock decides expiry.
type fakeValkey struct {
	mu      sync.Mutex
	entries map[string]fakeEntry
	now     time.Time
	cmds    [][]string
	failing bool
	hanging bool
}

type fakeEntry struct {
	value   string
	expires time.Time
}

func newFakeValkey(t *testing.T) (*fakeValkey, valkeygo.Client) {
	t.Helper()
	f := &fakeValkey{entries: map[string]fakeEntry{}, now: time.Unix(1_800_000_000, 0)}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(conn)
		}
	}()
	options, err := valkeygo.ParseURL("redis://" + ln.Addr().String() + "/0")
	if err != nil {
		t.Fatal(err)
	}
	options.DisableCache = true
	options.ForceSingleClient = true
	options.Dialer.Timeout = 2 * time.Second
	client, err := valkeygo.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return f, client
}

func (f *fakeValkey) advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

func (f *fakeValkey) set(failing, hanging bool) {
	f.mu.Lock()
	f.failing, f.hanging = failing, hanging
	f.mu.Unlock()
}

func (f *fakeValkey) evict(key string) {
	f.mu.Lock()
	delete(f.entries, key)
	f.mu.Unlock()
}

func (f *fakeValkey) commands() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.cmds)
}

func (f *fakeValkey) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for k, e := range f.entries {
		if f.now.Before(e.expires) {
			out = append(out, k)
		}
	}
	return out
}

func (f *fakeValkey) serve(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := readRESP(reader)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		reply := f.answer(args)
		if reply == "" {
			continue
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}

func (f *fakeValkey) answer(args []string) string {
	name := strings.ToUpper(args[0])
	switch name {
	case "HELLO":
		return "-ERR unknown command 'HELLO'\r\n"
	case "SET", "EVAL", "GET", "DEL", "GETDEL":
	default:
		return "+OK\r\n"
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cmds = append(f.cmds, slices.Clone(args))
	if f.hanging {
		return ""
	}
	if f.failing {
		return "-ERR simulated store failure\r\n"
	}
	live := func(key string) (fakeEntry, bool) {
		e, ok := f.entries[key]
		if ok && !f.now.Before(e.expires) {
			delete(f.entries, key)
			return fakeEntry{}, false
		}
		return e, ok
	}
	switch name {
	case "SET":
		if len(args) < 3 {
			return "-ERR syntax error\r\n"
		}
		nx, ttl := false, time.Duration(0)
		for i := 3; i < len(args); i++ {
			switch strings.ToUpper(args[i]) {
			case "NX":
				nx = true
			case "EX":
				if i+1 >= len(args) {
					return "-ERR syntax error\r\n"
				}
				seconds, err := strconv.Atoi(args[i+1])
				if err != nil {
					return "-ERR value is not an integer\r\n"
				}
				ttl = time.Duration(seconds) * time.Second
				i++
			default:
				return "-ERR syntax error\r\n"
			}
		}
		if _, ok := live(args[1]); ok && nx {
			return "$-1\r\n"
		}
		expires := f.now.Add(100 * 365 * 24 * time.Hour)
		if ttl > 0 {
			expires = f.now.Add(ttl)
		}
		f.entries[args[1]] = fakeEntry{value: args[2], expires: expires}
		return "+OK\r\n"
	case "EVAL":
		if len(args) != 5 || args[1] != wantConsumeLua || args[2] != "1" {
			return "-ERR unknown script\r\n"
		}
		if e, ok := live(args[3]); ok && e.value == args[4] {
			delete(f.entries, args[3])
			return ":1\r\n"
		}
		return ":0\r\n"
	case "GET":
		if e, ok := live(args[1]); ok {
			return fmt.Sprintf("$%d\r\n%s\r\n", len(e.value), e.value)
		}
		return "$-1\r\n"
	case "DEL":
		if _, ok := live(args[1]); ok {
			delete(f.entries, args[1])
			return ":1\r\n"
		}
		return ":0\r\n"
	}
	if e, ok := live(args[1]); ok {
		delete(f.entries, args[1])
		return fmt.Sprintf("$%d\r\n%s\r\n", len(e.value), e.value)
	}
	return "$-1\r\n"
}

func readRESP(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, nil
	}
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, count)
	for range count {
		lengthLine, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(strings.TrimRight(lengthLine, "\r\n")[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:length]))
	}
	return args, nil
}

// handoffCase is a delegated start case whose handlers keep their handoff records in a fake Valkey.
func newHandoffCase(t *testing.T) (*delegatedCase, *fakeValkey) {
	t.Helper()
	c := newDelegatedCase(true)
	f, client := newFakeValkey(t)
	c.h.handoffs = valkeyHandoffs{client: client}
	return c, f
}

func startHandoff(t *testing.T, c *delegatedCase) string {
	t.Helper()
	rec := c.do(http.HandlerFunc(c.h.startDelegatedImpersonation), c.target, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("start: %d %s", rec.Code, rec.Body.String())
	}
	var answer struct {
		HandoffID string `json:"handoff_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &answer); err != nil || answer.HandoffID == "" {
		t.Fatalf("start answer without handoff_id: %s", rec.Body.String())
	}
	return answer.HandoffID
}

func consumeAs(h *handlers, admin uuid.UUID, body string) *httptest.ResponseRecorder {
	return consumeAsWithin(context.Background(), h, admin, body)
}

func consumeAsWithin(ctx context.Context, h *handlers, admin uuid.UUID, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/v1/admin/impersonate/handoff/consume", strings.NewReader(body))
	parsed, _, _, _ := pybody.Read(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
	ctx = policy.WithBody(policy.WithUser(req.Context(), &policy.User{UserID: admin.String()}), parsed)
	rec := httptest.NewRecorder()
	h.consumeHandoff(rec, req.WithContext(ctx))
	return rec
}

func consumeBody(id string) string { return `{"handoff_id":"` + id + `"}` }

const (
	consumedBody = `{"status":"consumed"}`
	refusedBody  = `{"detail":"Impersonation handoff not admitted"}`
)

func wantAnswer(t *testing.T, name string, rec *httptest.ResponseRecorder, status int, body string) {
	t.Helper()
	if rec.Code != status || (body != "" && strings.TrimSpace(rec.Body.String()) != body) {
		t.Errorf("%s: %d %s, want %d %s", name, rec.Code, strings.TrimSpace(rec.Body.String()), status, body)
	}
}

// O1: a start handoff is admitted once; the replay (a re-login replays the same handoff) is refused.
func TestHandoffConsumeAdmitsOnceThenRefusesTheReplay(t *testing.T) {
	c, _ := newHandoffCase(t)
	id := startHandoff(t, c)
	wantAnswer(t, "first consume", consumeAs(c.h, c.admin, consumeBody(id)), http.StatusOK, consumedBody)
	wantAnswer(t, "replay", consumeAs(c.h, c.admin, consumeBody(id)), http.StatusConflict, refusedBody)
	wantAnswer(t, "second replay", consumeAs(c.h, c.admin, consumeBody(id)), http.StatusConflict, refusedBody)
}

// O2: N concurrent consumes of one handoff admit exactly one.
func TestHandoffConcurrentConsumesAdmitExactlyOne(t *testing.T) {
	c, _ := newHandoffCase(t)
	id := startHandoff(t, c)
	const n = 16
	codes := make([]int, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { codes[i] = consumeAs(c.h, c.admin, consumeBody(id)).Code })
	}
	wg.Wait()
	admitted, refused := 0, 0
	for _, code := range codes {
		switch code {
		case http.StatusOK:
			admitted++
		case http.StatusConflict:
			refused++
		}
	}
	if admitted != 1 || refused != n-1 {
		t.Fatalf("concurrent consumes: %d admitted, %d refused (codes %v), want 1 and %d", admitted, refused, codes, n-1)
	}
}

// O3: no store confirm is no admit: no client, a store error, a store that does not answer in time. The start does
// not start a delegated session when it cannot write the record.
func TestHandoffStoreDownIsRefused(t *testing.T) {
	t.Run("consume without a client", func(t *testing.T) {
		c := newDelegatedCase(true)
		wantAnswer(t, "consume", consumeAs(c.h, c.admin, consumeBody(strings.Repeat("A", 43))), http.StatusServiceUnavailable, "")
	})
	t.Run("start without a client", func(t *testing.T) {
		c := newDelegatedCase(true)
		rec := c.do(http.HandlerFunc(c.h.startDelegatedImpersonation), c.target, true)
		if rec.Code != http.StatusServiceUnavailable || len(c.delegator.started) != 0 || strings.Contains(rec.Body.String(), "a.b.c") {
			t.Errorf("start: %d %s, %d starts; want 503 and none", rec.Code, rec.Body.String(), len(c.delegator.started))
		}
	})
	for _, mode := range []struct {
		name             string
		failing, hanging bool
	}{{"store error", true, false}, {"store does not answer", false, true}} {
		t.Run("consume "+mode.name, func(t *testing.T) {
			c, f := newHandoffCase(t)
			id := startHandoff(t, c)
			f.set(mode.failing, mode.hanging)
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			wantAnswer(t, "consume", consumeAsWithin(ctx, c.h, c.admin, consumeBody(id)), http.StatusServiceUnavailable, "")
		})
		t.Run("start "+mode.name, func(t *testing.T) {
			c, f := newHandoffCase(t)
			f.set(mode.failing, mode.hanging)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/impersonate", nil)
			ctx, cancel := context.WithTimeout(req.Context(), 300*time.Millisecond)
			defer cancel()
			body, _, _, _ := pybody.Read(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"target_user_id":"`+c.target.String()+`"}`)))
			rec := httptest.NewRecorder()
			c.h.startDelegatedImpersonation(rec, req.WithContext(policy.WithBody(policy.WithUser(ctx, &policy.User{UserID: c.admin.String(), SessionID: &c.sid}), body)))
			if rec.Code != http.StatusServiceUnavailable || len(c.delegator.started) != 0 {
				t.Errorf("start: %d %s, %d starts; want 503 and none", rec.Code, rec.Body.String(), len(c.delegator.started))
			}
		})
	}
}

// O4: a record past its life is refused; inside its life (at least the 120 s of the web handoff) it is admitted.
func TestHandoffExpiredRecordIsRefused(t *testing.T) {
	c, f := newHandoffCase(t)
	inside := startHandoff(t, c)
	f.advance(120 * time.Second)
	wantAnswer(t, "at 120 s", consumeAs(c.h, c.admin, consumeBody(inside)), http.StatusOK, consumedBody)

	expired := startHandoff(t, c)
	f.advance(handoffTTL)
	wantAnswer(t, "past the record life", consumeAs(c.h, c.admin, consumeBody(expired)), http.StatusConflict, refusedBody)
}

// O5: another admin's handoff id is refused and does not use up the owner's record.
func TestHandoffOfAnotherUserIsRefused(t *testing.T) {
	c, _ := newHandoffCase(t)
	id := startHandoff(t, c)
	wantAnswer(t, "other admin", consumeAs(c.h, uuid.New(), consumeBody(id)), http.StatusConflict, refusedBody)
	wantAnswer(t, "owner after", consumeAs(c.h, c.admin, consumeBody(id)), http.StatusOK, consumedBody)
}

// O6: the delegated start writes the record (NX, the record life) BEFORE it asks auth-service, and answers its id; a
// refused start writes nothing, and go-api's own start (no delegator) has no handoff.
func TestDelegatedStartRecordsTheHandoffBeforeTheSession(t *testing.T) {
	c, f := newHandoffCase(t)
	var keysAtStart []string
	c.delegator.onStart = func() { keysAtStart = f.keys() }
	id := startHandoff(t, c)
	raw, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil || len(raw) != 32 {
		t.Fatalf("handoff id %q: %d bytes, %v; want 32 random bytes, base64url", id, len(raw), err)
	}
	key := handoffKeyPrefix + id
	if !slices.Equal(keysAtStart, []string{key}) {
		t.Fatalf("records when auth-service was asked: %v, want [%s]", keysAtStart, key)
	}
	cmds := f.commands()
	want := []string{"SET", key, c.admin.String(), "NX", "EX", "150"}
	if len(cmds) != 1 || !slices.Equal(cmds[0], want) {
		t.Fatalf("store commands %v, want [%v]", cmds, want)
	}
	if handoffTTL < 120*time.Second {
		t.Fatalf("record life %s, below the 120 s handoff life", handoffTTL)
	}
	if other := startHandoff(t, c); other == id {
		t.Fatalf("two starts share the handoff id %q", id)
	}

	refused, rf := newHandoffCase(t)
	refused.db.target = nil
	if rec := refused.do(http.HandlerFunc(refused.h.startDelegatedImpersonation), refused.target, true); rec.Code != http.StatusNotFound || len(rf.commands()) != 0 {
		t.Fatalf("refused start: %d, %d store commands; want 404 and none", rec.Code, len(rf.commands()))
	}
	noSession, nf := newHandoffCase(t)
	if rec := noSession.do(http.HandlerFunc(noSession.h.startDelegatedImpersonation), noSession.target, false); rec.Code != http.StatusConflict || len(nf.commands()) != 0 {
		t.Fatalf("start without a session: %d, %d store commands; want 409 and none", rec.Code, len(nf.commands()))
	}
	failed, ff := newHandoffCase(t)
	failed.delegator.err = sessionissue.ErrUnavailable
	if rec := failed.do(http.HandlerFunc(failed.h.startDelegatedImpersonation), failed.target, true); rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "handoff_id") {
		t.Fatalf("auth-service failure: %d %s; want 503 without a handoff id", rec.Code, rec.Body.String())
	} else if len(ff.commands()) != 1 {
		t.Fatalf("auth-service failure: %d store commands, want the one record write", len(ff.commands()))
	}
}

func TestGoAPIsOwnStartHasNoHandoff(t *testing.T) {
	c := newDelegatedCase(false)
	f, client := newFakeValkey(t)
	c.h.handoffs = valkeyHandoffs{client: client}
	rec := c.do(c.h.impersonateStart(), c.target, true)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "handoff_id") || len(f.commands()) != 0 {
		t.Fatalf("go-api start: %d %s, %d store commands; want 200, no handoff id, none", rec.Code, rec.Body.String(), len(f.commands()))
	}
}

// O7: a body without a string handoff_id is the 422 and asks the store nothing; an id that is not 32 base64url bytes
// is refused and asks the store nothing.
func TestHandoffConsumeBodyIsChecked(t *testing.T) {
	c, f := newHandoffCase(t)
	for _, body := range []string{`{}`, `{"handoff_id":null}`, `{"handoff_id":5}`, `[]`, `null`} {
		wantAnswer(t, body, consumeAs(c.h, c.admin, body), http.StatusUnprocessableEntity, "")
	}
	for _, id := range []string{"", "short", strings.Repeat("A", 42), strings.Repeat("A", 44), strings.Repeat("A", 42) + "+", strings.Repeat("A", 42) + "=", strings.Repeat("A", 42) + `\n`, strings.Repeat("A", 43) + `\n`, `\n` + strings.Repeat("A", 43)} {
		wantAnswer(t, "id "+id, consumeAs(c.h, c.admin, consumeBody(id)), http.StatusConflict, refusedBody)
	}
	if n := len(f.commands()); n != 0 {
		t.Fatalf("%d store commands, want 0", n)
	}
	h := &handlers{handoffs: c.h.handoffs, logger: c.h.logger}
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	parsed, _, _, _ := pybody.Read(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(consumeBody(strings.Repeat("A", 43)))))
	rec := httptest.NewRecorder()
	h.consumeHandoff(rec, req.WithContext(policy.WithBody(policy.WithUser(req.Context(), &policy.User{UserID: "not-a-uuid"}), parsed)))
	wantAnswer(t, "bad user id", rec, http.StatusBadRequest, "")
}

// O8: a record that is gone (eviction, a restart, a flush) is refused: no fallback.
func TestHandoffWhoseRecordIsGoneIsRefused(t *testing.T) {
	c, f := newHandoffCase(t)
	id := startHandoff(t, c)
	f.evict(handoffKeyPrefix + id)
	wantAnswer(t, "record gone", consumeAs(c.h, c.admin, consumeBody(id)), http.StatusConflict, refusedBody)
}

// O9: the store commands go-api's Valkey user needs are SET (NX EX) and EVAL of the consume script, nothing else.
func TestHandoffStoreCommandsAreSetAndEvalOnly(t *testing.T) {
	c, f := newHandoffCase(t)
	id := startHandoff(t, c)
	consumeAs(c.h, c.admin, consumeBody(id))
	key := handoffKeyPrefix + id
	want := [][]string{
		{"SET", key, c.admin.String(), "NX", "EX", "150"},
		{"EVAL", wantConsumeLua, "1", key, c.admin.String()},
	}
	if got := f.commands(); !slices.EqualFunc(got, want, slices.Equal[[]string]) {
		t.Fatalf("store commands %q, want %q", got, want)
	}
}

// The consume route is the policy package's live-row exempt pattern, behind the admin.impersonate platform wrap, and
// mounted with the admin routes.
func TestHandoffConsumeRouteIsTheExemptPatternAndMounted(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := &handlers{guard: policy.NewGuard(nil, logger), logger: logger}
	routes := h.handoffRoutes()
	if len(routes) != 1 || routes[0].Method+" "+routes[0].Pattern != policy.ImpersonationHandoffConsumePattern {
		t.Fatalf("consume routes %+v, want one at %q", routes, policy.ImpersonationHandoffConsumePattern)
	}
	mounted := 0
	for _, route := range h.routes() {
		if route.Method+" "+route.Pattern == policy.ImpersonationHandoffConsumePattern {
			mounted++
		}
	}
	if mounted != 1 {
		t.Fatalf("the consume route is mounted %d times, want 1", mounted)
	}
}

// Through the real scope and guard: with the admin's impersonation row live (it is live once the start ran), the
// consume route reaches its handler; the start route is refused in the session.
func TestHandoffConsumeRouteIsReachableWithALiveImpersonationRow(t *testing.T) {
	const key = "route-order-test-signing-key-32-bytes!!!!"
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, impersonatingStore{active: true}, logger)
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, logger)
	admin := uuid.New()
	bearer, err := signer.Access(edgetoken.AccessClaims{UserID: admin.String(), Email: "root@example.test", IsSuperuser: true}, time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	f, client := newFakeValkey(t)
	touched := new(bool)
	h := &handlers{store: pgStore{Pool: touchedDB{touched}}, guard: policy.NewGuard(auth, logger), logger: logger, handoffs: valkeyHandoffs{client: client}}
	id := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	if err := h.handoffs.record(context.Background(), id, admin.String()); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range append(h.impersonationRoutes(), h.handoffRoutes()...) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(scope.Principal(scope.OrgScope(scope.Impersonation(mux))))
	defer server.Close()
	post := func(path, body string) (int, string) {
		req, _ := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+bearer)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, strings.TrimSpace(string(b))
	}
	if status, body := post("/api/v1/admin/impersonate/handoff/consume", consumeBody(id)); status != http.StatusOK || body != consumedBody {
		t.Fatalf("consume with a live row: %d %s, want 200 %s", status, body, consumedBody)
	}
	if status, body := post("/api/v1/admin/impersonate", `{"target_user_id":"`+uuid.NewString()+`"}`); status != http.StatusForbidden || body != impersonationRefusal {
		t.Fatalf("start with a live row: %d %s, want 403 %s", status, body, impersonationRefusal)
	}
	if *touched || len(f.keys()) != 0 {
		t.Fatalf("touched db %v, records left %v", *touched, f.keys())
	}
}
