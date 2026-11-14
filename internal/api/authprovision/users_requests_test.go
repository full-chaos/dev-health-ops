package authprovision_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
)

// The members each user-admin route admits: auth-service's own admit lists, the slices its handlers pass to admit.
// The route refuses any other member as malformed, so a request carries only these.
var usersAdmitted = map[string][]string{
	authprovision.PathUsersCreate:   authapi.UsersCreateMembers(),
	authprovision.PathUsersUpdate:   authapi.UsersUpdateMembers(),
	authprovision.PathUsersPassword: authapi.UsersPasswordMembers(),
	authprovision.PathUsersDelete:   authapi.UsersDeleteMembers(),
}

type usersServer struct {
	mu       sync.Mutex
	bodies   map[string]map[string]any
	answer   func(path string) (int, string)
	exchange string
}

func newUsersClient(t *testing.T, s *usersServer) *authprovision.UsersClient {
	t.Helper()
	return authprovision.NewUsersClient(newCoreFor(t, s))
}

// newCoreFor serves s over a test auth-service and returns the workload Core that calls it.
func newCoreFor(t *testing.T, s *usersServer) *authprovision.Core {
	t.Helper()
	base, workload := newWorkloadFor(t, s)
	core, err := authprovision.NewWorkloadCore(base, workload, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return core
}

// newWorkloadFor serves s over a test auth-service and returns its URL and the workload token client that calls it.
func newWorkloadFor(t *testing.T, s *usersServer) (*url.URL, *workloadtoken.Client) {
	t.Helper()
	s.bodies = map[string]map[string]any{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == workloadtoken.ExchangePath {
			_, _ = io.WriteString(w, s.exchange)
			return
		}
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		s.mu.Lock()
		s.bodies[r.URL.Path] = body
		answerFor := s.answer
		s.mu.Unlock()
		status, answer := answerFor(r.URL.Path)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, answer)
	}))
	t.Cleanup(server.Close)
	base, _ := url.Parse(server.URL)
	token, _ := json.Marshal(map[string]any{"access_token": mintWorkloadToken(t), "issued_token_type": workloadtoken.TokenTypeJWT,
		"token_type": "Bearer", "expires_in": 300})
	s.exchange = string(token)
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+base64.RawURLEncoding.EncodeToString(random)), 0o600); err != nil {
		t.Fatal(err)
	}
	workload, err := workloadtoken.New(workloadtoken.Config{ExchangeURL: server.URL, AllowPlainHTTP: true, Issuer: workloadIssuer,
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: &http.Transport{}})
	if err != nil {
		t.Fatal(err)
	}
	return base, workload
}

func keysOf(body map[string]any) []string {
	out := make([]string, 0, len(body))
	for key := range body {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func sorted(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func usersOK(principal uuid.UUID) func(string) (int, string) {
	return func(path string) (int, string) {
		switch path {
		case authprovision.PathUsersCreate:
			return http.StatusOK, `{"principal_id":"` + principal.String() + `","email":"new@example.com"}`
		case authprovision.PathUsersDelete:
			return http.StatusOK, `{"principal_id":"` + principal.String() + `","was_active":true,"already_done":false}`
		}
		return http.StatusOK, `{"principal_id":"` + principal.String() + `","changed":true}`
	}
}

// Every user-admin request with all its fields set carries exactly the members its route admits.
func TestUsersAdminRequestsCarryTheAdmittedMembers(t *testing.T) {
	principal, actor, org := uuid.New(), uuid.New(), uuid.New()
	server := &usersServer{answer: usersOK(principal)}
	client := newUsersClient(t, server)
	ctx := context.Background()
	text, yes := "x", true
	metadata := json.RawMessage(`{"user_agent":"ua"}`)
	created, err := client.CreateUser(ctx, authprovision.UserCreateRequest{Actor: actor, Email: "New@Example.com", PasswordHash: &text, Username: &text,
		FullName: &text, AuthProvider: &text, AuthProviderID: &text, IsVerified: true, GrantPlatformAdmin: true, OrgID: &org, Role: "member",
		RequestMetadata: metadata})
	if err != nil || created.ID != principal {
		t.Fatalf("create = %+v, %v", created, err)
	}
	if err := client.UpdateUser(ctx, authprovision.UserUpdateRequest{Actor: actor, Principal: principal, OrgID: &org, Email: &text,
		Username: &text, FullName: &text, AvatarURL: &text, IsVerified: &yes, IsSuperuser: &yes, IsActive: &yes}); err != nil {
		t.Fatal(err)
	}
	if err := client.SetUserPassword(ctx, authprovision.UserPasswordRequest{Actor: actor, Principal: principal, OrgID: &org,
		AdminPassword: "admin", PasswordHash: "hash", RequestMetadata: metadata}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DeleteUser(ctx, authprovision.UserDeleteRequest{Actor: actor, Principal: principal, OrgID: &org, Mode: "soft"}); err != nil {
		t.Fatal(err)
	}
	for path, admitted := range usersAdmitted {
		if got := keysOf(server.bodies[path]); !reflect.DeepEqual(got, sorted(admitted)) {
			t.Errorf("%s members %v, want %v", path, got, sorted(admitted))
		}
	}
	if server.bodies[authprovision.PathUsersCreate]["email"] != "New@Example.com" {
		t.Errorf("create email %v, want the address as sent", server.bodies[authprovision.PathUsersCreate]["email"])
	}
}

// A request leaves out every field it does not set: an unset field is no change and no member.
func TestUsersAdminRequestsLeaveOutUnsetFields(t *testing.T) {
	principal, actor := uuid.New(), uuid.New()
	server := &usersServer{answer: usersOK(principal)}
	client := newUsersClient(t, server)
	ctx := context.Background()
	if _, err := client.CreateUser(ctx, authprovision.UserCreateRequest{Actor: actor, Email: "a@example.com"}); err != nil {
		t.Fatal(err)
	}
	if err := client.UpdateUser(ctx, authprovision.UserUpdateRequest{Actor: actor, Principal: principal}); err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		authprovision.PathUsersCreate: {"actor_principal_id", "email", "grant_platform_admin", "is_verified"},
		authprovision.PathUsersUpdate: {"actor_principal_id", "principal_id"},
	}
	for path, keys := range want {
		if got := keysOf(server.bodies[path]); !reflect.DeepEqual(got, keys) {
			t.Errorf("%s members %v, want %v", path, got, keys)
		}
	}
}

// A refusal carries its reason code; an answer for another user is a dependency failure.
func TestUsersAdminAnswers(t *testing.T) {
	principal := uuid.New()
	server := &usersServer{answer: func(string) (int, string) {
		return http.StatusConflict, `{"schema_version":"error.v1","status":409,"reason_code":"username_taken"}`
	}}
	client := newUsersClient(t, server)
	_, err := client.CreateUser(context.Background(), authprovision.UserCreateRequest{Actor: uuid.New(), Email: "a@example.com"})
	if !errors.Is(err, authprovision.ErrRefused) || authprovision.ReasonOf(err) != "username_taken" {
		t.Fatalf("create = %v, want the username_taken refusal", err)
	}
	server.mu.Lock()
	server.answer = usersOK(uuid.New())
	server.mu.Unlock()
	if err := client.UpdateUser(context.Background(), authprovision.UserUpdateRequest{Actor: uuid.New(), Principal: principal}); !errors.Is(err, authprovision.ErrUnavailable) {
		t.Fatalf("update for another principal = %v, want ErrUnavailable", err)
	}
	if _, err := client.DeleteUser(context.Background(), authprovision.UserDeleteRequest{Actor: uuid.New(), Principal: principal, Mode: "soft"}); !errors.Is(err, authprovision.ErrUnavailable) {
		t.Fatalf("delete for another principal = %v, want ErrUnavailable", err)
	}
}
