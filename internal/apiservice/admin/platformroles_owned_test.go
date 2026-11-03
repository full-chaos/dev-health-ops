package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// While auth-service owns the platform roles, a request that writes
// is_superuser answers the moved operation before any database work; a
// request without it is not refused by this switch.
func TestAdminSuperuserWritesRefuseWhileOwned(t *testing.T) {
	caller := &policy.User{UserID: uuid.NewString(), ID: uuid.New(), IsSuperuser: true}
	for name, c := range map[string]struct {
		route func(*handlers) http.HandlerFunc
		body  string
	}{
		"create superuser":  {func(h *handlers) http.HandlerFunc { return h.createUser }, `{"email":"a@example.test","password":"Long-enough-password-1","is_superuser":true}`},
		"update true":       {func(h *handlers) http.HandlerFunc { return h.updateUser }, `{"is_superuser":true}`},
		"update false":      {func(h *handlers) http.HandlerFunc { return h.updateUser }, `{"is_superuser":false}`},
		"update with other": {func(h *handlers) http.HandlerFunc { return h.updateUser }, `{"full_name":"x","is_superuser":true}`},
	} {
		t.Run(name, func(t *testing.T) {
			h := &handlers{store: pgStore{Pool: failingDB{t}, PlatformRolesOwned: true}, logger: slog.New(slog.NewTextHandler(&strings.Builder{}, nil))}
			recorder := httptest.NewRecorder()
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						t.Errorf("the handler went past the switch: %v", recovered)
					}
				}()
				request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users/"+uuid.NewString(), nil)
				request.SetPathValue("user_id", uuid.NewString())
				request = withBody(c.body)(request.WithContext(policy.WithUser(request.Context(), caller)))
				c.route(h)(recorder, request)
			}()
			if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), `"reason":"operation_moved"`) ||
				!strings.Contains(recorder.Body.String(), "platform roles") {
				t.Fatalf("status %d body %q, want 409 operation_moved for platform roles", recorder.Code, recorder.Body.String())
			}
		})
	}
}

// The store refuses the write too, before any statement.
func TestAdminStoreRefusesASuperuserWriteWhileOwned(t *testing.T) {
	store := pgStore{Pool: failingDB{t}, PlatformRolesOwned: true}
	ctx := context.Background()
	yes, no := true, false
	if _, err := store.insertUser(ctx, userCreateInput{IsSuperuser: true}); !errors.Is(err, ErrPlatformRolesOwned) {
		t.Errorf("insert superuser: %v", err)
	}
	for _, v := range []*bool{&yes, &no} {
		if _, err := store.updateUser(ctx, uuid.New(), userUpdate{IsSuperuser: v}); !errors.Is(err, ErrPlatformRolesOwned) {
			t.Errorf("update is_superuser=%v: %v", *v, err)
		}
	}
}

// The operator verbs refuse an is_superuser write before a transaction (the
// pool is nil: any database work panics).
func TestOperatorSuperuserWritesRefuseWhileOwned(t *testing.T) {
	operator := Operator{PlatformRolesOwned: true}
	ctx := context.Background()
	email, yes := "a@example.test", true
	for name, call := range map[string]func() error{
		"create": func() error {
			_, err := operator.CreateUser(ctx, CreateUserInput{Email: email, Password: "Long-enough-password-1", Superuser: true})
			return err
		},
		"update": func() error {
			_, err := operator.UpdateUser(ctx, UpdateUserInput{Email: &email, Superuser: &yes})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			var err error
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						err = fmt.Errorf("went past the switch to the database: %v", recovered)
					}
				}()
				err = call()
			}()
			if !errors.Is(err, ErrPlatformRolesOwned) {
				t.Fatalf("%v, want ErrPlatformRolesOwned", err)
			}
		})
	}
}
