package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// listRows is a pgx.Rows over fixed settings rows.
type listRows struct {
	pgx.Rows
	rows []setting
	next int
}

func (r *listRows) Next() bool { r.next++; return r.next <= len(r.rows) }
func (r *listRows) Close()     {}
func (r *listRows) Err() error { return nil }
func (r *listRows) Scan(dest ...any) error {
	row := r.rows[r.next-1]
	*dest[0].(*uuid.UUID) = row.ID
	*dest[1].(*string) = row.Key
	*dest[2].(**string) = row.Value
	*dest[3].(*bool) = row.IsEncrypted
	*dest[4].(**string) = row.Description
	return nil
}

// listDB serves settingsByCategory from rows keyed by "org/category".
type listDB struct {
	pgDB
	rows map[string][]setting
	err  error
}

func (db listDB) Query(_ context.Context, _ string, args ...any) (pgx.Rows, error) {
	if db.err != nil {
		return nil, db.err
	}
	return &listRows{rows: db.rows[args[0].(string)+"/"+args[1].(string)]}, nil
}

type listBody struct {
	Category string `json:"category"`
	Settings []struct {
		Key         string  `json:"key"`
		Value       *string `json:"value"`
		Category    string  `json:"category"`
		IsEncrypted bool    `json:"is_encrypted"`
		Description *string `json:"description"`
	} `json:"settings"`
}

func listCategory(t *testing.T, db listDB, org, category string, keyed bool) (int, string) {
	t.Helper()
	h := &handlers{store: pgStore{Pool: db}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if keyed {
		h.decryptor = unitDecryptor(t, unitKeyAfter)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/settings/"+category, nil)
	req.SetPathValue("category", category)
	req = req.WithContext(policy.WithUser(req.Context(), &policy.User{OrgID: org, Role: "admin"}))
	rec := httptest.NewRecorder()
	h.listSettingsByCategory(rec, req)
	return rec.Code, rec.Body.String()
}

func TestListSettingsByCategoryAnswersTheRows(t *testing.T) {
	plain := plainRow("site_name", "Acme")
	plain.Description = strp("the name")
	secret := sealedRow(t, "token", "placeholder-secret-value", unitKeyAfter)
	db := listDB{rows: map[string][]setting{
		"org-a/general": {plain, secret},
		"org-b/general": {plainRow("other_org", "b")},
	}}
	code, body := listCategory(t, db, "org-a", "general", true)
	if code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", code, body)
	}
	var got listBody
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("body %q: %v", body, err)
	}
	if got.Category != "general" || len(got.Settings) != 2 {
		t.Fatalf("body=%s, want category general and 2 settings", body)
	}
	a, b := got.Settings[0], got.Settings[1]
	if a.Key != "site_name" || a.Category != "general" || a.IsEncrypted || a.Value == nil || *a.Value != "Acme" || a.Description == nil || *a.Description != "the name" {
		t.Fatalf("plain row=%+v", a)
	}
	if b.Key != "token" || b.Category != "general" || !b.IsEncrypted || b.Value == nil || *b.Value != "[ENCRYPTED]" {
		t.Fatalf("encrypted row=%+v", b)
	}
	if want := *secret.Value; strings.Contains(body, want) || strings.Contains(body, "placeholder-secret-value") {
		t.Fatalf("body leaks the stored value: %s", body)
	}
}

func TestListSettingsByCategoryEmptyCategoryAnswersEmptyList(t *testing.T) {
	db := listDB{rows: map[string][]setting{"org-b/general": {plainRow("other_org", "b")}}}
	code, body := listCategory(t, db, "org-a", "general", true)
	if code != http.StatusOK || body != `{"category":"general","settings":[]}` {
		t.Fatalf("status=%d body=%s, want 200 with an empty list", code, body)
	}
}

func TestListSettingsByCategoryDoesNotListAnotherOrgsRows(t *testing.T) {
	db := listDB{rows: map[string][]setting{
		"org-a/general": {plainRow("mine", "1")},
		"org-b/general": {plainRow("theirs", "2")},
	}}
	_, body := listCategory(t, db, "org-a", "general", true)
	if !strings.Contains(body, `"mine"`) || strings.Contains(body, "theirs") {
		t.Fatalf("body=%s, want only org-a rows", body)
	}
}

func TestListSettingsByCategoryDedicatedCategoriesStay403(t *testing.T) {
	db := listDB{rows: map[string][]setting{"org-a/llm": {plainRow("k", "v")}, "org-a/ask_dev": {plainRow("k", "v")}}}
	for _, category := range []string{"llm", "llm_budget", "ask_dev"} {
		if code, body := listCategory(t, db, "org-a", category, true); code != http.StatusForbidden {
			t.Errorf("%s: status=%d body=%s, want 403", category, code, body)
		}
	}
}

func TestListSettingsByCategoryStoreErrorStays500(t *testing.T) {
	code, _ := listCategory(t, listDB{err: errors.New("boom")}, "org-a", "general", true)
	if code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500", code)
	}
}
