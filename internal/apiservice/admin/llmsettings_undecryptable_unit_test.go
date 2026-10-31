package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-12172 unit pins for the undecryptable-settings path. The real-route
// proof is TestLLMSettingsUndecryptableAfterKeyRotation (integration tag).

const (
	unitKeyBefore = "unit-llm-settings-fernet-key-before!!"
	unitKeyAfter  = "unit-llm-settings-fernet-key-after!!!"
)

func unitDecryptor(t *testing.T, key string) providerfoundation.FernetDecryptor {
	t.Helper()
	d, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), "")
	if err != nil {
		t.Fatalf("decryptor: %v", err)
	}
	return d
}

func sealed(t *testing.T, key, plain string) string {
	t.Helper()
	value, err := unitDecryptor(t, key).Encrypt([]byte(plain))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	return revealSecret(value)
}

// settingsDB serves settingByKeyOn from a map of rows keyed by settings key.
type settingsDB struct{ rows map[string]setting }

type settingRow struct {
	row   setting
	found bool
}

func (r settingRow) Scan(dest ...any) error {
	if !r.found {
		return pgx.ErrNoRows
	}
	*dest[0].(*uuid.UUID) = r.row.ID
	*dest[1].(*string) = r.row.Key
	*dest[2].(**string) = r.row.Value
	*dest[3].(*bool) = r.row.IsEncrypted
	*dest[4].(**string) = r.row.Description
	return nil
}

func (db settingsDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("settingsDB: no writes")
}

func (db settingsDB) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	key, _ := args[2].(string)
	row, ok := db.rows[key]
	return settingRow{row: row, found: ok}
}

func plainRow(key, value string) setting {
	return setting{ID: uuid.New(), Key: key, Value: &value}
}

func sealedRow(t *testing.T, key, value, encryptionKey string) setting {
	cipher := sealed(t, encryptionKey, value)
	return setting{ID: uuid.New(), Key: key, Value: &cipher, IsEncrypted: true}
}

func TestSettingValueNamesARowTheKeyCannotOpen(t *testing.T) {
	h := &handlers{decryptor: unitDecryptor(t, unitKeyAfter)}
	row := sealedRow(t, "api_key", "placeholder-secret-value", unitKeyBefore)
	_, err := h.settingValue(&row)
	var u *undecryptableSettingsError
	if !errors.As(err, &u) || !slices.Equal(u.keys, []string{"api_key"}) {
		t.Fatalf("err=%v, want an undecryptableSettingsError naming api_key", err)
	}
	if strings.Contains(err.Error(), *row.Value) || strings.Contains(err.Error(), "placeholder-secret-value") {
		t.Fatalf("the error text holds key material: %q", err.Error())
	}
	same := sealedRow(t, "api_key", "placeholder-secret-value", unitKeyAfter)
	if got, err := h.settingValue(&same); err != nil || got == nil || *got != "placeholder-secret-value" {
		t.Fatalf("a row the key opens: got=%v err=%v", got, err)
	}
	keyless := &handlers{}
	if _, err := keyless.settingValue(&row); !errors.Is(err, errEncryptionKeyMissing) || errors.As(err, &u) {
		t.Fatalf("no key configured: err=%v, want errEncryptionKeyMissing and not undecryptable", err)
	}
}

func TestLLMSettingsResponseNamesEveryUnreadableSetting(t *testing.T) {
	h := &handlers{decryptor: unitDecryptor(t, unitKeyAfter)}
	db := settingsDB{rows: map[string]setting{
		"provider":                 plainRow("provider", "anthropic"),
		"model":                    plainRow("model", "big-model"),
		"api_key":                  sealedRow(t, "api_key", "placeholder-default", unitKeyBefore),
		"purpose:explain:provider": plainRow("purpose:explain:provider", "openai"),
		"purpose:explain:model":    plainRow("purpose:explain:model", "small-model"),
		"purpose:explain:api_key":  sealedRow(t, "purpose:explain:api_key", "placeholder-explain", unitKeyBefore),
	}}
	_, err := h.llmSettingsResponse(context.Background(), db, uuid.NewString())
	var u *undecryptableSettingsError
	if !errors.As(err, &u) {
		t.Fatalf("err=%v, want undecryptableSettingsError", err)
	}
	got := slices.Clone(u.keys)
	slices.Sort(got)
	if want := []string{"api_key", "purpose:explain:api_key"}; !slices.Equal(got, want) {
		t.Fatalf("keys=%v, want %v", got, want)
	}

	readable := settingsDB{rows: map[string]setting{
		"provider": plainRow("provider", "anthropic"),
		"api_key":  sealedRow(t, "api_key", "placeholder-default", unitKeyAfter),
	}}
	if _, err := h.llmSettingsResponse(context.Background(), readable, uuid.NewString()); err != nil {
		t.Fatalf("a readable config: %v", err)
	}
}

func TestWriteUndecryptableAnswersTheDistinct409(t *testing.T) {
	h := &handlers{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	recorder := httptest.NewRecorder()
	err := &llmStepError{"build llm settings response", &undecryptableSettingsError{keys: []string{"purpose:explain:api_key", "api_key", "api_key"}}}
	h.byoReadFailed(context.Background(), recorder, "read llm settings", err)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status=%d, want 409", recorder.Code)
	}
	var body struct {
		Detail struct {
			Error    string   `json:"error"`
			Feature  string   `json:"feature"`
			Message  string   `json:"message"`
			Settings []string `json:"settings"`
			Purposes []string `json:"purposes"`
		} `json:"detail"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", recorder.Body.String(), err)
	}
	d := body.Detail
	if d.Error != "llm_settings_undecryptable" || d.Feature != "byo_llm" || d.Message != "The saved key can no longer be read. Replace or remove it." {
		t.Fatalf("detail=%+v", d)
	}
	if !slices.Equal(d.Settings, []string{"api_key", "purpose:explain:api_key"}) || !slices.Equal(d.Purposes, []string{"explain"}) {
		t.Fatalf("settings=%v purposes=%v", d.Settings, d.Purposes)
	}

	other := httptest.NewRecorder()
	h.byoReadFailed(context.Background(), other, "read llm settings", errors.New("connection reset"))
	if other.Code != http.StatusInternalServerError || strings.Contains(other.Body.String(), "llm_settings_undecryptable") {
		t.Fatalf("a transient failure: status=%d body=%s, want the generic 500", other.Code, other.Body.String())
	}
}

func TestReadableStoredKeyLeavesOnlyAnUnreadableKeyOut(t *testing.T) {
	if got, err := readableStoredKey("old", &undecryptableSettingsError{keys: []string{"api_key"}}); got != "" || err != nil {
		t.Fatalf("unreadable: got=%q err=%v, want \"\" and nil", got, err)
	}
	if got, err := readableStoredKey("kept", nil); got != "kept" || err != nil {
		t.Fatalf("readable: got=%q err=%v", got, err)
	}
	transient := errors.New("connection reset")
	if _, err := readableStoredKey("", transient); !errors.Is(err, transient) {
		t.Fatalf("a transient failure must pass through, got %v", err)
	}
}

func TestPurposeOfKey(t *testing.T) {
	for key, want := range map[string]string{"purpose:explain:api_key": "explain", "purpose:investigate:api_key": "investigate", "api_key": "", "purpose:": "", "purpose:x": ""} {
		got, ok := purposeOfKey(key)
		if got != want || ok != (want != "") {
			t.Errorf("purposeOfKey(%q)=%q,%v want %q", key, got, ok, want)
		}
	}
}
