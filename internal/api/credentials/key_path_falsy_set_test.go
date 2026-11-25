package credentials

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// falsyKeyPathValues is the value set of a key-path field, with whether today's truthiness reads it as a path.
var falsyKeyPathValues = []struct {
	name, json string
	truthy     bool
}{
	{"blank", `""`, false},
	{"space", `" "`, true},
	{"tab", `"\t"`, true},
	{"null", `null`, false},
	{"false", `false`, false},
	{"zero", `0`, false},
}

type falsySetRow struct {
	name, fields string
	refused      bool
}

func falsySetRows() []falsySetRow {
	var rows []falsySetRow
	for _, spelling := range []string{"private_key_path", "privateKeyPath"} {
		for _, v := range falsyKeyPathValues {
			rows = append(rows, falsySetRow{spelling + "=" + v.name, `"` + spelling + `":` + v.json, v.truthy})
		}
	}
	for _, order := range [][2]string{{"private_key_path", "privateKeyPath"}, {"privateKeyPath", "private_key_path"}} {
		for _, a := range falsyKeyPathValues {
			for _, b := range falsyKeyPathValues {
				rows = append(rows, falsySetRow{order[0] + "=" + a.name + "," + order[1] + "=" + b.name,
					`"` + order[0] + `":` + a.json + `,"` + order[1] + `":` + b.json, a.truthy || b.truthy})
			}
		}
	}
	return rows
}

// The falsy set of a key-path field ("", whitespace-only, null, false, 0), each spelling alone and both present in both
// orders, on create, PATCH and the connection test: a whitespace-only value is a path and gets the fixed refusal; every
// other row goes on, sends no provider request, and (for a write) leaves no key-path field in the secret it seals.
func TestCredentialWritesFalsyKeyPathTable(t *testing.T) {
	rows := falsySetRows()
	for _, surface := range []string{"create", "patch", "test"} {
		t.Run(surface, func(t *testing.T) {
			for _, row := range rows {
				t.Run(row.name, func(t *testing.T) {
					harness := newInputsHarness(t)
					credentials := `{"app_id":"1","installation_id":"2",` + row.fields + `,"base_url":"` + inputsStubBase + `"}`
					var recorder *httptest.ResponseRecorder
					var reachedStore any
					switch surface {
					case "create":
						recorder, reachedStore = harness.serve(t, handlers.create, http.MethodPost, `{"provider":"github","name":"app","credentials":`+credentials+`}`, nil)
					case "patch":
						recorder, reachedStore = harness.serve(t, handlers.update, http.MethodPatch, `{"credentials":`+credentials+`}`, map[string]string{"provider": "github", "name": "app"})
					case "test":
						recorder, reachedStore = harness.serve(t, handlers.testConnection, http.MethodPost, `{"provider":"github","credentials":`+credentials+`}`, nil)
					}
					gotRefused := reachedStore == nil && recorder.Code == http.StatusBadRequest && strings.TrimSpace(recorder.Body.String()) == wantKeyPathRefusal
					if gotRefused != row.refused {
						t.Fatalf("refused=%v, want %v (status %d body %s, reached store %v)", gotRefused, row.refused, recorder.Code, recorder.Body.String(), reachedStore)
					}
					if !row.refused && surface != "test" {
						if named := storedPathFields(t, normalizeCredentialKeys("github", decodeObject(t, credentials))); len(named) != 0 {
							t.Errorf("dropped=false: the sealed secret keeps the key-path field(s) %v", named)
						}
					}
					if sent := harness.hits.Load(); sent != 0 {
						t.Errorf("%d provider request(s); want 0", sent)
					}
				})
			}
		})
	}
}

// resealTx is the transaction a credentials write runs on: the existing row is found, and every statement is recorded.
type resealTx struct {
	pgx.Tx
	queries []string
	execs   [][]any
}

type existingRow struct{ id uuid.UUID }

func (r existingRow) Scan(dest ...any) error {
	now := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	config := `{"auth_mode":"github_app"}`
	values := []any{r.id, "github", "app", true, true, &config, (*time.Time)(nil), (*bool)(nil), (*string)(nil), now, now}
	for i, d := range dest {
		switch p := d.(type) {
		case *uuid.UUID:
			*p = values[i].(uuid.UUID)
		case *string:
			*p = values[i].(string)
		case *bool:
			*p = values[i].(bool)
		case **string:
			*p, _ = values[i].(*string)
		case **time.Time:
			*p, _ = values[i].(*time.Time)
		case **bool:
			*p, _ = values[i].(*bool)
		case *time.Time:
			*p = values[i].(time.Time)
		}
	}
	return nil
}

func (tx *resealTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	tx.queries = append(tx.queries, sql)
	return existingRow{id: uuid.MustParse("11111111-1111-4111-8111-111111111111")}
}

func (tx *resealTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.queries = append(tx.queries, sql)
	tx.execs = append(tx.execs, args)
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

// plainCipher seals by prefixing, so a cell reads back exactly what a write sealed.
type plainCipher struct{}

func (plainCipher) Encrypt(plaintext []byte) (secrets.Value, error) {
	return secrets.NewValue("sealed:" + string(plaintext)), nil
}
func (plainCipher) Decrypt(secrets.Value) ([]byte, error) { return nil, nil }
func (plainCipher) Configured() bool                      { return true }

// A credentials PATCH over a stored row that already holds a key-path field: the write seals only the object it was
// sent (it never reads the stored secret, so nothing is carried forward), and a blank, falsy or absent key-path field
// leaves no key-path field in that seal. A config-only PATCH (patchFlags) does not re-seal at all; a key-path field
// already stored stays until follow-up PR 2 handles stored key-path rows.
func TestCredentialPatchReSealCarriesNoStoredKeyPath(t *testing.T) {
	for name, sent := range map[string]string{
		"credentials patch, key path blank":  `{"app_id":"1","installation_id":"2","private_key":"pem","private_key_path":""}`,
		"credentials patch, key path false":  `{"app_id":"1","installation_id":"2","private_key":"pem","privateKeyPath":false}`,
		"credentials patch, key path null":   `{"app_id":"1","installation_id":"2","private_key":"pem","private_key_path":null}`,
		"credentials patch, key path absent": `{"app_id":"1","installation_id":"2","private_key":"pem"}`,
	} {
		t.Run(name, func(t *testing.T) {
			h := handlers{cipher: plainCipher{}, now: time.Now}
			tx := &resealTx{}
			if _, err := h.setIn(context.Background(), tx, "org-1", "github", "app", decodeObject(t, sent), nil, true); err != nil {
				t.Fatal(err)
			}
			for _, sql := range tx.queries {
				if strings.Contains(sql, "SELECT") && strings.Contains(sql, "credentials_encrypted") && !strings.Contains(sql, "credentials_encrypted IS NOT NULL") {
					t.Errorf("the write read the stored secret: %s", sql)
				}
			}
			if len(tx.execs) != 1 {
				t.Fatalf("%d write statement(s); want 1", len(tx.execs))
			}
			sealed, _ := tx.execs[0][0].(string)
			var stored map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(sealed, "sealed:")), &stored); err != nil {
				t.Fatalf("read the sealed secret %q: %v", sealed, err)
			}
			for _, field := range []string{"private_key_path", "privateKeyPath"} {
				if _, present := stored[field]; present {
					t.Errorf("the sealed secret holds %s: %v", field, stored)
				}
			}
			if stored["app_id"] != "1" || stored["private_key"] != "pem" {
				t.Errorf("the sealed secret is not the object sent: %v", stored)
			}
		})
	}
}
