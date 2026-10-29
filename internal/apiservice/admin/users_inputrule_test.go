package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

var inputRuleForms = []struct{ name, escape string }{
	{"replacement_char", `�`}, {"lone_high", `\ud800`}, {"lone_low", `\udc00`}, {"nul", `\u0000`},
	{"control_c0", `\u0001`}, {"tab", `\t`}, {"lf", `\n`}, {"cr", `\r`}, {"del", `\u007f`}, {"c1", `\u0085`},
}

func refusedType(t *testing.T, response string) (string, string) {
	t.Helper()
	var doc struct {
		Detail []struct {
			Type string `json:"type"`
			Loc  []any  `json:"loc"`
		} `json:"detail"`
	}
	if err := json.Unmarshal([]byte(response), &doc); err != nil || len(doc.Detail) == 0 {
		t.Fatalf("no validation list: %q (%v)", response, err)
	}
	return doc.Detail[0].Type, doc.Detail[0].Loc[len(doc.Detail[0].Loc)-1].(string)
}

// Every cell is refused before the database; U+FFFD alone stays accepted in a name.
func TestAdminCreateUserRefusesBadTextFormsBeforeTheDatabase(t *testing.T) {
	for _, form := range inputRuleForms {
		for _, field := range []string{"email", "full_name"} {
			value := `"A` + form.escape + `B"`
			wantType, accepted := "string_unicode", field == "full_name" && form.name == "replacement_char"
			if field == "email" {
				value = `"a` + form.escape + `b@example.test"`
				wantType = "value_error"
				if strings.HasPrefix(form.name, "lone_") {
					wantType = "string_unicode"
				}
			}
			body := `{"email":"a@example.test","` + field + `":` + value + `}`
			if field == "email" {
				body = `{"email":` + value + `}`
			}
			code, response, _, touched := createUserWithBody(t, body)
			if accepted {
				if code == http.StatusUnprocessableEntity {
					t.Errorf("create %s %s: refused: %s", field, form.name, response)
				}
				continue
			}
			if touched || code != http.StatusUnprocessableEntity {
				t.Errorf("create %s %s: status %d touched %v: %s", field, form.name, code, touched, response)
				continue
			}
			if gotType, gotField := refusedType(t, response); gotType != wantType || gotField != field {
				t.Errorf("create %s %s: answers %s on %s, want %s on %s", field, form.name, gotType, gotField, wantType, field)
			}
			if form.name != "lone_high" && form.name != "lone_low" && strings.Contains(response, `"input":"A`) {
				t.Errorf("create %s %s: the refusal echoes the input: %s", field, form.name, response)
			}
		}
	}
}

func TestAdminUpdateUserRefusesBadTextFormsWithTheSameAnswer(t *testing.T) {
	for _, form := range inputRuleForms {
		for _, field := range []string{"email", "full_name", "avatar_url"} {
			value := `"A` + form.escape + `B"`
			wantType, accepted := "string_unicode", field == "full_name" && form.name == "replacement_char"
			if field == "email" {
				value = `"a` + form.escape + `b@example.test"`
				wantType = "value_error"
			}
			if field == "avatar_url" {
				value = `"https://example.test/` + form.escape + `x.png"`
				accepted = false
			}
			request := httptest.NewRequest(http.MethodPatch, "/api/v1/admin/users/x", strings.NewReader(`{"`+field+`":`+value+`}`))
			request.Header.Set("Content-Type", "application/json")
			decoded, outcome, _, err := pybody.Read(request)
			if err != nil || outcome != pybody.Ready {
				t.Fatalf("body: %v %v", err, outcome)
			}
			var errs pybody.Errors
			parseUserUpdate(&errs, decoded)
			if accepted {
				if len(errs) != 0 {
					t.Errorf("update %s %s: refused", field, form.name)
				}
				continue
			}
			if len(errs) != 1 || errs[0].Type != wantType || errs[0].Loc[len(errs[0].Loc)-1] != pyjson.Value(field) {
				t.Errorf("update %s %s: errors %+v, want one %s on %s", field, form.name, errs, wantType, field)
			}
		}
	}
}
