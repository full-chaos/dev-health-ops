package saml

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// identity.ErrProviderAlreadyLinked from the owned re-link takes the refuse path that ErrRelinkRefused takes
// (ErrIdentityAmbiguous: ConsumeFor refuses with outcome identity_conflict, the handler answers 409), never a 500.
func TestProviderAlreadyLinkedIsRefusedAsAnIdentityConflict(t *testing.T) {
	for name, err := range map[string]error{
		"ErrProviderAlreadyLinked": identity.ErrProviderAlreadyLinked,
		"ErrRelinkRefused":         identity.ErrRelinkRefused,
	} {
		t.Run(name, func(t *testing.T) {
			mapped := LookupError(err)
			if !errors.Is(mapped, ErrIdentityAmbiguous) {
				t.Errorf("LookupError(%v) = %v; want ErrIdentityAmbiguous (the refuse path)", err, mapped)
			}
			rec := httptest.NewRecorder()
			Handlers{}.writeError(rec, httptest.NewRequest(http.MethodPost, "/acs", nil), mapped)
			want := `More than one account carries this sign-in. Contact your organization administrator.`
			if rec.Code != http.StatusConflict || !containsDetail(rec.Body.String(), want) {
				t.Errorf("answer = %d %s; want 409 %q", rec.Code, rec.Body.String(), want)
			}
		})
	}
}

func containsDetail(body, detail string) bool {
	var got struct {
		Detail string `json:"detail"`
	}
	return json.Unmarshal([]byte(body), &got) == nil && got.Detail == detail
}
