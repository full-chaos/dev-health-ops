package saml

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestSAMLACSOverrideMustEndInTheRowsOwnPath(t *testing.T) {
	id := uuid.New()
	for override, ok := range map[string]bool{
		"": true,
		"https://sp.example.com/api/v1/auth/saml/" + id.String() + "/acs":             true,
		"https://proxy.example.com/edge/api/v1/auth/saml/" + id.String() + "/acs":     true,
		"https://sp.example.com/api/v1/auth/saml/" + uuid.NewString() + "/acs":        false,
		"https://sp.example.com/api/v1/auth/saml/" + id.String() + "/acs/extra":       false,
		"https://sp.example.com/api/v1/auth/saml/" + id.String() + "x/acs":            false,
		"https://sp.example.com/custom/acs":                                           false,
		"https://sp.example.com/xsaml/" + id.String() + "/acs?q=/saml/" + id.String(): false,
		"https://sp.example.com/api/v1/auth/saml%2F" + id.String() + "%2Facs":         false,
		"https://sp.example.com/api/v1/auth/saml/" + id.String() + "%2facs":           false,
		"https://sp.example.com/api/v1/auth%2Fsaml/" + id.String() + "/acs":           true,
		"https://sp.example.com/api/v1/auth/saml/" + id.String() + "/%61cs":           false,
	} {
		err := ValidateACSOverride(Config{SPACSURL: override}, id)
		if (err == nil) != ok || (err != nil && !errors.Is(err, ErrConfig)) {
			t.Errorf("ValidateACSOverride(%q) = %v, want ok=%v", override, err, ok)
		}
	}
}

func TestSAMLConsumeRefusesARowWhoseACSPathNamesAnotherRow(t *testing.T) {
	p := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml", Status: "active",
		Config: Config{SPACSURL: "https://sp.example.com/api/v1/auth/saml/" + uuid.NewString() + "/acs"}}
	s := &Service{Providers: oneProvider{p}, Entitlements: allowAll{}, BaseURL: func() string { return "https://sp.example.com/api/v1/auth" }}
	_, err := s.Consume(context.Background(), p.ID, "PHg+", nil)
	if reason, ok := IsRefused(err); !ok || reason != ReasonAudience {
		t.Fatalf("err = %v, want a refusal with reason audience", err)
	}
	_, err = s.Consume(context.Background(), uuid.New(), "PHg+", nil)
	if reason, ok := IsRefused(err); !ok || reason != ReasonAudience {
		t.Fatalf("a row other than the route's: err = %v, want a refusal with reason audience", err)
	}
}

type allowAll struct{}

func (allowAll) OrgHasSSO(context.Context, uuid.UUID) (bool, error) { return true, nil }
