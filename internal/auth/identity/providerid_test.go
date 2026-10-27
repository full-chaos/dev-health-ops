package identity

import (
	"errors"
	"strings"
	"testing"
)

func TestRefuseInvalidProviderID(t *testing.T) {
	ptr := func(s string) *string { return &s }
	invalid := map[string]*string{"nil": nil, "empty": ptr(""), "blank": ptr(" \t"), "leading space": ptr(" 1"), "trailing space": ptr("1 "),
		"control": ptr("1\x01"), "replacement char": ptr("1�"), "over 1024 bytes": ptr(strings.Repeat("a", 1025))}
	for _, provider := range []string{"github", "gitlab", "google", "oidc", "saml", "6f0c2d58-6b43-4b8e-9a8e-3a6f7a1c9d11"} {
		for name, id := range invalid {
			if err := RefuseInvalidProviderID(provider, id); !errors.Is(err, ErrInvalidProviderID) {
				t.Errorf("%s/%s: err = %v, want ErrInvalidProviderID", provider, name, err)
			}
		}
		for _, valid := range []string{"1", "12345", "None", "0", "sub-1", strings.Repeat("a", 1024)} {
			if err := RefuseInvalidProviderID(provider, &valid); err != nil {
				t.Errorf("%s/%q: err = %v, want nil", provider, valid, err)
			}
		}
	}
	for _, provider := range []string{"", "local", "service"} {
		for name, id := range invalid {
			if err := RefuseInvalidProviderID(provider, id); err != nil {
				t.Errorf("%q/%s: err = %v, want nil (local and service rows are not checked)", provider, name, err)
			}
		}
	}
	if msg := ErrInvalidProviderID.Error(); strings.ContainsAny(msg, "0123456789") {
		t.Errorf("the refusal text should name no value: %q", msg)
	}
}
