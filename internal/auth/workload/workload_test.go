package workload

import (
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

func TestNarrowNeverWidens(t *testing.T) {
	r := Registration{Audiences: []string{"auth-service"}, AllowedActions: []string{"org.read", "metrics.read"}}
	cases := []struct {
		name     string
		audience string
		scope    []string
		want     []string
		refused  bool
	}{
		{"subset", "auth-service", []string{"org.read"}, []string{"org.read"}, false},
		{"dedupes", "auth-service", []string{"org.read", "org.read"}, []string{"org.read"}, false},
		{"empty request stays empty", "auth-service", nil, []string{}, false},
		{"action outside the allowed list", "auth-service", []string{"org.write"}, nil, true},
		{"wildcard", "auth-service", []string{"*"}, nil, true},
		{"audience not held", "dev-health-api", []string{"org.read"}, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := r.Narrow(c.audience, c.scope)
			if c.refused {
				if !errors.Is(err, ErrRefused) {
					t.Fatalf("err = %v, want ErrRefused", err)
				}
				return
			}
			if err != nil || len(got) != len(c.want) || (len(got) > 0 && got[0] != c.want[0]) {
				t.Fatalf("Narrow = %v, %v; want %v", got, err, c.want)
			}
		})
	}
	empty := Registration{Audiences: []string{"auth-service"}}
	if _, err := empty.Narrow("auth-service", []string{"org.read"}); !errors.Is(err, ErrRefused) {
		t.Fatalf("an empty allowed list allowed an action: %v", err)
	}
}

func TestValidators(t *testing.T) {
	cat := policycatalog.Default()
	if ValidateName("api") != nil || ValidateName("A") == nil || ValidateName("a") == nil || ValidateName("a b") == nil {
		t.Error("name pattern")
	}
	if ValidateAudiences([]string{"auth-service"}) != nil || ValidateAudiences([]string{"Auth"}) == nil ||
		ValidateAudiences([]string{"a1", "a1"}) == nil || ValidateAudiences(make([]string, 17)) == nil {
		t.Error("audience rules")
	}
	if ValidateActions(cat, []string{"org.read"}) != nil || ValidateActions(cat, []string{"*"}) == nil ||
		ValidateActions(cat, []string{"org.read", "org.read"}) == nil || ValidateActions(cat, []string{"nope.read"}) == nil {
		t.Error("action rules")
	}
	ok := BindingSpec{Environment: "prod", TrustDomain: "cluster.local", Issuer: "https://kubernetes.default.svc", Namespace: "dev-health",
		ServiceAccountName: "api", ServiceAccountUID: "6f1c3c2e-0b8e-4d2a-9d7e-2f4a5b6c7d8e"}
	if err := ValidateBinding(ok); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*BindingSpec){
		"empty namespace": func(b *BindingSpec) { b.Namespace = "" },
		"padded issuer":   func(b *BindingSpec) { b.Issuer = " x" },
		"bad environment": func(b *BindingSpec) { b.Environment = "Prod" },
		"bad uid":         func(b *BindingSpec) { b.ServiceAccountUID = "abc" },
		"over-long sa":    func(b *BindingSpec) { b.ServiceAccountName = strings.Repeat("a", 254) },
		"empty trust":     func(b *BindingSpec) { b.TrustDomain = "" },
		"empty sa":        func(b *BindingSpec) { b.ServiceAccountName = "" },
	} {
		b := ok
		mutate(&b)
		if !errors.Is(ValidateBinding(b), ErrBindingInvalid) {
			t.Errorf("%s accepted", name)
		}
	}
	noUID := ok
	noUID.ServiceAccountUID = ""
	if ValidateBinding(noUID) != nil {
		t.Error("an unpinned uid must be accepted")
	}
}

func TestRefusalErrorIsErrRefusedAndCarriesOnlyTheReasonWord(t *testing.T) {
	for _, r := range []Reason{ReasonUnknown, ReasonDisabled, ReasonUIDMismatch, ReasonNoAudience, ReasonStoreError} {
		err := error(&RefusalError{Reason: r})
		if !errors.Is(err, ErrRefused) || RefusalReason(err) != string(r) {
			t.Errorf("%s: Is/RefusalReason broken", r)
		}
		if err.Error() != "workload: refused: "+string(r) {
			t.Errorf("%s: text %q", r, err.Error())
		}
	}
	if RefusalReason(ErrRefused) != "" || RefusalReason(nil) != "" {
		t.Error("RefusalReason of a non-refusal must be empty")
	}
	want := map[reason]Reason{reasonUnknown: ReasonUnknown, reasonBadInput: ReasonUnknown, reasonWrongKind: ReasonUnknown,
		reasonWorkloadDisabled: ReasonDisabled, reasonPrincipalOff: ReasonDisabled, reasonBindingDisabled: ReasonDisabled,
		reasonUIDMismatch: ReasonUIDMismatch, reasonNoAudience: ReasonNoAudience}
	for in, out := range want {
		if publicReason(in) != out {
			t.Errorf("publicReason(%s) = %s, want %s", in, publicReason(in), out)
		}
	}
}
