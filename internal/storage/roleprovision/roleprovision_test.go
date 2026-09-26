package roleprovision

import (
	"errors"
	"strings"
	"testing"
)

func valid() Options {
	return Options{
		Domain:      Role{"d", "pw-d"},
		Queue:       Role{"q", "pw-q"},
		Coordinator: Role{"c", "pw-c"},
	}
}

func TestValidateAcceptsTheMandatoryRolesAndEveryOptionalOne(t *testing.T) {
	t.Parallel()
	if err := valid().Validate(); err != nil {
		t.Fatal(err)
	}
	all := valid()
	all.API, all.QueryAPI, all.Keda = Role{"a", "pw-a"}, Role{"qa", "pw-qa"}, Role{"k", "pw-k"}
	all.RiverSchema = "river"
	if err := all.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRefusesWhatWouldCreateAWrongOrAmbiguousRoleSet(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x", MaxIdentifierBytes+1)
	for name, mutate := range map[string]func(*Options){
		"an unset mandatory role":                            func(o *Options) { o.Queue.Name = "" },
		"an unset mandatory password":                        func(o *Options) { o.Coordinator.Password = "" },
		"domain equals queue":                                func(o *Options) { o.Queue.Name = "d" },
		"domain equals coordinator":                          func(o *Options) { o.Coordinator.Name = "d" },
		"queue equals coordinator":                           func(o *Options) { o.Coordinator.Name = "q" },
		"api equals a mandatory role":                        func(o *Options) { o.API = Role{"d", "pw"} },
		"query-api equals a mandatory role":                  func(o *Options) { o.QueryAPI = Role{"c", "pw"} },
		"query-api equals api":                               func(o *Options) { o.API, o.QueryAPI = Role{"x", "pw"}, Role{"x", "pw"} },
		"keda equals a mandatory role":                       func(o *Options) { o.Keda = Role{"d", "pw"} },
		"keda equals api":                                    func(o *Options) { o.API, o.Keda = Role{"x", "pw"}, Role{"x", "pw"} },
		"keda equals query-api":                              func(o *Options) { o.QueryAPI, o.Keda = Role{"x", "pw"}, Role{"x", "pw"} },
		"an optional role without a password":                func(o *Options) { o.API = Role{"a", ""} },
		"a name past the identifier limit":                   func(o *Options) { o.Domain.Name = long },
		"a NUL in a role name":                               func(o *Options) { o.Domain.Name = "d\x00x" },
		"a NUL in a password":                                func(o *Options) { o.Queue.Password = "p\x00w" },
		"an upper-case runtime role (river would refuse it)": func(o *Options) { o.Domain.Name = "Domain" },
		"a runtime role with a dot":                          func(o *Options) { o.Queue.Name = "q.x" },
		"a runtime role with a space":                        func(o *Options) { o.Coordinator.Name = " c" },
		"an api role with a quote":                           func(o *Options) { o.API = Role{`a"b`, "pw"} },
		"a query-api role starting with a digit":             func(o *Options) { o.QueryAPI = Role{"9qa", "pw"} },
		"a KEDA role with an unusable River schema":          func(o *Options) { o.Keda, o.RiverSchema = Role{"k", "pw"}, strings.Repeat("s", MaxIdentifierBytes+1) },
	} {
		options := valid()
		mutate(&options)
		err := options.Validate()
		if !errors.Is(err, ErrInvalidOptions) {
			t.Errorf("%s: err = %v, want ErrInvalidOptions", name, err)
			continue
		}
		for _, secret := range []string{"pw-d", "pw-q", "pw-c", "pw-a", "pw-qa", "pw-k", "pw"} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("%s: the error names a password: %v", name, err)
			}
		}
	}
}

func TestValidateLeavesTheKedaRoleNameFreeFormButNotTheSchema(t *testing.T) {
	t.Parallel()
	options := valid()
	options.Keda = Role{` Odd "Keda".Name É `, "pw-k"}
	options.RiverSchema = "river"
	if err := options.Validate(); err != nil {
		t.Fatalf("river never reads the KEDA role, so its name is free-form: %v", err)
	}
	options.RiverSchema = "Bad Schema"
	if err := options.Validate(); err == nil {
		t.Fatal("the River schema must satisfy river's own identifier rule")
	}
}

func TestOptionalRolesAreConfiguredOnlyWhenNamed(t *testing.T) {
	t.Parallel()
	options := valid()
	if got := len(options.configured()); got != 3 {
		t.Fatalf("configured roles = %d, want 3", got)
	}
	options.QueryAPI = Role{"qa", "pw"}
	if got := len(options.configured()); got != 4 {
		t.Fatalf("configured roles = %d, want 4", got)
	}
	if options.schema() != "river" {
		t.Fatalf("the default River schema must be river, got %q", options.schema())
	}
}
