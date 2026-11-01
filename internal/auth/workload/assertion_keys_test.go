package workload

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
)

var t0 = time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)

func goodKey() AssertionKeySpec {
	return AssertionKeySpec{Issuer: "https://issuer.example", KID: "key-2026-10", Status: AssertionKeyActive, RotatedAt: t0}
}

// credentialProbe is a JWT-shaped value built at run time by the shared scanner, so no credential-shaped literal sits in the tree.
func credentialProbe(t *testing.T) string {
	t.Helper()
	scanner, err := credshape.Default()
	if err != nil {
		t.Fatal("credshape unavailable")
	}
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal("credshape probes unavailable")
	}
	return probes[0]
}

func TestValidateAssertionKey(t *testing.T) {
	if err := ValidateAssertionKey(goodKey()); err != nil {
		t.Fatalf("a valid active key: %v", err)
	}
	accept := map[string]func(*AssertionKeySpec){
		"loopback http issuer":    func(s *AssertionKeySpec) { s.Issuer = "http://localhost:3000" },
		"loopback ipv4 issuer":    func(s *AssertionKeySpec) { s.Issuer = "http://127.0.0.1" },
		"loopback ipv6 issuer":    func(s *AssertionKeySpec) { s.Issuer = "http://[::1]:8080" },
		"issuer with a path":      func(s *AssertionKeySpec) { s.Issuer = "https://issuer.example/web" },
		"kid of 256 bytes":        func(s *AssertionKeySpec) { s.KID = strings.Repeat("k", 256) },
		"kid of one byte":         func(s *AssertionKeySpec) { s.KID = "k" },
		"kid with punctuation":    func(s *AssertionKeySpec) { s.KID = "web.2026-10_a:b/c" },
		"43 character thumbprint": func(s *AssertionKeySpec) { s.KID = "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs" },
		"retiring with a date": func(s *AssertionKeySpec) {
			s.Status = AssertionKeyRetiring
			s.RetireAfter = t0.Add(time.Hour)
		},
		"retired": func(s *AssertionKeySpec) { s.Status = AssertionKeyRetired },
		"retire-previous at the ceiling": func(s *AssertionKeySpec) {
			s.RetirePreviousAfter = AssertionOverlapCeiling
		},
		"retire-previous of one nanosecond": func(s *AssertionKeySpec) { s.RetirePreviousAfter = time.Nanosecond },
	}
	for name, mutate := range accept {
		spec := goodKey()
		mutate(&spec)
		if err := ValidateAssertionKey(spec); err != nil {
			t.Errorf("%s refused: %v", name, err)
		}
	}
	refuse := map[string]func(*AssertionKeySpec){
		"empty issuer":                    func(s *AssertionKeySpec) { s.Issuer = "" },
		"plain http issuer":               func(s *AssertionKeySpec) { s.Issuer = "http://issuer.example" },
		"ftp issuer":                      func(s *AssertionKeySpec) { s.Issuer = "ftp://issuer.example" },
		"issuer with userinfo":            func(s *AssertionKeySpec) { s.Issuer = "https://user:secret@issuer.example" },
		"issuer with a user only":         func(s *AssertionKeySpec) { s.Issuer = "https://user@issuer.example" },
		"issuer with a query":             func(s *AssertionKeySpec) { s.Issuer = "https://issuer.example?x=1" },
		"issuer with an empty query":      func(s *AssertionKeySpec) { s.Issuer = "https://issuer.example?" },
		"issuer with a fragment":          func(s *AssertionKeySpec) { s.Issuer = "https://issuer.example#x" },
		"issuer without a host":           func(s *AssertionKeySpec) { s.Issuer = "https://" },
		"issuer with a space":             func(s *AssertionKeySpec) { s.Issuer = "https://issuer.example/a b" },
		"issuer with a newline":           func(s *AssertionKeySpec) { s.Issuer = "https://issuer.example\n" },
		"issuer of 513 bytes":             func(s *AssertionKeySpec) { s.Issuer = "https://issuer.example/" + strings.Repeat("a", 513-23) },
		"issuer with a non-ASCII byte":    func(s *AssertionKeySpec) { s.Issuer = "https://issuer.example/é" },
		"empty kid":                       func(s *AssertionKeySpec) { s.KID = "" },
		"kid of 257 bytes":                func(s *AssertionKeySpec) { s.KID = strings.Repeat("k", 257) },
		"kid with a space":                func(s *AssertionKeySpec) { s.KID = "key 1" },
		"kid with a leading space":        func(s *AssertionKeySpec) { s.KID = " key" },
		"kid with a newline":              func(s *AssertionKeySpec) { s.KID = "key\n" },
		"kid with a NUL":                  func(s *AssertionKeySpec) { s.KID = "key\x00" },
		"kid with a non-ASCII byte":       func(s *AssertionKeySpec) { s.KID = "clé" },
		"kid with DEL":                    func(s *AssertionKeySpec) { s.KID = "key\x7f" },
		"kid shaped like a provider tok":  func(s *AssertionKeySpec) { s.KID = "ghp_" + strings.Repeat("a", 36) },
		"issuer shaped like a credential": func(s *AssertionKeySpec) { s.Issuer = "https://issuer.example/" + credentialProbe(t) },
		"unknown status":                  func(s *AssertionKeySpec) { s.Status = "paused" },
		"empty status":                    func(s *AssertionKeySpec) { s.Status = "" },
		"zero rotated_at":                 func(s *AssertionKeySpec) { s.RotatedAt = time.Time{} },
		"active with retire_after":        func(s *AssertionKeySpec) { s.RetireAfter = t0.Add(time.Hour) },
		"retiring without a date":         func(s *AssertionKeySpec) { s.Status = AssertionKeyRetiring },
		"retiring at rotated_at":          func(s *AssertionKeySpec) { s.Status = AssertionKeyRetiring; s.RetireAfter = t0 },
		"retiring before rotated_at": func(s *AssertionKeySpec) {
			s.Status = AssertionKeyRetiring
			s.RetireAfter = t0.Add(-time.Second)
		},
		"retired with retire_after": func(s *AssertionKeySpec) { s.Status = AssertionKeyRetired; s.RetireAfter = t0.Add(time.Hour) },
		"negative retire-previous":  func(s *AssertionKeySpec) { s.RetirePreviousAfter = -time.Nanosecond },
		"retire-previous over the ceiling": func(s *AssertionKeySpec) {
			s.RetirePreviousAfter = AssertionOverlapCeiling + time.Nanosecond
		},
		"retire-previous on a retiring key": func(s *AssertionKeySpec) {
			s.Status = AssertionKeyRetiring
			s.RetireAfter = t0.Add(time.Hour)
			s.RetirePreviousAfter = time.Hour
		},
		"retire-previous on a retired key": func(s *AssertionKeySpec) {
			s.Status = AssertionKeyRetired
			s.RetirePreviousAfter = time.Hour
		},
	}
	for name, mutate := range refuse {
		spec := goodKey()
		mutate(&spec)
		err := ValidateAssertionKey(spec)
		if !errors.Is(err, ErrAssertionKeyInvalid) {
			t.Errorf("%s: err = %v, want ErrAssertionKeyInvalid", name, err)
			continue
		}
		for _, value := range []string{spec.Issuer, spec.KID} {
			if len(value) > 8 && strings.Contains(err.Error(), value) {
				t.Errorf("%s: the error text carries the input", name)
			}
		}
	}
}

func row(status AssertionKeyStatus, rotated time.Time, retire time.Time) *assertionRow {
	return &assertionRow{Status: status, RotatedAt: rotated, RetireAfter: retire}
}

func TestPlanAssertionKey(t *testing.T) {
	later := t0.Add(48 * time.Hour)
	active := row(AssertionKeyActive, t0, time.Time{})
	next := func(mutate func(*AssertionKeySpec)) AssertionKeySpec {
		spec := goodKey()
		spec.KID = "key-next"
		spec.RotatedAt = later
		mutate(&spec)
		return spec
	}
	type want struct {
		action planAction
		err    error
		demote time.Time
	}
	cases := []struct {
		name     string
		spec     AssertionKeySpec
		existing *assertionRow
		active   *assertionRow
		want     want
	}{
		{"first key", goodKey(), nil, nil, want{action: planInsert}},
		{"first key with nothing to retire named", next(func(s *AssertionKeySpec) { s.RetirePreviousAfter = time.Hour }), nil, nil,
			want{err: ErrAssertionKeyInvalid}},
		{"second active without retire-previous", next(func(*AssertionKeySpec) {}), nil, active, want{err: ErrAssertionKeyActiveExists}},
		{"second active with retire-previous", next(func(s *AssertionKeySpec) { s.RetirePreviousAfter = 24 * time.Hour }), nil, active,
			want{action: planInsert, demote: later.Add(24 * time.Hour)}},
		{"second active, a short overlap", next(func(s *AssertionKeySpec) { s.RetirePreviousAfter = time.Minute }), nil, active,
			want{action: planInsert, demote: later.Add(time.Minute)}},
		{"second active rotated before the active one", next(func(s *AssertionKeySpec) {
			s.RotatedAt = t0.Add(-time.Hour)
			s.RetirePreviousAfter = time.Hour
		}), nil, active, want{err: ErrAssertionKeyTransition}},
		{"second active rotated at the active one", next(func(s *AssertionKeySpec) {
			s.RotatedAt = t0
			s.RetirePreviousAfter = time.Hour
		}), nil, active, want{err: ErrAssertionKeyTransition}},
		{"retiring insert without an active key", next(func(s *AssertionKeySpec) {
			s.Status = AssertionKeyRetiring
			s.RotatedAt = t0.Add(-time.Hour)
			s.RetireAfter = t0.Add(time.Hour)
		}), nil, nil, want{err: ErrAssertionKeyNoActive}},
		{"retiring insert at the bound", next(func(s *AssertionKeySpec) {
			s.Status = AssertionKeyRetiring
			s.RotatedAt = t0.Add(-time.Hour)
			s.RetireAfter = t0.Add(24 * time.Hour)
		}), nil, active, want{action: planInsert}},
		{"retiring insert one nanosecond past the bound", next(func(s *AssertionKeySpec) {
			s.Status = AssertionKeyRetiring
			s.RotatedAt = t0.Add(-time.Hour)
			s.RetireAfter = t0.Add(24*time.Hour + time.Nanosecond)
		}), nil, active, want{err: ErrAssertionKeyOverlap}},
		{"retired insert", next(func(s *AssertionKeySpec) { s.Status = AssertionKeyRetired }), nil, nil, want{err: ErrAssertionKeyInvalid}},

		{"repeat of an active key", goodKey(), row(AssertionKeyActive, t0, time.Time{}), nil, want{action: planNoop}},
		{"repeat of a retiring key", func() AssertionKeySpec {
			s := goodKey()
			s.Status = AssertionKeyRetiring
			s.RetireAfter = t0.Add(time.Hour)
			return s
		}(), row(AssertionKeyRetiring, t0, t0.Add(time.Hour)), active, want{action: planNoop}},
		{"repeat of a retired key", func() AssertionKeySpec { s := goodKey(); s.Status = AssertionKeyRetired; return s }(),
			row(AssertionKeyRetired, t0, time.Time{}), nil, want{action: planNoop}},
		{"changed rotated_at", func() AssertionKeySpec { s := goodKey(); s.RotatedAt = later; return s }(),
			row(AssertionKeyActive, t0, time.Time{}), nil, want{err: ErrAssertionKeyTransition}},
		{"active to retired", func() AssertionKeySpec { s := goodKey(); s.Status = AssertionKeyRetired; return s }(),
			row(AssertionKeyActive, t0, time.Time{}), nil, want{action: planUpdate}},
		{"active to retiring alone", func() AssertionKeySpec {
			s := goodKey()
			s.Status = AssertionKeyRetiring
			s.RetireAfter = t0.Add(time.Hour)
			return s
		}(), row(AssertionKeyActive, t0, time.Time{}), nil, want{err: ErrAssertionKeyNoActive}},
		{"retiring to retired", func() AssertionKeySpec { s := goodKey(); s.Status = AssertionKeyRetired; return s }(),
			row(AssertionKeyRetiring, t0, t0.Add(time.Hour)), active, want{action: planUpdate}},
		{"retiring brought forward", func() AssertionKeySpec {
			s := goodKey()
			s.Status = AssertionKeyRetiring
			s.RetireAfter = t0.Add(30 * time.Minute)
			return s
		}(), row(AssertionKeyRetiring, t0, t0.Add(time.Hour)), active, want{action: planUpdate}},
		{"retiring pushed past the bound", func() AssertionKeySpec {
			s := goodKey()
			s.Status = AssertionKeyRetiring
			s.RetireAfter = t0.Add(24*time.Hour + time.Nanosecond)
			return s
		}(), row(AssertionKeyRetiring, t0, t0.Add(time.Hour)), active, want{err: ErrAssertionKeyOverlap}},
		{"retiring back to active", goodKey(), row(AssertionKeyRetiring, t0, t0.Add(time.Hour)), nil, want{err: ErrAssertionKeyTransition}},
		{"retired back to active", goodKey(), row(AssertionKeyRetired, t0, time.Time{}), nil, want{err: ErrAssertionKeyTerminal}},
		{"retired back to retiring", func() AssertionKeySpec {
			s := goodKey()
			s.Status = AssertionKeyRetiring
			s.RetireAfter = t0.Add(time.Hour)
			return s
		}(), row(AssertionKeyRetired, t0, time.Time{}), active, want{err: ErrAssertionKeyTerminal}},
		{"existing key given a retire-previous", func() AssertionKeySpec { s := goodKey(); s.RetirePreviousAfter = time.Hour; return s }(),
			row(AssertionKeyActive, t0, time.Time{}), nil, want{err: ErrAssertionKeyInvalid}},
	}
	for _, c := range cases {
		got, err := planAssertionKey(c.spec, c.existing, c.active)
		if c.want.err != nil {
			if !errors.Is(err, c.want.err) {
				t.Errorf("%s: err = %v, want %v", c.name, err, c.want.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got.Action != c.want.action || !got.DemotePreviousUntil.Equal(c.want.demote) {
			t.Errorf("%s: plan = %+v, want action %v demote %v", c.name, got, c.want.action, c.want.demote)
		}
	}
}
