package identity

import "testing"

func callSiteGaps(path string, src string) []string { return nil }

func TestCallSiteGapsFindsALookupWithNoLaterServiceProviderRead(t *testing.T) {
	const head = "package p\nfunc f(s S) (Q, error) {\n"
	cases := []struct {
		name string
		src  string
		gaps int
	}{
		{"read after call", head + "p, _, _ := s.IdentityBySubject(1)\nreturn Q{ServiceProvider: p.ServiceProvider}, nil\n}\n", 0},
		{"selector read after call", head + "p, _, _ := s.IdentityBySubject(1)\nif p.ServiceProvider {\nreturn Q{}, nil\n}\nreturn Q{}, nil\n}\n", 0},
		{"no read at all", head + "p, _, _ := s.IdentityBySubject(1)\nreturn Q{ID: p.ID}, nil\n}\n", 1},
		{"read only before the call", head + "x := T{ServiceProvider: true}\n_ = x\np, _, _ := s.IdentityBySubject(1)\nreturn Q{ID: p.ID}, nil\n}\n", 1},
		{"second lookup in same file without read", head + "p, _, _ := s.IdentityBySubject(1)\nreturn Q{ServiceProvider: p.ServiceProvider}, nil\n}\nfunc g(s S) Q {\np, _, _ := s.IdentityBySubject(2)\nreturn Q{ID: p.ID}\n}\n", 1},
		{"two lookups one read after the first only", head + "p, _, _ := s.IdentityBySubject(1)\nq := Q{ServiceProvider: p.ServiceProvider}\nr, _, _ := s.IdentityBySubject(2)\nreturn Q{ID: r.ID}, nil\n}\n", 1},
		{"comment and string do not count", head + "p, _, _ := s.IdentityBySubject(1)\n// ServiceProvider\n_ = \"ServiceProvider\"\nreturn Q{ID: p.ID}, nil\n}\n", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := callSiteGaps("x.go", tc.src); len(got) != tc.gaps {
				t.Fatalf("gaps = %v, want %d", got, tc.gaps)
			}
		})
	}
}
