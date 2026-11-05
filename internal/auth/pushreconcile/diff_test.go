package pushreconcile

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
)

var (
	now    = time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	future = now.Add(48 * time.Hour)
	past   = now.Add(-48 * time.Hour)
	orgA   = "aaaaaaaa-0000-4000-8000-00000000000a"
	orgB   = "bbbbbbbb-0000-4000-8000-00000000000b"
	srcA   = "11111111-0000-4000-8000-000000000001"
	srcB   = "22222222-0000-4000-8000-000000000002"
)

func tok(id string) Token {
	return Token{ID: id, Organization: orgA, SourceID: srcA, Scopes: []string{"ingest:write", "schema:read"}, ExpiresAt: &future}
}

func proj(id string) Projected {
	return Projected{ID: id, Organization: orgA, SourceID: srcA, Status: pushlifecycle.StatusActive,
		Scopes: []string{"ingest:write", "schema:read"}, ExpiresAt: &future}
}

func run(t *testing.T, d []Token, p []Projected) []Finding {
	t.Helper()
	got, err := Diff(d, p, now)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	return got
}

func want(t *testing.T, got []Finding, w ...Finding) {
	t.Helper()
	if len(got) == 0 && len(w) == 0 {
		return
	}
	if !slices.Equal(got, w) {
		t.Fatalf("findings = %v, want %v", got, w)
	}
}

func TestC01CleanStoreZeroDiff(t *testing.T) {
	var d []Token
	var p []Projected
	for _, id := range []string{"t1", "t2", "t3", "t4", "t5", "t6"} {
		d = append(d, tok(id))
		p = append(p, proj(id))
	}
	d[1].SourceID, p[1].SourceID = "", "" // organization-wide
	d[2].Revoked, p[2].Status = true, pushlifecycle.StatusRevoked
	d[3].ExpiresAt, p[3].ExpiresAt = &past, &past
	p[3].Status = pushlifecycle.StatusExpired
	d[4].Scopes, p[4].Scopes = []string{"ingest:write"}, []string{"ingest:write"}
	d[5].ExpiresAt, p[5].ExpiresAt = nil, nil
	want(t, run(t, d, p))
}

func TestC02MissingToken(t *testing.T) {
	want(t, run(t, []Token{tok("t1")}, nil), Finding{ClassMissing, "t1"})
}

func TestC02bLegacyNeverMutatedTokenIsMissing(t *testing.T) {
	legacy := tok("legacy")
	legacy.ExpiresAt = nil
	want(t, run(t, []Token{tok("t1"), legacy}, []Projected{proj("t1")}), Finding{ClassMissing, "legacy"})
}

func TestC03ExtraActiveRow(t *testing.T) {
	want(t, run(t, nil, []Projected{proj("x1")}), Finding{ClassExtra, "x1"})
}

func TestC03bTerminalRowWithNoTokenIsNotExtra(t *testing.T) {
	a, b := proj("x1"), proj("x2")
	a.Status, b.Status = pushlifecycle.StatusRevoked, pushlifecycle.StatusExpired
	want(t, run(t, nil, []Projected{a, b}))
}

func TestC04StatusDomainRevokedProjectedActive(t *testing.T) {
	d := tok("t1")
	d.Revoked = true
	want(t, run(t, []Token{d}, []Projected{proj("t1")}), Finding{ClassStatus, "t1"})
}

func TestC04bStatusProjectedTerminalDomainLive(t *testing.T) {
	r, e := proj("r"), proj("e")
	r.Status, e.Status = pushlifecycle.StatusRevoked, pushlifecycle.StatusExpired
	want(t, run(t, []Token{tok("r"), tok("e")}, []Projected{r, e}), Finding{ClassStatus, "e"}, Finding{ClassStatus, "r"})
}

func TestC05ScopesWiderInProjection(t *testing.T) {
	d := tok("t1")
	d.Scopes = []string{"ingest:write"}
	want(t, run(t, []Token{d}, []Projected{proj("t1")}), Finding{ClassScopes, "t1"})
}

func TestC05bScopesNarrowerInProjection(t *testing.T) {
	p := proj("t1")
	p.Scopes = []string{"ingest:write"}
	want(t, run(t, []Token{tok("t1")}, []Projected{p}), Finding{ClassScopes, "t1"})
}

func TestC05cScopesSameSetOtherOrderIsNotDrift(t *testing.T) {
	d := tok("t1")
	d.Scopes = []string{"schema:read", "ingest:write", "ingest:write"}
	want(t, run(t, []Token{d}, []Projected{proj("t1")}))
}

func TestC06ExpiryDiffers(t *testing.T) {
	p := proj("t1")
	later := future.Add(time.Hour)
	p.ExpiresAt = &later
	want(t, run(t, []Token{tok("t1")}, []Projected{p}), Finding{ClassExpiry, "t1"})
}

func TestC06bExpirySameInstantOtherPrecisionIsNotDrift(t *testing.T) {
	d := tok("t1")
	exact := future.Add(123456 * time.Nanosecond)
	d.ExpiresAt = &exact // nanoseconds in the domain
	p := proj("t1")
	micro := future.Add(123456 * time.Nanosecond).Truncate(time.Microsecond)
	p.ExpiresAt = &micro
	want(t, run(t, []Token{d}, []Projected{p}))
}

func TestC06cExpiryNilVersusSetDrifts(t *testing.T) {
	d := tok("t1")
	d.ExpiresAt = nil
	want(t, run(t, []Token{d}, []Projected{proj("t1")}), Finding{ClassExpiry, "t1"})
}

func TestC07BindingOrganizationDiffers(t *testing.T) {
	p := proj("t1")
	p.Organization = orgB
	want(t, run(t, []Token{tok("t1")}, []Projected{p}), Finding{ClassBinding, "t1"})
}

func TestC07bBindingSourceDiffers(t *testing.T) {
	p := proj("t1")
	p.SourceID = srcB
	want(t, run(t, []Token{tok("t1")}, []Projected{p}), Finding{ClassBinding, "t1"})
}

func TestC07cOrganizationWideEqualsEmptySource(t *testing.T) {
	d, p := tok("t1"), proj("t1")
	d.SourceID, p.SourceID = "", ""
	want(t, run(t, []Token{d}, []Projected{p}))
	p.SourceID = srcA
	want(t, run(t, []Token{d}, []Projected{p}), Finding{ClassBinding, "t1"})
}

func TestC08UnwireableOrganization(t *testing.T) {
	d := tok("t1")
	d.Unwireable = true
	want(t, run(t, []Token{d}, []Projected{proj("t1")}), Finding{ClassUnwireable, "t1"})
	want(t, run(t, []Token{d}, nil), Finding{ClassUnwireable, "t1"})
}

func TestC09DuplicateIdIsAnErrorInEitherInput(t *testing.T) {
	if _, err := Diff([]Token{tok("t1"), tok("t1")}, nil, now); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("domain duplicate: err = %v", err)
	}
	if _, err := Diff(nil, []Projected{proj("t1"), proj("t1")}, now); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("projection duplicate: err = %v", err)
	}
}

func TestC09bUnknownStatusIsAnErrorNotNoDrift(t *testing.T) {
	for _, status := range []pushlifecycle.Status{"", "paused", pushlifecycle.StatusUnknown} {
		p := proj("t1")
		p.Status = status
		if _, err := Diff([]Token{tok("t1")}, []Projected{p}, now); !errors.Is(err, ErrUnknownStatus) {
			t.Fatalf("status %q: err = %v", status, err)
		}
	}
}

// Terminal over a gap (CHAOS-12699): the domain revoked at revision 5 and the projection
// jumped to revision 5. The skipped revisions are Stale. Revisions are not compared.
func TestC10NotDriftTerminalOverGap(t *testing.T) {
	d, p := tok("t1"), proj("t1")
	d.Revoked, p.Status = true, pushlifecycle.StatusRevoked
	want(t, run(t, []Token{d}, []Projected{p}))
}

// A refused or decided revision leaves the state equal; the domain's aggregate revision is ahead
// of the projection's. The diff has no revision input, so the difference cannot show.
func TestC11NotDriftRefusedAndDecidedRevisions(t *testing.T) {
	d, p := tok("t1"), proj("t1")
	d.Revoked, p.Status = true, pushlifecycle.StatusRevoked
	want(t, run(t, []Token{d}, []Projected{p}))
	want(t, run(t, []Token{tok("t2")}, []Projected{proj("t2")}))
}

func TestC12NotDriftExpiredProjectionPastExpiry(t *testing.T) {
	d, p := tok("t1"), proj("t1")
	d.ExpiresAt, p.ExpiresAt = &past, &past
	p.Status = pushlifecycle.StatusExpired
	want(t, run(t, []Token{d}, []Projected{p}))
}

func TestC12bNotDriftActivePastExpiryNoEvent(t *testing.T) {
	d, p := tok("t1"), proj("t1")
	d.ExpiresAt, p.ExpiresAt = &past, &past
	want(t, run(t, []Token{d}, []Projected{p}))
}

func TestC12cNotDriftBothTerminalDifferentKind(t *testing.T) {
	a := tok("a")
	a.Revoked = true
	pa := proj("a")
	pa.Status = pushlifecycle.StatusExpired
	want(t, run(t, []Token{a}, []Projected{pa}))
	// Domain past expiry (dead), projection revoked (dead): both non-live, no status drift.
	b, pb := tok("b"), proj("b")
	b.ExpiresAt, pb.ExpiresAt = &past, &past
	pb.Status = pushlifecycle.StatusRevoked
	want(t, run(t, []Token{b}, []Projected{pb}))
}

func TestC13ConfirmKeepsOnlyTheSameFindingInBothPasses(t *testing.T) {
	first := []Finding{{ClassMissing, "a"}, {ClassStatus, "b"}, {ClassScopes, "c"}}
	second := []Finding{{ClassMissing, "a"}, {ClassExpiry, "b"}, {ClassScopes, "d"}}
	want(t, Confirm(first, second), Finding{ClassMissing, "a"})
	want(t, Confirm(first, nil))
	want(t, Confirm(nil, second))
}

func TestC14FindingsSortedByIdThenClass(t *testing.T) {
	a, b := tok("b"), tok("a")
	pa, pb := proj("b"), proj("a")
	pa.Scopes, pa.Organization = []string{"x"}, orgB // scopes and binding: one finding, the higher class
	pb.Organization = orgB
	got := run(t, []Token{a, b}, []Projected{pa, pb})
	want(t, got, Finding{ClassBinding, "a"}, Finding{ClassScopes, "b"})
}

func TestC14bClassesAreTheClosedSetOfSevenLabels(t *testing.T) {
	if len(Classes) != 7 {
		t.Fatalf("classes = %v", Classes)
	}
	seen := map[Class]bool{}
	for _, c := range Classes {
		if seen[c] || c == "" {
			t.Fatalf("bad class list %v", Classes)
		}
		seen[c] = true
	}
}

// The effective-liveness matrix (r1 P1 1): revoked_at unset/set x expiry nil/future/past x projected
// active/revoked/expired, every other field equal. Status drifts exactly when the two sides'
// liveness at now differs; each side's liveness is its revocation or status AND its expiry.
func TestC16StatusComparesEffectiveLivenessMatrix(t *testing.T) {
	type expiry struct {
		name string
		at   *time.Time
	}
	expiries := []expiry{{"no_expiry", nil}, {"future_expiry", &future}, {"past_expiry", &past}}
	statuses := []pushlifecycle.Status{pushlifecycle.StatusActive, pushlifecycle.StatusRevoked, pushlifecycle.StatusExpired}
	// wantStatus[revoked][expiry index][status index]
	wantStatus := map[bool][3][3]bool{
		false: {{false, true, true}, {false, true, true}, {false, false, false}},
		true:  {{true, false, false}, {true, false, false}, {false, false, false}},
	}
	for _, revoked := range []bool{false, true} {
		for ei, e := range expiries {
			for si, st := range statuses {
				d, p := tok("c"), proj("c")
				d.Revoked, d.ExpiresAt, p.ExpiresAt, p.Status = revoked, e.at, e.at, st
				got := run(t, []Token{d}, []Projected{p})
				if wantStatus[revoked][ei][si] {
					want(t, got, Finding{ClassStatus, "c"})
				} else if len(got) != 0 {
					t.Errorf("revoked=%v %s/%s: findings = %v, want none", revoked, e.name, st, got)
				}
				if wantStatus[revoked][ei][si] && len(got) != 1 {
					t.Errorf("revoked=%v %s/%s: %v", revoked, e.name, st, got)
				}
			}
		}
	}
}

// One class per credential (r1 P1 2): the 15 non-empty combinations of binding, status, scopes and
// expiry differences each yield exactly one finding, the highest-precedence class:
// status > scopes > binding > expiry.
func TestC17OneClassPerCredentialByPrecedence(t *testing.T) {
	later := future.Add(time.Hour)
	for mask := 1; mask < 16; mask++ {
		d, p := tok("c"), proj("c")
		var wantClass Class
		if mask&8 != 0 { // expiry (both sides stay live)
			p.ExpiresAt, wantClass = &later, ClassExpiry
		}
		if mask&1 != 0 { // binding
			p.Organization, wantClass = orgB, ClassBinding
		}
		if mask&4 != 0 { // scopes
			p.Scopes, wantClass = []string{"ingest:write"}, ClassScopes
		}
		if mask&2 != 0 { // status
			p.Status, wantClass = pushlifecycle.StatusRevoked, ClassStatus
		}
		got := run(t, []Token{d}, []Projected{p})
		if len(got) != 1 || got[0].Class != wantClass || got[0].ID != "c" {
			t.Errorf("mask %04b: findings = %v, want exactly one %s", mask, got, wantClass)
		}
	}
}

func TestC17bMissingExtraAndUnwireableAreExclusive(t *testing.T) {
	d := tok("u")
	d.Unwireable, d.Revoked = true, true
	p := proj("u")
	p.Scopes, p.Organization = nil, orgB
	want(t, run(t, []Token{d}, []Projected{p}), Finding{ClassUnwireable, "u"})
	x := proj("x")
	x.Scopes, x.Organization = nil, orgB
	want(t, run(t, nil, []Projected{x}), Finding{ClassExtra, "x"})
}
