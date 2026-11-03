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
	a, b := tok("a"), tok("b")
	a.Revoked, b.ExpiresAt = true, &past
	pa, pb := proj("a"), proj("b")
	pa.Status, pb.Status = pushlifecycle.StatusExpired, pushlifecycle.StatusRevoked
	pb.ExpiresAt = &past
	// b: domain not revoked and past expiry, projection revoked: the domain reads live, so that is drift.
	want(t, run(t, []Token{a, b}, []Projected{pa, pb}), Finding{ClassStatus, "b"})
	// a: domain revoked, projection expired: both dead, no drift.
	want(t, run(t, []Token{a}, []Projected{pa}))
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
	pa.Scopes, pa.Organization = []string{"x"}, orgB
	pb.Organization = orgB
	got := run(t, []Token{a, b}, []Projected{pa, pb})
	want(t, got, Finding{ClassBinding, "a"}, Finding{ClassBinding, "b"}, Finding{ClassScopes, "b"})
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
