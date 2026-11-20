package policy

import (
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// The seam judges the org_id the billing handlers serve: both read the last
// org_id of the same parse, so a pair net/url would drop (a ";", a bad escape)
// or one decoding to U+FFFD names the same organization to both, and the
// seam's verdict is what the handler's own UUID parse does with it.
func TestServedOrgQueryReadIsTheHandlerRead(t *testing.T) {
	row := ShadowRow{OrgFromQuery: true}
	for _, tc := range []struct {
		query string
		named bool
		raw   string
	}{
		{"", false, ""},
		{"org_id=" + ownOrg, true, ownOrg},
		{"org_id=" + strangerOrg, true, strangerOrg},
		{"org_id=" + ownOrg + ";x", true, ownOrg + ";x"},
		{"org_id=" + strangerOrg + ";x", true, strangerOrg + ";x"},
		{"org_id=" + strangerOrg + "&org_id=" + ownOrg + ";", true, ownOrg + ";"},
		{"org_id=" + ownOrg + "&org_id=" + strangerOrg + ";", true, strangerOrg + ";"},
		{"org_id=" + strangerOrg + "&x=;", true, strangerOrg},
		{"org_id=" + ownOrg + "&org_id=%zz", true, "%zz"},
		{"org_id=" + strangerOrg + "%zz", true, strangerOrg + "%zz"},
		{"org_id=%34" + strangerOrg[1:], true, strangerOrg},
		{"org_id=" + strangerOrg + "%FF", true, strangerOrg + "�"},
		{"org_id=%FF&org_id=%FE", true, "�"},
		{"org_id=" + ownOrg + "&org_id=", true, ""},
		{"org_id;=" + strangerOrg, false, ""},
	} {
		r := httptest.NewRequest("GET", "/api/v1/billing/invoices?"+tc.query, nil)
		seam := servedOrgOf(r, row)
		if seam.named != tc.named || seam.raw != tc.raw {
			t.Errorf("%q: seam named=%v raw=%q, want named=%v raw=%q", tc.query, seam.named, seam.raw, tc.named, tc.raw)
		}
		handler := pybody.LastQueryValue(httpapi.Query(r), "org_id")
		if (handler != nil) != seam.named || (handler != nil && *handler != seam.raw) {
			t.Errorf("%q: handler reads %v, seam named=%v raw=%q", tc.query, handler, seam.named, seam.raw)
			continue
		}
		var errs pybody.Errors
		served, ok := errs.QueryUUID("org_id", handler)
		switch verdict := seam.against(ownOrg); {
		case !ok:
			if verdict != servedOrgUnparsed {
				t.Errorf("%q: the handler refuses the value, the seam verdict is %d", tc.query, verdict)
			}
		case served == nil || served.String() == ownOrg:
			if verdict != servedOrgJudged {
				t.Errorf("%q: the handler serves the token org, the seam verdict is %d", tc.query, verdict)
			}
		default:
			if verdict != servedOrgOther {
				t.Errorf("%q: the handler serves %s, the seam verdict is %d", tc.query, served, verdict)
			}
		}
	}
}
