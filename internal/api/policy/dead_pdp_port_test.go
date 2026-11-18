package policy

import (
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// A later test's allow PDP can bind the address a dead PDP left behind; the "no verdict" decision must stay error_deny.
func deadPDPAddressStaysDead(t *testing.T) {
	rows := orglessOrgRows(t)
	actionRows := productionGoAPIRows(t)
	var admin orglessOrgRow
	for _, row := range rows {
		if row.level == Admin && admin.pattern == "" {
			admin = row
		}
	}
	dead := deadPDP(t)
	next := orglessPDP(t, true)
	ln, err := net.Listen("tcp", strings.TrimPrefix(dead.srv.URL, "http://"))
	if err == nil {
		srv := &http.Server{Handler: next.srv.Config.Handler}
		go func() { _ = srv.Serve(ln) }()
		t.Cleanup(func() { _ = srv.Close() })
	}
	rec := recordEnforce(t)
	on := orglessMux(t, orglessStore(orglessCallers[0]), orglessAccessClaims(), seamEnforcer(t, dead.client(t), actionRows, orglessOrgNamespaces...), rows, false, orglessEcho)
	orglessServe(on, admin.pattern, orglessTargets(admin)[0])
	if err == nil {
		t.Errorf("dead PDP address %s was free to rebind", dead.srv.URL)
	}
	if hits := dead.deadHits.Load(); hits == 0 {
		t.Errorf("request never reached the held dead PDP listener")
	}
	want := []string{admin.row.Namespace + "/error_deny"}
	if got := rec.snapshot(); !slices.Equal(got, want) {
		t.Errorf("decisions %v, want %v", got, want)
	}
}
