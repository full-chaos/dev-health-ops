package goapiproof

import (
	"strings"
	"testing"
)

// AllUnroutedDisagreements is the whole-picture preflight: every cell of
// {registry, catalog, rows-at-running-digest} that disagrees is reported, for
// operations that are NOT about to be seeded too.
func TestAllUnroutedDisagreementsReportsEveryCell(t *testing.T) {
	const a, b = "aaaa", "bbbb"
	registry := map[string]string{"home": a, "recs": a}
	for name, tc := range map[string]struct {
		registry, catalog map[string]string
		rows              []RunningRow
		want              []string // substrings, one per expected line
	}{
		"all agree":                                       {registry, map[string]string{"home": a, "recs": a}, []RunningRow{{"home", a}}, nil},
		"nothing routed, catalog agrees":                  {registry, map[string]string{"home": a, "recs": a}, nil, nil},
		"routed op: catalog disagrees":                    {registry, map[string]string{"home": b, "recs": a}, []RunningRow{{"home", a}}, []string{"home: catalog=bbbb registry=aaaa"}},
		"unrouted op: catalog disagrees":                  {registry, map[string]string{"home": a, "recs": b}, nil, []string{"recs: catalog=bbbb registry=aaaa"}},
		"registered op outside catalog":                   {registry, map[string]string{"home": a}, nil, []string{"recs: registered but outside the catalog"}},
		"row for an unregistered op":                      {registry, map[string]string{"home": a, "recs": a}, []RunningRow{{"retired", a}}, []string{"retired: routing row at the running schema digest but the registry does not register it"}},
		"row under another document":                      {registry, map[string]string{"home": a, "recs": a}, []RunningRow{{"home", b}}, []string{"home: routing row document=bbbb registry=aaaa"}},
		"two rows, one matching, one not (not collapsed)": {registry, map[string]string{"home": a, "recs": a}, []RunningRow{{"home", a}, {"home", b}}, []string{"home: routing row document=bbbb registry=aaaa"}},
		"two rows, the mismatching one first":             {registry, map[string]string{"home": a, "recs": a}, []RunningRow{{"home", b}, {"home", a}}, []string{"home: routing row document=bbbb registry=aaaa"}},
		"several at once":                                 {registry, map[string]string{"home": b}, []RunningRow{{"x", a}}, []string{"home: catalog", "recs: registered but outside", "x: routing row"}},
	} {
		t.Run(name, func(t *testing.T) {
			got := AllUnroutedDisagreements(tc.registry, tc.catalog, tc.rows)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %d line(s) %v", got, len(tc.want), tc.want)
			}
			for i, want := range tc.want {
				if !strings.Contains(got[i], want) {
					t.Fatalf("line %d = %q, want it to contain %q (all: %v)", i, got[i], want, got)
				}
			}
		})
	}
}
