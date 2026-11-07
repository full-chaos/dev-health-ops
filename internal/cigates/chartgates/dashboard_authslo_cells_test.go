package chartgates_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The cells of the auth SLO dashboard guard: each cell edits one field of the real dashboard (parsed and written
// back) and runs every dashboard check (authSLODashboardAll). A defect cell must be refused with its message; an
// equivalent-form cell (want "") must be accepted. The cells are generated from the real file, one per panel,
// query, filter clause, group-by entry, threshold and layout item, so a new panel or query gets its cells too.

// authSLOCellCopy are the real-tree inputs of the dashboard checks; authSLOCellLinks are linked read-only.
var (
	authSLOCellCopy  = []string{authSLODashboardRel, authSLOLabelsRel, sloDurationSrc, "internal/auth", "internal/audit", "internal/api/externalingest"}
	authSLOCellLinks = []string{"internal/auth", "internal/audit", "internal/api/externalingest", "internal/platform/otlpmetrics"}
)

type authSLOCell struct {
	class, name string
	edit        func(t *testing.T, doc map[string]any)
	want        string
}

func authSLOCellAt(t *testing.T, v any, p ...any) any {
	t.Helper()
	for _, s := range p {
		switch k := s.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				t.Fatalf("cell path %v: %q is not under an object, the defect did not land", p, k)
			}
			if v, ok = m[k]; !ok {
				t.Fatalf("cell path %v: no key %q, the defect did not land", p, k)
			}
		case int:
			l, ok := v.([]any)
			if !ok || k >= len(l) {
				t.Fatalf("cell path %v: no item %d, the defect did not land", p, k)
			}
			v = l[k]
		}
	}
	return v
}

func authSLOCellMap(t *testing.T, v any, p ...any) map[string]any {
	t.Helper()
	m, ok := authSLOCellAt(t, v, p...).(map[string]any)
	if !ok {
		t.Fatalf("cell path %v is not an object, the defect did not land", p)
	}
	return m
}

func authSLOCellClone(v any) any {
	b, _ := json.Marshal(v)
	var out any
	_ = json.Unmarshal(b, &out)
	return out
}

func authSLOReversed(l []any) []any {
	out := make([]any, len(l))
	for i, x := range l {
		out[len(l)-1-i] = x
	}
	return out
}

var (
	authSLOSpaceFlip = map[string]string{"p95": "p50", "p99": "p95", "sum": "avg", "max": "avg", "min": "max"}
	authSLOTimeFlip  = map[string]string{"rate": "increase", "increase": "rate", "latest": "avg"}
)

func authSLOCells(base map[string]any) []authSLOCell {
	var cells []authSLOCell
	add := func(class, name, want string, edit func(t *testing.T, doc map[string]any)) {
		cells = append(cells, authSLOCell{class, name, edit, want})
	}
	add("c0", "reformatted", "", func(*testing.T, map[string]any) {})
	panels := mp(mp(base["spec"])["panels"])
	var ids []string
	for id := range panels {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	pp := func(id string, rest ...any) []any { return append([]any{"spec", "panels", id}, rest...) }
	for _, id := range ids {
		id := id
		ps := mp(mp(mp(panels[id])["spec"])["plugin"])["spec"]
		qs := list(mp(mp(panels[id])["spec"])["queries"])
		hasLine := len(list(mp(ps)["thresholds"])) > 0
		pw := "panel " + id + " "

		// class 2: the panel inventory
		add("c2-inventory", id+"/panel-removed", fmt.Sprintf("dashboard panels lack %q", id), func(t *testing.T, doc map[string]any) {
			delete(authSLOCellMap(t, doc, "spec", "panels"), id)
			lay := authSLOCellMap(t, doc, "spec", "layouts", 0, "spec")
			var keep []any
			for _, it := range list(lay["items"]) {
				if str(mp(mp(it)["content"])["$ref"]) != "#/spec/panels/"+id {
					keep = append(keep, it)
				}
			}
			lay["items"] = keep
		})
		add("c2-inventory", id+"/title-changed", pw+"key spec.display.name = ", func(t *testing.T, doc map[string]any) {
			authSLOCellMap(t, doc, pp(id, "spec", "display")...)["name"] = "Something else"
		})
		add("c2-inventory", id+"/panel-kind-changed", pw+"key spec.plugin.kind = ", func(t *testing.T, doc map[string]any) {
			authSLOCellMap(t, doc, pp(id, "spec", "plugin")...)["kind"] = "signoz/BarChartPanel"
		})
		add("c2-inventory", id+"/panel-field-added", pw+"key spec.plugin.spec.stackedBarChart is not a known field", func(t *testing.T, doc map[string]any) {
			authSLOCellMap(t, doc, pp(id, "spec", "plugin", "spec")...)["stackedBarChart"] = true
		})
		next := string(rune('A' + len(qs)))
		add("c2-inventory", id+"/query-added", fmt.Sprintf("%squeries hold %q", pw, next), func(t *testing.T, doc map[string]any) {
			sp := authSLOCellMap(t, doc, pp(id, "spec")...)
			l := list(sp["queries"])
			q := authSLOCellClone(l[len(l)-1])
			mp(mp(q)["spec"])["name"] = next
			mp(mp(mp(q)["spec"])["plugin"])["spec"].(map[string]any)["name"] = next
			sp["queries"] = append(l, q)
		})
		if len(qs) > 1 {
			add("c2-inventory", id+"/queries-reordered", "", func(t *testing.T, doc map[string]any) {
				sp := authSLOCellMap(t, doc, pp(id, "spec")...)
				sp["queries"] = authSLOReversed(list(sp["queries"]))
			})
		}

		// class 5: threshold value, unit and label bound to the alert; no line without an alert; hover text
		if hasLine {
			th := func(t *testing.T, doc map[string]any) map[string]any {
				return authSLOCellMap(t, doc, pp(id, "spec", "plugin", "spec", "thresholds", 0)...)
			}
			add("c5-threshold", id+"/label-changed", pw+`key spec.plugin.spec.thresholds[0].label = "999 ms"`, func(t *testing.T, doc map[string]any) { th(t, doc)["label"] = "999 ms" })
			add("c5-threshold", id+"/unit-changed", pw+`key spec.plugin.spec.thresholds[0].unit = "ms"`, func(t *testing.T, doc map[string]any) { th(t, doc)["unit"] = "ms" })
			add("c5-threshold", id+"/value-changed", pw+`key spec.plugin.spec.thresholds[0].value = 999`, func(t *testing.T, doc map[string]any) { th(t, doc)["value"] = 999.0 })
			add("c5-threshold", id+"/color-changed", pw+`key spec.plugin.spec.thresholds[0].color = "#000000"`, func(t *testing.T, doc map[string]any) { th(t, doc)["color"] = "#000000" })
			add("c5-threshold", id+"/label-dropped", pw+`key spec.plugin.spec.thresholds[0].label is missing`, func(t *testing.T, doc map[string]any) { delete(th(t, doc), "label") })
			add("c5-threshold", id+"/second-line-added", pw+`key spec.plugin.spec.thresholds[1].label is not a known field`, func(t *testing.T, doc map[string]any) {
				s := authSLOCellMap(t, doc, pp(id, "spec", "plugin", "spec")...)
				s["thresholds"] = append(list(s["thresholds"]), map[string]any{"color": "#e5484d", "label": "1 s", "unit": "s", "value": 1.0})
			})
			add("c5-threshold", id+"/line-dropped", pw+`key spec.plugin.spec.thresholds[0].value is missing`, func(t *testing.T, doc map[string]any) {
				authSLOCellMap(t, doc, pp(id, "spec", "plugin", "spec")...)["thresholds"] = nil
			})
			add("c5-threshold", id+"/axis-unit-changed", pw+`key spec.plugin.spec.formatting.unit = "ms"`, func(t *testing.T, doc map[string]any) {
				authSLOCellMap(t, doc, pp(id, "spec", "plugin", "spec", "formatting")...)["unit"] = "ms"
			})
			add("c5-threshold", id+"/description-line-changed", pw+"description", func(t *testing.T, doc map[string]any) {
				d := authSLOCellMap(t, doc, pp(id, "spec", "display")...)
				s := str(d["description"])
				i := strings.Index(s, "SLO line ")
				if i < 0 {
					t.Fatalf("no SLO line in %q, the defect did not land", s)
				}
				d["description"] = s[:i] + "SLO line 999 ms (" + s[i:]
			})
		} else {
			add("c5-threshold", id+"/line-added", pw+`key spec.plugin.spec.thresholds[0].label is not a known field`, func(t *testing.T, doc map[string]any) {
				authSLOCellMap(t, doc, pp(id, "spec", "plugin", "spec")...)["thresholds"] = []any{map[string]any{"color": "#e5484d", "label": "1 s", "unit": "s", "value": 1.0}}
			})
			add("c5-threshold", id+"/description-claims-line", pw+"description", func(t *testing.T, doc map[string]any) {
				d := authSLOCellMap(t, doc, pp(id, "spec", "display")...)
				d["description"] = str(d["description"]) + " SLO line 5 ms (alert AuthzLatencyP95High)."
			})
		}

		for qi, q := range qs {
			qi := qi
			n := str(mp(mp(q)["spec"])["name"])
			qs0 := mp(mp(mp(mp(q)["spec"])["plugin"])["spec"])
			qw := fmt.Sprintf("panel %s query %s ", id, n)
			cn := id + "/" + n + "/"
			qp := func(rest ...any) []any { return pp(id, append([]any{"spec", "queries", qi}, rest...)...) }
			spec := func(t *testing.T, doc map[string]any) map[string]any {
				return authSLOCellMap(t, doc, qp("spec", "plugin", "spec")...)
			}
			agg := func(t *testing.T, doc map[string]any) map[string]any {
				return authSLOCellMap(t, doc, qp("spec", "plugin", "spec", "aggregations", 0)...)
			}

			// class 1: the query is a metrics builder query
			add("c1-signal", cn+"signal-logs", qw+`key spec.plugin.spec.signal = "logs", want "metrics"`, func(t *testing.T, doc map[string]any) { spec(t, doc)["signal"] = "logs" })
			add("c1-signal", cn+"signal-dropped", qw+`key spec.plugin.spec.signal is missing`, func(t *testing.T, doc map[string]any) { delete(spec(t, doc), "signal") })
			add("c1-signal", cn+"source-set", qw+`key spec.plugin.spec.source = "meter"`, func(t *testing.T, doc map[string]any) { spec(t, doc)["source"] = "meter" })
			add("c1-signal", cn+"datasource-added", qw+`key spec.plugin.spec.dataSource is not a known field`, func(t *testing.T, doc map[string]any) { spec(t, doc)["dataSource"] = "logs" })
			add("c1-signal", cn+"query-kind", qw+`key kind = "clickhouse_sql"`, func(t *testing.T, doc map[string]any) {
				authSLOCellMap(t, doc, qp()...)["kind"] = "clickhouse_sql"
			})
			add("c1-signal", cn+"plugin-kind", qw+`key spec.plugin.kind = "signoz/ClickHouseQuery"`, func(t *testing.T, doc map[string]any) {
				authSLOCellMap(t, doc, qp("spec", "plugin")...)["kind"] = "signoz/ClickHouseQuery"
			})
			for gi := range list(qs0["groupBy"]) {
				gi := gi
				add("c1-signal", fmt.Sprintf("%sgroupby%d-signal-logs", cn, gi), fmt.Sprintf(`%sgroupBy[%d] key signal = "logs"`, qw, gi), func(t *testing.T, doc map[string]any) {
					authSLOCellMap(t, doc, qp("spec", "plugin", "spec", "groupBy", gi)...)["signal"] = "logs"
				})
			}

			// class 2: every query field and group-by label is pinned
			add("c2-inventory", cn+"query-removed", fmt.Sprintf("%squeries lack %q", pw, n), func(t *testing.T, doc map[string]any) {
				sp := authSLOCellMap(t, doc, pp(id, "spec")...)
				l := list(sp["queries"])
				sp["queries"] = append(append([]any{}, l[:qi]...), l[qi+1:]...)
			})
			add("c2-inventory", cn+"legend-changed", qw+`key spec.plugin.spec.legend = "zz"`, func(t *testing.T, doc map[string]any) { spec(t, doc)["legend"] = "zz" })
			add("c2-inventory", cn+"limit-changed", qw+`key spec.plugin.spec.limit = 10`, func(t *testing.T, doc map[string]any) { spec(t, doc)["limit"] = 10.0 })
			add("c2-inventory", cn+"disabled", qw+`key spec.plugin.spec.disabled = true`, func(t *testing.T, doc map[string]any) { spec(t, doc)["disabled"] = true })
			add("c2-inventory", cn+"having-added", qw+`key spec.plugin.spec.having.expression is not a known field`, func(t *testing.T, doc map[string]any) {
				spec(t, doc)["having"] = map[string]any{"expression": "A > 0"}
			})
			metric := "devhealth_auth_outbox_delivered_total"
			if str(mp(list(qs0["aggregations"])[0])["metricName"]) == metric {
				metric = "devhealth_auth_outbox_publisher_enabled"
			}
			add("c2-inventory", cn+"metric-changed", qw+`key spec.plugin.spec.aggregations[0].metricName = "`+metric+`"`, func(t *testing.T, doc map[string]any) { agg(t, doc)["metricName"] = metric })
			add("c2-inventory", cn+"aggregation-added", qw+`key spec.plugin.spec.aggregations[1].metricName is not a known field`, func(t *testing.T, doc map[string]any) {
				s := spec(t, doc)
				s["aggregations"] = append(list(s["aggregations"]), authSLOCellClone(list(s["aggregations"])[0]))
			})
			add("c2-inventory", cn+"groupby-added", qw+`group-by labels hold "method"`, func(t *testing.T, doc map[string]any) {
				s := spec(t, doc)
				s["groupBy"] = append(list(s["groupBy"]), map[string]any{"fieldContext": "attribute", "fieldDataType": "string", "name": "method", "signal": "metrics"})
			})
			if gb := list(qs0["groupBy"]); len(gb) > 0 {
				add("c2-inventory", cn+"groupby-dropped", fmt.Sprintf("%sgroup-by labels lack %q", qw, str(mp(gb[0])["name"])), func(t *testing.T, doc map[string]any) {
					s := spec(t, doc)
					s["groupBy"] = list(s["groupBy"])[1:]
				})
				if len(gb) > 1 {
					add("c2-inventory", cn+"groupby-reordered", "", func(t *testing.T, doc map[string]any) {
						s := spec(t, doc)
						s["groupBy"] = authSLOReversed(list(s["groupBy"]))
					})
				}
			}

			// class 3: aggregation fields
			a0 := mp(list(qs0["aggregations"])[0])
			sp, tm := authSLOSpaceFlip[str(a0["spaceAggregation"])], authSLOTimeFlip[str(a0["timeAggregation"])]
			add("c3-aggregation", cn+"space-"+sp, qw+`key spec.plugin.spec.aggregations[0].spaceAggregation = "`+sp+`"`, func(t *testing.T, doc map[string]any) { agg(t, doc)["spaceAggregation"] = sp })
			add("c3-aggregation", cn+"time-"+tm, qw+`key spec.plugin.spec.aggregations[0].timeAggregation = "`+tm+`"`, func(t *testing.T, doc map[string]any) { agg(t, doc)["timeAggregation"] = tm })
			add("c3-aggregation", cn+"space-dropped", qw+`key spec.plugin.spec.aggregations[0].spaceAggregation is missing`, func(t *testing.T, doc map[string]any) { delete(agg(t, doc), "spaceAggregation") })

			// class 4: filter clauses and route sets compared as sets
			filt := func(t *testing.T, doc map[string]any) map[string]any {
				return authSLOCellMap(t, doc, qp("spec", "plugin", "spec", "filter")...)
			}
			fe := str(mp(qs0["filter"])["expression"])
			clauses := strings.Split(fe, " AND ")
			set := func(f func(string) string) func(t *testing.T, doc map[string]any) {
				return func(t *testing.T, doc map[string]any) {
					m := filt(t, doc)
					old := str(m["expression"])
					if m["expression"] = f(old); m["expression"] == old {
						t.Fatalf("filter %q unchanged, the defect did not land", old)
					}
				}
			}
			add("c4-route", cn+"clause-added", qw+"filter is", set(func(s string) string { return s + " AND status_class = '2xx'" }))
			add("c4-route", cn+"or-widened", qw+"filter is", set(func(s string) string { return s + " OR route IN ('/v1/zz')" }))
			add("c4-route", cn+"clause-dropped", qw+"filter is", set(func(s string) string { return strings.Join(strings.Split(s, " AND ")[1:], " AND ") }))
			add("c4-route", cn+"clause-negated", qw+"filter is", set(func(s string) string { return "NOT " + s }))
			if len(clauses) > 1 {
				add("c4-route", cn+"lowercase-or", qw+"filter is", set(func(s string) string { return strings.Replace(s, " AND ", " or ", 1) }))
				add("c4-route", cn+"clauses-reordered", "", set(func(string) string {
					r := append([]string{}, clauses...)
					sort.Sort(sort.Reverse(sort.StringSlice(r)))
					return strings.Join(r, " AND ")
				}))
			}
			for ci, c := range clauses {
				if !strings.HasPrefix(c, "route IN (") {
					continue
				}
				ci := ci
				members := strings.Split(strings.TrimSuffix(strings.TrimPrefix(c, "route IN ("), ")"), ", ")
				route := func(f func([]string) []string) func(t *testing.T, doc map[string]any) {
					return func(t *testing.T, doc map[string]any) {
						cs := append([]string{}, clauses...)
						cs[ci] = "route IN (" + strings.Join(f(append([]string{}, members...)), ", ") + ")"
						filt(t, doc)["expression"] = strings.Join(cs, " AND ")
					}
				}
				add("c4-route", cn+"route-added", qw+"filter is", route(func(m []string) []string { return append(m, "'/v1/zz'") }))
				add("c4-route", cn+"route-swapped", qw+"filter is", route(func(m []string) []string { m[0] = "'/v1/zz'"; return m }))
				add("c4-route", cn+"route-not-in", qw+"filter is", func(t *testing.T, doc map[string]any) {
					cs := append([]string{}, clauses...)
					cs[ci] = strings.Replace(cs[ci], "route IN (", "route NOT IN (", 1)
					filt(t, doc)["expression"] = strings.Join(cs, " AND ")
				})
				if len(members) > 1 {
					add("c4-route", cn+"route-narrowed", qw+"filter is", route(func(m []string) []string { return m[1:] }))
					add("c4-route", cn+"route-repeated", qw+"filter is", route(func(m []string) []string { return append(m, m[0]) }))
					add("c4-route", cn+"routes-reordered", "", route(func(m []string) []string {
						for i, j := 0, len(m)-1; i < j; i, j = i+1, j-1 {
							m[i], m[j] = m[j], m[i]
						}
						return m
					}))
				}
			}
		}
	}

	// sibling: layout places and tags
	items := list(mp(mp(list(mp(base["spec"])["layouts"])[0])["spec"])["items"])
	for ii, it := range items {
		ii := ii
		ref := str(mp(mp(it)["content"])["$ref"])
		add("c6-layout", fmt.Sprintf("item%d-moved", ii), "layout item "+ref+" key x = ", func(t *testing.T, doc map[string]any) {
			m := authSLOCellMap(t, doc, "spec", "layouts", 0, "spec", "items", ii)
			m["x"] = m["x"].(float64) + 1
		})
		add("c6-layout", fmt.Sprintf("item%d-field-added", ii), "layout item "+ref+" key static is not a known field", func(t *testing.T, doc map[string]any) {
			authSLOCellMap(t, doc, "spec", "layouts", 0, "spec", "items", ii)["static"] = true
		})
	}
	add("c6-layout", "items-reordered", "", func(t *testing.T, doc map[string]any) {
		m := authSLOCellMap(t, doc, "spec", "layouts", 0, "spec")
		m["items"] = authSLOReversed(list(m["items"]))
	})
	add("c6-layout", "second-layout", "dashboard key spec.layouts[1].kind is not a known field", func(t *testing.T, doc map[string]any) {
		s := authSLOCellMap(t, doc, "spec")
		s["layouts"] = append(list(s["layouts"]), map[string]any{"kind": "Grid", "spec": map[string]any{"items": []any{}}})
	})
	add("c6-layout", "tags-reordered", "", func(t *testing.T, doc map[string]any) { doc["tags"] = authSLOReversed(list(doc["tags"])) })
	add("c6-layout", "tag-changed", `dashboard tags lack "ticket=CHAOS-11221"`, func(t *testing.T, doc map[string]any) { // gitleaks:allow
		authSLOCellMap(t, doc, "tags", 2)["value"] = "CHAOS-1"
	})
	add("c6-layout", "tag-added", `dashboard tags hold "team=x"`, func(t *testing.T, doc map[string]any) {
		doc["tags"] = append(list(doc["tags"]), map[string]any{"key": "team", "value": "x"})
	})
	add("c6-layout", "variable-added", "dashboard key spec.variables[0].name is not a known field", func(t *testing.T, doc map[string]any) {
		authSLOCellMap(t, doc, "spec")["variables"] = []any{map[string]any{"name": "env"}}
	})
	return cells
}

func authSLOCellRoot(t *testing.T, real string, doc map[string]any) string {
	t.Helper()
	root := t.TempDir()
	for _, rel := range authSLOCellLinks {
		dst := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(real, filepath.FromSlash(rel)), dst); err != nil {
			t.Fatal(err)
		}
	}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, root, authSLODashboardRel, string(b)+"\n")
	return root
}

func authSLOCellBase(t *testing.T, real string) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(mustRead(t, filepath.Join(real, filepath.FromSlash(authSLODashboardRel)))), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestAuthSLODashboardCells(t *testing.T) {
	real := realRoot(t)
	for _, c := range authSLOCells(authSLOCellBase(t, real)) {
		c := c
		t.Run(c.class+"/"+c.name, func(t *testing.T) {
			t.Parallel()
			doc := authSLOCellBase(t, real)
			c.edit(t, doc)
			g := &guard{root: authSLOCellRoot(t, real, doc), tmp: t.TempDir()}
			authSLODashboardAll(g)
			if c.want == "" {
				if len(g.v) > 0 {
					t.Fatalf("equivalent form %q was refused:\n  %s", c.name, strings.Join(g.v, "\n  "))
				}
				return
			}
			if len(g.v) == 0 {
				t.Fatalf("planted defect %q was not caught: the guard passed", c.name)
			}
			for _, v := range g.v {
				if strings.Contains(v, c.want) {
					return
				}
			}
			t.Fatalf("planted defect %q was caught, but no violation contains %q:\n  %s", c.name, c.want, strings.Join(g.v, "\n  "))
		})
	}
}
