package pgmigrate

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The routing audit rows record the credential class the routing verb presented. The class set only grows: every row
// an earlier schema admitted stays valid, and the classes that must name a principal are exactly the ones that carry a
// verified subject.

const (
	routingAuditClassCheck   = "ck_go_api_routing_audits_credential_class"
	routingAuditPairingCheck = "ck_go_api_routing_audits_principal_pairing"
)

var quotedLiteral = regexp.MustCompile(`'([a-z_]+)'`)

// checkExpression is the parenthesised expression after "CONSTRAINT <name> CHECK" in the last place sql defines it.
func checkExpression(sql, name string) (string, bool) {
	found := regexp.MustCompile(`CONSTRAINT\s+`+regexp.QuoteMeta(name)+`\s+CHECK`).FindAllStringIndex(sql, -1)
	if len(found) == 0 {
		return "", false
	}
	rest := sql[found[len(found)-1][1]:]
	open := strings.Index(rest, "(")
	if open < 0 {
		return "", false
	}
	depth := 0
	for i := open; i < len(rest); i++ {
		switch rest[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return rest[open : i+1], true
			}
		}
	}
	return "", false
}

func literals(expression string) []string {
	var out []string
	for _, m := range quotedLiteral.FindAllStringSubmatch(expression, -1) {
		if !slices.Contains(out, m[1]) {
			out = append(out, m[1])
		}
	}
	slices.Sort(out)
	return out
}

// routingAuditChecks reads the class set and the principal-pairing classes the baseline defines and the ones the
// newest chain file that redefines each CHECK leaves in place.
func routingAuditChecks(baseline string, chain []ChainFile) (baseClasses, baseNamed, headClasses, headNamed []string) {
	classes, _ := checkExpression(baseline, routingAuditClassCheck)
	named, _ := checkExpression(baseline, routingAuditPairingCheck)
	baseClasses, baseNamed = literals(classes), literals(named)
	for _, file := range chain {
		if expression, ok := checkExpression(file.SQL, routingAuditClassCheck); ok {
			classes = expression
		}
		if expression, ok := checkExpression(file.SQL, routingAuditPairingCheck); ok {
			named = expression
		}
	}
	return baseClasses, baseNamed, literals(classes), literals(named)
}

func routingAuditCheckDefects(baseline string, chain []ChainFile) []string {
	baseClasses, baseNamed, headClasses, headNamed := routingAuditChecks(baseline, chain)
	var defects []string
	if len(baseClasses) == 0 || len(baseNamed) == 0 {
		return []string{"the baseline defines no routing audit class or pairing CHECK: the read is vacuous"}
	}
	for _, class := range baseClasses {
		if !slices.Contains(headClasses, class) {
			defects = append(defects, "the head class set drops "+class+": an earlier audit row would no longer be valid")
		}
	}
	if want := []string{"effective_principal_envelope", "operator_direct", "user_access_token"}; !slices.Equal(headClasses, want) {
		defects = append(defects, "the head class set is "+strings.Join(headClasses, ",")+", want "+strings.Join(want, ","))
	}
	if want := []string{"effective_principal_envelope", "user_access_token"}; !slices.Equal(headNamed, want) {
		defects = append(defects, "the classes that must name a principal are "+strings.Join(headNamed, ",")+", want "+strings.Join(want, ","))
	}
	for _, class := range baseNamed {
		if !slices.Contains(headNamed, class) {
			defects = append(defects, "the head pairing drops "+class+": an earlier row that names its principal would no longer be valid")
		}
	}
	return defects
}

func TestTheRoutingAuditChecksAdmitTheUserAccessTokenAndEveryEarlierRow(t *testing.T) {
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	for _, defect := range routingAuditCheckDefects(baseline.Schema, chain) {
		t.Error(defect)
	}
}

func TestTheRoutingAuditCheckGuardCatchesPlantedChains(t *testing.T) {
	baseline := `CONSTRAINT ck_go_api_routing_audits_credential_class CHECK ((credential_class = ANY (ARRAY['effective_principal_envelope'::text, 'operator_direct'::text]))),
	CONSTRAINT ck_go_api_routing_audits_principal_pairing CHECK (((credential_class = 'effective_principal_envelope'::text) = (principal_id IS NOT NULL))),`
	file := func(classes, named string) []ChainFile {
		return []ChainFile{{Revision: "0164", Name: "0164_x.sql", SQL: "ALTER TABLE go_api_routing_audits\n" +
			"    ADD CONSTRAINT ck_go_api_routing_audits_credential_class CHECK (credential_class IN (" + classes + ")),\n" +
			"    ADD CONSTRAINT ck_go_api_routing_audits_principal_pairing CHECK ((credential_class IN (" + named + ")) = (principal_id IS NOT NULL));\n"}}
	}
	good := file("'effective_principal_envelope', 'operator_direct', 'user_access_token'", "'effective_principal_envelope', 'user_access_token'")
	if defects := routingAuditCheckDefects(baseline, good); len(defects) != 0 {
		t.Fatalf("the clean chain is refused: %v", defects)
	}
	for name, c := range map[string]struct {
		chain []ChainFile
		want  string
	}{
		"no chain file":            {nil, "want effective_principal_envelope,operator_direct,user_access_token"},
		"operator_direct dropped":  {file("'effective_principal_envelope', 'user_access_token'", "'effective_principal_envelope', 'user_access_token'"), "drops operator_direct"},
		"envelope pairing dropped": {file("'effective_principal_envelope', 'operator_direct', 'user_access_token'", "'user_access_token'"), "the head pairing drops effective_principal_envelope"},
		"user token not paired":    {file("'effective_principal_envelope', 'operator_direct', 'user_access_token'", "'effective_principal_envelope'"), "must name a principal are effective_principal_envelope,"},
	} {
		if joined := strings.Join(routingAuditCheckDefects(baseline, c.chain), "\n"); !strings.Contains(joined, c.want) {
			t.Errorf("%s: the guard did not report %q, it reported %q", name, c.want, joined)
		}
	}
	if joined := strings.Join(routingAuditCheckDefects("", nil), "\n"); !strings.Contains(joined, "vacuous") {
		t.Errorf("a baseline with no CHECK: %q", joined)
	}
}
