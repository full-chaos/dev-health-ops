package docs_test

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"
)

// The workload pages (CHAOS-11191): the registry reference, the Kubernetes federation runbook, the bootstrap credential
// runbook and the auth-service configuration page. Four guards pin them to their sources, so a page cannot drift:
// the registry table to workload-registry.json and audiences.json, the troubleshooting table to the closed reason sets of
// reasons.go, every runbook step that names a workloads verb to an audit event that exists in source, and the code blocks
// to carry no credential value.

const (
	workloadRegistryPage   = "docs/reference/auth/workload-registry.md"
	workloadFederationPage = "docs/operate/runbooks/auth-workload-kubernetes-federation.md"
	workloadBootstrapPage  = "docs/operate/runbooks/auth-workload-bootstrap-credential.md"
	authConfigPage         = "docs/reference/auth/configuration.md"

	workloadRegistryJSON  = "contracts/auth/v1/vocabulary/workload-registry.json"
	workloadAudiencesJSON = "contracts/auth/v1/vocabulary/audiences.json"
	workloadReasonsGo     = "internal/auth/workloadexchange/reasons.go"
)

// ---- markdown helpers -------------------------------------------------------------------------------------------------

// sectionOf returns the lines under the heading line (exclusive) up to the next heading of the same or a higher level.
func sectionOf(text, heading string) (string, bool) {
	level := len(heading) - len(strings.TrimLeft(heading, "#"))
	lines := strings.Split(text, "\n")
	start := -1
	for i, l := range lines {
		if strings.TrimRight(l, " \t") == heading {
			start = i + 1
			break
		}
	}
	if start < 0 {
		return "", false
	}
	end := len(lines)
	fenced := false
	for i := start; i < len(lines); i++ {
		l := lines[i]
		if strings.HasPrefix(strings.TrimSpace(l), "```") {
			fenced = !fenced
		}
		if fenced || !strings.HasPrefix(l, "#") {
			continue
		}
		if n := len(l) - len(strings.TrimLeft(l, "#")); n <= level && strings.HasPrefix(l[n:], " ") {
			end = i
			break
		}
	}
	return strings.Join(lines[start:end], "\n"), true
}

// mdTableRows returns the body rows of the first pipe table in text, each cell trimmed with its backticks removed.
func mdTableRows(text string) [][]string {
	var rows [][]string
	seenSep := false
	for _, l := range strings.Split(text, "\n") {
		l = strings.TrimSpace(l)
		if !strings.HasPrefix(l, "|") {
			if seenSep {
				break
			}
			continue
		}
		cells := strings.Split(strings.Trim(l, "|"), "|")
		for i := range cells {
			cells[i] = strings.Trim(strings.TrimSpace(cells[i]), "`")
		}
		if !seenSep {
			if len(cells) > 0 && strings.Trim(cells[0], "-: ") == "" {
				seenSep = true
			}
			continue
		}
		rows = append(rows, cells)
	}
	return rows
}

func splitList(cell string) []string {
	var out []string
	for _, p := range strings.Split(cell, ",") {
		p = strings.Trim(strings.TrimSpace(p), "`")
		if p != "" && p != "none" {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// fencedBlocks returns the lines inside ``` fences.
func fencedBlocks(text string) []string {
	var out []string
	fenced := false
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "```") {
			fenced = !fenced
			continue
		}
		if fenced {
			out = append(out, l)
		}
	}
	return out
}

func diffSets(label string, got, want []string) []string {
	g, w := map[string]bool{}, map[string]bool{}
	for _, s := range got {
		g[s] = true
	}
	for _, s := range want {
		w[s] = true
	}
	var out []string
	for _, s := range want {
		if !g[s] {
			out = append(out, fmt.Sprintf("%s: missing row %q", label, s))
		}
	}
	for _, s := range got {
		if !w[s] {
			out = append(out, fmt.Sprintf("%s: row %q is not in the source", label, s))
		}
	}
	if len(got) != len(g) {
		out = append(out, label+": a row is repeated")
	}
	return out
}

// ---- (a) the registry page ---------------------------------------------------------------------------------------------

type registryFile struct {
	Workloads []struct {
		Name           string   `json:"name"`
		Audiences      []string `json:"audiences"`
		AllowedActions []string `json:"allowed_actions"`
	} `json:"workloads"`
}

type audiencesFile struct {
	Audiences []struct {
		Audience string   `json:"audience"`
		Classes  []string `json:"classes"`
		Services []string `json:"services"`
	} `json:"audiences"`
}

func workloadRegistryGuard(d docTree) []string {
	var out []string
	page, ok := readOrNote(&out, d, workloadRegistryPage)
	rawReg, err1 := d.read(workloadRegistryJSON)
	rawAud, err2 := d.read(workloadAudiencesJSON)
	if err1 != nil || err2 != nil {
		return append(out, "the registry or audiences JSON cannot be read")
	}
	if !ok {
		return out
	}
	var reg registryFile
	var aud audiencesFile
	if err := json.Unmarshal([]byte(rawReg), &reg); err != nil {
		return append(out, "workload-registry.json: "+err.Error())
	}
	if err := json.Unmarshal([]byte(rawAud), &aud); err != nil {
		return append(out, "audiences.json: "+err.Error())
	}

	section, found := sectionOf(page, "## Workloads")
	if !found {
		return append(out, workloadRegistryPage+": no ## Workloads section")
	}
	var got, want []string
	for _, r := range mdTableRows(section) {
		if len(r) != 3 {
			out = append(out, fmt.Sprintf("workloads table: a row has %d cells, want 3", len(r)))
			continue
		}
		auds, acts := splitList(r[1]), splitList(r[2])
		got = append(got, fmt.Sprintf("%s|%s|%s", r[0], strings.Join(auds, ","), strings.Join(acts, ",")))
	}
	for _, w := range reg.Workloads {
		auds := append([]string(nil), w.Audiences...)
		acts := append([]string(nil), w.AllowedActions...)
		sort.Strings(auds)
		sort.Strings(acts)
		want = append(want, fmt.Sprintf("%s|%s|%s", w.Name, strings.Join(auds, ","), strings.Join(acts, ",")))
	}
	out = append(out, diffSets("workloads table", got, want)...)
	for _, r := range mdTableRows(section) {
		if len(r) == 3 && strings.TrimSpace(r[2]) == "" {
			out = append(out, "workloads table: an empty allowed-actions cell must read none: "+r[0])
		}
	}

	section, found = sectionOf(page, "## Audiences")
	if !found {
		return append(out, workloadRegistryPage+": no ## Audiences section")
	}
	got, want = nil, nil
	for _, r := range mdTableRows(section) {
		if len(r) != 2 {
			out = append(out, fmt.Sprintf("audiences table: a row has %d cells, want 2", len(r)))
			continue
		}
		got = append(got, r[0]+"|"+strings.Join(splitList(r[1]), ","))
	}
	for _, a := range aud.Audiences {
		for _, c := range a.Classes {
			if c == "workload_token" {
				svc := append([]string(nil), a.Services...)
				sort.Strings(svc)
				want = append(want, a.Audience+"|"+strings.Join(svc, ","))
			}
		}
	}
	out = append(out, diffSets("audiences table", got, want)...)
	return out
}

// ---- (b) the troubleshooting table -------------------------------------------------------------------------------------

type reasonRow struct{ subject, outcome, reason string }

func (r reasonRow) key() string { return r.subject + "|" + r.outcome + "|" + r.reason }

// reasonSets reads the closed reason sets of reasons.go: the constant values, the two resolve slices and the self-check
// identifiers come from the source; which set a name sits in mirrors recorder.declare. A reason constant that no set
// uses is an error, so a new reason cannot be added without this guard noticing.
func reasonSets(d docTree) ([]reasonRow, []string) {
	src, err := d.read(workloadReasonsGo)
	if err != nil {
		return nil, []string{err.Error()}
	}
	file, err := parser.ParseFile(token.NewFileSet(), workloadReasonsGo, src, 0)
	if err != nil {
		return nil, []string{err.Error()}
	}
	consts := map[string]string{}
	slices := map[string][]string{}
	var selfIdents []string
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gen.Specs {
			vs, isValue := spec.(*ast.ValueSpec)
			if !isValue {
				continue
			}
			for i, name := range vs.Names {
				if i >= len(vs.Values) {
					continue
				}
				switch v := vs.Values[i].(type) {
				case *ast.BasicLit:
					if gen.Tok == token.CONST && v.Kind == token.STRING {
						s, _ := strconv.Unquote(v.Value)
						consts[name.Name] = s
					}
				case *ast.CompositeLit:
					for _, el := range v.Elts {
						switch e := el.(type) {
						case *ast.BasicLit:
							s, _ := strconv.Unquote(e.Value)
							slices[name.Name] = append(slices[name.Name], s)
						case *ast.SelectorExpr:
							if name.Name == "selfCheckReasons" {
								selfIdents = append(selfIdents, e.Sel.Name)
							}
						}
					}
				}
			}
		}
	}
	var errs []string
	used := map[string]bool{}
	c := func(name string) string {
		v, ok := consts[name]
		if !ok {
			errs = append(errs, "reasons.go: constant "+name+" not found")
		}
		used[name] = true
		return v
	}
	known := map[string]string{
		"ReasonContract": string(authverify.ReasonContract), "ReasonUnknownKid": string(authverify.ReasonUnknownKid),
		"ReasonSignature": string(authverify.ReasonSignature), "ReasonIssuer": string(authverify.ReasonIssuer),
		"ReasonAudience": string(authverify.ReasonAudience), "ReasonNotYetValid": string(authverify.ReasonNotYetValid),
		"ReasonExpired": string(authverify.ReasonExpired),
	}
	var selfCheck []string
	for _, id := range selfIdents {
		v, ok := known[id]
		if !ok {
			errs = append(errs, "reasons.go: self-check reason "+id+" is unknown to this guard")
			continue
		}
		selfCheck = append(selfCheck, c("selfCheckPrefix")+v)
	}
	if len(selfCheck) == 0 {
		errs = append(errs, "reasons.go: selfCheckReasons not found")
	}
	if len(slices["resolveReasons"]) == 0 || len(slices["bootstrapResolveReasons"]) == 0 {
		errs = append(errs, "reasons.go: the resolve reason slices were not found")
	}
	common := []string{c("reasonResolveRefused"), c("reasonAudienceRefused"), c("reasonScopeRefused"), c("reasonLifetimeTooShort")}
	var rows []reasonRow
	add := func(subject, outcome string, reasons ...[]string) {
		for _, rs := range reasons {
			for _, r := range rs {
				rows = append(rows, reasonRow{subject, outcome, r})
			}
		}
	}
	add("kubernetes", "refused", []string{c("reasonSubjectTokenInvalid")}, slices["resolveReasons"], common)
	add("kubernetes", "unavailable", []string{c("reasonTokenReviewUnavailable"), c("reasonSaturated")},
		[]string{c("reasonSignerUnavailable"), c("reasonAuditUnavailable")}, selfCheck)
	add("bootstrap", "refused", []string{c("reasonMalformed")}, slices["bootstrapResolveReasons"], common)
	add("bootstrap", "unavailable", []string{c("reasonSignerUnavailable"), c("reasonAuditUnavailable")}, selfCheck)
	add("kubernetes", "issued", []string{c("reasonIssued")})
	add("bootstrap", "issued", []string{c("reasonIssued")})
	add("none", "invalid", []string{c("reasonInvalidRequest")})
	for name := range consts {
		if strings.HasPrefix(name, "reason") && !used[name] {
			errs = append(errs, "reasons.go: "+name+" is in no set this guard knows")
		}
	}
	return rows, errs
}

func workloadReasonGuard(d docTree) []string {
	var out []string
	page, ok := readOrNote(&out, d, workloadFederationPage)
	want, errs := reasonSets(d)
	out = append(out, errs...)
	if !ok {
		return out
	}
	section, found := sectionOf(page, "## Troubleshooting by reason")
	if !found {
		return append(out, workloadFederationPage+": no ## Troubleshooting by reason section")
	}
	var got, wantKeys []string
	for _, r := range mdTableRows(section) {
		if len(r) < 5 {
			out = append(out, fmt.Sprintf("troubleshooting table: a row has %d cells, want 5", len(r)))
			continue
		}
		if strings.TrimSpace(r[3]) == "" || strings.TrimSpace(r[4]) == "" {
			out = append(out, "troubleshooting table: an empty meaning or check cell: "+r[2])
		}
		got = append(got, reasonRow{r[0], r[1], r[2]}.key())
	}
	for _, r := range want {
		wantKeys = append(wantKeys, r.key())
	}
	return append(out, diffSets("troubleshooting table", got, wantKeys)...)
}

// ---- (c) runbook verbs and audit events --------------------------------------------------------------------------------

var verbEvents = map[string][]string{
	"seed":              {"auth.workload.registered", "auth.workload.audiences_changed", "auth.workload.allowed_actions_changed"},
	"bind":              {"auth.workload.binding_added"},
	"bootstrap-create":  {"auth.workload.bootstrap_created"},
	"bootstrap-rotate":  {"auth.workload.bootstrap_rotated"},
	"bootstrap-revoke":  {"auth.workload.bootstrap_revoked"},
	"bootstrap-disable": {"auth.workload.bootstrap_disabled"},
	"enable":            {"auth.workload.enabled"},
}

var (
	verbPattern  = regexp.MustCompile(`auth-migrate workloads (seed|bind|bootstrap-create|bootstrap-rotate|bootstrap-revoke|bootstrap-disable|enable)\b`)
	eventPattern = regexp.MustCompile(`auth\.workload\.[a-z_]+`)

	auditStringPattern = regexp.MustCompile(`"(auth\.workload\.[a-z_]+)"`)
)

// workloadSourceStrings lists every "auth.workload.*" string literal in the non-test Go files of the workload, workloadexchange and audit packages.
func workloadSourceStrings(d docTree) (map[string]bool, error) {
	set := map[string]bool{}
	for _, dir := range []string{"internal/auth/workload", "internal/auth/workloadexchange", "internal/audit"} {
		found := false
		for _, rel := range d.tracked {
			name := strings.TrimPrefix(rel, dir+"/")
			if name == rel || strings.Contains(name, "/") || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			found = true
			text, err := d.read(rel)
			if err != nil {
				return nil, err
			}
			for _, m := range auditStringPattern.FindAllStringSubmatch(text, -1) {
				set[m[1]] = true
			}
		}
		if !found {
			return nil, fmt.Errorf("no tracked Go file under %s", dir)
		}
	}
	return set, nil
}

func markdownSections(text string) []string {
	var out []string
	var cur []string
	fenced := false
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "```") {
			fenced = !fenced
		}
		if !fenced && strings.HasPrefix(l, "#") && len(cur) > 0 {
			out = append(out, strings.Join(cur, "\n"))
			cur = nil
		}
		cur = append(cur, l)
	}
	return append(out, strings.Join(cur, "\n"))
}

func workloadVerbEventGuard(d docTree) []string {
	var out []string
	source, err := workloadSourceStrings(d)
	if err != nil {
		return []string{err.Error()}
	}
	for verb, events := range verbEvents {
		for _, e := range events {
			if !source[e] {
				out = append(out, fmt.Sprintf("the guard expects %s for %s but the source has no such string", e, verb))
			}
		}
	}
	for _, rel := range []string{workloadFederationPage, workloadBootstrapPage} {
		page, ok := readOrNote(&out, d, rel)
		if !ok {
			continue
		}
		for _, e := range eventPattern.FindAllString(page, -1) {
			if e != "auth.workload.exchanges" && !source[e] {
				out = append(out, fmt.Sprintf("%s: names %s, which is not a string in internal/auth/workload", rel, e))
			}
		}
		for _, section := range markdownSections(page) {
			title := strings.SplitN(section, "\n", 2)[0]
			for _, verb := range uniqueVerbs(section) {
				if strings.Contains(fold(section), "no audit row") {
					continue
				}
				for _, e := range verbEvents[verb] {
					if !strings.Contains(section, e) {
						out = append(out, fmt.Sprintf("%s: section %q runs the %s verb but does not name the audit event %s (or say \"no audit row\")",
							rel, title, verb, e))
					}
				}
			}
		}
	}
	return out
}

func uniqueVerbs(section string) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range verbPattern.FindAllStringSubmatch(section, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, m[1])
		}
	}
	sort.Strings(out)
	return out
}

// ---- (d) no credential value in a code block ---------------------------------------------------------------------------

var credentialPatterns = []struct {
	why string
	re  *regexp.Regexp
}{
	{"a literal bootstrap credential", regexp.MustCompile(`fcwlb_[A-Za-z0-9_-]{8,}`)},
	{"a literal JWT", regexp.MustCompile(`eyJ[A-Za-z0-9_-]{10,}`)},
	{"a secret flag with a value on argv", regexp.MustCompile(`(?i)(^|\s)--?(token|secret|password|passwd|bearer|api-key|value)(=|\s+)\S`)},
	{"an Authorization header on argv", regexp.MustCompile(`(?i)authorization:\s*bearer\s+\S`)},
	{"a secret literal on argv", regexp.MustCompile(`--from-literal`)},
	{"a secret environment value on the command line", regexp.MustCompile(`\b[A-Z][A-Z0-9_]*(SECRET|TOKEN|PASSWORD|URI|KEY)=\S`)},
	{"a subject token in a request", regexp.MustCompile(`(?i)subject[_-]token=\S`)},
	{"a command that prints a credential", regexp.MustCompile(`(?i)\bcat\s+\S*(bootstrap|token|secret|\.key|\.pem)`)},
	{"an echo of a variable", regexp.MustCompile(`\becho\s+"?\$`)},
	{"a command that prints a token or a Secret", regexp.MustCompile(`(?i)kubectl\s+(create\s+token|get\s+secrets?|describe\s+secrets?)`)},
	{"a base64 decode", regexp.MustCompile(`(?i)\bbase64\s+(-d|--decode)`)},
}

func workloadSecretGuard(d docTree) []string {
	var out []string
	for _, rel := range []string{workloadRegistryPage, workloadFederationPage, workloadBootstrapPage, authConfigPage} {
		page, ok := readOrNote(&out, d, rel)
		if !ok {
			continue
		}
		for _, line := range fencedBlocks(page) {
			for _, p := range credentialPatterns {
				if p.re.MatchString(line) {
					out = append(out, fmt.Sprintf("%s: a code block puts %s: %q", rel, p.why, strings.TrimSpace(line)))
				}
			}
		}
	}
	return out
}

// ---- (e) the Compose one-shots ------------------------------------------------------------------------------------------

const composeFile = "compose.yml"

var composeOneShots = []string{"auth-workload-volume", "auth-workload-seed", "auth-workload-bootstrap"}

// workloadComposeGuard pins the Compose one-shot text of the bootstrap runbook to compose.yml: the three service names,
// the profile, the seed and create commands, the volume and its mount path, and the read-only mount of go-api.
func workloadComposeGuard(d docTree) []string {
	var out []string
	page, ok := readOrNote(&out, d, workloadBootstrapPage)
	cfg, err := parseYAML(d, composeFile)
	if err != nil {
		return append(out, err.Error())
	}
	if !ok {
		return out
	}
	section, found := sectionOf(page, "## Compose one-shots")
	if !found {
		return append(out, workloadBootstrapPage+": no ## Compose one-shots section")
	}
	services := asMap(cfg["services"])
	for _, name := range composeOneShots {
		svc := asMap(services[name])
		if svc == nil {
			out = append(out, composeFile+": no service "+name)
			continue
		}
		mustContain(&out, "Compose one-shots", section, "`"+name+"`")
		profiles := asList(svc["profiles"])
		if len(profiles) != 1 || asString(profiles[0]) != "auth" {
			out = append(out, composeFile+": "+name+" is not in the profile auth alone")
		}
	}
	for _, name := range []string{"auth-workload-seed", "auth-workload-bootstrap"} {
		var words []string
		for _, w := range asList(asMap(services[name])["command"]) {
			words = append(words, asString(w))
		}
		if len(words) > 0 {
			mustContain(&out, "Compose one-shots", section, strings.Join(words, " "))
		}
	}
	mustContain(&out, "Compose one-shots", section, "docker compose --profile auth run --rm auth-workload-bootstrap")
	mustContain(&out, "Compose one-shots", section, "workload_bootstrap")
	var api map[string]any
	if svc := asMap(services["go-api"]); svc != nil {
		for _, v := range asList(svc["volumes"]) {
			if m := asMap(v); asString(m["source"]) == "workload_bootstrap" {
				api = m
			}
		}
	}
	if api == nil {
		out = append(out, composeFile+": go-api does not mount workload_bootstrap")
	} else {
		mustContain(&out, "Compose one-shots", section, asString(api["target"]))
		if api["read_only"] != true {
			out = append(out, composeFile+": the go-api mount of workload_bootstrap is not read-only")
		}
	}
	mustContain(&out, "Compose one-shots", section, "read-only")
	return out
}

// ---- tests -------------------------------------------------------------------------------------------------------------

func TestWorkloadRegistryPageMatchesTheRegistryJSON(t *testing.T) {
	assertGuard(t, workloadRegistryGuard, []plantCase{
		{"row-drift", func(t *testing.T, d docTree) docTree {
			return d.edit(t, workloadRegistryJSON, `"name": "web",`, `"name": "webx",`)
		}, `row "web|`},
		{"audience-drift", func(t *testing.T, d docTree) docTree {
			return d.edit(t, workloadRegistryJSON, `"audiences": ["auth-service"],`, `"audiences": ["auth-service", "dev-health-api"],`)
		}, "workloads table"},
		{"action-drift", func(t *testing.T, d docTree) docTree {
			return d.edit(t, workloadRegistryJSON, `"name": "web",
      "audiences": [],
      "allowed_actions": []`, `"name": "web",
      "audiences": [],
      "allowed_actions": ["audit.read"]`)
		}, "workloads table"},
		{"audience-table-drift", func(t *testing.T, d docTree) docTree {
			return d.edit(t, workloadAudiencesJSON, `"dev-health-ops-api",`, `"dev-health-ops-apix",`)
		}, "audiences table"},
	})
}

func TestWorkloadTroubleshootingTableMatchesTheReasonSets(t *testing.T) {
	assertGuard(t, workloadReasonGuard, []plantCase{
		{"new-resolve-reason", func(t *testing.T, d docTree) docTree {
			return d.edit(t, workloadReasonsGo, `{"unknown", "disabled", "uid_mismatch", "no_audience", "store_error"}`,
				`{"unknown", "disabled", "uid_mismatch", "no_audience", "store_error", "frozen"}`)
		}, `"kubernetes|refused|frozen"`},
		{"new-selfcheck-reason", func(t *testing.T, d docTree) docTree {
			return d.edit(t, workloadReasonsGo, `authverify.ReasonAudience, authverify.ReasonNotYetValid, authverify.ReasonExpired,`,
				`authverify.ReasonAudience, authverify.ReasonNotYetValid,`)
		}, "selfcheck_expired"},
		{"new-constant", func(t *testing.T, d docTree) docTree {
			return d.edit(t, workloadReasonsGo, `reasonMalformed   `, `reasonBrandNew = "brand_new"
	reasonMalformed   `)
		}, "reasonBrandNew"},
	})
}

func TestWorkloadRunbookVerbsNameAnExistingAuditEvent(t *testing.T) {
	assertGuard(t, workloadVerbEventGuard, []plantCase{
		{"verb-without-event", func(t *testing.T, d docTree) docTree {
			return d.edit(t, workloadBootstrapPage, "Audit event: `auth.workload.bootstrap_created` (reason `credential_issued`).", "Audit event: an audit row.")
		}, "bootstrap-create verb"},
		{"event-not-in-source", func(t *testing.T, d docTree) docTree {
			return d.edit(t, workloadBootstrapPage, "`auth.workload.bootstrap_rotated` (reason `credential_rotated`)", "`auth.workload.bootstrap_rotatd` (reason `credential_rotated`)")
		}, "auth.workload.bootstrap_rotatd"},
	})
}

func TestWorkloadPagesPutNoCredentialOnArgvOrOutput(t *testing.T) {
	plant := func(line string) func(*testing.T, docTree) docTree {
		return func(t *testing.T, d docTree) docTree {
			return d.appended(t, workloadBootstrapPage, "\n```sh\n"+line+"\n```\n")
		}
	}
	assertGuard(t, workloadSecretGuard, []plantCase{
		{"secret-on-argv", plant("auth-migrate workloads bootstrap-create -workload NAME -token " + glue("fcwlb_", "AAAAAAAAAAAAAAAA")), "a code block puts"},
		{"literal-credential", plant("echo " + glue("fcwlb_", "AAAAAAAAAAAAAAAA")), "a literal bootstrap credential"},
		{"env-secret", plant(glue("AUTH_DATABASE_", "URI") + "=postgres://u:p@h/d dho api"), "secret environment value"},
		{"prints-token", plant("kubectl create token SA"), "prints a token"},
		{"cat-credential", plant("cat /secure/dir/NAME.bootstrap"), "prints a credential"},
	})
}

func TestWorkloadComposeOneShotsMatchComposeYML(t *testing.T) {
	assertGuard(t, workloadComposeGuard, []plantCase{
		{"renamed-service", func(t *testing.T, d docTree) docTree {
			return d.editAll(t, composeFile, "auth-workload-seed", "auth-workload-seeded")
		}, "no service auth-workload-seed"},
		{"changed-command", func(t *testing.T, d docTree) docTree {
			return d.edit(t, composeFile, `command: ["workloads", "seed", "-apply"]`, `command: ["workloads", "seed", "-apply", "-x"]`)
		}, "workloads seed -apply -x"},
		{"writable-mount", func(t *testing.T, d docTree) docTree {
			return d.edit(t, composeFile, `        source: workload_bootstrap
        target: /run/workload
        read_only: true`, `        source: workload_bootstrap
        target: /run/workload
        read_only: false`)
		}, "not read-only"},
	})
}
