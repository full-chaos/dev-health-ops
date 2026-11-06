// Package exceptionledger is the census of the Auth Control Plane's temporary exceptions and named gaps
// (CHAOS-11258, W11-27). Every such exception is one row of ci/auth_exception_ledger.tsv with an anchor in the tree
// and an owner: an open ticket that removes it, or a ruling for a permanent row. The census refuses an exception the
// ledger does not hold, a row whose anchor is gone, and a temporary row whose owner is missing or closed.
//
// owner_state is a point reading of the owner's Linear state, written by whoever re-reads Linear (the W11-28
// sign-off, a re-home). The census cannot see Linear: a ticket closed outside a pull request is not caught here.
package exceptionledger

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// LedgerPath is the ledger, relative to the repository root.
const LedgerPath = "ci/auth_exception_ledger.tsv"

// Kinds of a ledger row.
const (
	KindTemporary = "temporary"
	KindDecision  = "decision"
	KindPermanent = "permanent"
)

// MarkerScope are the directories (and single files) whose Go sources are scanned for exception markers.
var MarkerScope = []string{
	"cmd/auth-migrate",
	"cmd/auth-service",
	"contracts/auth",
	"internal/api/policy",
	"internal/apiservice/accessverify.go",
	"internal/auth",
	"internal/edgetokenmint",
	"internal/queryapi/pdp",
	"internal/queryapi/principal",
	"internal/storage/postgres/authschema",
	"internal/testsupport/authrows",
	"internal/testsupport/sessionscenario",
}

// ProfilesGlob are the endpoint-profile files whose legacy and declared-gap rows are counted.
const ProfilesGlob = "contracts/auth/v1/endpoint-profiles.*.json"

// PDPFile holds the query-api PDP exempt class list.
const PDPFile = "internal/queryapi/pdp/pdp.go"

// RefusalsGlob are the parity tables whose divergent rows are named gaps.
const RefusalsGlob = "ci/*_refusals.tsv"

// VulnExceptionsFile is the govulncheck exception table; it is optional.
const VulnExceptionsFile = "ci/auth-vuln-exceptions.tsv"

var (
	commentMarker = regexp.MustCompile(`\bTEMPORARY:|\bis TEMPORARY\b|//.*\b[Ii]nterim\b`)
	fieldMarker   = regexp.MustCompile(`\b(Tracked|RemovedBy):\s*"`)
	idPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	ticketPattern = regexp.MustCompile(`^CHAOS-[0-9]+$`)
	rulingPattern = regexp.MustCompile(`^D[0-9]+$`)
	exactPrefixes = []string{"count:", "ExemptClasses:", "divergent:", "vuln:"}
)

// Row is one ledger row.
type Row struct {
	Line                                              int
	ID, Kind, Path, Needle, Owner, OwnerState, Reason string
}

func (r Row) exact() bool {
	for _, prefix := range exactPrefixes {
		if strings.HasPrefix(r.Needle, prefix) {
			return true
		}
	}
	return false
}

// Site is one exception the census found in the tree. An exact site is matched by a row whose needle equals Key; a
// text site by a row whose needle is a substring of Key (the source line).
type Site struct {
	Path  string
	Line  int
	Key   string
	Exact bool
}

func (s Site) String() string {
	if s.Line > 0 {
		return fmt.Sprintf("%s:%d %s", s.Path, s.Line, strings.TrimSpace(s.Key))
	}
	return fmt.Sprintf("%s %s", s.Path, s.Key)
}

// ParseLedger reads the ledger text. Defects name the line.
func ParseLedger(data []byte) ([]Row, []string) {
	var rows []Row
	var defects []string
	seen := map[string]int{}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		text := scanner.Text()
		if strings.TrimSpace(text) == "" || strings.HasPrefix(text, "#") {
			continue
		}
		cols := strings.Split(text, "\t")
		if len(cols) != 7 {
			defects = append(defects, fmt.Sprintf("ledger:%d: want 7 tab-separated columns (id, kind, path, needle, owner, owner_state, reason), got %d", line, len(cols)))
			continue
		}
		row := Row{Line: line, ID: cols[0], Kind: cols[1], Path: cols[2], Needle: cols[3], Owner: cols[4], OwnerState: cols[5], Reason: cols[6]}
		if first, dup := seen[row.ID]; dup {
			defects = append(defects, fmt.Sprintf("ledger:%d: id %q repeats line %d", line, row.ID, first))
			continue
		}
		seen[row.ID] = line
		defects = append(defects, rowDefects(row)...)
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		defects = append(defects, fmt.Sprintf("ledger: %v", err))
	}
	return rows, defects
}

func rowDefects(r Row) []string {
	var out []string
	at := fmt.Sprintf("ledger:%d %s", r.Line, r.ID)
	if !idPattern.MatchString(r.ID) {
		out = append(out, fmt.Sprintf("%s: id is not lower-case kebab", at))
	}
	if r.Path == "" || filepath.IsAbs(r.Path) || strings.Contains(r.Path, "..") {
		out = append(out, fmt.Sprintf("%s: path %q is not a repository-relative path", at, r.Path))
	}
	if strings.TrimSpace(r.Needle) == "" {
		out = append(out, fmt.Sprintf("%s: empty needle", at))
	}
	switch r.Kind {
	case KindTemporary, KindDecision:
		valid := ticketPattern.MatchString(r.Owner) || (r.Kind == KindDecision && rulingPattern.MatchString(r.Owner))
		switch {
		case !valid:
			out = append(out, fmt.Sprintf("%s: a %s row needs an owner ticket (CHAOS-n), got %q", at, r.Kind, r.Owner))
		case r.OwnerState == "closed":
			out = append(out, fmt.Sprintf("%s: owner %s is closed; re-home the row to the open ticket that removes it", at, r.Owner))
		case r.OwnerState != "open":
			out = append(out, fmt.Sprintf("%s: owner_state %q, want open", at, r.OwnerState))
		}
	case KindPermanent:
		if r.Owner != "-" && !ticketPattern.MatchString(r.Owner) && !rulingPattern.MatchString(r.Owner) {
			out = append(out, fmt.Sprintf("%s: a permanent row's owner is a ruling (Dn), a ticket or -, got %q", at, r.Owner))
		}
		if r.OwnerState != "-" {
			out = append(out, fmt.Sprintf("%s: a permanent row has owner_state -, got %q", at, r.OwnerState))
		}
		if len(strings.TrimSpace(r.Reason)) < 30 {
			out = append(out, fmt.Sprintf("%s: a permanent row states why it is permanent (reason under 30 characters)", at))
		}
	default:
		out = append(out, fmt.Sprintf("%s: kind %q, want temporary, decision or permanent", at, r.Kind))
	}
	if strings.TrimSpace(r.Reason) == "" {
		out = append(out, fmt.Sprintf("%s: empty reason", at))
	}
	return out
}

// Discover lists every exception site under root.
func Discover(root string) ([]Site, error) {
	var sites []Site
	for _, discover := range []func(string) ([]Site, error){markerSites, profileSites, exemptClassSites, refusalSites, vulnSites} {
		found, err := discover(root)
		if err != nil {
			return nil, err
		}
		sites = append(sites, found...)
	}
	sort.Slice(sites, func(i, j int) bool {
		if sites[i].Path != sites[j].Path {
			return sites[i].Path < sites[j].Path
		}
		if sites[i].Line != sites[j].Line {
			return sites[i].Line < sites[j].Line
		}
		return sites[i].Key < sites[j].Key
	})
	return sites, nil
}

func markerSites(root string) ([]Site, error) {
	var sites []Site
	for _, scope := range MarkerScope {
		base := filepath.Join(root, filepath.FromSlash(scope))
		info, err := os.Stat(base)
		if err != nil {
			return nil, fmt.Errorf("marker scope %s: %w", scope, err)
		}
		files := 0
		walk := func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			files++
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			found, err := fileMarkers(path, filepath.ToSlash(rel))
			sites = append(sites, found...)
			return err
		}
		if info.IsDir() {
			err = filepath.WalkDir(base, walk)
		} else {
			err = walk(base, fs.FileInfoToDirEntry(info), nil)
		}
		if err != nil {
			return nil, err
		}
		if files == 0 {
			return nil, fmt.Errorf("marker scope %s holds no Go file; the scan would be vacuous", scope)
		}
	}
	return sites, nil
}

func fileMarkers(path, rel string) ([]Site, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	test := strings.HasSuffix(rel, "_test.go")
	var sites []Site
	for i, line := range strings.Split(string(data), "\n") {
		if commentMarker.MatchString(line) || (!test && fieldMarker.MatchString(line)) {
			sites = append(sites, Site{Path: rel, Line: i + 1, Key: line})
		}
	}
	return sites, nil
}

func profileSites(root string) ([]Site, error) {
	files, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(ProfilesGlob)))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no endpoint-profile file matches %s", ProfilesGlob)
	}
	var sites []Site
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var doc struct {
			Rows []json.RawMessage `json:"rows"`
		}
		if err := json.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		if len(doc.Rows) == 0 {
			return nil, fmt.Errorf("%s: no rows; the count would be vacuous", file)
		}
		legacy, gaps := 0, 0
		for _, raw := range doc.Rows {
			var row struct {
				AccessTokenValidation *struct {
					LegacyHS256 bool `json:"legacy_hs256"`
				} `json:"access_token_validation"`
			}
			if err := json.Unmarshal(raw, &row); err != nil {
				return nil, fmt.Errorf("%s: %w", file, err)
			}
			if row.AccessTokenValidation != nil && row.AccessTokenValidation.LegacyHS256 {
				legacy++
			}
			if bytes.Contains(raw, []byte("declared gap")) {
				gaps++
			}
		}
		rel, err := filepath.Rel(root, file)
		if err != nil {
			return nil, err
		}
		rel = filepath.ToSlash(rel)
		if legacy > 0 {
			sites = append(sites, Site{Path: rel, Key: fmt.Sprintf("count:legacy_hs256=%d", legacy), Exact: true})
		}
		if gaps > 0 {
			sites = append(sites, Site{Path: rel, Key: fmt.Sprintf("count:declared_gap_rows=%d", gaps), Exact: true})
		}
	}
	return sites, nil
}

func exemptClassSites(root string) ([]Site, error) {
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, filepath.FromSlash(PDPFile)), nil, 0)
	if err != nil {
		return nil, err
	}
	var sites []Site
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range spec.Names {
			if name.Name != "ExemptClasses" || i >= len(spec.Values) {
				continue
			}
			found = true
			literal, ok := spec.Values[i].(*ast.CompositeLit)
			if !ok {
				sites = append(sites, Site{Path: PDPFile, Key: "ExemptClasses:<not a literal>", Exact: true})
				continue
			}
			for _, element := range literal.Elts {
				sites = append(sites, Site{Path: PDPFile, Key: "ExemptClasses:" + exprText(element), Exact: true})
			}
		}
		return true
	})
	if !found {
		return nil, fmt.Errorf("%s: no ExemptClasses declaration; the exempt list moved or the scan is stale", PDPFile)
	}
	return sites, nil
}

func exprText(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.BasicLit:
		return typed.Value
	case *ast.SelectorExpr:
		return exprText(typed.X) + "." + typed.Sel.Name
	}
	return fmt.Sprintf("<%T>", expr)
}

func refusalSites(root string) ([]Site, error) {
	files, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(RefusalsGlob)))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no parity table matches %s", RefusalsGlob)
	}
	var sites []Site
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		rel, err := filepath.Rel(root, file)
		if err != nil {
			return nil, err
		}
		for i, line := range strings.Split(string(data), "\n") {
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			cols := strings.Split(line, "\t")
			for _, col := range cols[1:] {
				if col == "divergent" {
					sites = append(sites, Site{Path: filepath.ToSlash(rel), Line: i + 1, Key: "divergent:" + cols[0], Exact: true})
					break
				}
			}
		}
	}
	return sites, nil
}

func vulnSites(root string) ([]Site, error) {
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(VulnExceptionsFile)))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var sites []Site
	for i, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cols := strings.Split(line, "\t")
		if len(cols) < 3 {
			sites = append(sites, Site{Path: VulnExceptionsFile, Line: i + 1, Key: "vuln:<malformed>", Exact: true})
			continue
		}
		sites = append(sites, Site{Path: VulnExceptionsFile, Line: i + 1, Key: "vuln:" + strings.Join(cols[:3], " "), Exact: true})
	}
	return sites, nil
}

// Check runs the census of the ledger text against the tree under root and returns its defects.
func Check(root string, ledger []byte) ([]string, error) {
	rows, defects := ParseLedger(ledger)
	sites, err := Discover(root)
	if err != nil {
		return nil, err
	}
	matched := make([]bool, len(rows))
	for _, site := range sites {
		hit := false
		for i, row := range rows {
			if row.Path != site.Path || row.exact() != site.Exact {
				continue
			}
			if (site.Exact && row.Needle == site.Key) || (!site.Exact && strings.Contains(site.Key, row.Needle)) {
				matched[i], hit = true, true
			}
		}
		if !hit {
			defects = append(defects, fmt.Sprintf("unregistered exception %s: add a ledger row with its owner", site))
		}
	}
	for i, row := range rows {
		if matched[i] {
			continue
		}
		at := fmt.Sprintf("ledger:%d %s", row.Line, row.ID)
		if row.exact() {
			defects = append(defects, fmt.Sprintf("%s: stale row, no exception %q at %s", at, row.Needle, row.Path))
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(row.Path)))
		switch {
		case err != nil:
			defects = append(defects, fmt.Sprintf("%s: stale row, %s unreadable: %v", at, row.Path, err))
		case !bytes.Contains(data, []byte(row.Needle)):
			defects = append(defects, fmt.Sprintf("%s: stale row, needle %q not in %s", at, row.Needle, row.Path))
		}
	}
	return defects, nil
}
