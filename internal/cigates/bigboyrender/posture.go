package bigboyrender

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
)

// pgPosture: `pg-posture api|domain <old-sha> <new-sha> <pg-expect-out-file>`.
//
// Reads a postgres posture function (RequiredTables {table, insert, update, delete}, SELECT implied, and
// ColumnScoped {table, column, privilege}) at two git shas of BIGBOY_OPS_REPO and appends every privilege ADDED
// between them to the out file as an expectation line for pg-grants-check.sh:
//
//	devhealth_<role> <table> <PRIV>[,<PRIV>]            (has_table_privilege read-back)
//	devhealth_<role> <table>.<column> <PRIV>[,<PRIV>]   (has_column_privilege read-back)
//
// Scope: an added-privileges read-back. It names what a cut grants and proves the grants are held; it cannot see
// an unwanted extra (a table-wide UPDATE also makes has_column_privilege true). Exactness is the readiness gate's
// job (postgres.CheckRolePosture / CheckAPIAuthorization).
func pgPosture(args []string, stdout, stderr io.Writer) int {
	if len(args) < 4 {
		fmt.Fprintln(stderr, "usage: cigate bigboy pg-posture api|domain <old-sha> <new-sha> <pg-expect-out-file>")
		return 2
	}
	var role, gofile, fn, unparsable string
	switch args[0] {
	case "api":
		role, gofile, fn, unparsable = "api", "api_authorization.go", "apiPosture", "PG_POSTURE_DELTA unparsable (manifest missing at a sha)"
	case "domain":
		role, gofile, fn, unparsable = "domain", "domain_authorization.go", "domainPosture", "PG_DOMAIN_POSTURE_DELTA unparsable"
	default:
		fmt.Fprintf(stderr, "cigate bigboy pg-posture: unknown role %q (api or domain)\n", args[0])
		return 2
	}
	oldSHA, newSHA, outFile := args[1], args[2], args[3]
	repo := os.Getenv("BIGBOY_OPS_REPO")
	before, okBefore := loadPosture(repo, oldSHA, gofile, fn)
	after, okAfter := loadPosture(repo, newSHA, gofile, fn)
	if !okBefore || !okAfter {
		fmt.Fprintln(stdout, unparsable)
		return 1
	}
	lines := postureDelta(role, before, after)
	prefix := "devhealth_" + role + " "
	trimmed := make([]string, len(lines))
	for i, line := range lines {
		trimmed[i] = strings.TrimPrefix(line, prefix)
	}
	fmt.Fprintf(stdout, "PG %s posture delta (privileges added): %s\n", role, strings.Join(trimmed, "; "))
	text := strings.Join(lines, "\n")
	if len(lines) > 0 {
		text += "\n"
	}
	f, err := os.OpenFile(outFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if _, err := f.WriteString(text); err != nil {
		_ = f.Close()
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := f.Close(); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

var pgPrivileges = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES"}

// posture is the tables (privilege -> held) and columns (privileges) of one posture function.
type posture struct {
	tables  map[string]map[string]bool
	columns map[string]map[string]bool
}

var (
	lineComment = regexp.MustCompile(`//[^\n]*`)
	tableRow    = regexp.MustCompile(`\{\s*"(\w+)"\s*,\s*(true|false)\s*,\s*(true|false)\s*,\s*(true|false)\s*\}`)
	columnRow   = regexp.MustCompile(`\{\s*"(\w+)"\s*,\s*"(\w+)"\s*,\s*"(SELECT|INSERT|UPDATE|REFERENCES)"\s*\}`)
	keyedField  = regexp.MustCompile(`\b(\w+)\s*:`)
)

// LoadPostureSource parses the posture function fn out of Go source; ok is false when it cannot be read.
func loadPostureSource(src, fn string) (posture, bool) {
	rest, found := afterFirst(src, "func "+fn+"()")
	if !found {
		return posture{}, false
	}
	body := beforeFirst(rest, "\nfunc ")
	body = lineComment.ReplaceAllString(body, "")
	p := posture{tables: map[string]map[string]bool{}, columns: map[string]map[string]bool{}}
	tablesSeen := 0
	for _, m := range tableRow.FindAllStringSubmatch(body, -1) {
		tablesSeen++
		p.tables[m[1]] = map[string]bool{"SELECT": true, "INSERT": m[2] == "true", "UPDATE": m[3] == "true", "DELETE": m[4] == "true"}
	}
	columnRows := 0
	for _, m := range columnRow.FindAllStringSubmatch(body, -1) {
		key := m[1] + "." + m[2]
		if p.columns[key] == nil {
			p.columns[key] = map[string]bool{}
		}
		p.columns[key][m[3]] = true
		columnRows++
	}
	// A posture function whose rows the patterns do not recognise must fail, never read as "no privileges".
	// Every `{` of the body after its signature line is a row, except the struct and slice literals that hold
	// the rows (a `{` that follows an identifier or `]`); each row must be one of the two recognised shapes.
	rows := ""
	if _, after, ok := strings.Cut(body, "\n"); ok {
		rows = after
	}
	literalRows := 0
	for i := 0; i < len(rows); i++ {
		if rows[i] != '{' {
			continue
		}
		if i > 0 && (isWord(rows[i-1]) || rows[i-1] == ']') {
			continue
		}
		literalRows++
	}
	// The read-back covers tables and columns only: any other keyed grant list, whatever its value
	// (RequiredSequences: []string{...}, a name or a call) is a grant it cannot read back, so it must fail
	// rather than vanish from the delta.
	for _, loc := range keyedField.FindAllStringSubmatchIndex(body, -1) {
		if loc[1] < len(body) && body[loc[1]] == '=' {
			continue
		}
		if key := body[loc[2]:loc[3]]; key != "RequiredTables" && key != "ColumnScoped" {
			return posture{}, false
		}
	}
	if len(p.tables) == 0 || literalRows != tablesSeen+columnRows {
		return posture{}, false
	}
	return p, true
}

func isWord(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// loadPosture reads posture fn of gofile at sha with `git -C repo show`; ok is false when it cannot be read.
func loadPosture(repo, sha, gofile, fn string) (posture, bool) {
	if repo == "" {
		return posture{}, false
	}
	cmd := exec.Command("git", "-C", repo, "show", sha+":internal/storage/postgres/"+gofile)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return posture{}, false
	}
	return loadPostureSource(out.String(), fn)
}

// postureDelta is the expectation lines of the privileges after has and before lacks, tables then columns, sorted.
func postureDelta(role string, before, after posture) []string {
	var lines []string
	for _, table := range sortedKeys(after.tables) {
		had := before.tables[table]
		var added []string
		for _, p := range pgPrivileges {
			if after.tables[table][p] && !had[p] {
				added = append(added, p)
			}
		}
		if len(added) > 0 {
			lines = append(lines, fmt.Sprintf("devhealth_%s %s %s", role, table, strings.Join(added, ",")))
		}
	}
	for _, column := range sortedKeys(after.columns) {
		had := before.columns[column]
		var added []string
		for _, p := range pgPrivileges {
			if after.columns[column][p] && !had[p] {
				added = append(added, p)
			}
		}
		if len(added) > 0 {
			lines = append(lines, fmt.Sprintf("devhealth_%s %s %s", role, column, strings.Join(added, ",")))
		}
	}
	return lines
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
