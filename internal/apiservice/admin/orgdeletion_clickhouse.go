package admin

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// The org deletion fails closed on ClickHouse: it never answers success while
// the organization's analytics rows may stay. Before anything is counted,
// revoked or deleted, the ClickHouse login must connect, hold SELECT and
// ALTER DELETE on its whole database, and find the org_id tables; else the
// delete is refused (route 503, CLI exit 1) with nothing touched. A purge
// failure after the Postgres commit answers 500 (CLI exit 1); running the
// delete again purges ClickHouse by org_id, without the organization row.
const (
	orgDeletionRefusedDetail    = "ClickHouse is not configured, not reachable or not permitted to purge; organization deletion refused. Nothing was deleted."
	orgDeletionIncompleteDetail = "Organization Postgres data was deleted, but the ClickHouse purge did not complete. Run the delete again."
)

// Refusal reasons: the reason field of the "admin: org deletion refused" line.
const (
	refusalDSNUnset         = "dsn_unset"
	refusalOpenFailed       = "open_failed"
	refusalPrivilegeMissing = "privilege_missing"
	refusalCatalogEmpty     = "catalog_empty"
	refusalCountFailed      = "count_failed"
)

// orgDeletionRefusedError is a delete refused before it changed anything.
type orgDeletionRefusedError struct {
	reason string
	err    error
}

func (e *orgDeletionRefusedError) Error() string {
	return "org deletion refused (" + e.reason + "): " + e.err.Error()
}
func (e *orgDeletionRefusedError) Unwrap() error { return e.err }

// orgDeletionIncompleteError is a ClickHouse purge that failed for tables after
// the Postgres purge committed.
type orgDeletionIncompleteError struct {
	tables []string
	err    error
}

func (e *orgDeletionIncompleteError) Error() string {
	return "org deletion clickhouse purge incomplete (" + strings.Join(e.tables, ", ") + "): " + e.err.Error()
}
func (e *orgDeletionIncompleteError) Unwrap() error { return e.err }

// orgDeletionClickHouse is the purge connection the preflight proved usable,
// and the org_id tables it found.
type orgDeletionClickHouse struct {
	conn   driver.Conn
	tables []ChTable
}

// orgDeletionClickHousePreflight connects (Open pings), checks the login's
// privileges and discovers the org_id tables. Any miss is an
// *orgDeletionRefusedError.
func (h *handlers) orgDeletionClickHousePreflight(ctx context.Context) (*orgDeletionClickHouse, error) {
	if h.clickHouseDSN == "" {
		return nil, &orgDeletionRefusedError{refusalDSNUnset, errors.New("ClickHouse URI not configured")}
	}
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(h.clickHouseDSN))
	if err != nil {
		return nil, &orgDeletionRefusedError{refusalOpenFailed, err}
	}
	if err := h.checkOrgDeletionPrivileges(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, &orgDeletionRefusedError{refusalPrivilegeMissing, err}
	}
	tables, err := DiscoverClickHouseOrgTables(ctx, conn)
	if err == nil && len(tables) == 0 {
		err = errors.New("no table of the database has an org_id column")
	}
	if err != nil {
		_ = conn.Close()
		return nil, &orgDeletionRefusedError{refusalCatalogEmpty, err}
	}
	return &orgDeletionClickHouse{conn: conn, tables: tables}, nil
}

// OrgDeletionPreflight runs the org deletion's ClickHouse preflight once (go-api
// start): "" when DELETE /orgs/{org_id} could purge, else the refusal reason.
func OrgDeletionPreflight(ctx context.Context, dsn string) string {
	h := &handlers{clickHouseDSN: dsn}
	ch, err := h.orgDeletionClickHousePreflight(ctx)
	if err != nil {
		var refused *orgDeletionRefusedError
		if errors.As(err, &refused) {
			return refused.reason
		}
		return "preflight_failed"
	}
	_ = ch.conn.Close()
	return ""
}

// checkOrgDeletionPrivileges checks the login's effective grants for the
// connection's database. A grant read that fails is a refusal too.
func (h *handlers) checkOrgDeletionPrivileges(ctx context.Context, conn driver.Conn) error {
	var database string
	if err := conn.QueryRow(ctx, "SELECT currentDatabase()").Scan(&database); err != nil {
		return fmt.Errorf("read current database: %w", err)
	}
	read := h.readGrants
	if read == nil {
		read = showGrantsFinal
	}
	lines, err := read(ctx, conn)
	if err != nil {
		return err
	}
	return orgDeletionGrantProblem(database, lines)
}

// showGrantsFinal reads the login's own and its roles' grants (SHOW GRANTS FOR
// CURRENT_USER FINAL: self-inspection needs no privilege, unlike system.grants).
func showGrantsFinal(ctx context.Context, conn driver.Conn) ([]string, error) {
	rows, err := conn.Query(ctx, "SHOW GRANTS FOR CURRENT_USER FINAL")
	if err != nil {
		return nil, fmt.Errorf("read current grants: %w", err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, fmt.Errorf("scan grant row: %w", err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read grants: %w", err)
	}
	return lines, nil
}

var grantLinePattern = regexp.MustCompile(`(?i)^(GRANT|REVOKE)\s+(.+?)\s+ON\s+(\S+)\s+(?:TO|FROM)\s`)

var (
	selectCoveringPrivileges = map[string]bool{"SELECT": true, "ALL": true, "ALL PRIVILEGES": true}
	deleteCoveringPrivileges = map[string]bool{"ALTER DELETE": true, "DELETE": true, "ALTER TABLE": true, "ALTER": true, "ALL": true, "ALL PRIVILEGES": true}
)

// orgDeletionGrantProblem is nil when lines (SHOW GRANTS output) grant SELECT
// and ALTER DELETE on database.* (or *.*) and revoke neither on any part of
// database. A column-level SELECT, a table-level grant or a wildcard pattern
// does not count: the purge reads and deletes every org_id table.
func orgDeletionGrantProblem(database string, lines []string) error {
	var canSelect, canDelete bool
	var revokes []string
	for _, line := range lines {
		match := grantLinePattern.FindStringSubmatch(strings.TrimSpace(line))
		if match == nil {
			continue
		}
		db, table, ok := grantTarget(match[3])
		if !ok || (!db.all && db.name != database) {
			continue
		}
		privileges := grantPrivileges(match[2])
		if strings.EqualFold(match[1], "REVOKE") {
			for _, privilege := range privileges {
				name, _, _ := strings.Cut(privilege, "(")
				name = strings.TrimSpace(name)
				if selectCoveringPrivileges[name] || deleteCoveringPrivileges[name] {
					revokes = append(revokes, strings.TrimSpace(line))
					break
				}
			}
			continue
		}
		if !table.all {
			continue
		}
		for _, privilege := range privileges {
			canSelect = canSelect || selectCoveringPrivileges[privilege]
			canDelete = canDelete || deleteCoveringPrivileges[privilege]
		}
	}
	var problems []string
	if !canSelect {
		problems = append(problems, "no SELECT on "+database+".*")
	}
	if !canDelete {
		problems = append(problems, "no ALTER DELETE on "+database+".*")
	}
	if len(revokes) > 0 {
		problems = append(problems, "partial revoke: "+strings.Join(revokes, "; "))
	}
	if len(problems) > 0 {
		return fmt.Errorf("the ClickHouse login cannot purge database %s: %s", database, strings.Join(problems, ", "))
	}
	return nil
}

// grantName is one part of a grant target: all is the unquoted wildcard *; a
// backtick-quoted part is always a name (`*` is a table named *).
type grantName struct {
	name string
	all  bool
}

// grantTarget splits "db.table" as SHOW GRANTS prints it. A target it cannot
// read for certain (an unbalanced backtick, an unknown escape, a stray dot or
// backtick) is not ok, so its grant never counts.
func grantTarget(target string) (database, table grantName, ok bool) {
	database, rest, ok := readGrantName(target, true)
	if !ok || !strings.HasPrefix(rest, ".") {
		return grantName{}, grantName{}, false
	}
	table, rest, ok = readGrantName(rest[1:], false)
	if !ok || rest != "" {
		return grantName{}, grantName{}, false
	}
	return database, table, true
}

// readGrantName reads one target part: a backtick-quoted name, or an unquoted
// run up to the first dot (stopAtDot) or the end. Inside backticks a doubled
// backtick and the escapes \` and \\ are one literal character; any other
// escape or an unbalanced backtick is not ok.
func readGrantName(text string, stopAtDot bool) (grantName, string, bool) {
	if strings.HasPrefix(text, "`") {
		var name strings.Builder
		for i := 1; i < len(text); i++ {
			switch c := text[i]; c {
			case '`':
				if i+1 < len(text) && text[i+1] == '`' {
					name.WriteByte('`')
					i++
					continue
				}
				if name.Len() == 0 {
					return grantName{}, "", false
				}
				return grantName{name: name.String()}, text[i+1:], true
			case '\\':
				if i+1 >= len(text) || (text[i+1] != '`' && text[i+1] != '\\') {
					return grantName{}, "", false
				}
				name.WriteByte(text[i+1])
				i++
			default:
				name.WriteByte(c)
			}
		}
		return grantName{}, "", false
	}
	part, rest := text, ""
	if stopAtDot {
		if i := strings.IndexByte(text, '.'); i >= 0 {
			part, rest = text[:i], text[i:]
		}
	}
	if part == "" || strings.ContainsAny(part, "`.\\") {
		return grantName{}, "", false
	}
	return grantName{name: part, all: part == "*"}, rest, true
}

// grantPrivileges splits a privilege list at top-level commas (a column list
// "SELECT(a, b)" stays one item), upper-cased with single spaces.
func grantPrivileges(text string) []string {
	var out []string
	depth, start := 0, 0
	for i, r := range text {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, normalizePrivilege(text[start:i]))
				start = i + 1
			}
		}
	}
	return append(out, normalizePrivilege(text[start:]))
}

func normalizePrivilege(text string) string {
	return strings.ToUpper(strings.Join(strings.Fields(text), " "))
}

// redactClickHouse is err's text without the DSN or its password.
func (h *handlers) redactClickHouse(err error) string {
	values := []string{h.clickHouseDSN}
	if parsed, parseErr := url.Parse(h.clickHouseDSN); parseErr == nil && parsed.User != nil {
		if password, ok := parsed.User.Password(); ok {
			values = append(values, password)
		}
	}
	return secrets.RedactValues(err.Error(), values...)
}

// logOrgDeletionRefused is the ERROR line of a refused delete.
func (h *handlers) logOrgDeletionRefused(ctx context.Context, orgID string, dryRun bool, refused *orgDeletionRefusedError) {
	h.logger.ErrorContext(ctx, "admin: org deletion refused",
		"org_id", orgID, "reason", refused.reason, "dry_run", dryRun, "error", h.redactClickHouse(refused.err))
}
