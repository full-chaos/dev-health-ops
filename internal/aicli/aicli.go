// Package aicli is the `ai` group of dho: administration of the org-level AI
// tool allowlist (`ai allowlist set|list`), which `dev-hops ai allowlist` did.
//
// Entries are admin-seeded rows of the ClickHouse table ai_tool_allowlist, a
// ReplacingMergeTree versioned by computed_at, so an update is a plain
// re-insert of the same (org_id, tool_name, model_name) and a reader takes the
// latest version. Go's AI governance compute only READS the allowlist; this is
// its write path.
package aicli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// Statuses a policy may be set to (ToolAllowlistStatus minus the derived
// "unknown").
var Statuses = []string{"allowed", "disallowed", "deprecated"}

// Command is the `ai` group.
func Command() cli.Command {
	return cli.Command{
		Name:    "ai",
		Summary: "AI governance administration",
		Kind:    cli.Group,
		Children: []cli.Command{{
			Name:    "allowlist",
			Summary: "the org-level AI tool allowlist",
			Kind:    cli.Group,
			Children: []cli.Command{
				{Name: "set", Summary: "create or update an allowlist entry", Kind: cli.Verb, Run: runSet},
				{Name: "list", Summary: "show the latest allowlist entries for the org", Kind: cli.Verb, Run: runList},
			},
		}},
	}
}

// Entry is one allowlist policy row. A nil ModelName is the wildcard (the
// policy applies to every model of the tool); a nil Reason is no rationale.
type Entry struct {
	OrgID     string
	ToolName  string
	ModelName *string
	Status    string
	Reason    *string
}

// NewEntry normalizes as AIToolAllowlistEntry does: the tool is trimmed and
// must be non-empty; a blank model is the wildcard, because ” and NULL share
// the table's ReplacingMergeTree key (ORDER BY ifNull(model_name, ”)) and a
// blank "exact" row would silently replace the wildcard policy.
func NewEntry(orgID, tool string, model *string, status string, reason *string) (Entry, error) {
	tool = strings.TrimSpace(tool)
	if tool == "" {
		return Entry{}, errors.New("tool_name must be non-empty")
	}
	var normalizedModel *string
	if model != nil {
		if trimmed := strings.TrimSpace(*model); trimmed != "" {
			normalizedModel = &trimmed
		}
	}
	valid := false
	for _, known := range Statuses {
		valid = valid || status == known
	}
	if !valid {
		return Entry{}, fmt.Errorf("status %q is not one of %s", status, strings.Join(Statuses, ", "))
	}
	return Entry{OrgID: orgID, ToolName: tool, ModelName: normalizedModel, Status: status, Reason: reason}, nil
}

// Insert writes the entry as one row, versioned now.
func Insert(ctx context.Context, conn driver.Conn, entry Entry, now time.Time) error {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO ai_tool_allowlist (org_id, tool_name, model_name, status, reason, updated_at, computed_at)")
	if err != nil {
		return err
	}
	stamp := now.UTC()
	if err := batch.Append(entry.OrgID, entry.ToolName, entry.ModelName, entry.Status, entry.Reason, stamp, stamp); err != nil {
		_ = batch.Abort()
		return err
	}
	return batch.Send()
}

// Row is one listed policy: the latest version of a (tool, model).
type Row struct {
	ToolName  string
	ModelName *string
	Status    string
	Reason    *string
	UpdatedAt time.Time
}

// List reads the latest version of every entry of the org, ordered by tool then
// model (a wildcard last within its tool).
func List(ctx context.Context, conn driver.Conn, orgID string) ([]Row, error) {
	rows, err := conn.Query(ctx, "SELECT tool_name,"+
		" model_name,"+
		" argMax(status, computed_at) AS status,"+
		" argMax(reason, computed_at) AS reason,"+
		" max(updated_at) AS updated_at"+
		" FROM ai_tool_allowlist"+
		" WHERE org_id = {org_id:String}"+
		" GROUP BY tool_name, model_name"+
		" ORDER BY tool_name, model_name", clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var row Row
		if err := rows.Scan(&row.ToolName, &row.ModelName, &row.Status, &row.Reason, &row.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// Render is the text `list` prints.
func Render(orgID string, rows []Row) string {
	if len(rows) == 0 {
		return fmt.Sprintf("No allowlist entries for org %s.\n", orgID)
	}
	var out strings.Builder
	fmt.Fprintf(&out, "AI tool allowlist for org %s:\n", orgID)
	for _, row := range rows {
		model := "*"
		if row.ModelName != nil && *row.ModelName != "" {
			model = *row.ModelName
		}
		line := fmt.Sprintf("  %s / %s: %s", row.ToolName, model, row.Status)
		if row.Reason != nil && *row.Reason != "" {
			line += " — " + *row.Reason
		}
		out.WriteString(line + "\n")
	}
	return out.String()
}

const setUsage = `Usage: dho ai allowlist set --tool <name> --status <allowed|disallowed|deprecated> [--model <name>] [--reason <text>] [--org <uuid>]

Creates or updates one entry of the org's AI tool allowlist. Omit --model to
apply the policy to every model of the tool; a blank --model is the same
wildcard.

Environment:
  ORG_ID                       the organization, when --org is not given
  CLICKHOUSE_URI (or _FILE, or the DEV_HEALTH_CH_* component form)   ClickHouse DSN, native protocol
`

const listUsage = `Usage: dho ai allowlist list [--org <uuid>]

Shows the latest allowlist entries for the org, one line per tool and model.

Environment:
  ORG_ID                       the organization, when --org is not given
  CLICKHOUSE_URI (or _FILE, or the DEV_HEALTH_CH_* component form)   ClickHouse DSN, native protocol
`

func writeError(stderr io.Writer, exit int, code, detail string) int {
	fmt.Fprintf(stderr, "{\"error\":{\"code\":%q,\"detail\":%q}}\n", code, detail)
	return exit
}

// connect resolves the DSN the way `dho migrate clickhouse` does and opens the
// native client; the returned func closes it.
func connect(ctx context.Context, env cli.Env) (driver.Conn, func(), secrets.Boundary, int) {
	dsn, configured, err := config.ResolveDSN(env.Lookup, "CLICKHOUSE_URI", config.ClickHouseSpec)
	if err != nil {
		return nil, nil, secrets.Boundary{}, writeError(env.Stderr, cli.ExitFailure, "configuration_error", err.Error())
	}
	if !configured {
		return nil, nil, secrets.Boundary{}, writeError(env.Stderr, cli.ExitFailure, "configuration_error", "CLICKHOUSE_URI is required")
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	chConfig := chstorage.DefaultConfig(dsn.Reveal())
	chConfig.MaxOpenConns, chConfig.MaxIdleConns = 1, 1
	conn, err := chstorage.Open(ctx, chConfig)
	if err != nil {
		return nil, nil, secrets.Boundary{}, writeError(env.Stderr, cli.ExitFailure, "clickhouse_unavailable", boundary.Redact(err).Error())
	}
	return conn, func() { _ = conn.Close() }, boundary, cli.ExitOK
}

func resolveOrg(env cli.Env, flagValue string) string {
	if org := strings.TrimSpace(flagValue); org != "" {
		return org
	}
	value, _ := env.Lookup("ORG_ID")
	return strings.TrimSpace(value)
}

func runSet(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho ai allowlist set", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, setUsage) }
	tool := flags.String("tool", "", "tool name (e.g. claude-code)")
	model := flags.String("model", "", "optional model name; omit for every model of the tool")
	status := flags.String("status", "", "policy status: "+strings.Join(Statuses, "|"))
	reason := flags.String("reason", "", "optional policy rationale")
	org := flags.String("org", "", "the organization id")
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted")
		return cli.ExitUsage
	}
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}
	given := map[string]bool{}
	flags.Visit(func(set *flag.Flag) { given[set.Name] = true })
	orgID := resolveOrg(env, *org)
	if orgID == "" {
		fmt.Fprintln(env.Stderr, "argument error: --org (or ORG_ID) is required for allowlist commands")
		return cli.ExitUsage
	}
	if !given["tool"] || !given["status"] {
		fmt.Fprintln(env.Stderr, "argument error: --tool and --status are required")
		return cli.ExitUsage
	}
	var modelValue, reasonValue *string
	if given["model"] {
		modelValue = model
	}
	if given["reason"] {
		reasonValue = reason
	}
	entry, err := NewEntry(orgID, *tool, modelValue, *status, reasonValue)
	if err != nil {
		fmt.Fprintf(env.Stderr, "argument error: invalid allowlist entry: %v\n", err)
		return cli.ExitUsage
	}
	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	if modelValue != nil && entry.ModelName == nil {
		logger.Info("blank --model treated as wildcard (applies to every model)")
	}
	conn, closeConn, boundary, code := connect(ctx, env)
	if conn == nil {
		return code
	}
	defer closeConn()
	if err := Insert(ctx, conn, entry, time.Now()); err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "write_failed", boundary.Redact(err).Error())
	}
	scope := entry.ToolName
	if entry.ModelName != nil {
		scope += "/" + *entry.ModelName
	}
	logger.Info("allowlist updated", "org", orgID, "entry", scope, "status", entry.Status)
	return cli.ExitOK
}

func runList(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho ai allowlist list", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, listUsage) }
	org := flags.String("org", "", "the organization id")
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted")
		return cli.ExitUsage
	}
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}
	orgID := resolveOrg(env, *org)
	if orgID == "" {
		fmt.Fprintln(env.Stderr, "argument error: --org (or ORG_ID) is required for allowlist commands")
		return cli.ExitUsage
	}
	conn, closeConn, boundary, code := connect(ctx, env)
	if conn == nil {
		return code
	}
	defer closeConn()
	rows, err := List(ctx, conn, orgID)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "read_failed", boundary.Redact(err).Error())
	}
	if _, err := io.WriteString(env.Stdout, Render(orgID, rows)); err != nil {
		fmt.Fprintln(env.Stderr, "could not write the result")
		return cli.ExitFailure
	}
	return cli.ExitOK
}
