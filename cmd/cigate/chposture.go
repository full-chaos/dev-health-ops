package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

var chPostureUsage = `usage: cigate ch-posture compare --user ` + clickhouse.WorkerUser + ` --grants FILE

FILE is the TabSeparatedWithNames output of this SELECT for the user:

  clickhouse-client --param_user=` + clickhouse.WorkerUser + ` --format TabSeparatedWithNames --query "<SELECT>"

` + clickhouse.WorkerGrantsSelect + `

Line 1 is the header (the 8 column names, tab-separated); each other line is one
row: 8 tab-separated fields, \N for NULL, is_partial_revoke and grant_option 0
or 1. Exit 0: the rows are exactly the declared posture. Exit 1: they are not
(the problems are listed). Exit 2: usage, an unreadable file, or a user with no
declared posture (only ` + clickhouse.WorkerUser + ` is declared).
`

func chPosture(args []string, stdout, stderr io.Writer) int {
	flags := map[string]string{}
	if len(args) != 5 || args[0] != "compare" {
		fmt.Fprint(stderr, chPostureUsage)
		return 2
	}
	for i := 1; i < len(args); i += 2 {
		name := args[i]
		if _, repeated := flags[name]; repeated || (name != "--user" && name != "--grants") {
			fmt.Fprint(stderr, chPostureUsage)
			return 2
		}
		flags[name] = args[i+1]
	}
	user, path := flags["--user"], flags["--grants"]
	if user != clickhouse.WorkerUser {
		fmt.Fprintf(stderr, "ch-posture: user %q has no declared posture (declared: %s)\n", user, clickhouse.WorkerUser)
		return 2
	}
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(stderr, "ch-posture: %v\n", err)
		return 2
	}
	rows, err := parseSystemGrantsTSV(string(data))
	if err != nil {
		fmt.Fprintf(stderr, "ch-posture: %s: %v\n", path, err)
		return 2
	}
	if err := clickhouse.CompareWorkerGrants(rows); err != nil {
		fmt.Fprintf(stderr, "ch-posture: %s MISMATCH\n", user)
		problems := strings.TrimPrefix(err.Error(), clickhouse.ErrWorkerPostureMismatch.Error()+": ")
		for _, problem := range strings.Split(problems, "; ") {
			fmt.Fprintf(stderr, "  %s\n", problem)
		}
		return 1
	}
	fmt.Fprintf(stdout, "ch-posture: %s matches the declared posture (%d rows)\n", user, len(rows))
	return 0
}

func parseSystemGrantsTSV(text string) ([]clickhouse.SystemGrantRow, error) {
	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	if lines[0] != strings.Join(clickhouse.WorkerGrantsColumns, "\t") {
		return nil, fmt.Errorf("line 1: header %q, want %q", lines[0], strings.Join(clickhouse.WorkerGrantsColumns, "\t"))
	}
	rows := make([]clickhouse.SystemGrantRow, 0, len(lines)-1)
	for index, line := range lines[1:] {
		number := index + 2
		fields := strings.Split(line, "\t")
		if len(fields) != len(clickhouse.WorkerGrantsColumns) {
			return nil, fmt.Errorf("line %d: %d fields, want %d", number, len(fields), len(clickhouse.WorkerGrantsColumns))
		}
		var row clickhouse.SystemGrantRow
		nullable := []**string{&row.UserName, &row.RoleName, nil, &row.Database, &row.Table, &row.Column}
		for i, target := range nullable {
			field, column := fields[i], clickhouse.WorkerGrantsColumns[i]
			if target != nil && field == `\N` {
				continue
			}
			if strings.Contains(field, `\`) {
				return nil, fmt.Errorf("line %d: %s: escaped value %q", number, column, field)
			}
			if target == nil {
				row.AccessType = field
				continue
			}
			value := field
			*target = &value
		}
		for i, target := range []*uint8{&row.IsPartialRevoke, &row.GrantOption} {
			field, column := fields[6+i], clickhouse.WorkerGrantsColumns[6+i]
			switch field {
			case "0":
			case "1":
				*target = 1
			default:
				return nil, fmt.Errorf("line %d: %s: %q, want 0 or 1", number, column, field)
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}
