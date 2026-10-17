// Package teamprovidertest reads back a teamprovider.Insert statement in a
// test that stands in for a ClickHouse connection.
package teamprovidertest

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
)

var columnList = regexp.MustCompile(`^INSERT INTO teams \(([^)]*)\) SELECT `)

// IsInsert reports whether query is a teamprovider.Insert statement.
func IsInsert(query string) bool { return columnList.MatchString(query) }

// Decode returns the columns a writer stated and one Write per row, with each
// value as the writer gave it except that a time comes back as a UTC
// time.Time and a nil of any type as nil.
func Decode(query string, args []any) ([]string, []teamprovider.Write, error) {
	match := columnList.FindStringSubmatch(query)
	if match == nil {
		return nil, nil, errors.New("teamprovidertest: not a teamprovider.Insert statement")
	}
	all := strings.Split(match[1], ", ")
	if len(all) < 4 || strings.Join(all[len(all)-3:], ", ") != teamprovider.Columns {
		return nil, nil, fmt.Errorf("teamprovidertest: column list %q does not end with the identity", match[1])
	}
	columns := all[:len(all)-3]
	named := map[string][]any{}
	for _, arg := range args {
		value, ok := arg.(driver.NamedValue)
		if !ok {
			return nil, nil, fmt.Errorf("teamprovidertest: argument %T is not named", arg)
		}
		if list, ok := value.Value.([]any); ok {
			named[value.Name] = list
		}
	}
	types := teamprovider.ColumnTypes()
	count := len(named["ordinal"])
	writes := make([]teamprovider.Write, count)
	for r := range writes {
		writes[r].Values = make([]any, len(columns))
		for c, column := range columns {
			value := named[fmt.Sprintf("v%d", c)][r]
			if text, ok := value.(string); ok && strings.Contains(types[column], "DateTime64") {
				parsed, err := time.Parse("2006-01-02 15:04:05.000000", text)
				if err != nil {
					return nil, nil, err
				}
				value = parsed.UTC()
			}
			writes[r].Values[c] = value
		}
		writes[r].ProviderTeamID, _ = named["identity_id"][r].(string)
		writes[r].ProviderName, _ = named["identity_name"][r].(string)
		rule, _ := named["gone_rule"][r].(uint8)
		writes[r].Gone = teamprovider.Gone(rule)
		if text, ok := named["gone_in"][r].(string); ok {
			parsed, err := time.Parse("2006-01-02 15:04:05.000000", text)
			if err != nil {
				return nil, nil, err
			}
			gone := parsed.UTC()
			writes[r].GoneAt = &gone
		}
	}
	return columns, writes, nil
}
