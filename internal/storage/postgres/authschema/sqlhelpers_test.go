package authschema

import (
	"regexp"
	"strings"
)

// createdTablesFromSQL lists the tables a body of migration SQL creates.
func createdTablesFromSQL(sql string) []string {
	var tables []string
	for _, match := range createTableStatement.FindAllStringSubmatch(stripComments(sql), -1) {
		tables = append(tables, match[1])
	}
	return tables
}

// tableBody returns the parenthesised body of one CREATE TABLE statement, so a
// test can ask what column types it declares without a SQL parser.
func tableBody(sql, table string) string {
	pattern := regexp.MustCompile(`(?is)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + regexp.QuoteMeta(table) + `\s*\((.*?)\n\);`)
	match := pattern.FindStringSubmatch(stripComments(sql))
	if match == nil {
		return ""
	}
	return strings.ToLower(match[1])
}
