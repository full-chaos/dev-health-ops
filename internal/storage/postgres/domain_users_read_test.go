package postgres

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

// domainUsersReadColumns are the public.users columns the domain role's readers name: the heartbeat counts rows
// (any one column grant admits COUNT(*)) and the billing owner lookup joins on id and reads email and full_name.
var domainUsersReadColumns = []string{"id", "email", "full_name"}

// The packages whose code runs on the domain pool and names public.users, each file with its reader.
var domainUsersReaderFiles = map[string]string{
	"../../jobs/system/heartbeat_native.go":  "SELECT COUNT(*) FROM public.users",
	"../../jobs/operational/billingfence.go": "FROM public.users u",
}

func domainUsersSelectColumns(posture RolePosture) []string {
	var columns []string
	for _, column := range posture.ColumnScoped {
		if column.TableName == "users" && column.Privilege == "SELECT" {
			columns = append(columns, column.ColumnName)
		}
	}
	return columns
}

func TestDomainPostureReadsOnlyTheUsersColumnsItsReadersName(t *testing.T) {
	t.Parallel()

	posture := domainPosture()
	for _, table := range posture.RequiredTables {
		if table.TableName == "users" {
			t.Fatalf("domain posture declares users table-wide, so the role can read password_hash: %+v", table)
		}
	}
	for _, column := range posture.ColumnScoped {
		if column.TableName == "users" && column.Privilege != "SELECT" {
			t.Fatalf("domain posture grants %s on users.%s, want SELECT only", column.Privilege, column.ColumnName)
		}
	}
	got := domainUsersSelectColumns(posture)
	for _, secret := range []string{"password_hash", "token_version"} {
		if slices.Contains(got, secret) {
			t.Fatalf("domain posture grants SELECT on users.%s", secret)
		}
	}
	if !slices.Equal(got, domainUsersReadColumns) {
		t.Fatalf("domain posture users SELECT columns = %v, want %v", got, domainUsersReadColumns)
	}
}

func TestDomainPostureCoversTheDomainUsersReads(t *testing.T) {
	t.Parallel()

	declared := domainUsersSelectColumns(domainPosture())
	if len(declared) == 0 {
		t.Fatal("domain posture declares no users column SELECT rows; the heartbeat COUNT(*) needs one")
	}
	source, err := os.ReadFile("../../jobs/operational/billingfence.go")
	if err != nil {
		t.Fatal(err)
	}
	found := regexp.MustCompile("(?s)`([^`]*FROM public\\.users u[^`]*)`").FindSubmatch(source)
	if found == nil {
		t.Fatal("billing owner lookup on public.users not found")
	}
	var named []string
	for _, match := range regexp.MustCompile(`\bu\.(\w+)`).FindAllSubmatch(found[1], -1) {
		if !slices.Contains(named, string(match[1])) {
			named = append(named, string(match[1]))
		}
	}
	sort.Strings(named)
	if want := []string{"email", "full_name", "id"}; !slices.Equal(named, want) {
		t.Fatalf("billing owner lookup names users columns %v, want %v; update domainUsersReadColumns", named, want)
	}
	for _, column := range named {
		if !slices.Contains(declared, column) {
			t.Fatalf("billing owner lookup reads users.%s but domain posture has no SELECT row for it", column)
		}
	}
}

func TestDomainUsersReadersAreTheKnownFiles(t *testing.T) {
	t.Parallel()

	usersSQL := regexp.MustCompile(`(?i)\b(FROM|JOIN|UPDATE|INTO)\s+(public\.)?users\b`)
	var readers []string
	for _, dir := range []string{"../../jobs/system", "../../jobs/operational"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil || len(files) == 0 {
			t.Fatalf("no sources in %s (err=%v)", dir, err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			source, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if usersSQL.Match(source) {
				readers = append(readers, filepath.ToSlash(file))
			}
		}
	}
	sort.Strings(readers)
	var want []string
	for file, statement := range domainUsersReaderFiles {
		want = append(want, file)
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(source), statement) {
			t.Fatalf("%s no longer holds %q; recheck the domain users read set", file, statement)
		}
	}
	sort.Strings(want)
	if !slices.Equal(readers, want) {
		t.Fatalf("domain-pool files naming public.users = %v, want %v; recheck the domain users read set", readers, want)
	}
}
