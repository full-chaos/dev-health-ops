package postgres

import (
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"sort"
	"testing"
)

func tableOf(t *testing.T, posture RolePosture, name string) TablePrivilege {
	t.Helper()
	var found []TablePrivilege
	for _, table := range posture.RequiredTables {
		if table.TableName == name {
			found = append(found, table)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%s is declared %d times, want once", name, len(found))
	}
	return found[0]
}

func updateColumnsOf(posture RolePosture, table string) []string {
	var columns []string
	for _, column := range posture.ColumnScoped {
		if column.TableName == table && column.Privilege == "UPDATE" {
			columns = append(columns, column.ColumnName)
		}
	}
	sort.Strings(columns)
	return columns
}

// While auth-service owns organizations the api role reads memberships and
// writes none; it inserts no organization and updates only the columns auth
// does not write. DELETE stays on organizations for the purge.
func TestAPIPostureOrgsOwnedNarrowsMembershipsAndOrganizations(t *testing.T) {
	for _, sessions := range []bool{false, true} {
		posture := APIPostureFor(sessions, true)
		if got := tableOf(t, posture, "memberships"); got != (TablePrivilege{"memberships", false, false, false}) {
			t.Errorf("sessions=%v memberships = %+v, want read only", sessions, got)
		}
		if got := tableOf(t, posture, "organizations"); got != (TablePrivilege{"organizations", false, false, true}) {
			t.Errorf("sessions=%v organizations = %+v, want select and delete at table level", sessions, got)
		}
		want := []string{"description", "managed_by", "onboarding_integration_skipped_at", "settings", "stripe_customer_id", "tier", "updated_at"}
		if got := updateColumnsOf(posture, "organizations"); !reflect.DeepEqual(got, want) {
			t.Errorf("sessions=%v organizations UPDATE columns = %v, want %v", sessions, got, want)
		}
		if len(posture.RequiredTables) != len(APIPosture(sessions).RequiredTables) {
			t.Errorf("sessions=%v: the switch changes more than two tables", sessions)
		}
	}
}

// Both switches off, the manifest is byte for byte what it was.
func TestAPIPostureOrgsFlagOffIsUnchanged(t *testing.T) {
	for _, sessions := range []bool{false, true} {
		if !reflect.DeepEqual(APIPostureFor(sessions, false), APIPosture(sessions)) {
			t.Errorf("sessions=%v: orgsOwned=false changes the manifest", sessions)
		}
	}
	off := APIPosture(false)
	if got := tableOf(t, off, "memberships"); got != (TablePrivilege{"memberships", true, true, true}) {
		t.Errorf("memberships off = %+v", got)
	}
	if got := tableOf(t, off, "organizations"); got != (TablePrivilege{"organizations", true, true, true}) {
		t.Errorf("organizations off = %+v", got)
	}
	if len(updateColumnsOf(off, "organizations")) != 0 {
		t.Error("organizations has column grants with the switch off")
	}
}

// Every column of public.organizations is classified here, so a new column
// fails this cell until someone decides whether go-api may still update it.
func TestAPIPostureOrganizationsColumnsCoverTheDDL(t *testing.T) {
	raw, err := os.ReadFile("../../pgmigrate/baseline/head.json")
	if err != nil {
		t.Fatal(err)
	}
	var baseline struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(raw, &baseline); err != nil {
		t.Fatal(err)
	}
	body := regexp.MustCompile(`(?s)CREATE TABLE public\.organizations \((.*?)\n\);`).FindStringSubmatch(baseline.Schema)
	if body == nil {
		t.Fatal("organizations is not in the baseline")
	}
	columns := map[string]bool{}
	for _, match := range regexp.MustCompile(`(?m)^\s+([a-z_]+) `).FindAllStringSubmatch(body[1], -1) {
		columns[match[1]] = true
	}
	files, err := os.ReadDir("../../pgmigrate/sql")
	if err != nil {
		t.Fatal(err)
	}
	add := regexp.MustCompile(`(?is)ALTER TABLE\s+(?:ONLY\s+)?(?:public\.)?organizations\s+ADD COLUMN\s+(?:IF NOT EXISTS\s+)?([a-z_]+)`)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		text, err := os.ReadFile("../../pgmigrate/sql/" + file.Name())
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range add.FindAllStringSubmatch(string(text), -1) {
			columns[match[1]] = true
		}
	}
	refused := map[string]bool{"id": true, "slug": true, "name": true, "is_active": true, "created_at": true}
	granted := updateColumnsOf(APIPostureFor(false, true), "organizations")
	grantedSet := map[string]bool{}
	for _, column := range granted {
		grantedSet[column] = true
	}
	for column := range columns {
		if refused[column] == grantedSet[column] {
			t.Errorf("organizations.%s is not classified: refused=%v granted=%v", column, refused[column], grantedSet[column])
		}
	}
	for column := range refused {
		if !columns[column] {
			t.Errorf("refused column %s is not in the DDL", column)
		}
	}
	for _, column := range granted {
		if !columns[column] {
			t.Errorf("granted column %s is not in the DDL", column)
		}
	}
}
