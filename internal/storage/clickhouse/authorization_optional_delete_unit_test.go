package clickhouse

import (
	"errors"
	"strings"
	"testing"
)

const (
	teamsSelectInsert       = "GRANT SELECT, INSERT ON default.teams TO dho_api_ch"
	teamsSelectInsertDelete = "GRANT SELECT, INSERT, ALTER DELETE ON default.teams TO dho_api_ch"
)

// withTeamsLine is the live grants of posture with the teams statement replaced by teamsLine
// ("" drops it), so each cell varies the teams grant only.
func withTeamsLine(posture Posture, teamsLine string, extra ...string) []string {
	var lines []string
	for _, line := range liveGrants(posture) {
		if !strings.Contains(line, " ON default.teams ") {
			lines = append(lines, line)
		}
	}
	if teamsLine != "" {
		lines = append(lines, teamsLine)
	}
	return append(lines, extra...)
}

func TestTeamsWithoutAlterDeleteIsAccepted(t *testing.T) {
	for _, projectScope := range []bool{false, true} {
		posture := APIPostureFor("default", projectScope)
		if err := checkWith(t, posture, withTeamsLine(posture, teamsSelectInsert)); err != nil {
			t.Errorf("projectScope=%v: teams SELECT, INSERT must pass: %v", projectScope, err)
		}
	}
}

func TestTeamsWithAlterDeleteIsStillAccepted(t *testing.T) {
	for _, projectScope := range []bool{false, true} {
		posture := APIPostureFor("default", projectScope)
		if err := checkWith(t, posture, withTeamsLine(posture, teamsSelectInsertDelete)); err != nil {
			t.Errorf("projectScope=%v: teams SELECT, INSERT, ALTER DELETE (prod today) must pass: %v", projectScope, err)
		}
		split := withTeamsLine(posture, teamsSelectInsert, "GRANT ALTER DELETE ON default.teams TO dho_api_ch")
		if err := checkWith(t, posture, split); err != nil {
			t.Errorf("projectScope=%v: ALTER DELETE on teams on its own line must pass: %v", projectScope, err)
		}
	}
}

func TestTeamsWithAnyOtherExtraPrivilegeIsRefused(t *testing.T) {
	posture := APIPosture("default")
	for _, line := range []string{
		"GRANT SELECT, INSERT, ALTER UPDATE ON default.teams TO dho_api_ch",
		"GRANT SELECT, INSERT, ALTER DELETE, ALTER UPDATE ON default.teams TO dho_api_ch",
		"GRANT SELECT, INSERT, TRUNCATE ON default.teams TO dho_api_ch",
		"GRANT SELECT, INSERT, ALTER DELETE, DROP TABLE ON default.teams TO dho_api_ch",
		"GRANT SELECT, INSERT, ALTER ON default.teams TO dho_api_ch",
	} {
		err := checkWith(t, posture, withTeamsLine(posture, line))
		if !errors.Is(err, ErrPostureMismatch) || !strings.Contains(err.Error(), "default.teams has unexpected") {
			t.Errorf("%q: want ErrPostureMismatch naming the extra teams privilege, got %v", line, err)
		}
	}
}

func TestTeamsAlterDeleteDoesNotStandInForARequiredPrivilege(t *testing.T) {
	posture := APIPosture("default")
	for _, line := range []string{
		"GRANT INSERT, ALTER DELETE ON default.teams TO dho_api_ch",
		"GRANT SELECT, ALTER DELETE ON default.teams TO dho_api_ch",
		"GRANT ALTER DELETE ON default.teams TO dho_api_ch",
		"",
	} {
		err := checkWith(t, posture, withTeamsLine(posture, line))
		if !errors.Is(err, ErrPostureMismatch) || !strings.Contains(err.Error(), "default.teams missing") {
			t.Errorf("%q: want ErrPostureMismatch naming the missing teams privilege, got %v", line, err)
		}
	}
}

func TestAlterDeleteOnAnotherTableIsRefused(t *testing.T) {
	posture := APIPosture("default")
	for _, extra := range []string{
		"GRANT ALTER DELETE ON default.other_table TO dho_api_ch",
		"GRANT ALTER DELETE ON default.team_memberships TO dho_api_ch",
		"GRANT ALTER DELETE ON default.repos TO dho_api_ch",
		"GRANT ALTER DELETE ON other_db.teams TO dho_api_ch",
	} {
		err := checkWith(t, posture, withTeamsLine(posture, teamsSelectInsert, extra))
		if !errors.Is(err, ErrPostureMismatch) {
			t.Errorf("%q: want ErrPostureMismatch, got %v", extra, err)
		}
	}
}

func TestIdentitiesStillRequireAlterDelete(t *testing.T) {
	posture := APIPosture("default")
	var lines []string
	for _, line := range withTeamsLine(posture, teamsSelectInsert) {
		if strings.Contains(line, " ON default.identities ") {
			line = "GRANT SELECT, INSERT ON default.identities TO dho_api_ch"
		}
		lines = append(lines, line)
	}
	err := checkWith(t, posture, lines)
	if !errors.Is(err, ErrPostureMismatch) || !strings.Contains(err.Error(), "default.identities missing ALTER DELETE") {
		t.Fatalf("identities without ALTER DELETE must be refused, got %v", err)
	}
}

func TestTeamsGrantOptionIsStillRefused(t *testing.T) {
	posture := APIPosture("default")
	for _, line := range []string{
		teamsSelectInsert + " WITH GRANT OPTION",
		teamsSelectInsertDelete + " WITH GRANT OPTION",
	} {
		err := checkWith(t, posture, withTeamsLine(posture, line))
		if !errors.Is(err, ErrPostureMismatch) {
			t.Errorf("%q: want ErrPostureMismatch, got %v", line, err)
		}
	}
	err := checkWith(t, posture, withTeamsLine(posture, teamsSelectInsert, "GRANT ALTER DELETE ON default.teams TO dho_api_ch WITH GRANT OPTION"))
	if !errors.Is(err, ErrPostureMismatch) {
		t.Errorf("ALTER DELETE on teams WITH GRANT OPTION: want ErrPostureMismatch, got %v", err)
	}
}

func TestGrantStatementsRenderTeamsWithoutAlterDelete(t *testing.T) {
	for _, projectScope := range []bool{false, true} {
		statements := GrantStatements("dho_api_ch", APIPostureFor("default", projectScope))
		var teams, identities []string
		for _, statement := range statements {
			switch {
			case strings.Contains(statement, " ON default.teams "):
				teams = append(teams, statement)
			case strings.Contains(statement, " ON default.identities "):
				identities = append(identities, statement)
			}
		}
		if len(teams) != 1 || teams[0] != teamsSelectInsert {
			t.Errorf("projectScope=%v: teams statements %q, want exactly %q", projectScope, teams, teamsSelectInsert)
		}
		if len(identities) != 1 || identities[0] != "GRANT SELECT, INSERT, ALTER DELETE ON default.identities TO dho_api_ch" {
			t.Errorf("projectScope=%v: identities statements %q, want SELECT, INSERT, ALTER DELETE", projectScope, identities)
		}
	}
}

// Only teams may hold ALTER DELETE without declaring it: the optional delete is one privilege on one table.
func TestOnlyTeamsHasTheOptionalDelete(t *testing.T) {
	var optional []TableGrant
	for _, table := range APIPosture("default").RequiredTables {
		if table.OptionalDelete {
			optional = append(optional, table)
		}
	}
	want := TableGrant{Database: "default", Table: "teams", AllowInsert: true, AllowSelect: true, OptionalDelete: true}
	if len(optional) != 1 || optional[0] != want {
		t.Fatalf("optional-delete rows %+v, want exactly %+v", optional, want)
	}
}
