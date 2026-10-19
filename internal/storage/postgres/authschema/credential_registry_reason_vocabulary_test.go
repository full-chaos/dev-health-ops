package authschema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// approvedRevokedReasons is written by hand on purpose: widening the vocabulary
// file and the CHECK together must also need a deliberate edit here
// (CHAOS-10194). A value enters with the producer that emits it and its own
// migration.
var approvedRevokedReasons = []string{"compromised", "owner_request"}

// TestRevokedReasonCheckEqualsTheVocabularyFile: the newest migration that
// defines credential_registry_revoked_reason_code closes revoked_reason to the
// IN-list of contracts/auth/v1/vocabulary/reason-codes.json, in both
// directions, and to the hand-pinned approved list.
func TestRevokedReasonCheckEqualsTheVocabularyFile(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "contracts", "auth", "v1", "vocabulary", "reason-codes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vocab struct {
		RevokedReason struct {
			Values []string `json:"values"`
		} `json:"revoked_reason"`
	}
	if err := json.Unmarshal(raw, &vocab); err != nil {
		t.Fatal(err)
	}
	want := vocab.RevokedReason.Values
	if len(want) == 0 {
		t.Fatal("the vocabulary file lists no revoked_reason values; the comparison would be vacuous")
	}
	sorted := func(in []string) []string {
		out := append([]string(nil), in...)
		sort.Strings(out)
		return out
	}
	if strings.Join(sorted(want), ",") != strings.Join(sorted(approvedRevokedReasons), ",") {
		t.Errorf("the vocabulary file lists %v, the approved list is %v", want, approvedRevokedReasons)
	}

	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var sql string
	var version int
	for _, m := range migrations {
		if strings.Contains(m.SQL, "ADD CONSTRAINT credential_registry_revoked_reason_code") && m.Version >= version {
			sql, version = m.SQL, m.Version
		}
	}
	if version == 0 {
		t.Fatal("no migration defines credential_registry_revoked_reason_code")
	}
	start := strings.LastIndex(sql, "ADD CONSTRAINT credential_registry_revoked_reason_code")
	body := sql[start:]
	end := strings.Index(body, ";")
	if end < 0 {
		t.Fatalf("migration %04d: the constraint statement does not end", version)
	}
	body = body[:end]
	open := strings.Index(body, "IN (")
	if open < 0 {
		t.Fatalf("migration %04d still defines credential_registry_revoked_reason_code by pattern, not by a closed IN-list", version)
	}
	list := body[open+len("IN ("):]
	list = list[:strings.Index(list, ")")]
	got := map[string]bool{}
	for _, part := range strings.Split(list, ",") {
		got[strings.Trim(strings.TrimSpace(part), "'")] = true
	}
	for _, value := range want {
		if !got[value] {
			t.Errorf("vocabulary value %q is missing from the migration %04d CHECK", value, version)
		}
		delete(got, value)
	}
	for extra := range got {
		t.Errorf("the migration %04d CHECK admits %q, which the vocabulary file does not list", version, extra)
	}
}
