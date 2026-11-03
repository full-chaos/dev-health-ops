package authapi

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type inputRuleCell struct{ id, field, form, value, want string }

func readInputRuleCells(t *testing.T) []inputRuleCell {
	t.Helper()
	file, err := os.Open(filepath.Join("..", "..", "..", "ci", "auth_input_rule_cells.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var cells []inputRuleCell
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 5 {
			t.Fatalf("cell row has %d columns: %q", len(parts), line)
		}
		cells = append(cells, inputRuleCell{parts[0], parts[1], parts[2], parts[3], parts[4]})
	}
	if err := scanner.Err(); err != nil || len(cells) == 0 {
		t.Fatalf("cells: %d, %v", len(cells), err)
	}
	return cells
}

func (c inputRuleCell) body(t *testing.T) string {
	t.Helper()
	if raw, isHex := strings.CutPrefix(c.value, "hex:"); isHex {
		decoded, err := hex.DecodeString(raw)
		if err != nil {
			t.Fatal(err)
		}
		return string(decoded)
	}
	fields := map[string]string{"email": `"a@example.com"`, "password": `"Correct-horse-battery-9"`}
	fields[c.field] = c.value
	var parts []string
	for _, name := range []string{"email", "password", "full_name", "org_name"} {
		if value, ok := fields[name]; ok {
			parts = append(parts, `"`+name+`":`+value)
		}
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func inputRuleShape(t *testing.T, status int, body string) string {
	t.Helper()
	var doc struct {
		Detail []struct {
			Type string `json:"type"`
			Loc  []any  `json:"loc"`
		} `json:"detail"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil || len(doc.Detail) == 0 {
		t.Fatalf("answer %d is no validation list: %q (%v)", status, body, err)
	}
	var loc []string
	for _, part := range doc.Detail[0].Loc {
		loc = append(loc, part.(string))
	}
	return strings.Join([]string{strconv.Itoa(status), doc.Detail[0].Type, strings.Join(loc, ".")}, " ")
}

func TestRegisterInputRuleCellsMatchTheSharedTable(t *testing.T) {
	registerEnv(t, "")
	for _, cell := range readInputRuleCells(t) {
		c := newCredFixture(t)
		r := c.call(http.MethodPost, PatternRegister, cell.body(t), nil)
		var got string
		switch {
		case r.Code == http.StatusAccepted:
			got = "ok"
			if len(c.fakes.provisioned) != 1 {
				t.Errorf("%s: accepted but provisioned %d", cell.id, len(c.fakes.provisioned))
			}
		case r.Code == http.StatusBadRequest:
			got = "400"
		default:
			got = inputRuleShape(t, r.Code, r.Body.String())
			if len(c.fakes.provisioned) != 0 || len(c.fakes.mails) != 0 {
				t.Errorf("%s: refused but provisioned %d, mailed %d", cell.id, len(c.fakes.provisioned), len(c.fakes.mails))
			}
			switch {
			case cell.form == "replacement_char" && cell.field == "email",
				cell.field != "email" && cell.form != "lone_high" && cell.form != "lone_low":
				if strings.Contains(r.Body.String(), `"input":"A`) || strings.Contains(r.Body.String(), `"input":"a`) {
					t.Errorf("%s: the refusal echoes the input: %s", cell.id, r.Body.String())
				}
			}
		}
		if got != cell.want {
			t.Errorf("%s (%s, %s): auth-service answers %q, the table says %q", cell.id, cell.field, cell.form, got, cell.want)
		}
	}
}
