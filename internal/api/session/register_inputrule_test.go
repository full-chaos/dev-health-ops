package session

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
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

func (c inputRuleCell) body(t *testing.T) []byte {
	t.Helper()
	if raw, isHex := strings.CutPrefix(c.value, "hex:"); isHex {
		decoded, err := hex.DecodeString(raw)
		if err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	fields := map[string]string{"email": `"a@example.com"`, "password": `"Correct-horse-battery-9"`}
	fields[c.field] = c.value
	var parts []string
	for _, name := range []string{"email", "password", "full_name", "org_name"} {
		if value, ok := fields[name]; ok {
			parts = append(parts, `"`+name+`":`+value)
		}
	}
	return []byte("{" + strings.Join(parts, ",") + "}")
}

// answerShape reads a validation answer as "<status> <type> <loc>" the way the cell table states it.
func answerShape(t *testing.T, status int, body string) string {
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
	for _, cell := range readInputRuleCells(t) {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", strings.NewReader(string(cell.body(t))))
		request.Header.Set("Content-Type", "application/json")
		_, outcome, _, err := pybody.Read(request)
		if err != nil {
			t.Fatalf("%s: read: %v", cell.id, err)
		}
		var got string
		if outcome == pybody.ParseFailed {
			got = "400"
		} else if outcome != pybody.Ready {
			t.Fatalf("%s: outcome %v", cell.id, outcome)
		} else {
			w := httptest.NewRecorder()
			if _, ok := validateRegister(w, registerRequest(t, string(cell.body(t)))); ok {
				got = "ok"
			} else {
				got = answerShape(t, w.Code, w.Body.String())
				switch {
				case cell.form == "replacement_char" && cell.field == "email",
					cell.field != "email" && cell.form != "lone_high" && cell.form != "lone_low":
					if strings.Contains(w.Body.String(), `"input":"A`) || strings.Contains(w.Body.String(), `"input":"a`) {
						t.Errorf("%s: the refusal echoes the input: %s", cell.id, w.Body.String())
					}
				}
			}
		}
		if got != cell.want {
			t.Errorf("%s (%s, %s): go-api answers %q, the table says %q", cell.id, cell.field, cell.form, got, cell.want)
		}
	}
}
