package session

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

func readEdgeCells(t *testing.T) [][3]string {
	t.Helper()
	file, err := os.Open(filepath.Join("..", "..", "..", "ci", "auth_input_rule_edge_cells.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var cells [][3]string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 3 {
			t.Fatalf("edge row has %d columns: %q", len(parts), line)
		}
		cells = append(cells, [3]string{parts[0], parts[1], parts[2]})
	}
	if len(cells) == 0 {
		t.Fatal("no edge cells")
	}
	return cells
}

// A character the address validator trims at an edge is accepted and the stored address is the trimmed one: the
// validator accepts the body and the handler stores Strip(Lower(email)), the expression register uses.
func TestRegisterAcceptsEdgeTrimmedEmailsAndStoresTheTrimmedAddress(t *testing.T) {
	for _, cell := range readEdgeCells(t) {
		for _, where := range []string{"leading", "trailing", "both"} {
			lead, trail := cell[1], cell[1]
			if where == "leading" {
				trail = ""
			}
			if where == "trailing" {
				lead = ""
			}
			body := `{"email":"` + lead + `a@example.com` + trail + `","password":"Correct-horse-battery-9"}`
			w := httptest.NewRecorder()
			r, ok := validateRegister(w, registerRequest(t, body))
			if !ok {
				t.Errorf("%s %s: refused %d %s", cell[0], where, w.Code, w.Body.String())
				continue
			}
			input, _ := r.Context().Value(registerInputKey{}).(registerInput)
			if got := pythonparity.Strip(pythonparity.Lower(input.email)); got != cell[2] {
				t.Errorf("%s %s: stored %q, the table says %q", cell[0], where, got, cell[2])
			}
			if w.Code != http.StatusOK {
				t.Errorf("%s %s: validator wrote status %d", cell[0], where, w.Code)
			}
		}
	}
}
