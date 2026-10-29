package authapi

import (
	"bufio"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

// A character the address validator trims at an edge is accepted with 202 and the provisioned address is the trimmed one.
func TestRegisterAcceptsEdgeTrimmedEmailsAndStoresTheTrimmedAddress(t *testing.T) {
	registerEnv(t, "")
	for _, cell := range readEdgeCells(t) {
		for _, where := range []string{"leading", "trailing", "both"} {
			lead, trail := cell[1], cell[1]
			if where == "leading" {
				trail = ""
			}
			if where == "trailing" {
				lead = ""
			}
			c := newCredFixture(t)
			r := c.call(http.MethodPost, PatternRegister, `{"email":"`+lead+`a@example.com`+trail+`","password":"Correct-horse-battery-9"}`, nil)
			if r.Code != http.StatusAccepted || len(c.fakes.provisioned) != 1 {
				t.Errorf("%s %s: status %d provisioned %d", cell[0], where, r.Code, len(c.fakes.provisioned))
				continue
			}
			if got := c.fakes.provisioned[0].Email; got != cell[2] {
				t.Errorf("%s %s: stored %q, the table says %q", cell[0], where, got, cell[2])
			}
		}
	}
}
