package bigboyrender

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const databaseName = "default"

// renderCHUsers: `render-ch-users <authorization.go> <out-file>`; the password is in API_CH_PASSWORD only.
// The file is built from a fixed element table: no text of the input but the grant rows reaches it, and the
// SHA-256 of the password is the only derived secret.
func renderCHUsers(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 {
		fmt.Fprintln(stderr, "usage: cigate bigboy render-ch-users <authorization.go> <out-file>")
		return 2
	}
	password := os.Getenv("API_CH_PASSWORD")
	if password == "" {
		fmt.Fprintln(stderr, "API_CH_PASSWORD is not set in the environment")
		return 2
	}
	manifest := args[0]
	if info, err := os.Stat(manifest); err != nil || !info.Mode().IsRegular() {
		fmt.Fprintf(stderr, "%s is not a file\n", manifest)
		return 2
	}
	raw, err := os.ReadFile(manifest)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	statements := rowStatements(string(raw))
	xml, err := renderUsersXML(statements, password)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	out := args[1]
	if err := writeAtomic(out, xml, 0o644); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "wrote %s mode=644 grants=%d\n", out, strings.Count(xml, "<query>"))
	return 0
}

// rowStatements is the GRANT statements of func APIPosture, one per row that grants anything.
func rowStatements(source string) []string {
	source = universalNewlines(source)
	rest, ok := afterFirst(source, "func APIPosture(")
	if !ok {
		return nil
	}
	var statements []string
	for _, line := range pyLines(beforeFirst(rest, "\n}\n")) {
		m := apiTable.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		var privileges []string
		for _, p := range chPrivileges {
			if strings.Contains(m[2], p[0]+": true") {
				privileges = append(privileges, p[1])
			}
		}
		if len(privileges) > 0 {
			statements = append(statements, fmt.Sprintf("GRANT %s ON %s.%s", strings.Join(privileges, ", "), databaseName, m[1]))
		}
	}
	return statements
}

// GrantsFromManifest is the exported view of the statement list, for the tests.
func GrantsFromManifest(source string) []string { return rowStatements(source) }

// RenderUsersXML builds the users.d file from the grant list and the password (an error is a refusal).
func RenderUsersXML(grants []string, password string) (string, error) {
	return renderUsersXML(grants, password)
}

func renderUsersXML(grants []string, password string) (string, error) {
	if len(grants) == 0 {
		return "", fmt.Errorf("the posture manifest yielded no grants, nothing written")
	}
	sum := sha256.Sum256([]byte(password))
	var b strings.Builder
	b.WriteString("<clickhouse><users><dho_api_ch><networks><ip>::/0</ip></networks>")
	b.WriteString("<profile>default</profile><quota>default</quota><access_management>0</access_management>")
	b.WriteString("<password_sha256_hex>" + hex.EncodeToString(sum[:]) + "</password_sha256_hex><grants>")
	for _, g := range grants {
		b.WriteString("<query>" + xmlText(g) + "</query>")
	}
	b.WriteString("</grants></dho_api_ch></users></clickhouse>\n")
	rendered := b.String()
	if strings.Contains(rendered, password) {
		return "", fmt.Errorf("the rendered file is refused, nothing written: the password would appear in the world-readable output")
	}
	return rendered, nil
}

// xmlText escapes element text the way ElementTree does.
func xmlText(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	return strings.ReplaceAll(s, ">", "&gt;")
}

// writeAtomic writes a sibling temp file, sets its mode and renames it over path.
func writeAtomic(path, content string, mode os.FileMode) (err error) {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.WriteString(content); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Chmod(tmp, mode); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
