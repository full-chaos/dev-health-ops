package bigboyrender

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

var armComment = regexp.MustCompile(`(The arm64 child \(prod's own arch\) is\n\s+# sha256:)[0-9a-f]{64}`)

// bumpSwapValues: `bump-swap-values <old-sha> <new-sha> <o7> <s7> <old-operator-digest> <new-operator-digest>
// <old-dho-digest> <new-dho-digest> <dho-arm-child-digest>`, run from the deploy worktree. It rewrites
// values.prod.yaml, then values.local.yaml (the prod file is already written when the local file refuses).
func bumpSwapValues(args []string, stderr io.Writer) int {
	if len(args) != 9 {
		fmt.Fprintln(stderr, "usage: cigate bigboy bump-swap-values <old-sha> <new-sha> <o7> <s7> <old-operator-digest> <new-operator-digest> <old-dho-digest> <new-dho-digest> <dho-arm-child-digest>")
		return 2
	}
	old, newSHA, o7, s7, oo, no, od, nd, arm := args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8]
	const prod, local = "values.prod.yaml", "values.local.yaml"
	raw, err := os.ReadFile(prod)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	s := universalNewlines(string(raw))
	for _, pair := range [][2]string{{old, newSHA}, {o7, s7}, {oo, no}, {od, nd}} {
		if !strings.Contains(s, pair[0]) {
			fmt.Fprintf(stderr, "AssertionError: %s\n", pair[0])
			return 1
		}
		s = strings.ReplaceAll(s, pair[0], pair[1])
	}
	matches := armComment.FindAllStringSubmatchIndex(s, -1)
	if len(matches) > 0 && !strings.Contains(arm, ":") {
		fmt.Fprintf(stderr, "IndexError: no sha256: prefix in %s\n", arm)
		return 1
	}
	if len(matches) != 1 {
		fmt.Fprintln(stderr, "AssertionError: arm64 child comment not found")
		return 1
	}
	armHex := strings.Split(arm, ":")[1]
	m := matches[0]
	s = s[:m[0]] + s[m[2]:m[3]] + armHex + s[m[1]:]
	if err := os.WriteFile(prod, []byte(s), 0o666); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	rawLocal, err := os.ReadFile(local)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	t := universalNewlines(string(rawLocal))
	o12, n12 := prefixRunes(old, 12), prefixRunes(newSHA, 12)
	if !strings.Contains(t, o12) || !strings.Contains(t, od) {
		fmt.Fprintln(stderr, "AssertionError")
		return 1
	}
	t = strings.ReplaceAll(strings.ReplaceAll(t, o12, n12), od, nd)
	if err := os.WriteFile(local, []byte(t), 0o666); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func prefixRunes(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		r = r[:n]
	}
	return string(r)
}
