package bigboyrender

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"
)

var armComment = regexp.MustCompile(`(The arm64 child \(prod's own arch\) is\n[` + pySpace + `]+# sha256:)[0-9a-f]{64}`)

// bumpSwapValues: `bump-swap-values <old-sha> <new-sha> <o7> <s7> <old-operator-digest> <new-operator-digest>
// <old-dho-digest> <new-dho-digest> <dho-arm-child-digest>`, run from the deploy worktree. It rewrites
// values.prod.yaml, then values.local.yaml (the prod file is already written when the local file refuses).
func bumpSwapValues(args []string, stderr io.Writer) int {
	if len(args) != 9 {
		fmt.Fprintln(stderr, "usage: cigate bigboy bump-swap-values <old-sha> <new-sha> <o7> <s7> <old-operator-digest> <new-operator-digest> <old-dho-digest> <new-dho-digest> <dho-arm-child-digest>")
		return 1
	}
	old, newSHA, o7, s7, oo, no, od, nd, arm := args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8]
	const prod, local = "values.prod.yaml", "values.local.yaml"
	raw, err := os.ReadFile(prod)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if !utf8.Valid(raw) {
		return utf8Refusal(stderr, prod)
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
	if !utf8.ValidString(s) {
		return textEncodeRefusal(prod, stderr)
	}
	if err := os.WriteFile(prod, []byte(s), 0o666); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	rawLocal, err := os.ReadFile(local)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if !utf8.Valid(rawLocal) {
		return utf8Refusal(stderr, local)
	}
	t := universalNewlines(string(rawLocal))
	o12, n12 := prefixRunes(old, 12), prefixRunes(newSHA, 12)
	var lacks []string
	if !strings.Contains(t, o12) {
		lacks = append(lacks, o12)
	}
	if !strings.Contains(t, od) {
		lacks = append(lacks, "the old dho digest "+od)
	}
	if len(lacks) > 0 {
		fmt.Fprintf(stderr, "AssertionError: %s does not hold %s\n", local, strings.Join(lacks, " nor "))
		return 1
	}
	t = strings.ReplaceAll(strings.ReplaceAll(t, o12, n12), od, nd)
	if !utf8.ValidString(t) {
		return textEncodeRefusal(local, stderr)
	}
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

// textEncodeRefusal is what Python did on a new value that is not UTF-8 (its argv holds lone surrogates): open(path,
// "w") had already truncated the file, then the text write raised UnicodeEncodeError (rc 1). The file is left empty.
func textEncodeRefusal(path string, stderr io.Writer) int {
	if err := os.WriteFile(path, nil, 0o666); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stderr, "UnicodeEncodeError: a value written to %s is not valid UTF-8\n", path)
	return 1
}
