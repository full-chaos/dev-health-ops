// Command lcovmerge merges Go cover profiles into one lcov report.
//
// Usage: lcovmerge -strip MODULE/ -o OUT.info PROFILE_OR_DIR...
//
// A directory argument stands for every *.out file under it. All profiles must share one mode.
// Counts of equal blocks add up; a line takes the highest count of the blocks that cover it.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type block struct {
	file                                 string
	startLine, startCol, endLine, endCol int
}

func main() {
	strip := flag.String("strip", "", "module path prefix (with trailing slash) removed from every file name")
	out := flag.String("o", "", "output lcov file")
	flag.Parse()
	if *out == "" || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: lcovmerge -strip MODULE/ -o OUT.info PROFILE_OR_DIR...")
		os.Exit(2)
	}
	var files []string
	for _, arg := range flag.Args() {
		st, err := os.Stat(arg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "lcovmerge:", err)
			os.Exit(1)
		}
		if !st.IsDir() {
			files = append(files, arg)
			continue
		}
		err = filepath.WalkDir(arg, func(p string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(p, ".out") {
				files = append(files, p)
			}
			return err
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "lcovmerge:", err)
			os.Exit(1)
		}
	}
	sort.Strings(files)
	var readers []io.Reader
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			fmt.Fprintln(os.Stderr, "lcovmerge:", err)
			os.Exit(1)
		}
		defer fh.Close()
		readers = append(readers, fh)
	}
	report, err := Merge(readers, *strip)
	if err != nil {
		fmt.Fprintln(os.Stderr, "lcovmerge:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, []byte(report), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "lcovmerge:", err)
		os.Exit(1)
	}
	fmt.Printf("lcovmerge: %d profile(s) -> %s\n", len(files), *out)
}

// Merge reads cover profiles and returns the lcov text.
func Merge(profiles []io.Reader, strip string) (string, error) {
	counts := map[block]int64{}
	mode := ""
	for i, r := range profiles {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		first := true
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			if first {
				first = false
				m, ok := strings.CutPrefix(line, "mode: ")
				if !ok {
					return "", fmt.Errorf("profile %d: first line is not a mode line: %q", i, line)
				}
				if mode != "" && m != mode {
					return "", fmt.Errorf("profile %d: mode %q differs from %q", i, m, mode)
				}
				mode = m
				continue
			}
			b, n, err := parseBlock(line)
			if err != nil {
				return "", fmt.Errorf("profile %d: %w", i, err)
			}
			if mode == "set" {
				if n > 0 {
					n = 1
				}
				if counts[b] > 0 {
					n = 1
				}
				counts[b] = n
			} else {
				counts[b] += n
			}
		}
		if err := sc.Err(); err != nil {
			return "", fmt.Errorf("profile %d: %w", i, err)
		}
		if first {
			return "", fmt.Errorf("profile %d is empty", i)
		}
	}
	perFile := map[string]map[int]int64{}
	for b, n := range counts {
		lines := perFile[b.file]
		if lines == nil {
			lines = map[int]int64{}
			perFile[b.file] = lines
		}
		for l := b.startLine; l <= b.endLine; l++ {
			if cur, ok := lines[l]; !ok || n > cur {
				lines[l] = n
			}
		}
	}
	names := make([]string, 0, len(perFile))
	for f := range perFile {
		names = append(names, f)
	}
	sort.Strings(names)
	var sb strings.Builder
	for _, f := range names {
		lines := perFile[f]
		nums := make([]int, 0, len(lines))
		for l := range lines {
			nums = append(nums, l)
		}
		sort.Ints(nums)
		hit := 0
		fmt.Fprintf(&sb, "TN:\nSF:%s\n", strings.TrimPrefix(f, strip))
		for _, l := range nums {
			fmt.Fprintf(&sb, "DA:%d,%d\n", l, lines[l])
			if lines[l] > 0 {
				hit++
			}
		}
		fmt.Fprintf(&sb, "LF:%d\nLH:%d\nend_of_record\n", len(nums), hit)
	}
	return sb.String(), nil
}

// parseBlock reads "file:startLine.startCol,endLine.endCol numStmt count".
func parseBlock(line string) (block, int64, error) {
	bad := func() (block, int64, error) { return block{}, 0, fmt.Errorf("bad profile line %q", line) }
	colon := strings.LastIndexByte(line, ':')
	if colon < 1 {
		return bad()
	}
	var b block
	b.file = line[:colon]
	rest := strings.Fields(line[colon+1:])
	if len(rest) != 3 {
		return bad()
	}
	pos := strings.Split(rest[0], ",")
	if len(pos) != 2 {
		return bad()
	}
	var err error
	if b.startLine, b.startCol, err = parsePos(pos[0]); err != nil {
		return bad()
	}
	if b.endLine, b.endCol, err = parsePos(pos[1]); err != nil {
		return bad()
	}
	if b.endLine < b.startLine {
		return bad()
	}
	if _, err = strconv.Atoi(rest[1]); err != nil {
		return bad()
	}
	n, err := strconv.ParseInt(rest[2], 10, 64)
	if err != nil || n < 0 {
		return bad()
	}
	return b, n, nil
}

func parsePos(s string) (int, int, error) {
	l, c, ok := strings.Cut(s, ".")
	if !ok {
		return 0, 0, fmt.Errorf("bad position")
	}
	li, err := strconv.Atoi(l)
	if err != nil {
		return 0, 0, err
	}
	ci, err := strconv.Atoi(c)
	return li, ci, err
}
