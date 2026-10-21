package docstools

import (
	"errors"
	"fmt"
	"io"
	"time"
)

// check-freshness: every page of docs-data/freshness-inventory.yml exists, has an owner, a refresh or retire
// disposition and a last-reviewed date inside the 180-day cadence, and a retired page names an existing replacement.
// Was scripts/check_freshness_inventory.py.

func init() { register("check-freshness", freshnessVerb) }

const frCadenceDays = 180

type frEntry struct {
	page, disposition, owner string
	lastReviewed             time.Time
	replacement              *string
}

// frInventoryError is the script's FreshnessInventoryError: "ERROR: <msg>" on stderr, exit 1.
type frInventoryError struct{ msg string }

func (e frInventoryError) Error() string { return e.msg }

func frLoad(path string) ([]frEntry, error) {
	if !puIsFile(path) {
		return nil, frInventoryError{"missing freshness inventory: " + path}
	}
	text, err := a1ReadText(path)
	if err != nil {
		return nil, err
	}
	raw, err := ylLoad(text)
	if err != nil {
		return nil, err
	}
	if raw.falsy() {
		raw = &ylVal{kind: ylList}
	}
	if raw.kind != ylList {
		return nil, frInventoryError{path + " must be a YAML list of entries"}
	}
	var entries []frEntry
	for _, item := range raw.items {
		if item.kind != ylMap {
			return nil, frInventoryError{fmt.Sprintf("%s entries must be mappings, got %s", path, item.repr())}
		}
		page, disposition, owner := item.get("page"), item.get("disposition"), item.get("owner")
		lastReviewed, replacement := item.get("last-reviewed"), item.get("replacement")
		if !ylIsStr(page) || !ylIsStr(owner) || !ylIsStr(lastReviewed) {
			return nil, frInventoryError{fmt.Sprintf("%s entry missing required string fields: %s", path, item.repr())}
		}
		if !ylIsStr(disposition) || disposition.s != "refresh" && disposition.s != "retire" {
			return nil, frInventoryError{fmt.Sprintf("%s entry for %s has invalid disposition %s", path, page.s, ylReprOrNone(disposition))}
		}
		var repl *string
		if replacement != nil && replacement.kind != ylNone {
			if replacement.kind != ylStr {
				return nil, frInventoryError{fmt.Sprintf("%s entry for %s has a non-string replacement %s", path, page.s, replacement.repr())}
			}
			repl = &replacement.s
		}
		reviewed, err := pdFromISOFormat(lastReviewed.s)
		if err != nil {
			return nil, err
		}
		entries = append(entries, frEntry{page: page.s, disposition: disposition.s, owner: owner.s, lastReviewed: reviewed, replacement: repl})
	}
	return entries, nil
}

func ylIsStr(v *ylVal) bool { return v != nil && v.kind == ylStr }

func ylReprOrNone(v *ylVal) string {
	if v == nil {
		return "None"
	}
	return v.repr()
}

func frCheck(entries []frEntry, docsRoot string, today time.Time) []string {
	var errs []string
	for _, e := range entries {
		if !puIsFile(puJoin(docsRoot, e.page)) {
			errs = append(errs, e.page+": freshness inventory references a missing page")
			continue
		}
		if age := pdDays(today, e.lastReviewed); age > frCadenceDays {
			errs = append(errs, fmt.Sprintf("%s: last-reviewed %s is %d days old, exceeding the %d-day review cadence", e.page, pdISO(e.lastReviewed), age, frCadenceDays))
		}
		if e.disposition == "retire" {
			if e.replacement == nil || *e.replacement == "" {
				errs = append(errs, e.page+": retired pages must name a replacement page")
				continue
			}
			if !puIsFile(puJoin(docsRoot, *e.replacement)) {
				errs = append(errs, fmt.Sprintf("%s: replacement page %s does not exist", e.page, *e.replacement))
			}
		}
	}
	return errs
}

// frToday is date.today(): the local calendar date.
func frToday() time.Time {
	y, m, d := time.Now().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func freshnessVerb(args []string, stdout, stderr io.Writer) int {
	return freshnessRun(args, stdout, stderr, frToday())
}

func freshnessRun(args []string, stdout, stderr io.Writer, today time.Time) int {
	p := apParser{
		prog:        "cigate docs check-freshness",
		description: "Validate the canonical documentation freshness inventory",
		opts:        []apOpt{{flag: "--inventory", metavar: "INVENTORY"}, {flag: "--docs-root", metavar: "DOCS_ROOT"}},
	}
	vals, rc, done := p.parse(args, stdout, stderr)
	if done {
		return rc
	}
	root, err := puRealpath(".")
	if err != nil {
		return puTracebackError(stderr, err)
	}
	inventory, docsRoot := root+"/docs-data/freshness-inventory.yml", root+"/docs"
	if v, ok := vals["--inventory"]; ok {
		inventory = apPathStr(v)
	}
	if v, ok := vals["--docs-root"]; ok {
		docsRoot = apPathStr(v)
	}
	entries, err := frLoad(inventory)
	var ie frInventoryError
	if errors.As(err, &ie) {
		fmt.Fprintf(stderr, "ERROR: %s\n", ie.msg)
		return 1
	}
	if err != nil {
		return puTracebackError(stderr, err)
	}
	if errs := frCheck(entries, docsRoot, today); len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(stdout, "ERROR: %s\n", e)
		}
		return 1
	}
	fmt.Fprintln(stdout, "Documentation freshness inventory check passed")
	return 0
}
