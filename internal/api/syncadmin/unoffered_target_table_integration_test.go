//go:build integration

package syncadmin

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// unofferedMainRecord holds, for every state of the table below, the line
// the save that rebuilt the rows from the submitted list (the code before
// CHAOS-8816, commit 168feb0253f6) answers: recorded from an executed run of
// this test's own probe on that code, never written by hand.
// unofferedDifferences holds every line this code answers differently, with
// its class.
const (
	unofferedMainRecord  = "testdata/unoffered_targets_main_168feb0253f6.txt"
	unofferedDifferences = "testdata/unoffered_targets_differences.txt"
)

// unofferedDifferenceClasses is every class a line may differ by, with the
// number of lines in it.
//
// stored-list: only the stored list differs, and with it the order of the
// list the save answers and GET shows (stored targets first, then the
// targets the rows derive). This code stores what requests asked for; the
// other code stored the submitted list, so it also stored a target that was
// submitted only because its rows are on.
//
// kept-git-partial-family: "git" is kept by the save (shown before,
// submitted again) while the blame row is off or absent: the save does not
// switch blame on. The other code rebuilt every row from the submitted list,
// and "git" names the blame key, so it switched blame on; this code changes
// only the rows of the targets a save adds or drops. Blame comes on by a
// request that names "blame", by a request that adds "git", or by the
// dataset switch.
var unofferedDifferenceClasses = map[string]int{
	"stored-list":             unofferedStoredListLines,
	"kept-git-partial-family": unofferedKeptGitLines,
}

type unofferedLine struct {
	save                              string
	rows, stored, answer, shown, plan string
}

func (line unofferedLine) String() string {
	return fmt.Sprintf("save=%s rows=[%s] stored=%s answer=%s get=%s units=[%s]", line.save, line.rows, line.stored, line.answer, line.shown, line.plan)
}

func parseUnofferedLine(t *testing.T, text string) unofferedLine {
	t.Helper()
	var line unofferedLine
	fields := map[string]*string{"save=": &line.save, " rows=[": &line.rows, "] stored=": &line.stored, " answer=": &line.answer, " get=": &line.shown, " units=[": &line.plan}
	order := []string{"save=", " rows=[", "] stored=", " answer=", " get=", " units=["}
	rest := text
	for index, name := range order {
		if !strings.HasPrefix(rest, name) {
			t.Fatalf("line %q: no %q at %q", text, name, rest)
		}
		rest = rest[len(name):]
		end := len(rest)
		if index+1 < len(order) {
			if end = strings.Index(rest, order[index+1]); end < 0 {
				t.Fatalf("line %q: no %q", text, order[index+1])
			}
		} else {
			if !strings.HasSuffix(rest, "]") {
				t.Fatalf("line %q does not end the units list", text)
			}
			end--
		}
		*fields[name] = rest[:end]
		rest = rest[end:]
	}
	return line
}

func readUnofferedRecord(t *testing.T, path string, parts int) map[string][]string {
	t.Helper()
	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	record := map[string][]string{}
	for _, row := range strings.Split(strings.TrimSpace(string(text)), "\n") {
		if row == "" {
			continue
		}
		fields := strings.SplitN(row, " :: ", parts)
		if len(fields) != parts {
			t.Fatalf("%s: row %q has %d parts, want %d", path, row, len(fields), parts)
		}
		if _, twice := record[fields[0]]; twice {
			t.Fatalf("%s: state %q is recorded twice", path, fields[0])
		}
		record[fields[0]] = fields[1:]
	}
	return record
}

// TestTheRowsOfBlameAndSecurityAfterASaveEqualTheSaveThatRebuiltTheRows is
// the table of the targets the form does not offer ("blame", "security")
// against the code before CHAOS-8816. GitHub and GitLab; the four git rows
// are on; the stored list and the submitted list are each one of [], [git],
// [blame], [git,blame], [security], [git,security]; the blame row and the
// security row are both on, both off or both absent: 2 x 6 x 3 x 6 = 216
// states, none left out. For each state one PATCH of the submitted list,
// then: the dataset rows, the stored list, the list the save answers, the
// list GET shows, and the dataset keys of the units a scheduled plan writes.
//
// What it pins: a line equals the recorded one, or it is a named difference
// of a named class; a stored-list difference changes no row, no answer and
// no planned unit; the number of lines of each class.
func TestTheRowsOfBlameAndSecurityAfterASaveEqualTheSaveThatRebuiltTheRows(t *testing.T) {
	v := startCascadeVenue(t, true)
	recorded := readUnofferedRecord(t, unofferedMainRecord, 2)
	differences := readUnofferedRecord(t, unofferedDifferences, 3)
	lists := [][]string{{}, {"git"}, {"blame"}, {"git", "blame"}, {"security"}, {"git", "security"}}
	gitOn := []string{"repo-metadata", "commits", "commit-stats", "files"}
	asJSON := func(items []string) string {
		if items == nil {
			items = []string{}
		}
		out, _ := json.Marshal(items)
		return string(out)
	}
	states, counted := 0, map[string]int{}
	for _, provider := range []string{"github", "gitlab"} {
		for _, stored := range lists {
			for _, rowState := range []string{"on", "off", "absent"} {
				for _, request := range lists {
					states++
					label := fmt.Sprintf("%s stored=%s blame+security rows %s request=%s", provider, asJSON(stored), rowState, asJSON(request))
					on := append([]string{}, gitOn...)
					if rowState == "on" {
						on = append(on, "blame", "security")
					}
					parent := v.seed(provider, asJSON(stored), on)
					if rowState == "off" {
						for _, key := range []string{"blame", "security"} {
							v.exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, false, '{}'::json)`,
								uuid.New(), v.org, parent.integration, key)
						}
					}
					v.plannedSource(parent, provider)
					status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+parent.config.String(), `{"sync_targets":`+asJSON(request)+`}`)
					got := unofferedLine{save: fmt.Sprint(status), rows: v.rows(parent.integration), stored: v.storedList(parent.config), shown: v.shown(parent.config)}
					if status == 200 {
						got.answer = asJSON(answeredTargets(t, body))
					}
					// The plan comes last: it makes the security row of an
					// integration that has none.
					got.plan = v.fetched(label, parent)
					t.Logf("UNOFFERED %s :: %s", label, got)
					main, ok := recorded[label]
					if !ok {
						t.Errorf("%s: no recorded line", label)
						continue
					}
					if got.String() == main[0] {
						if _, named := differences[label]; named {
							t.Errorf("%s: named as a difference, and the line equals the recorded one", label)
						}
						continue
					}
					named, ok := differences[label]
					if !ok {
						t.Errorf("%s: differs from the recorded line and is not a named difference:\n got      %s\n recorded %s", label, got, main[0])
						continue
					}
					class, here := named[0], named[1]
					counted[class]++
					if _, known := unofferedDifferenceClasses[class]; !known {
						t.Errorf("%s: class %q is not a known class", label, class)
					}
					if got.String() != here {
						t.Errorf("%s (%s):\n got  %s\n want %s", label, class, got, here)
					}
					was := parseUnofferedLine(t, main[0])
					switch class {
					case "stored-list":
						if got.save != was.save || got.rows != was.rows || got.plan != was.plan ||
							sortedList(t, got.answer) != sortedList(t, was.answer) || sortedList(t, got.shown) != sortedList(t, was.shown) {
							t.Errorf("%s: a stored-list difference moves a row, a planned unit or an item of the shown list:\n got      %s\n recorded %s", label, got, main[0])
						}
					case "kept-git-partial-family":
						// Only the blame row and the blame unit differ, the request
						// submits "git", and the blame row was not on.
						blameOff := strings.Replace(strings.Replace(","+was.rows, ",blame=true", ",blame=false", 1), ",blame=false", "", 1)
						gotNoBlame := strings.Replace(","+got.rows, ",blame=false", "", 1)
						if rowState == "on" || !strings.Contains(asJSON(request), `"git"`) || gotNoBlame != blameOff ||
							strings.TrimPrefix(was.plan, "blame,") != got.plan || got.save != was.save {
							t.Errorf("%s: not a kept git with the blame row off or absent, or more than the blame row and unit differ:\n got      %s\n recorded %s", label, got, main[0])
						}
					}
				}
			}
		}
	}
	if states != 216 || len(recorded) != 216 {
		t.Errorf("%d states ran and %d are recorded, want 216 and 216", states, len(recorded))
	}
	classes := []string{}
	for class := range unofferedDifferenceClasses {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	for _, class := range classes {
		if counted[class] != unofferedDifferenceClasses[class] {
			t.Errorf("class %s: %d lines differ, want %d", class, counted[class], unofferedDifferenceClasses[class])
		}
	}
	if len(differences) != counted["stored-list"]+counted["kept-git-partial-family"] {
		t.Errorf("%d named differences, %d of them met", len(differences), counted["stored-list"]+counted["kept-git-partial-family"])
	}
}

// TestTheBlameRowFollowsTheSubmittedListAsASet: "git" names the blame key,
// so a save that drops one of the two and submits the other must not let the
// dropped one decide the blame row. A request that adds "blame" and drops
// "git" leaves blame on and fetched; so does one that keeps "blame" and drops
// "git"; a request that drops a stored "blame" (nothing else names its key)
// switches it off and the plan stops fetching it.
func TestTheBlameRowFollowsTheSubmittedListAsASet(t *testing.T) {
	v := startCascadeVenue(t, true)
	gitOn := []string{"repo-metadata", "commits", "commit-stats", "files"}
	cases := 0
	for _, provider := range []string{"github", "gitlab"} {
		for _, testCase := range []struct {
			name, stored, request string
			on, off               []string
			wantBlame             bool
			wantShown             string
		}{
			{name: "blame added and git dropped, blame row off", stored: `["git"]`, on: gitOn, off: []string{"blame"}, request: `["blame"]`, wantBlame: true, wantShown: `["blame"]`},
			{name: "blame added and git dropped, blame row absent", stored: `["git"]`, on: gitOn, request: `["blame"]`, wantBlame: true, wantShown: `["blame"]`},
			{name: "blame kept and git dropped", stored: `["git","blame"]`, on: append([]string{"blame"}, gitOn...), request: `["blame"]`, wantBlame: true, wantShown: `["blame"]`},
			{name: "a stored blame dropped", stored: `["blame"]`, on: []string{"blame"}, request: `[]`, wantShown: `[]`},
		} {
			cases++
			label := provider + ": " + testCase.name
			parent := v.seed(provider, testCase.stored, testCase.on)
			for _, key := range testCase.off {
				v.exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, false, '{}'::json)`,
					uuid.New(), v.org, parent.integration, key)
			}
			v.plannedSource(parent, provider)
			status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+parent.config.String(), `{"sync_targets":`+testCase.request+`}`)
			if status != 200 {
				t.Fatalf("%s: save: %d %s", label, status, body)
			}
			rows := "," + v.rows(parent.integration) + ","
			if strings.Contains(rows, ",blame=true,") != testCase.wantBlame {
				t.Errorf("%s: rows [%s], want the blame row on = %v", label, strings.Trim(rows, ","), testCase.wantBlame)
			}
			for _, key := range gitOn {
				if strings.Contains(rows, ","+key+"=true,") {
					t.Errorf("%s: the %s row is on after a save that does not submit git: [%s]", label, key, strings.Trim(rows, ","))
				}
			}
			answer, _ := json.Marshal(answeredTargets(t, body))
			if string(answer) != testCase.wantShown && !(testCase.wantShown == `[]` && string(answer) == `null`) {
				t.Errorf("%s: the save answers %s, want %s", label, answer, testCase.wantShown)
			}
			if got := v.shown(parent.config); got != testCase.wantShown {
				t.Errorf("%s: GET shows %s, want %s", label, got, testCase.wantShown)
			}
			if got := v.storedList(parent.config); got != testCase.wantShown {
				t.Errorf("%s: stored %s, want %s", label, got, testCase.wantShown)
			}
			plan := "," + v.fetched(label, parent) + ","
			if strings.Contains(plan, ",blame,") != testCase.wantBlame || strings.Contains(plan, ",commits,") {
				t.Errorf("%s: a scheduled sync fetches [%s], want blame fetched = %v and no git dataset", label, strings.Trim(plan, ","), testCase.wantBlame)
			}
		}
	}
	if cases != 8 {
		t.Fatalf("%d cases ran, want 8", cases)
	}
}

// sortedList is the items of a JSON list of strings, sorted, as one text.
func sortedList(t *testing.T, text string) string {
	t.Helper()
	var items []string
	if err := json.Unmarshal([]byte(text), &items); err != nil {
		t.Fatalf("list %q: %v", text, err)
	}
	sort.Strings(items)
	return strings.Join(items, ",")
}
