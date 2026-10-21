package providersync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// oraclePairPythonBuild is the build whose production Python answered the
// frozen pairs: python_generic_row_oracle.py executed there, once per
// comparison, against the real functions under src/dev_health_ops. A pair
// named in oraclePairLaterPythonBuilds was answered on that later build.
const oraclePairPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// oraclePairGitLabEpicBuild is the first build that holds the
// gitlab/work-items/issue-provider pair file, the GitLab provider batch's
// issue row with its label priority and epic link (CHAOS-9854). The GitLab
// issue and sprint pairs, answered before on the due_date build a4d97f56
// (CHAOS-9537), key on every harness file, so they are answered on it too.
const oraclePairGitLabEpicBuild = "efb85285578233689bf30c09e3fadf157db56f97"

// oraclePairGitLabEpicRowsBuild is the first build that holds the
// gitlab/work-items/epic and epic-transition pair files, the GitLab provider
// batch's group-epic work item and its transitions (CHAOS-9857).
const oraclePairGitLabEpicRowsBuild = "9c3493ae5dd6b817562c36096e3c1cd1f3552ef9"

// oraclePairLaterPythonBuilds names, by pair, a build later than
// oraclePairPythonBuild that answered the pair: its answer needs Python that
// oraclePairPythonBuild does not hold (a producer change, or a pair file added
// after it). Such a pair is recorded from a checkout of the later build.
var oraclePairLaterPythonBuilds = map[string]string{
	"gitlab/work-items/issue":           oraclePairGitLabEpicBuild,
	"gitlab/work-items/issue-provider":  oraclePairGitLabEpicBuild,
	"gitlab/work-items/sprint":          oraclePairGitLabEpicBuild,
	"gitlab/work-items/epic":            oraclePairGitLabEpicRowsBuild,
	"gitlab/work-items/epic-transition": oraclePairGitLabEpicRowsBuild,
}

// oraclePairBuild is the build that answered pairID.
func oraclePairBuild(pairID string) string {
	if build, later := oraclePairLaterPythonBuilds[pairID]; later {
		return build
	}
	return oraclePairPythonBuild
}

// oraclePairGoldenDir holds one golden per comparison: the pair's Python answer
// for the cases of the one test (or subtest) that asks for it.
const oraclePairGoldenDir = "testdata/oracle_golden"

// oraclePairGoldensOpened records, per golden, the test run that last opened
// it: one frozen answer stands in for one comparison of that run.
var oraclePairGoldensOpened sync.Map

// frozenPairAnswer returns what python_generic_row_oracle printed for pairID
// over encodedCases, from the pair's golden. No Python runs. A golden that is
// missing, changed or recorded for other cases fails the test.
func frozenPairAnswer(t *testing.T, pairID string, encodedCases []byte) []byte {
	t.Helper()
	name := oraclePairGoldenName(pairID, t.Name())
	build := oraclePairBuild(pairID)
	if err := claimOraclePairGolden(&oraclePairGoldensOpened, name, t, pairID, t.Name()); err != nil {
		t.Fatal(err)
	}
	pin, pinned := oraclePairGoldenPins[name]
	if !pinned {
		t.Fatalf("pair %q in test %s has no frozen golden: oraclePairGoldenPins names no %s", pairID, t.Name(), name)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        filepath.Join(oraclePairGoldenDir, name),
		PythonBuild: build,
		SHA256:      pin,
		Scrub:       pairPerRunScrub(pairID),
	})
	request := venueoracle.ProgramRequest(pairID, encodedCases)
	answers := golden.Produce(t, []venueoracle.Request{request})
	golden.Consumed(t, answers...)
	output := []byte(oraclePairAnswerText(t, answers[0].Body))
	if err := untaggedLeafErr(output); err != nil {
		t.Fatalf("pair %q: %v", pairID, err)
	}
	if err := pairPerRunErr(pairID, output); err != nil {
		t.Fatalf("golden %s: %v", name, err)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
	return output
}

// claimOraclePairGolden is an error when run already opened the golden name: a
// test that compares one pair twice would read the same answer for both
// comparisons. run identifies one execution of a test (its *testing.T), so the
// same test executed again in the process (go test -count=2) opens its golden
// again.
func claimOraclePairGolden(opened *sync.Map, name string, run any, pairID, testName string) error {
	if previous, _ := opened.Swap(name, run); previous == run {
		return fmt.Errorf("pair %q is asked twice by test %s: one frozen answer stands in for one "+
			"comparison, so give each comparison of a pair its own subtest", pairID, testName)
	}
	return nil
}

// oraclePairPackAbove is the answer size above which a golden stores the
// answer packed (venueoracle.PackBody): the same bytes, compressed.
const oraclePairPackAbove = 64 << 10

// oraclePairAnswerText is the runner's stdout a golden body holds.
func oraclePairAnswerText(t *testing.T, body string) string {
	t.Helper()
	if strings.HasPrefix(body, "gzip+base64:") {
		return venueoracle.UnpackBody(t, body)
	}
	return body
}

var unsafeGoldenNameRune = regexp.MustCompile(`[^A-Za-z0-9_.-]`)

// oraclePairGoldenName is the golden file of one comparison: the pair and the
// test that asks, so a golden belongs to one test.
func oraclePairGoldenName(pairID, testName string) string {
	clean := func(part string) string {
		return unsafeGoldenNameRune.ReplaceAllString(strings.ReplaceAll(part, "/", "."), "_")
	}
	return strings.ReplaceAll(pairID, "/", "_") + "." + clean(testName) + ".golden.json"
}

// untaggedLeafErr is an error when a runner answer holds a row value that is
// not null, a container, or a type-tagged leaf {"t": type, "v": string}. A bare
// JSON number, string or boolean would be compared by its JSON text, which is
// the precision and type collapse the tags exist to stop; a golden must never
// freeze one.
func untaggedLeafErr(output []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.UseNumber()
	var decoded struct {
		Cases []struct {
			ID  string         `json:"id"`
			Row map[string]any `json:"row"`
		} `json:"cases"`
	}
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("the answer is not the runner's JSON: %w", err)
	}
	if len(decoded.Cases) == 0 {
		return fmt.Errorf("the answer holds no case")
	}
	for _, entry := range decoded.Cases {
		for field, value := range entry.Row {
			if path, found := untaggedLeaf(value, field); found {
				return fmt.Errorf("case %q holds an untagged value at %s: every leaf of a frozen row is {\"t\": type, \"v\": string} or null", entry.ID, path)
			}
		}
	}
	return nil
}

func untaggedLeaf(value any, path string) (string, bool) {
	switch typed := value.(type) {
	case nil:
		return "", false
	case []any:
		for index, item := range typed {
			if found, bad := untaggedLeaf(item, fmt.Sprintf("%s[%d]", path, index)); bad {
				return found, true
			}
		}
		return "", false
	case map[string]any:
		if _, hasTag := typed["t"]; hasTag && len(typed) == 2 {
			if _, hasValue := typed["v"]; hasValue {
				_, tagIsString := typed["t"].(string)
				_, valueIsString := typed["v"].(string)
				if tagIsString && valueIsString {
					return "", false
				}
			}
		}
		for key, item := range typed {
			if found, bad := untaggedLeaf(item, path+"."+key); bad {
				return found, true
			}
		}
		return "", false
	default:
		return path, true
	}
}
