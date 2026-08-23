package cpyrandom

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// The golden vectors are captured FROM the live CPython random module by
// tests/fixtures/generate_cpython_random_golden.py. They are compared here
// without an interpreter so this runs in the ordinary unit suite; the live-
// oracle lane separately re-derives them from CPython and fails if the
// recording has gone stale, which is the pairing the frozen numerical golden
// lacked before it was guarded.
type goldenDocument struct {
	PythonVersion string       `json:"python_version"`
	Cases         []goldenCase `json:"cases"`
}

type goldenCase struct {
	Kind       string            `json:"kind"`
	Seed       string            `json:"seed"`
	K          int               `json:"k"`
	N          int               `json:"n"`
	Rejects    bool              `json:"rejects"`
	Draws      []json.RawMessage `json:"draws"`
	Operations []goldenOperation `json:"operations"`
}

type goldenOperation struct {
	Op    string `json:"op"`
	K     int    `json:"k"`
	N     int    `json:"n"`
	Value string `json:"value"`
}

func loadGolden(t *testing.T) goldenDocument {
	t.Helper()
	path := filepath.Join(repoRoot(t), "tests", "fixtures", "cpython_random_golden.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var document goldenDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(document.Cases) == 0 {
		t.Fatal("golden carries no cases -- an empty fixture compares equal to everything")
	}
	return document
}

func seedOf(t *testing.T, text string) *big.Int {
	t.Helper()
	seed, ok := new(big.Int).SetString(text, 10)
	if !ok {
		t.Fatalf("unparseable seed %q", text)
	}
	return seed
}

func TestGoStreamMatchesCPython(t *testing.T) {
	document := loadGolden(t)

	var (
		rejectingChoiceCases int
		wideBitCases         int
		negativeSeedCases    int
		longStreamDraws      int
	)

	for _, test := range document.Cases {
		test := test
		name := test.Kind + "/seed=" + test.Seed
		if test.Kind == "getrandbits" {
			name += "/k=" + strconv.Itoa(test.K)
		} else if test.N > 0 {
			name += "/n=" + strconv.Itoa(test.N)
		}

		seed := seedOf(t, test.Seed)
		if seed.Sign() < 0 {
			negativeSeedCases++
		}
		if test.K > 32 {
			wideBitCases++
		}
		if test.Rejects {
			rejectingChoiceCases++
		}
		if test.Kind == "choice_index_long" {
			longStreamDraws += len(test.Draws)
		}

		t.Run(name, func(t *testing.T) {
			source := &Source{}
			source.Seed(seed)

			switch test.Kind {
			case "getrandbits":
				for index, want := range test.Draws {
					got, err := source.GetRandBits(test.K)
					if err != nil {
						t.Fatalf("draw %d: %v", index, err)
					}
					assertSame(t, index, got, want)
				}
			case "choice_index", "choice_index_long":
				for index, want := range test.Draws {
					got, err := source.Choice(test.N)
					if err != nil {
						t.Fatalf("draw %d: %v", index, err)
					}
					assertSame(t, index, uint64(got), want)
				}
			case "interleaved":
				// One seeding, mixed call shapes. Every other case reseeds and
				// therefore only ever inspects the stream's opening; this is
				// what catches a port that advances state by the wrong amount
				// for one call type.
				for index, operation := range test.Operations {
					var got uint64
					var err error
					switch operation.Op {
					case "getrandbits":
						got, err = source.GetRandBits(operation.K)
					case "choice":
						var choice int
						choice, err = source.Choice(operation.N)
						got = uint64(choice)
					default:
						t.Fatalf("unknown operation %q", operation.Op)
					}
					if err != nil {
						t.Fatalf("operation %d: %v", index, err)
					}
					if strconv.FormatUint(got, 10) != operation.Value {
						t.Fatalf("operation %d (%s): got %d, want %s",
							index, operation.Op, got, operation.Value)
					}
				}
			default:
				t.Fatalf("unknown case kind %q", test.Kind)
			}
		})
	}

	// ---- Anti-vacuity ----------------------------------------------------
	//
	// Each of these covers a bug class that is INVISIBLE without it, so a
	// golden that quietly lost them would still pass while the port was wrong:
	//
	//   - no rejecting length  -> a broken _randbelow rejection loop matches
	//   - no k > 32            -> inverted word assembly matches
	//   - no negative seed     -> a missing abs() matches
	//   - no long stream       -> a broken twist matches for 623 draws
	if rejectingChoiceCases == 0 {
		t.Error("no non-power-of-two choice lengths: the rejection loop is untested")
	}
	if wideBitCases == 0 {
		t.Error("no k > 32 cases: multi-word assembly and its order are untested")
	}
	if negativeSeedCases == 0 {
		t.Error("no negative seeds: CPython's abs() on the seed is untested")
	}
	if longStreamDraws < 5000 {
		t.Errorf(
			"longest stream is %d draws; MT19937 regenerates every %d, so a "+
				"twist bug survives anything shorter", longStreamDraws, stateSize)
	}
}

func assertSame(t *testing.T, index int, got uint64, want json.RawMessage) {
	t.Helper()
	var text string
	if err := json.Unmarshal(want, &text); err != nil {
		var number uint64
		if err := json.Unmarshal(want, &number); err != nil {
			t.Fatalf("draw %d: unparseable golden value %s", index, want)
		}
		text = strconv.FormatUint(number, 10)
	}
	if strconv.FormatUint(got, 10) != text {
		t.Fatalf("draw %d: got %d, want %s", index, got, text)
	}
}

func TestChoiceRefusesAnEmptySequence(t *testing.T) {
	// CPython raises IndexError. Returning 0 would index a slice that has no
	// element 0, so this must refuse rather than produce a plausible number.
	if _, err := New(1).Choice(0); err == nil {
		t.Fatal("choosing from an empty sequence must refuse")
	}
}

func TestGetRandBitsRefusesBeyondItsAuditedCeiling(t *testing.T) {
	// The ceiling is not arbitrary: this port was audited against capacity's
	// call surface, where k is at most 63. A wider request means an unaudited
	// caller, and guessing would be worse than refusing.
	if _, err := New(1).GetRandBits(65); err == nil {
		t.Fatal("a width beyond the audited ceiling must refuse")
	}
	if _, err := New(1).GetRandBits(-1); err == nil {
		t.Fatal("a negative width must refuse")
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("no go.mod above the test")
		}
		directory = parent
	}
}
