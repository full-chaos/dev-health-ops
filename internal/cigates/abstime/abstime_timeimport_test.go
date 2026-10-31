package abstime_test

import (
	"strings"
	"testing"
)

func importTree(imp, pkg, cond string) string {
	src := "package p\n\nimport (\n\t\"testing\"\n\t" + imp + "\n)\n\n" +
		"func TestImp(t *testing.T) {\n\ts := " + pkg + "Now()\n\tbase := " + pkg + "Since(s)\n\ts = " + pkg + "Now()\n\tbig := " + pkg + "Since(s)\n" +
		"\tif " + cond + " {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	return src
}

func floorForms(pkg string) []string {
	return []string{
		"big > max(25*base, " + pkg + "Second)",
		"big > min(base, " + pkg + "Millisecond)",
		"big > 2*base+50*" + pkg + "Millisecond",
		"big-base > " + pkg + "Hour",
		"float64(big) > math.Max(float64(base)*2, float64(" + pkg + "Minute))",
		"float64(big) > math.Min(float64(base)*2, float64(" + pkg + "Microsecond))",
		"big > 2*base+(3*" + pkg + "Nanosecond)",
	}
}

func TestAliasedTimeImportFloorsAreRefused(t *testing.T) {
	for _, cond := range floorForms("clock.") {
		src := strings.Replace(importTree(`clock "time"`, "clock.", cond), "\t\"testing\"\n", "\t\"math\"\n\t\"testing\"\n", 1)
		wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": src}), nil), "TestImp")
	}
}

func TestDotTimeImportFloorsAreRefused(t *testing.T) {
	for _, cond := range floorForms("") {
		src := strings.Replace(importTree(`. "time"`, "", cond), "\t\"testing\"\n", "\t\"math\"\n\t\"testing\"\n", 1)
		wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": src}), nil), "TestImp")
	}
}

func TestAnIdentifierNamedTimeThatIsNotThePackageIsNotAFloor(t *testing.T) {
	src := "package p\n\nimport (\n\t\"testing\"\n\tclock \"time\"\n)\n\ntype tm struct{ Second, Hour int }\n\nvar time tm\n\n" +
		"func TestImp(t *testing.T) {\n\ts := clock.Now()\n\tbase := clock.Since(s)\n\ts = clock.Now()\n\tbig := clock.Since(s)\n" +
		"\tif big > max(2*base, clock.Duration(time.Second)) {\n\t\tt.Fatal(\"x\")\n\t}\n\tif big+base > clock.Duration(time.Hour) {\n\t\tt.Fatal(\"y\")\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": src}), nil))
}

func TestAFileThatDoesNotImportTimeHasNoUnitSelector(t *testing.T) {
	src := "package p\n\nimport \"testing\"\n\nvar time struct{ Second int }\n\nfunc TestImp(t *testing.T) {\n\tbase, big := elapsed, took\n\tif big > max(2*base, time.Second) {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": src}), nil))
}

func TestDefaultTimeImportStillRefusesAndRatiosStillPass(t *testing.T) {
	src := strings.Replace(importTree(`"time"`, "time.", "big > max(25*base, time.Second)"), "\t\"testing\"\n", "\t\"testing\"\n", 1)
	wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": src}), nil), "TestImp")
	ok := importTree(`clock "time"`, "clock.", "big > 2*base")
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": ok}), nil))
	okDot := importTree(`. "time"`, "", "big > 2*base")
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": okDot}), nil))
}
