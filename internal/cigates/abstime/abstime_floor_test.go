package abstime_test

import "testing"

const floorPrelude = "\ts := time.Now()\n\tbase := time.Since(s)\n\ts = time.Now()\n\tbig := time.Since(s)\n"

func floorTree(t *testing.T, imports, cond string) string {
	t.Helper()
	src := "package p\n\nimport (\n\t\"context\"\n" + imports + "\t\"testing\"\n\t\"time\"\n)\n\nvar _ = context.Background\n\n" +
		"func TestFloor(t *testing.T) {\n" + floorPrelude + "\tif " + cond + " {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	return writeTree(t, map[string]string{"p/x_test.go": src})
}

func TestMaxFloorOnMeasuredPairIsRefused(t *testing.T) {
	wantOne(t, check(t, floorTree(t, "", "big > max(25*base, time.Second)"), nil), "p/x_test.go:", "TestFloor", "D6418")
}

func TestAdditiveFloorIsRefused(t *testing.T) {
	wantOne(t, check(t, floorTree(t, "", "big > 2*base+50*time.Millisecond"), nil), "TestFloor")
	wantOne(t, check(t, floorTree(t, "", "big-base > 100*time.Millisecond"), nil), "TestFloor")
}

func TestMinFloorIsRefused(t *testing.T) {
	wantOne(t, check(t, floorTree(t, "", "big > min(base, time.Second)"), nil), "TestFloor")
}

func TestMathMaxFloorIsRefused(t *testing.T) {
	wantOne(t, check(t, floorTree(t, "\t\"math\"\n", "float64(big) > math.Max(float64(base)*2, float64(time.Second))"), nil), "TestFloor")
}

func TestRatiosWithoutAFloorStillPass(t *testing.T) {
	for _, cond := range []string{"big > 2*base", "big > 3*base", "big+base > 4*base", "max(big, base) > 2*base", "big > max(2*base, 3*base)", "float64(big)/float64(base) > 2", "big > base+base*time.Nanosecond"} {
		wantClean(t, check(t, floorTree(t, "", cond), nil))
	}
}

func TestAFloorHeldInANamedValueIsOutOfScope(t *testing.T) {
	src := "package p\n\nimport (\n\t\"testing\"\n\t\"time\"\n)\n\nconst floor = time.Second\n\nfunc TestNamed(t *testing.T) {\n" + floorPrelude +
		"\tif big > max(2*base, floor) {\n\t\tt.Fatal(\"x\")\n\t}\n\tif big > max(2*base, 1000) {\n\t\tt.Fatal(\"y\")\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": src}), nil))
}
