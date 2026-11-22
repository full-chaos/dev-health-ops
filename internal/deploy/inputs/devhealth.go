package inputs

import (
	"regexp"
	"strconv"
	"strings"
)

// PGFloorRev is the first filtered dev-health revision: a rollback target
// below it re-creates the chart Secret and flips the postgres env (kit v3l).
const PGFloorRev = 449

// The dev-health key regexes: the text of the kit's keyre() for the same key
// (agentkit prodroll.sh v3z21). rK8SList, rScalers and rImgRow nest counted
// repeats past RE2's limit of 1000, so they run as matchK8SList, matchScalers
// and matchImgRow.
const (
	rSHA40   = `^[0-9a-f]{40}$`
	rSHA256  = `^[0-9a-f]{64}$`
	rRev     = `^[1-9][0-9]{0,5}$`
	rRevList = `^[1-9][0-9]{0,5}(,[1-9][0-9]{0,5}){0,9}$`
	rStep    = `^[1-9][0-9]{0,2}$`
	rGuard   = `^[56]$`
	rPGVer   = `^[0-9a-z_]{1,32}(,[0-9a-z_]{1,32}){0,7}$`
	rHex     = `^[0-9a-f]{12,64}$`
	rYesNo   = `^(yes|no)$`
	rK8S     = `[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?`
	rK8SList = `^` + rK8S + `(,` + rK8S + `){0,31}$`
	rScalers = `^(none|` + rK8S + `(,` + rK8S + `){0,31})$`
	rKN      = `[A-Z][A-Za-z]{1,40}/` + rK8S
	rFPath   = `\.[A-Za-z0-9_][A-Za-z0-9_./-]{0,200}(\[([0-9]{1,3}|[A-Za-z_][A-Za-z0-9_]{0,127})\][A-Za-z0-9_./-]{0,200}){0,8}`
	rFVal    = `(<none>|[A-Za-z0-9./:,!_=@+-]{1,200}|[A-Za-z0-9./:,!_=@+-]{1,199}[$])`
	rImgRow  = `^` + rKN + ` ` + rFPath + ` (<secret>|` + rFVal + `) (<empty>|` + rFVal + `)$`
)

// devHealthNotBuilt are the dev-health keys of kit forms this binary refuses
// until their parity PR: the kit stays the path for them.
var devHealthNotBuilt = map[string]bool{
	"IMG_COUPLED": true, "CMDELTA": true, "LIVEABSENT": true, "ADDDOC": true, "REMOVED_DOC": true,
	"ADDED_DOC": true, "ADDCONT": true, "ENVREORDER": true, "GRANTOPT_WANT": true, "GRANTWIDEN": true,
	"AUTHVER": true, "GRANTADD": true, "CFGROLL": true, "LONEJOB_RUNLOG": true, "LONEJOB_RUNLOG_SHA256": true,
	"OLDJOB_READBACK": true, "SECRET_KEEP": true, "HOOK_SECRET": true, "SECRET_KEYS": true, "SERIES_FILE": true,
	"DSSTEP": true, "SECRET_DROP": true, "PGFILTER_OFF_REASON": true,
}

func spec(re string, required bool) keySpec {
	return keySpec{match: regexp.MustCompile(re).MatchString, text: re, required: required}
}

func specFunc(match func(string) bool, text string, required bool) keySpec {
	return keySpec{match: match, text: text, required: required}
}

var (
	k8sName   = regexp.MustCompile(`^` + rK8S + `$`)
	kindName  = regexp.MustCompile(`^[A-Z][A-Za-z]{1,40}/` + rK8S + `$`)
	pathHead  = regexp.MustCompile(`^\.[A-Za-z0-9_][A-Za-z0-9_./-]{0,200}$`)
	pathIndex = regexp.MustCompile(`^([0-9]{1,3}|[A-Za-z_][A-Za-z0-9_]{0,127})$`)
	pathTail  = regexp.MustCompile(`^[A-Za-z0-9_./-]{0,200}$`)
	fieldVal  = regexp.MustCompile(`^` + rFVal + `$`)
)

// matchK8SList is rK8SList: 1..32 comma-separated Kubernetes names.
func matchK8SList(v string) bool {
	parts := strings.Split(v, ",")
	if len(parts) > 32 {
		return false
	}
	for _, p := range parts {
		if !k8sName.MatchString(p) {
			return false
		}
	}
	return true
}

// matchScalers is rScalers: "none" alone, or a K8S list.
func matchScalers(v string) bool { return v == "none" || matchK8SList(v) }

// matchPath is rFPath: a head, then at most 8 `[index]tail` groups. No class
// of the path holds '[' or ']', so the split at the brackets is the only one.
func matchPath(v string) bool {
	head, rest, _ := strings.Cut(v, "[")
	if !pathHead.MatchString(head) {
		return false
	}
	if !strings.Contains(v, "[") {
		return true
	}
	for groups := 1; ; groups++ {
		if groups > 8 {
			return false
		}
		index, after, closed := strings.Cut(rest, "]")
		if !closed || !pathIndex.MatchString(index) {
			return false
		}
		tail, next, more := strings.Cut(after, "[")
		if !pathTail.MatchString(tail) {
			return false
		}
		if !more {
			return true
		}
		rest = next
	}
}

// matchImgRow is rImgRow: `<Kind/name> <path> <old> <new>`, single spaces (no
// field class holds a space), old <secret> or a value, new <empty> or a value.
func matchImgRow(v string) bool {
	f := strings.Split(v, " ")
	return len(f) == 4 && kindName.MatchString(f[0]) && matchPath(f[1]) &&
		(f[2] == "<secret>" || fieldVal.MatchString(f[2])) &&
		(f[3] == "<empty>" || fieldVal.MatchString(f[3]))
}

var targets = map[string]targetSpec{
	"dev-health": {
		keys: map[string]keySpec{
			"STEP":                    spec(rStep, true),
			"SVC_SHA":                 spec(rSHA40, true),
			"ROLLBACK_SHA":            spec(rSHA40, true),
			"EXP_START_REV":           spec(rRev, true),
			"ROLLBACK_REV":            spec(rRev, true),
			"EXPECT_NEWEST_REV":       spec(rRev, true),
			"GUARD_LIVE_STEP":         spec(rGuard, true),
			"PGVER":                   spec(rPGVer, true),
			"RENDER_SHA256":           spec(rSHA256, true),
			"PODSNAP_TARGETS":         specFunc(matchK8SList, rK8SList, true),
			"PODSNAP_SCALERS":         specFunc(matchScalers, rScalers, true),
			"SCRAPE":                  spec(rYesNo, true),
			"MIGJOBLOG":               spec(rYesNo, true),
			"PGF_LIB_SHA256":          spec(rSHA256, true),
			"PGF_LIB_SHA256_ROLLBACK": spec(rSHA256, false),
			"PGFILTER":                spec(rYesNo, false),
			"FAILED_REVS":             spec(rRevList, false),
			"IMG_OLD":                 spec(rHex, false),
			"IMG_NEW":                 spec(rHex, false),
			"IMGROW":                  {match: matchImgRow, text: rImgRow, maxRepeat: 64, required: true},
		},
		notBuilt: devHealthNotBuilt,
		check:    checkDevHealth,
	},
}

// ImgRow is one IMGROW line: <Kind/name> <path> <old> <new>, and its file line.
type ImgRow struct {
	Doc, Path, Old, New string
	Line                int
}

// DevHealth is the derived plan of a dev-health one-service app roll.
type DevHealth struct {
	Step            int
	SvcSHA          string
	RollbackSHA     string
	ExpStartRev     int
	RollbackRev     int
	ExpectNewestRev int
	FailedRevs      []int
	ImgOld, ImgNew  string
	Rows            []ImgRow
	RenderSHA256    string
}

var (
	podImagePath = regexp.MustCompile(`^\.spec\.template\.spec\.(containers|initContainers)\[[0-9]{1,3}\]\.image$`)
	hexDigest    = regexp.MustCompile(rHex)
)

// migrateHooks are the chart's migrate, data and grant hook Jobs
// (deploy/helm/dev-health/templates: river-hooks.yaml, migrate-job.yaml,
// auth-migrate-job.yaml, auth-grants-job.yaml). Their pins are never rolled
// back (acp Q5) and the kit gates their rows with post-roll posture reads, so
// a row on a doc of one of these names, of any kind, is a kit form. The one
// other chart hook, route-activate, rides an app roll in the kit (s41-a).
var migrateHooks = []string{"river-migrate", "provision-roles", "migrate", "streak-index", "open-units-index", "finalize-outbox-index", "auth-migrate", "auth-grants"}

// hookDoc: a name is a hook doc when it is <hook> or ends in "-<hook>", under any release prefix and any kind.
func hookDoc(name string) bool {
	for _, h := range migrateHooks {
		if name == h || strings.HasSuffix(name, "-"+h) {
			return true
		}
	}
	return false
}

// authDoc is any doc of the auth-service family: every chart auth resource
// is named <fullname>-auth-... (auth-service and its Services, keyset,
// token-review RBAC and PDB, the auth Jobs). The kit rolls them with the
// restore-fence and GRANTWIDEN gates.
func authDoc(name string) bool { return strings.Contains(name, "-auth-") }

func checkDevHealth(in *Inputs) error {
	v := in.Values
	dh := &DevHealth{SvcSHA: v["SVC_SHA"], RollbackSHA: v["ROLLBACK_SHA"], RenderSHA256: v["RENDER_SHA256"]}
	dh.Step, _ = strconv.Atoi(v["STEP"])
	dh.ExpStartRev, _ = strconv.Atoi(v["EXP_START_REV"])
	dh.RollbackRev, _ = strconv.Atoi(v["ROLLBACK_REV"])
	dh.ExpectNewestRev, _ = strconv.Atoi(v["EXPECT_NEWEST_REV"])

	if pf, set := v["PGFILTER"]; set && pf != "yes" {
		return notBuilt(ClassKitKey, "PGFILTER", in.line("PGFILTER"), "PGFILTER=no (the one escape needs DSSTEP=yes + PGFILTER_OFF_REASON)")
	}
	if dh.RollbackRev != dh.ExpStartRev {
		return refuse(ClassRevRule, "ROLLBACK_REV", in.line("ROLLBACK_REV"), "ROLLBACK_REV must equal EXP_START_REV: the rollback target is the revision before the step")
	}
	want := dh.ExpStartRev + 1
	if fr, set := v["FAILED_REVS"]; set {
		for i, s := range strings.Split(fr, ",") {
			n, _ := strconv.Atoi(s)
			if n != dh.ExpStartRev+i+1 {
				return refuse(ClassRevRule, "FAILED_REVS", in.line("FAILED_REVS"), "FAILED_REVS must be EXP_START_REV + 1 .. EXP_START_REV + n in order")
			}
			dh.FailedRevs = append(dh.FailedRevs, n)
		}
		want += len(dh.FailedRevs)
	}
	if dh.ExpectNewestRev != want {
		return refuse(ClassRevRule, "EXPECT_NEWEST_REV", in.line("EXPECT_NEWEST_REV"), "EXPECT_NEWEST_REV must be EXP_START_REV + the failed rows + 1 (%d): the roll makes exactly one revision", want)
	}
	if dh.SvcSHA == dh.RollbackSHA {
		return refuse(ClassShaRule, "SVC_SHA", in.line("SVC_SHA"), "SVC_SHA equals ROLLBACK_SHA")
	}
	if dh.RollbackRev < PGFloorRev {
		return refuse(ClassRevRule, "ROLLBACK_REV", in.line("ROLLBACK_REV"), "ROLLBACK_REV is below %d (a rollback past it re-creates the chart Secret)", PGFloorRev)
	}

	pairs := map[[2]string]bool{}
	var pair [2]string
	pairLine := map[[2]string]int{}
	var pairLines []int
	for i, raw := range in.Lists["IMGROW"] {
		f := strings.Split(raw, " ")
		r := ImgRow{Doc: f[0], Path: f[1], Old: f[2], New: f[3], Line: in.lines["IMGROW"][i]}
		_, name, _ := strings.Cut(r.Doc, "/")
		switch {
		case r.Old == "<secret>" || r.New == "<empty>":
			return notBuilt(ClassKitKey, "IMGROW", r.Line, "an IMGROW <secret> / <empty> row")
		case hookDoc(name):
			return notBuilt(ClassHookRow, "IMGROW", r.Line, "an IMGROW row on a migrate, data or grant hook Job doc")
		case authDoc(name):
			return notBuilt(ClassAuthRow, "IMGROW", r.Line, "an IMGROW row on an auth doc (restore-fence, GRANTWIDEN)")
		case strings.HasPrefix(r.Doc, "StatefulSet/"):
			return notBuilt(ClassKitKey, "IMGROW", r.Line, "an IMGROW row on a StatefulSet (DSSTEP)")
		}
		if strings.HasPrefix(r.Doc, "Deployment/") && podImagePath.MatchString(r.Path) && hexDigest.MatchString(r.Old) && hexDigest.MatchString(r.New) {
			pair = [2]string{r.Old, r.New}
			if !pairs[pair] {
				pairs[pair] = true
				pairLine[pair] = r.Line
				pairLines = append(pairLines, r.Line)
			}
		}
		dh.Rows = append(dh.Rows, r)
	}
	switch len(pairs) {
	case 0:
		return notBuilt(ClassKitKey, "IMGROW", in.line("IMGROW"), "no Deployment .image digest pair (the norolls, config-only and ADDCONT forms)")
	case 1:
	default:
		return refuse(ClassDigestRule, "IMGROW", pairLines[1], "this Deployment .image row and the one on line %d carry two digest pairs (D6415: one service per roll)", pairLines[0])
	}
	dh.ImgOld, dh.ImgNew = pair[0], pair[1]
	if dh.ImgOld == dh.ImgNew || strings.HasPrefix(dh.ImgOld, dh.ImgNew) || strings.HasPrefix(dh.ImgNew, dh.ImgOld) {
		return refuse(ClassDigestRule, "IMGROW", pairLine[pair], "the old and new digest prefixes are not two different hex prefixes")
	}
	for _, r := range dh.Rows {
		if strings.HasSuffix(r.Path, ".image") && (r.Old != dh.ImgOld || r.New != dh.ImgNew) {
			return refuse(ClassDigestRule, "IMGROW", r.Line, "this .image row is not the one digest pair of the step (line %d; D6415: one service per roll)", pairLine[pair])
		}
	}
	if o, set := v["IMG_OLD"]; set && o != dh.ImgOld {
		return refuse(ClassDigestRule, "IMG_OLD", in.line("IMG_OLD"), "IMG_OLD differs from the Deployment .image rows")
	}
	if n, set := v["IMG_NEW"]; set && n != dh.ImgNew {
		return refuse(ClassDigestRule, "IMG_NEW", in.line("IMG_NEW"), "IMG_NEW differs from the Deployment .image rows")
	}
	in.DevHealth = dh
	return nil
}
