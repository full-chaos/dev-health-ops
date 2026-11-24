package inputs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
)

const (
	svcSHA  = "1111111111111111111111111111111111111111"
	rbSHA   = "033140968a4fd14b31cb3606a7fe1ec749f5b8ba"
	zero64  = "0000000000000000000000000000000000000000000000000000000000000000"
	oldDig  = "2c87c3613bca"
	newDig  = "3c61345532cc"
	canary  = "canary_pass"
	dhHead  = "# a one-service app roll (the kit s41-a example form)\nDHO_DEPLOY_INPUTS=1\nTARGET=dev-health\nRUN_TAG=s41-a\nAUTHOR=fcsup\n"
	dhScal  = "STEP=41\nSVC_SHA=" + svcSHA + "\nROLLBACK_SHA=" + rbSHA + "\nEXP_START_REV=449\nGUARD_LIVE_STEP=6\nPGVER=0066,0162\nRENDER_SHA256=" + zero64 + "\nPODSNAP_TARGETS=dev-health-ops-go-reconciler,dev-health-ops-go-scheduler\nPODSNAP_SCALERS=dev-health-ops-web\nPGF_LIB_SHA256=" + zero64 + "\nSCRAPE=no\nMIGJOBLOG=yes\nROLLBACK_REV=449\nEXPECT_NEWEST_REV=450\n"
	dhRows  = "IMGROW=Deployment/dev-health-ops-go-reconciler .spec.template.spec.containers[0].image " + oldDig + " " + newDig + "\nIMGROW=Deployment/dev-health-ops-go-scheduler .spec.template.spec.containers[0].image " + oldDig + " " + newDig + "\nIMGROW=Job/dev-health-ops-route-activate .spec.template.spec.initContainers[0].image " + oldDig + " " + newDig + "\n"
	goodDev = dhHead + dhScal + dhRows
)

// kitKeyre is the output of the kit's keyre() for TGT=dev-health (agentkit
// 7022161ce650 prodroll/prodroll.sh v3z21, sha256 17ccd0517b88...), one key per
// line: a drift between the kit and this table is a red cell.
const kitKeyre = `TARGET	^(dev-health|ingress-nginx|f1pipe|acr)$
RUN_TAG	^[a-z0-9][a-z0-9-]{0,31}$
AUTHOR	^[a-z][a-z0-9-]{1,31}$
STEP	^[1-9][0-9]{0,2}$
SVC_SHA	^[0-9a-f]{40}$
ROLLBACK_SHA	^[0-9a-f]{40}$
EXP_START_REV	^[1-9][0-9]{0,5}$
ROLLBACK_REV	^[1-9][0-9]{0,5}$
EXPECT_NEWEST_REV	^[1-9][0-9]{0,5}$
GUARD_LIVE_STEP	^[56]$
PGVER	^[0-9a-z_]{1,32}(,[0-9a-z_]{1,32}){0,7}$
IMG_OLD	^[0-9a-f]{12,64}$
IMG_NEW	^[0-9a-f]{12,64}$
IMGROW	^[A-Z][A-Za-z]{1,40}/[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])? \.[A-Za-z0-9_][A-Za-z0-9_./-]{0,200}(\[([0-9]{1,3}|[A-Za-z_][A-Za-z0-9_]{0,127})\][A-Za-z0-9_./-]{0,200}){0,8} (<secret>|(<none>|[A-Za-z0-9./:,!_=@+-]{1,200}|[A-Za-z0-9./:,!_=@+-]{1,199}[$])) (<empty>|(<none>|[A-Za-z0-9./:,!_=@+-]{1,200}|[A-Za-z0-9./:,!_=@+-]{1,199}[$]))$
RENDER_SHA256	^[0-9a-f]{64}$
PODSNAP_TARGETS	^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(,[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?){0,31}$
PODSNAP_SCALERS	^(none|[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(,[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?){0,31})$
SCRAPE	^(yes|no)$
MIGJOBLOG	^(yes|no)$
PGFILTER	^(yes|no)$
PGF_LIB_SHA256	^[0-9a-f]{64}$
PGF_LIB_SHA256_ROLLBACK	^[0-9a-f]{64}$
FAILED_REVS	^[1-9][0-9]{0,5}(,[1-9][0-9]{0,5}){0,9}$`

func regexOf(t *testing.T, key string) string {
	t.Helper()
	switch key {
	case "TARGET":
		return reTarget.String()
	case "RUN_TAG":
		return reRunTag.String()
	case "AUTHOR":
		return reAuthor.String()
	}
	ks, ok := targets["dev-health"].keys[key]
	if !ok {
		t.Fatalf("key %s is not a dev-health key of this table", key)
	}
	return ks.text
}

func TestKeyRegexesAreTheKitText(t *testing.T) {
	n := 0
	for _, row := range strings.Split(kitKeyre, "\n") {
		key, re, _ := strings.Cut(row, "\t")
		if got := regexOf(t, key); got != re {
			t.Errorf("%s: regex %q, kit %q", key, got, re)
		}
		n++
	}
	if want := len(targets["dev-health"].keys) + 3; n != want {
		t.Errorf("%d kit rows, %d keys in the table: every key needs a kit row", n, want)
	}
}

func mustRefuse(t *testing.T, buf string, wantNotBuilt bool, wantIn string) *Refusal {
	t.Helper()
	_, err := Parse([]byte(buf))
	var r *Refusal
	if !errors.As(err, &r) {
		t.Fatalf("want a refusal holding %q, got %v", wantIn, err)
	}
	if r.NotBuilt != wantNotBuilt {
		t.Fatalf("NotBuilt = %v, want %v (%v)", r.NotBuilt, wantNotBuilt, r)
	}
	if !strings.Contains(r.Error(), wantIn) {
		t.Fatalf("refusal %q does not hold %q", r.Error(), wantIn)
	}
	return r
}

func TestTheGoodDevHealthFormPasses(t *testing.T) {
	in, err := Parse([]byte(goodDev))
	if err != nil {
		t.Fatal(err)
	}
	dh := in.DevHealth
	if in.Target != "dev-health" || in.RunTag != "s41-a" || in.Author != "fcsup" || dh == nil {
		t.Fatalf("parsed %+v", in)
	}
	if dh.ImgOld != oldDig || dh.ImgNew != newDig || len(dh.Rows) != 3 || dh.ExpStartRev != 449 || dh.ExpectNewestRev != 450 || dh.Step != 41 {
		t.Fatalf("derived %+v", dh)
	}
}

func TestTheHashIsOfTheParsedBuffer(t *testing.T) {
	buf := []byte(goodDev)
	sum := sha256.Sum256(buf)
	in, err := Parse(buf)
	if err != nil || in.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("sha %s, err %v", in.SHA256, err)
	}
	// A refused buffer still reports the sha of the bytes it was given.
	bad := append([]byte(goodDev), 0)
	sum = sha256.Sum256(bad)
	if in, _ := Parse(bad); in.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("refused buffer sha %s", in.SHA256)
	}
}

// onceReader returns a different buffer on every Read pass: a second pass would
// be visible as a different sha or parse.
type onceReader struct{ passes int }

func (o *onceReader) Read(p []byte) (int, error) {
	o.passes++
	if o.passes > 1 {
		return 0, fmt.Errorf("read twice")
	}
	return copy(p, goodDev), nil
}

func TestReadTakesTheBufferInOnePassWithABound(t *testing.T) {
	o := &onceReader{}
	buf, err := Read(o)
	if err == nil {
		t.Fatalf("a reader that fails on its 2nd call must surface the error, got %d bytes", len(buf))
	}
	big := bytes.Repeat([]byte("#"), MaxBytes+10)
	buf, err = Read(bytes.NewReader(big))
	if err != nil || len(buf) != MaxBytes+1 {
		t.Fatalf("bounded read: %d bytes, %v", len(buf), err)
	}
	mustRefuse(t, string(buf), false, "larger than 65536 bytes")
	buf, err = Read(nil)
	if err != nil || len(buf) != 0 {
		t.Fatalf("nil stdin: %d bytes, %v", len(buf), err)
	}
}

func TestBytesOutsideTheAlphabetAreRefused(t *testing.T) {
	cases := map[string]string{
		"NUL":       goodDev + "\x00",
		"CR":        strings.Replace(goodDev, "\n", "\r\n", 1),
		"TAB":       goodDev + "#\tx\n",
		"non-ASCII": goodDev + "# \xc3\xa9\n",
		"DEL":       goodDev + "#\x7f\n",
	}
	for name, buf := range cases {
		t.Run(name, func(t *testing.T) {
			want := "outside printable ASCII"
			if name == "NUL" {
				want = "NUL"
			}
			mustRefuse(t, buf, false, want)
		})
	}
	mustRefuse(t, goodDev+strings.Repeat("#\n", MaxLines), false, "more than 300 lines")
}

func TestStructureAndVersion(t *testing.T) {
	body := strings.TrimPrefix(goodDev, dhHead)
	tail := "TARGET=dev-health\nRUN_TAG=s41-a\nAUTHOR=fcsup\n" + body
	for name, c := range map[string]struct{ buf, want string }{
		"not key=value":     {goodDev + "STEP 41\n", "line 23: not KEY=VALUE"},
		"lower-case key":    {goodDev + "step=41\n", "not KEY=VALUE"},
		"empty":             {"# only a comment\n", "no KEY=VALUE line"},
		"no version":        {tail, "the first KEY=VALUE line must be DHO_DEPLOY_INPUTS=1"},
		"version later":     {tail + "DHO_DEPLOY_INPUTS=1\n", "the first KEY=VALUE line must be"},
		"kit file":          {"PRODROLL_INPUTS=3\n" + tail, "a kit inputs file"},
		"version 2":         {"DHO_DEPLOY_INPUTS=2\n" + tail, "inputs version is not 1"},
		"duplicate version": {"DHO_DEPLOY_INPUTS=1\n" + tail + "DHO_DEPLOY_INPUTS=1\n", "duplicate key DHO_DEPLOY_INPUTS"},
		"no target":         {strings.Replace(goodDev, "TARGET=dev-health\n", "", 1), "missing key TARGET"},
		"two targets":       {goodDev + "TARGET=dev-health\n", "duplicate key TARGET"},
		"bad target":        {strings.Replace(goodDev, "TARGET=dev-health", "TARGET=prod", 1), "bad value for TARGET"},
		"unknown key":       {goodDev + "NOT_A_KEY=1\n", "unknown key for this target"},
		"render file":       {goodDev + "RENDER_FILE=/var/lib/oci-cache/lane-scratch/x/r.yaml\n", "RENDER_FILE is not a dho key"},
		"dup scalar":        {goodDev + "STEP=42\n", "duplicate key STEP"},
		"dup run tag":       {goodDev + "RUN_TAG=s41-b\n", "duplicate key RUN_TAG"},
		"bad run tag":       {strings.Replace(goodDev, "RUN_TAG=s41-a", "RUN_TAG=S41", 1), "bad value for RUN_TAG"},
		"bad author":        {strings.Replace(goodDev, "AUTHOR=fcsup", "AUTHOR=f", 1), "bad value for AUTHOR"},
		"65 rows":           {goodDev + strings.Repeat(strings.SplitAfter(dhRows, "\n")[0], 62), "more than 64 IMGROW lines"},
	} {
		t.Run(name, func(t *testing.T) { mustRefuse(t, c.buf, false, c.want) })
	}
	for _, target := range []string{"ingress-nginx", "acr", "f1pipe"} {
		mustRefuse(t, strings.Replace(goodDev, "TARGET=dev-health", "TARGET="+target, 1), true, "TARGET names a kit-only target")
	}
}

func TestEveryRequiredKeyIsRequired(t *testing.T) {
	for key, ks := range targets["dev-health"].keys {
		if !ks.required {
			continue
		}
		var kept []string
		for _, l := range strings.Split(goodDev, "\n") {
			if !strings.HasPrefix(l, key+"=") {
				kept = append(kept, l)
			}
		}
		mustRefuse(t, strings.Join(kept, "\n"), false, "missing key "+key)
	}
	for _, key := range []string{"RUN_TAG", "AUTHOR"} {
		mustRefuse(t, strings.Replace(goodDev, key+"=", "#"+key+"=", 1), false, "missing key "+key)
	}
}

func TestBadValuesAreRefusedWithoutEchoingThem(t *testing.T) {
	for key := range targets["dev-health"].keys {
		buf := goodDev
		if strings.Contains(buf, "\n"+key+"=") {
			buf = strings.Replace(buf, "\n"+key+"=", "\n"+key+"="+canary+" ", 1)
		} else {
			buf += key + "=" + canary + "\n"
		}
		r := mustRefuse(t, buf, false, "bad value for "+key)
		if strings.Contains(r.Error(), canary) {
			t.Fatalf("%s: the refusal echoes the value: %q", key, r.Error())
		}
	}
}

func TestKitOnlyKeysAreNotBuilt(t *testing.T) {
	for key := range devHealthNotBuilt {
		mustRefuse(t, goodDev+key+"=x\n", true, "key "+key)
	}
	mustRefuse(t, goodDev+"PGFILTER=no\n", true, "PGFILTER=no")
	if _, err := Parse([]byte(goodDev + "PGFILTER=yes\nPGF_LIB_SHA256_ROLLBACK=" + zero64 + "\nIMG_OLD=" + oldDig + "\nIMG_NEW=" + newDig + "\n")); err != nil {
		t.Fatalf("the optional keys at their right values: %v", err)
	}
}

func TestRevisionAndTreeRules(t *testing.T) {
	sub := func(old, new string) string { return strings.Replace(goodDev, old, new, 1) }
	for name, c := range map[string]struct{ buf, want string }{
		"rollback rev != start":    {sub("ROLLBACK_REV=449", "ROLLBACK_REV=450"), "ROLLBACK_REV must equal EXP_START_REV"},
		"newest not start+1":       {sub("EXPECT_NEWEST_REV=450", "EXPECT_NEWEST_REV=451"), "EXPECT_NEWEST_REV must be"},
		"failed revs out of order": {goodDev + "FAILED_REVS=451\n", "FAILED_REVS must be"},
		"failed revs, newest +1":   {goodDev + "FAILED_REVS=450\n", "EXPECT_NEWEST_REV must be"},
		"svc == rollback":          {sub("SVC_SHA="+svcSHA, "SVC_SHA="+rbSHA), "SVC_SHA equals ROLLBACK_SHA"},
		"below the pg floor":       {strings.NewReplacer("EXP_START_REV=449", "EXP_START_REV=448", "ROLLBACK_REV=449", "ROLLBACK_REV=448", "EXPECT_NEWEST_REV=450", "EXPECT_NEWEST_REV=449").Replace(goodDev), "below 449"},
	} {
		t.Run(name, func(t *testing.T) { mustRefuse(t, c.buf, false, c.want) })
	}
	ok := sub("EXPECT_NEWEST_REV=450", "EXPECT_NEWEST_REV=452") + "FAILED_REVS=450,451\n"
	in, err := Parse([]byte(ok))
	if err != nil || len(in.DevHealth.FailedRevs) != 2 {
		t.Fatalf("two failed rows then the roll: %v", err)
	}
}

func row(doc, path, old, new string) string {
	return "IMGROW=" + doc + " " + path + " " + old + " " + new + "\n"
}

func TestImageRowForms(t *testing.T) {
	img := ".spec.template.spec.containers[0].image"
	other := "4d61345532cc"
	for name, c := range map[string]struct {
		buf      string
		notBuilt bool
		want     string
	}{
		"secret old":         {goodDev + row("Deployment/dev-health-ops-go-reconciler", ".spec.template.spec.containers[0].env[DB_PASSWORD].value", "<secret>", "<none>"), true, "<secret> / <empty> row"},
		"river-migrate hook": {goodDev + row("Job/dev-health-ops-river-migrate", img, oldDig, newDig), true, "migrate, data or grant hook"},
		"auth-migrate hook":  {goodDev + row("Job/dev-health-ops-auth-migrate", img, oldDig, newDig), true, "migrate, data or grant hook"},
		"auth-service":       {goodDev + row("Deployment/dev-health-ops-auth-service", img, oldDig, newDig), true, "restore-fence"},
		"statefulset":        {goodDev + row("StatefulSet/dev-health-ops-postgresql", ".spec.template.spec.containers[0].resources.requests.cpu", "300m", "400m"), true, "DSSTEP"},
		"no image pair":      {dhHead + dhScal + row("Deployment/dev-health-ops-web", ".spec.replicas", "2", "3"), true, "no Deployment .image digest pair"},
		"two pairs":          {goodDev + row("Deployment/dev-health-ops-web", img, oldDig, other), false, "carry two digest pairs (D6415"},
		"job other pair":     {goodDev + row("Job/dev-health-ops-route-activate", ".spec.template.spec.initContainers[1].image", other, newDig), false, "is not the one digest pair"},
		"same digest":        {dhHead + dhScal + row("Deployment/dev-health-ops-web", img, oldDig, oldDig), false, "not two different hex prefixes"},
		"prefix overlap":     {dhHead + dhScal + row("Deployment/dev-health-ops-web", img, oldDig, oldDig+"ab"), false, "not two different hex prefixes"},
		"img old differs":    {goodDev + "IMG_OLD=" + other + "\n", false, "IMG_OLD differs"},
		"img new differs":    {goodDev + "IMG_NEW=" + other + "\n", false, "IMG_NEW differs"},
	} {
		t.Run(name, func(t *testing.T) { mustRefuse(t, c.buf, c.notBuilt, c.want) })
	}
	// A config row beside the image pair on a rolled Deployment stays legal (the render gate covers it).
	if _, err := Parse([]byte(goodDev + row("Deployment/dev-health-ops-go-reconciler", ".spec.template.spec.containers[0].env[LOG_LEVEL].value", "info", "debug"))); err != nil {
		t.Fatalf("an env row on a rolled Deployment: %v", err)
	}
}

func TestARowRefusalNamesItsOwnLine(t *testing.T) {
	r := mustRefuse(t, goodDev+row("Job/dev-health-ops-river-migrate", ".spec.template.spec.containers[0].image", oldDig, newDig), true, "hook Job")
	if r.Line != 23 {
		t.Fatalf("refusal line %d, want 23 (the 4th IMGROW line)", r.Line)
	}
}

// The chart's hook Jobs (deploy/helm/dev-health/templates). Eight are kit
// forms on any row; route-activate rides an app roll, as in the kit.
func TestEveryChartHookJobIsClassified(t *testing.T) {
	img := ".spec.template.spec.containers[0].image"
	for _, name := range []string{"river-migrate", "provision-roles", "migrate", "streak-index", "open-units-index", "finalize-outbox-index", "auth-migrate", "auth-grants"} {
		for _, r := range []string{
			row("Job/dev-health-ops-"+name, img, oldDig, newDig),
			row("Job/dev-health-ops-"+name, ".spec.template.spec.initContainers[0].image", oldDig, newDig),
			row("Job/dev-health-ops-"+name, ".spec.backoffLimit", "1", "2"),
			row("NetworkPolicy/dev-health-ops-"+name, ".spec.podSelector.matchLabels.app", "a", "b"),
		} {
			mustRefuse(t, goodDev+r, true, "an IMGROW row on a")
		}
	}
	for _, r := range []string{
		row("Job/dev-health-ops-route-activate", img, oldDig, newDig),
		row("Job/dev-health-ops-route-activate", ".spec.backoffLimit", "1", "2"),
	} {
		if _, err := Parse([]byte(goodDev + r)); err != nil {
			t.Fatalf("route-activate rides the app roll: %v", err)
		}
	}
}

func TestEveryAuthDocIsNotBuilt(t *testing.T) {
	for _, r := range []string{
		row("Service/dev-health-ops-auth-service", ".spec.ports[http].port", "80", "81"),
		row("Service/dev-health-ops-auth-service-internal", ".spec.ports[http].port", "80", "81"),
		row("NetworkPolicy/dev-health-ops-auth-service", ".spec.podSelector.matchLabels.app", "a", "b"),
		row("PodDisruptionBudget/dev-health-ops-auth-service", ".spec.minAvailable", "1", "2"),
		row("ServiceAccount/dev-health-ops-auth-service", ".metadata.labels.x", "a", "b"),
		row("ConfigMap/dev-health-ops-auth-service-keyset", ".data.kid", "a", "b"),
		row("ClusterRole/dev-health-ops-auth-service-token-review", ".rules[0].verbs[0]", "create", "get"),
		row("ClusterRoleBinding/dev-health-ops-auth-service-token-review", ".subjects[0].name", "a", "b"),
		row("Deployment/dev-health-ops-auth-service", ".spec.template.spec.containers[0].image", oldDig, newDig),
		row("Job/dev-health-ops-auth-backfill-cafed00d", ".spec.template.spec.containers[0].image", oldDig, newDig),
	} {
		mustRefuse(t, goodDev+r, true, "an IMGROW row on an auth doc")
	}
}

func TestRowRefusalsNeverEchoTheRow(t *testing.T) {
	img := ".spec.template.spec.containers[0].image"
	other := "4d61345532cc"
	for _, r := range []string{
		row("Deployment/dev-health-ops-query-api", ".spec.template.spec.containers[0].env[DB_PASSWORD].value", "<secret>", "<none>"),
		row("Deployment/dev-health-ops-query-api", ".spec.template.spec.containers[0].env[A_TOKEN].value", "<secret>", "<empty>"),
		row("Job/dev-health-ops-migrate", img, oldDig, newDig),
		row("Service/dev-health-ops-auth-service", ".spec.ports[http].port", "80", "81"),
		row("StatefulSet/dev-health-ops-postgresql", ".spec.replicas", "1", "2"),
		row("Deployment/dev-health-ops-web", img, oldDig, other),
		row("Job/dev-health-ops-route-activate", ".spec.template.spec.initContainers[1].image", other, newDig),
		row("CronJob/dev-health-ops-sweep", ".spec.jobTemplate.spec.template.spec.containers[0].image", other, newDig),
	} {
		_, err := Parse([]byte(goodDev + r))
		if err == nil {
			t.Fatalf("row accepted: %s", r)
		}
		f := strings.Fields(strings.TrimPrefix(strings.TrimSpace(r), "IMGROW="))
		_, name, _ := strings.Cut(f[0], "/")
		for _, part := range []string{name, f[1], other} {
			if strings.Contains(err.Error(), part) {
				t.Fatalf("refusal %q echoes %q of the row", err.Error(), part)
			}
		}
	}
}

func TestDigestRefusalsPointAtTheirRow(t *testing.T) {
	img := ".spec.template.spec.containers[0].image"
	other := "4d61345532cc"
	cfg := row("Deployment/dev-health-ops-web", ".spec.replicas", "2", "3")
	head := dhHead + dhScal // 19 lines: the first row is line 20
	for name, c := range map[string]struct {
		buf  string
		line int
		want string
	}{
		"job pair mismatch":                  {head + row("Deployment/dev-health-ops-web", img, oldDig, newDig) + row("Job/dev-health-ops-route-activate", img, other, newDig), 21, "(line 20;"},
		"two pairs behind a config row":      {head + cfg + row("Deployment/dev-health-ops-web", img, oldDig, newDig) + row("Deployment/dev-health-ops-api", img, oldDig, other), 22, "the one on line 21"},
		"same digest behind a config row":    {head + cfg + row("Deployment/dev-health-ops-web", img, oldDig, oldDig), 21, "not two different"},
		"prefix overlap behind a config row": {head + cfg + row("Deployment/dev-health-ops-web", img, oldDig, oldDig+"ab"), 21, "not two different"},
	} {
		t.Run(name, func(t *testing.T) {
			r := mustRefuse(t, c.buf, false, c.want)
			if r.Line != c.line {
				t.Fatalf("refusal on line %d, want %d: %v", r.Line, c.line, r)
			}
		})
	}
}

// ACP-WORD-11788-4 (b): route-activate is an app-roll row (the kit s41-a form)
// under the same one-pair image check as every .image row, while the eight
// migrate, data and grant hook names stay kit forms on any kind.
func TestRouteActivateIsAnAppRollRowUnderTheOnePairCheck(t *testing.T) {
	img := ".spec.template.spec.containers[0].image"
	other := "4d61345532cc"
	for _, r := range []string{
		row("Job/dev-health-ops-route-activate", img, oldDig, newDig),
		row("Job/dev-health-ops-route-activate", ".spec.template.spec.initContainers[2].image", oldDig, newDig),
		row("Job/dev-health-ops-route-activate", ".spec.backoffLimit", "1", "2"),
	} {
		if _, err := Parse([]byte(goodDev + r)); err != nil {
			t.Fatalf("route-activate on the step's pair is admitted: %v", err)
		}
	}
	for _, r := range []string{
		row("Job/dev-health-ops-route-activate", img, other, newDig),
		row("Job/dev-health-ops-route-activate", img, oldDig, other),
		row("Job/dev-health-ops-route-activate", ".spec.template.spec.initContainers[2].image", other, other+"ab"),
	} {
		mustRefuse(t, goodDev+r, false, "is not the one digest pair of the step")
	}
	for _, name := range []string{"river-migrate", "provision-roles", "migrate", "streak-index", "open-units-index", "finalize-outbox-index", "auth-migrate", "auth-grants"} {
		for _, kind := range []string{"Job", "NetworkPolicy", "ServiceAccount"} {
			mustRefuse(t, goodDev+row(kind+"/dev-health-ops-"+name, ".metadata.labels.x", "a", "b"), true, "an IMGROW row on a")
		}
	}
}

// (d): a byte refusal names the line of the first bad byte.
func TestByteRefusalsNameTheLineOfTheFirstBadByte(t *testing.T) {
	for name, c := range map[string]struct {
		buf  string
		line int
		want string
	}{
		"CR on line 1":        {strings.Replace(goodDev, "\n", "\r\n", 1), 1, "outside printable ASCII"},
		"TAB on line 3":       {strings.Replace(goodDev, "TARGET=dev-health\n", "TARGET=dev-health\t\n", 1), 3, "outside printable ASCII"},
		"NUL on line 3":       {strings.Replace(goodDev, "TARGET=dev-health\n", "TARGET=dev-health\x00\n", 1), 3, "NUL"},
		"non-ASCII on line 3": {strings.Replace(goodDev, "TARGET=dev-health\n", "TARGET=dev-health\xc3\xa9\n", 1), 3, "outside printable ASCII"},
		"DEL on line 3":       {strings.Replace(goodDev, "TARGET=dev-health\n", "TARGET=dev-health\x7f\n", 1), 3, "outside printable ASCII"},
	} {
		t.Run(name, func(t *testing.T) {
			if r := mustRefuse(t, c.buf, false, c.want); r.Line != c.line {
				t.Fatalf("refusal on line %d, want %d: %v", r.Line, c.line, r)
			}
		})
	}
}

// (a): the hook rule is the -<hook> name suffix under any release prefix. The
// hard negatives carry a hook word without the "-" boundary at the end of the
// name (xmigrate, provision-rolesx) or as an inner segment (migrate-docs): a
// bare HasSuffix(name, hook) or a Contains match would refuse them.
func TestHookNamesMatchBySuffixOnAnyPrefix(t *testing.T) {
	img := ".spec.template.spec.containers[0].image"
	for _, prefix := range []string{"", "dev-health-", "dev-health-ops-", "acme-dev-health-"} {
		for _, h := range []string{"river-migrate", "provision-roles", "migrate", "streak-index", "open-units-index", "finalize-outbox-index", "auth-migrate", "auth-grants"} {
			mustRefuse(t, goodDev+row("Job/"+prefix+h, img, oldDig, newDig), true, "migrate, data or grant hook")
		}
	}
	for _, name := range []string{"dev-health-ops-xmigrate", "dev-health-ops-migrate-docs", "dev-health-ops-provision-rolesx", "dev-health-ops-streak-indexer", "dev-health-ops-open-units-indexes", "dev-health-ops-finalize-outbox-indexer", "dev-health-migratex"} {
		if _, err := Parse([]byte(goodDev + row("Deployment/"+name, img, oldDig, newDig))); err != nil {
			t.Fatalf("%s is not a hook doc: %v", name, err)
		}
	}
}
