package devhealth_test

import (
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// postgresql.backup.enabled (default false) adds a nightly pg_dump of the bundled PostgreSQL: a CronJob and a dedicated
// PersistentVolumeClaim the dumps are written to. It runs the StatefulSet's own image (pg_dump major == server major)
// and reads the password from the Secret and key the StatefulSet reads. Off, or with the bundled PostgreSQL off, the
// render does not change at all.

const (
	pgBackupCronJob = "a-dev-health-postgresql-backup"
	pgBackupPVC     = "a-dev-health-postgresql-backups"
)

var (
	pgBackupOn       = append(append([]string{}, pgCredsOff...), "--set", "postgresql.backup.enabled=true")
	pgBackupKnobOn   = append(append([]string{}, pgCredsOn...), "--set", "postgresql.backup.enabled=true")
	pgBackupExisting = append(append([]string{}, pgExistingOff...), "--set", "postgresql.backup.enabled=true")
	pgBackupKeyed    = append(append([]string{}, pgExistingKeyed...), "--set", "postgresql.backup.enabled=true")
)

func pgBackupRender(t *testing.T, args ...string) (cronJob, pvc, sts map[string]any, out string) {
	t.Helper()
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	return authDoc(docs, "CronJob", pgBackupCronJob), authDoc(docs, "PersistentVolumeClaim", pgBackupPVC),
		authDoc(docs, "StatefulSet", "a-dev-health-postgresql"), out
}

func pgBackupPod(t *testing.T, cronJob map[string]any) map[string]any {
	t.Helper()
	if cronJob == nil {
		t.Fatalf("CronJob %s does not render", pgBackupCronJob)
	}
	pod, _ := at(cronJob, "spec", "jobTemplate", "spec", "template", "spec").(map[string]any)
	if pod == nil {
		t.Fatal("the CronJob has no pod spec")
	}
	return pod
}

func pgBackupContainer(t *testing.T, cronJob map[string]any) map[string]any {
	t.Helper()
	containers, _ := at(pgBackupPod(t, cronJob), "containers").([]any)
	if len(containers) != 1 || at(containers[0], "name") != "pg-dump" {
		t.Fatalf("the backup pod containers are %v, want exactly one named pg-dump", containers)
	}
	return containers[0].(map[string]any)
}

func pgBackupScript(t *testing.T, cronJob map[string]any) string {
	t.Helper()
	c := pgBackupContainer(t, cronJob)
	command, _ := c["command"].([]any)
	if len(command) != 3 || command[0] != "/bin/sh" || command[1] != "-c" {
		t.Fatalf("command = %v, want [/bin/sh -c <script>]", command)
	}
	script, _ := command[2].(string)
	return script
}

func pgBackupEnv(t *testing.T, cronJob map[string]any, name string) map[string]any {
	t.Helper()
	entry, _ := namedEntry(at(pgBackupContainer(t, cronJob), "env"), name).(map[string]any)
	if entry == nil {
		t.Fatalf("the pg-dump container has no %s env", name)
	}
	return entry
}

// Off (the default, explicit or unset): no backup object renders, with the bundled PostgreSQL on or off, and the
// explicit false changes no document.
func TestPostgresBackupOffRendersNothing(t *testing.T) {
	requireHelmUnderCI(t)
	explicitOff := append(append([]string{}, pgCredsOff...), "--set", "postgresql.backup.enabled=false")
	for name, args := range map[string][]string{"store off": authBase, "store on": pgCredsOff, "explicit false": explicitOff} {
		t.Run(name, func(t *testing.T) {
			cronJob, pvc, _, out := pgBackupRender(t, args...)
			if cronJob != nil || pvc != nil {
				t.Errorf("a backup object renders with postgresql.backup.enabled off: CronJob=%v PVC=%v", cronJob != nil, pvc != nil)
			}
			if strings.Contains(out, "kind: CronJob") || strings.Contains(out, "postgresql-backup") {
				t.Error("the render holds a CronJob or a postgresql-backup object")
			}
		})
	}
	if got := changedDocs(t, pgCredsOff, explicitOff); len(got) != 0 {
		t.Errorf("postgresql.backup.enabled=false changes %v", got)
	}
}

// On without the bundled PostgreSQL: there is nothing in the chart to dump, so nothing renders.
func TestPostgresBackupNeedsTheBundledPostgres(t *testing.T) {
	requireHelmUnderCI(t)
	args := append(append([]string{}, authBase...), "--set", "postgresql.backup.enabled=true")
	cronJob, pvc, _, out := pgBackupRender(t, args...)
	if cronJob != nil || pvc != nil || strings.Contains(out, "postgresql-backup") {
		t.Errorf("a backup object renders with postgresql.enabled=false: CronJob=%v PVC=%v", cronJob != nil, pvc != nil)
	}
}

// On: the switch adds exactly the CronJob and the PVC; no other document changes.
func TestPostgresBackupAddsOnlyTheCronJobAndThePVC(t *testing.T) {
	requireHelmUnderCI(t)
	got := changedDocs(t, pgCredsOff, pgBackupOn)
	want := []string{"CronJob/" + pgBackupCronJob, "PersistentVolumeClaim/" + pgBackupPVC}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("documents changed by postgresql.backup.enabled=true = %v, want %v", got, want)
	}
}

// The PVC: its own claim (never the database volume), the cluster default StorageClass unless one is named, 20Gi,
// and kept by helm on uninstall.
func TestPostgresBackupPVC(t *testing.T) {
	requireHelmUnderCI(t)
	_, pvc, _, _ := pgBackupRender(t, pgBackupOn...)
	if pvc == nil {
		t.Fatalf("PersistentVolumeClaim %s does not render", pgBackupPVC)
	}
	if got := at(pvc, "metadata", "annotations", "helm.sh/resource-policy"); got != "keep" {
		t.Errorf("helm.sh/resource-policy = %v, want keep", got)
	}
	if got := at(pvc, "spec", "resources", "requests", "storage"); got != "20Gi" {
		t.Errorf("storage = %v, want 20Gi", got)
	}
	if modes, _ := at(pvc, "spec", "accessModes").([]any); len(modes) != 1 || modes[0] != "ReadWriteOnce" {
		t.Errorf("accessModes = %v, want [ReadWriteOnce]", modes)
	}
	if _, set := at(pvc, "spec").(map[string]any)["storageClassName"]; set {
		t.Errorf("storageClassName is set by default (%v); empty must leave the cluster default", at(pvc, "spec", "storageClassName"))
	}
	if got := at(pvc, "metadata", "labels", "app.kubernetes.io/component"); got != "postgresql-backup" {
		t.Errorf("component label = %v, want postgresql-backup", got)
	}
	named := append(append([]string{}, pgBackupOn...), "--set", "postgresql.backup.storageClass=local-path", "--set", "postgresql.backup.size=35Gi")
	_, pvc, _, _ = pgBackupRender(t, named...)
	if at(pvc, "spec", "storageClassName") != "local-path" || at(pvc, "spec", "resources", "requests", "storage") != "35Gi" {
		t.Errorf("storageClassName/size overrides not rendered: %v", at(pvc, "spec"))
	}
}

// The CronJob schedule and Job bounds.
func TestPostgresBackupCronJobScheduleAndBounds(t *testing.T) {
	requireHelmUnderCI(t)
	cronJob, _, _, _ := pgBackupRender(t, pgBackupOn...)
	if cronJob == nil {
		t.Fatalf("CronJob %s does not render", pgBackupCronJob)
	}
	spec := at(cronJob, "spec")
	for key, want := range map[string]any{
		"schedule": "17 3 * * *", "timeZone": "Etc/UTC", "concurrencyPolicy": "Forbid",
		"successfulJobsHistoryLimit": 3, "failedJobsHistoryLimit": 3,
	} {
		if got := at(spec, key); got != want {
			t.Errorf("spec.%s = %v, want %v", key, got, want)
		}
	}
	job := at(spec, "jobTemplate", "spec")
	for key, want := range map[string]any{"backoffLimit": 1, "activeDeadlineSeconds": 3600, "ttlSecondsAfterFinished": 43200} {
		if got := at(job, key); got != want {
			t.Errorf("jobTemplate.spec.%s = %v, want %v", key, got, want)
		}
	}
	pod := pgBackupPod(t, cronJob)
	if pod["restartPolicy"] != "Never" {
		t.Errorf("restartPolicy = %v, want Never", pod["restartPolicy"])
	}
	if v, ok := pod["automountServiceAccountToken"].(bool); !ok || v {
		t.Errorf("automountServiceAccountToken = %v, want false", pod["automountServiceAccountToken"])
	}
	override := append(append([]string{}, pgBackupOn...), "--set", "postgresql.backup.schedule=5 1 * * *", "--set", "postgresql.backup.timeZone=")
	cronJob, _, _, _ = pgBackupRender(t, override...)
	if at(cronJob, "spec", "schedule") != "5 1 * * *" {
		t.Errorf("schedule override = %v, want 5 1 * * *", at(cronJob, "spec", "schedule"))
	}
	if _, set := at(cronJob, "spec").(map[string]any)["timeZone"]; set {
		t.Error("an empty postgresql.backup.timeZone still renders spec.timeZone")
	}
}

// The image, pull policy, user and database are the StatefulSet's, and PGHOST is the bundled Service.
func TestPostgresBackupRunsTheStatefulSetImageAgainstTheBundledService(t *testing.T) {
	requireHelmUnderCI(t)
	cronJob, _, sts, _ := pgBackupRender(t, pgBackupOn...)
	c, pg := pgBackupContainer(t, cronJob), postgresContainer(t, sts)
	if c["image"] == nil || c["image"] != at(pg, "image") {
		t.Errorf("image = %v, want the StatefulSet image %v", c["image"], at(pg, "image"))
	}
	if !strings.Contains(c["image"].(string), "@sha256:") {
		t.Errorf("image %v is not digest-pinned", c["image"])
	}
	// The StatefulSet sets no imagePullPolicy; the Job takes postgresql.image.pullPolicy, never image.pullPolicy.
	if c["imagePullPolicy"] != "IfNotPresent" {
		t.Errorf("imagePullPolicy = %v, want postgresql.image.pullPolicy IfNotPresent", c["imagePullPolicy"])
	}
	never := append(append([]string{}, pgBackupOn...), "--set", "image.pullPolicy=Never")
	if cj, _, _, _ := pgBackupRender(t, never...); pgBackupContainer(t, cj)["imagePullPolicy"] != "IfNotPresent" {
		t.Error("image.pullPolicy=Never leaks onto the backup Job's registry image")
	}
	for name, want := range map[string]any{
		"PGHOST":      "a-dev-health-postgresql",
		"PGPORT":      "5432",
		"PGUSER":      at(namedEntry(pgEnv(t, sts), "POSTGRES_USER"), "value"),
		"PGDATABASE":  at(namedEntry(pgEnv(t, sts), "POSTGRES_DB"), "value"),
		"BACKUP_KEEP": "7",
	} {
		if got := pgBackupEnv(t, cronJob, name)["value"]; got != want || want == nil {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	other := append(append([]string{}, pgBackupOn...), "--set", "postgresql.image.tag=17-alpine@sha256:"+strings.Repeat("ab", 32), "--set", "postgresql.backup.keep=14")
	cronJob, _, sts, _ = pgBackupRender(t, other...)
	if got, want := pgBackupContainer(t, cronJob)["image"], at(postgresContainer(t, sts), "image"); got != want {
		t.Errorf("after a tag change image = %v, want the StatefulSet image %v", got, want)
	}
	if got := pgBackupEnv(t, cronJob, "BACKUP_KEEP")["value"]; got != "14" {
		t.Errorf("BACKUP_KEEP = %v, want 14", got)
	}
}

// PGPASSWORD is always a secretKeyRef on the Secret and key the StatefulSet's POSTGRES_PASSWORD names: the chart
// Secret (knob off: the StatefulSet still has the plain value, the chart Secret holds the same value), or the
// existing Secret and its key. The value is never in the CronJob.
func TestPostgresBackupPasswordIsTheStatefulSetSecret(t *testing.T) {
	requireHelmUnderCI(t)
	cases := map[string]struct {
		args      []string
		name, key string
	}{
		"knob off":     {pgBackupOn, pgCredentialsSecret, pgPasswordKey},
		"knob on":      {pgBackupKnobOn, pgCredentialsSecret, pgPasswordKey},
		"existing":     {pgBackupExisting, pgExistingName, pgPasswordKey},
		"existing key": {pgBackupKeyed, pgExistingName, pgExistingKey},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			cronJob, _, sts, _ := pgBackupRender(t, c.args...)
			entry := pgBackupEnv(t, cronJob, "PGPASSWORD")
			ref := at(entry, "valueFrom", "secretKeyRef")
			if at(ref, "name") != c.name || at(ref, "key") != c.key || len(entry) != 2 {
				t.Errorf("PGPASSWORD = %v, want only a secretKeyRef on %s/%s", entry, c.name, c.key)
			}
			if stsRef := at(pgPasswordEntry(t, sts), "valueFrom", "secretKeyRef"); stsRef != nil && !reflect.DeepEqual(stsRef, ref) {
				t.Errorf("PGPASSWORD reads %v, the StatefulSet reads %v", ref, stsRef)
			}
			if strings.Contains(yamlText(cronJob), pgDummyPassword) {
				t.Error("the CronJob holds the password value")
			}
		})
	}
}

// Non-root as the image's postgres user (70), no privilege escalation, a read-only root with /tmp and /backups the
// only writable paths, and /backups is the PVC.
func TestPostgresBackupPodIsLockedDown(t *testing.T) {
	requireHelmUnderCI(t)
	cronJob, _, _, _ := pgBackupRender(t, pgBackupOn...)
	pod, c := pgBackupPod(t, cronJob), pgBackupContainer(t, cronJob)
	for key, want := range map[string]any{"runAsNonRoot": true, "runAsUser": 70, "runAsGroup": 70, "fsGroup": 70} {
		if got := at(pod, "securityContext", key); got != want {
			t.Errorf("pod securityContext.%s = %v, want %v", key, got, want)
		}
	}
	for key, want := range map[string]any{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "runAsNonRoot": true, "runAsUser": 70} {
		if got := at(c, "securityContext", key); got != want {
			t.Errorf("container securityContext.%s = %v, want %v", key, got, want)
		}
	}
	if drop, _ := at(c, "securityContext", "capabilities", "drop").([]any); len(drop) != 1 || drop[0] != "ALL" {
		t.Errorf("capabilities.drop = %v, want [ALL]", drop)
	}
	mounts := map[string]string{}
	for _, m := range asList(c["volumeMounts"]) {
		mounts[toString(at(m, "mountPath"))] = toString(at(m, "name"))
	}
	if !reflect.DeepEqual(mounts, map[string]string{"/backups": "backups", "/tmp": "tmp"}) {
		t.Errorf("volumeMounts = %v, want /backups (backups) and /tmp (tmp)", mounts)
	}
	if got := at(namedEntry(pod["volumes"], "backups"), "persistentVolumeClaim", "claimName"); got != pgBackupPVC {
		t.Errorf("backups volume claim = %v, want %s", got, pgBackupPVC)
	}
	if at(namedEntry(pod["volumes"], "tmp"), "emptyDir") == nil {
		t.Error("the tmp volume is not an emptyDir")
	}
}

// The script: a custom-format dump to a .partial file, a pg_restore --list check, the sha256 file, the rename, one ok
// line, and the prune only after all of that.
func TestPostgresBackupScriptWritesChecksAndPrunesInOrder(t *testing.T) {
	requireHelmUnderCI(t)
	cronJob, _, _, _ := pgBackupRender(t, pgBackupOn...)
	script := pgBackupScript(t, cronJob)
	steps := []string{
		"set -eu",
		`pg_dump --format=custom --file="$part"`,
		`pg_restore --list "$part" >/dev/null`,
		`sha256sum "$part"`,
		`> "$ts.dump.sha256"`,
		`mv -- "$part" "$ts.dump"`,
		"pg-backup ok file=",
		`tail -n "+$((BACKUP_KEEP + 1))"`,
		`-name '*.partial' -mtime +0`,
	}
	last := -1
	for _, step := range steps {
		i := strings.Index(script, step)
		if i < 0 {
			t.Errorf("the script lacks %q", step)
			continue
		}
		if i < last {
			t.Errorf("%q comes before the step above it", step)
		}
		last = i
	}
	if strings.Contains(script, "PGPASSWORD=") || strings.Contains(script, pgDummyPassword) {
		t.Error("the script sets or holds the password")
	}
}

// NetworkPolicy: the backup pod is a release pod, so the release-wide policy admits it at PostgreSQL and lets it
// reach PostgreSQL. No extra policy is needed, and none renders.
func TestPostgresBackupReachesPostgresUnderTheReleasePolicy(t *testing.T) {
	requireHelmUnderCI(t)
	policies, egress, out := renderPolicySet(t, "--set", "postgresql.enabled=true", "--set", "postgresql.backup.enabled=true")
	var podLabels map[string]string
	for _, document := range strings.Split(out, "\n---") {
		var parsed struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec struct {
				JobTemplate struct {
					Spec struct {
						Template struct {
							Metadata struct {
								Labels map[string]string `yaml:"labels"`
							} `yaml:"metadata"`
						} `yaml:"template"`
					} `yaml:"spec"`
				} `yaml:"jobTemplate"`
			} `yaml:"spec"`
		}
		if err := yaml.Unmarshal([]byte(document), &parsed); err == nil && parsed.Kind == "CronJob" && parsed.Metadata.Name == "np-dev-health-postgresql-backup" {
			podLabels = parsed.Spec.JobTemplate.Spec.Template.Metadata.Labels
		}
	}
	if !reflect.DeepEqual(podLabels, npComponent("postgresql-backup")) {
		t.Fatalf("backup pod labels = %v, want %v", podLabels, npComponent("postgresql-backup"))
	}
	source := netpolSource{"the backup pod", map[string]string{namespaceNameLabel: releaseNamespace}, podLabels}
	if got, selected := admittedFrom(policies, npComponent("postgresql"), []int{5432}, source); !selected || len(got) != 1 {
		t.Errorf("postgresql admits the backup pod on %v (selected=%v), want 5432", got, selected)
	}
	target := egressTarget{name: "in-namespace Postgres", namespaceLabels: map[string]string{namespaceNameLabel: releaseNamespace}, podLabels: npComponent("postgresql"), protocol: "TCP", port: 5432}
	if allowed, selected := egressAllowed(egress, podLabels, target); !selected || !allowed {
		t.Errorf("the backup pod cannot reach postgresql:5432 (selected=%v allowed=%v)", selected, allowed)
	}
	if strings.Contains(out, "kind: NetworkPolicy\nmetadata:\n  name: np-dev-health-postgresql-backup") {
		t.Error("a backup NetworkPolicy renders; the release-wide policy already covers the pod")
	}
}
