package docstools

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// gen-queue-mapping and check-queue-mapping-drift: the worker/queue table in go-worker-runtime.md is generated from
// deploy/go-workers/deployment.json, contracts/jobs/v1/registry.json and contracts/jobs/v1/migration-state.json
// (CHAOS-4044), plus the curated historical Celery correspondence below. Was scripts/gen_queue_mapping_docs.py and
// scripts/check_queue_mapping_docs_drift.py.

const (
	qmBegin         = "<!-- BEGIN GENERATED QUEUE MAP -->"
	qmEnd           = "<!-- END GENERATED QUEUE MAP -->"
	qmDocRel        = "docs/contribute/architecture/go-worker-runtime.md"
	qmDeploymentRel = "deploy/go-workers/deployment.json"
	qmRegistryRel   = "contracts/jobs/v1/registry.json"
	qmMigrationRel  = "contracts/jobs/v1/migration-state.json"
)

func init() {
	register("gen-queue-mapping", qmGenVerb)
	register("check-queue-mapping-drift", qmDriftVerb)
}

type qmEntry struct {
	celeryQueues    []string
	celeryConsumers []string
	plane           string
	note            string
}

const qmDormant = "Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live"
const qmNative = "Go-native -- no Celery predecessor"
const qmStreamNote = "Valkey stream consumer, not a River queue -- no -Q for this process."
const qmLoopNote = "Control loop, not a River queue -- no -Q for this process."

// qmCorrespondence is the curated historical record (CHAOS-4044): go process -> go queue -> entry.
var qmCorrespondence = map[string]map[string]qmEntry{
	"heavy": {
		"investment": {plane: qmNative},
		"metrics": {
			celeryQueues: []string{"metrics", "backfill"}, celeryConsumers: []string{"worker-heavy"},
			plane: qmDormant,
			note:  "backfill's family (metrics.remaining.membership_backfill) rides this queue, not a dedicated Go 'backfill' queue.",
		},
		"reports": {celeryQueues: []string{"reports"}, celeryConsumers: []string{"worker"}, plane: qmDormant},
		"workgraph": {
			celeryQueues: []string{"default"}, celeryConsumers: []string{"worker"}, plane: qmDormant,
			note: "work_graph_tasks.py routed through the shared 'default' catch-all, not a dedicated queue.",
		},
	},
	"ops": {
		"coverage": {plane: qmNative},
		"heartbeat": {
			celeryQueues: []string{"default"}, celeryConsumers: []string{"worker"}, plane: qmDormant,
			note: "system_ops.phone_home_heartbeat routed through 'default', not a dedicated queue.",
		},
		"retention": {
			celeryConsumers: []string{"beat"},
			plane:           "Go-native consolidated sweep; historical retention work was several discrete Beat-scheduled tasks (retired under CHAOS-4026, e.g. ask-dev-retention-sweep)",
		},
		"webhooks": {celeryQueues: []string{"webhooks"}, celeryConsumers: []string{"worker"}, plane: qmDormant},
	},
	"sync": {
		"sync": {
			celeryQueues: []string{"sync"}, celeryConsumers: []string{"worker"},
			plane: "Celery dormant since 2026-08-19 (CHAOS-4026); Go/River live. Historically also the shared fallback queue for all providers with PROVIDER_SYNC_QUEUES_ENABLED off.",
		},
	},
	"sync-provider": {
		"sync_provider": {
			celeryQueues: []string{
				"sync.github", "sync.gitlab", "sync.linear", "sync.jira", "sync.launchdarkly",
				"sync.github.light", "sync.github.medium", "sync.github.heavy",
				"sync.gitlab.light", "sync.gitlab.medium", "sync.gitlab.heavy",
				"sync.jira.medium", "sync.linear.medium",
			},
			celeryConsumers: []string{"worker", "worker-heavy"},
			plane:           "Celery fleet-wide dormant since 2026-08-19 (CHAOS-4026); Go/River live. This queue's one kind (`sync.provider_unit`) was still `canary` per-kind for a time after the fleet-wide Celery retirement -- its own migration has since completed too (see below). Enablement is -Q topology plus the user's own sync config: CHAOS-4054 deleted the provider/dataset WORKER_*_ENABLED switch plane outright, so a shipped route is always executable and nothing hides it behind an environment flag.",
			note:            "The per-provider cost-class split (light/medium/heavy) has no Go equivalent yet -- CHAOS-4027, parked. All cost classes collapse onto the single sync_provider queue in Go.",
		},
	},
	"scheduler": {
		"—": {celeryQueues: []string{"scheduler"}, celeryConsumers: []string{"beat"},
			plane: "Celery Beat retired 2026-08-21 (CHAOS-4026); Go scheduler is sole production owner", note: qmLoopNote},
	},
	"reconciler": {
		"—": {plane: qmNative, note: qmLoopNote},
	},
	"stream-ingest": {
		"—": {celeryQueues: []string{"ingest"}, celeryConsumers: []string{"worker-ingest"},
			plane: "Celery dormant since 2026-08-19 (CHAOS-4026); Go stream runner live", note: qmStreamNote},
	},
	"stream-external": {
		"—": {celeryQueues: []string{"external-ingest"}, celeryConsumers: []string{"worker-external-ingest"},
			plane: "Celery dormant since 2026-08-19 (CHAOS-4026); Go stream runner live", note: qmStreamNote},
	},
	"stream-pagerduty": {
		"—": {plane: qmNative, note: qmStreamNote},
	},
}

// qmNoGoQueue lists Celery queues that carry no work reachable through a Go queue.
var qmNoGoQueue = map[string]string{
	"monitoring": "queue-depth telemetry; superseded by native worker_jobs_available / worker_job_oldest_age_seconds / worker_execution_saturation_ratio metrics, not a queue",
}

// qmFail is a deliberate refusal (the Python SystemExit with a message); any other error is an unexpected failure.
type qmFail struct{ msg string }

func (e qmFail) Error() string { return e.msg }

type qmProcess struct {
	Name     string
	Binary   string
	Queues   []string
	JobKinds []string
	has      map[string]bool
}

type qmJob struct {
	Kind           string
	Queue          string
	TimeoutSeconds int
	MaxAttempts    int
	State          string
	Route          string
	RollbackRoute  string
	has            map[string]bool
	bad            map[string]bool
}

// qmRecord decodes one producer record to its keys. A Go struct tag would match "KIND" or U+017F "ſtate" to "kind" or
// "state" (encoding/json folds case and Unicode); Python's dict lookup is exact, so every key is read by its exact name.
func qmRecord(b []byte) (map[string]json.RawMessage, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func qmPresent(raw map[string]json.RawMessage) map[string]bool {
	has := make(map[string]bool, len(raw))
	for k := range raw {
		has[k] = true
	}
	return has
}

// qmTyped reads key into dst, a string or an integer. A JSON null, or any other type (a float such as 30.0, a bool,
// a number-like string, an overflowing integer), gives false: Python rendered most of those (None, 30.0, True, 30),
// so the refusal is stricter than Python by decision. An absent key gives true (need reports it).
func qmTyped(raw map[string]json.RawMessage, key string, dst any) bool {
	v, present := raw[key]
	if !present {
		return true
	}
	if string(bytes.TrimSpace(v)) == "null" {
		return false
	}
	return json.Unmarshal(v, dst) == nil
}

func (p *qmProcess) UnmarshalJSON(b []byte) error {
	raw, err := qmRecord(b)
	if err != nil {
		return err
	}
	for key, dst := range map[string]any{"name": &p.Name, "binary": &p.Binary, "queues": &p.Queues, "job_kinds": &p.JobKinds} {
		if v, present := raw[key]; present {
			if err := json.Unmarshal(v, dst); err != nil {
				return err
			}
		}
	}
	p.has = qmPresent(raw)
	return nil
}

func (j *qmJob) UnmarshalJSON(b []byte) error {
	raw, err := qmRecord(b)
	if err != nil {
		return err
	}
	j.bad = map[string]bool{}
	for key, dst := range map[string]any{
		"kind": &j.Kind, "queue": &j.Queue, "state": &j.State, "route": &j.Route, "rollback_route": &j.RollbackRoute,
		"timeout_seconds": &j.TimeoutSeconds, "max_attempts": &j.MaxAttempts,
	} {
		if !qmTyped(raw, key, dst) {
			j.bad[key] = true
		}
	}
	j.has = qmPresent(raw)
	return nil
}

// need is Python's d[key] for each key in turn: the first absent one is a KeyError.
func qmNeed(has map[string]bool, keys []string) error {
	for _, k := range keys {
		if !has[k] {
			return qmKeyError{k}
		}
	}
	return nil
}

func (p qmProcess) need(keys ...string) error { return qmNeed(p.has, keys) }

// need is Python's d[key] for each key: an absent one is a KeyError; a present one of the wrong type is refused.
func (j qmJob) need(keys ...string) error {
	for _, k := range keys {
		if err := qmNeed(j.has, []string{k}); err != nil {
			return err
		}
		if j.bad[k] {
			return qmFail{"gen-queue-mapping: " + k + " of a producer job record must be a non-null " + qmTypeName(k) +
				" (a null, float, bool or number-like string is refused)"}
		}
	}
	return nil
}

func qmTypeName(key string) string {
	if key == "timeout_seconds" || key == "max_attempts" {
		return "integer"
	}
	return "string"
}

// qmLoadList reads a producer file and returns the list under key (Python: data[key]); an absent key is a KeyError.
func qmLoadList[T any](path, key string) ([]T, error) {
	text, err := a1ReadText(path)
	if err != nil {
		return nil, err
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal([]byte(text), &top); err != nil {
		return nil, err
	}
	raw, ok := top[key]
	if !ok {
		return nil, qmKeyError{key}
	}
	var out []T
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// qmReport prints the one stderr line for a failed verb step and returns exit code 1.
func qmReport(stderr io.Writer, prefix string, err error) int {
	if f, ok := err.(qmFail); ok {
		fmt.Fprintln(stderr, f.msg)
		return 1
	}
	var pe *fs.PathError
	if errors.As(err, &pe) && errors.Is(err, fs.ErrNotExist) {
		return a1Traceback(stderr, "FileNotFoundError", "[Errno 2] No such file or directory: "+a1Repr(pe.Path))
	}
	var se *json.SyntaxError
	var te *json.UnmarshalTypeError
	if errors.As(err, &se) || errors.As(err, &te) {
		return a1Traceback(stderr, "json.decoder.JSONDecodeError", err.Error())
	}
	var ke qmKeyError
	if errors.As(err, &ke) {
		return a1Traceback(stderr, "KeyError", a1Repr(ke.key))
	}
	return a1ReadFail(stderr, prefix, err)
}

// qmKeyError is Python's KeyError on a dict lookup.
type qmKeyError struct{ key string }

func (e qmKeyError) Error() string { return e.key }

func qmKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// qmList renders a Python list repr of strings: ['a', 'b'].
func qmList(items []string) string {
	parts := make([]string, len(items))
	for i, s := range items {
		parts[i] = "'" + s + "'"
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func qmHas(items []string, want string) bool {
	for _, s := range items {
		if s == want {
			return true
		}
	}
	return false
}

func qmRange(vals []int) string {
	if len(vals) > 1 {
		return strconv.Itoa(vals[0]) + "-" + strconv.Itoa(vals[len(vals)-1])
	}
	return strconv.Itoa(vals[0])
}

func qmUniqSorted(vals []int) []int {
	sort.Ints(vals)
	out := vals[:0]
	for i, v := range vals {
		if i == 0 || v != vals[i-1] {
			out = append(out, v)
		}
	}
	return out
}

func qmTicks(items []string) string {
	if len(items) == 0 {
		return "—"
	}
	parts := make([]string, len(items))
	for i, s := range items {
		parts[i] = "`" + s + "`"
	}
	return strings.Join(parts, ", ")
}

// qmRender builds the generated block from the three producers under root.
func qmRender(root string) (string, error) {
	// Python loads each file, then indexes "name" / "kind" of every record, before it reads the next file.
	processes, err := qmLoadList[qmProcess](filepath.Join(root, qmDeploymentRel), "processes")
	if err != nil {
		return "", err
	}
	for _, p := range processes {
		if err := p.need("name"); err != nil {
			return "", err
		}
	}
	regJobs, err := qmLoadList[qmJob](filepath.Join(root, qmRegistryRel), "jobs")
	if err != nil {
		return "", err
	}
	for _, j := range regJobs {
		if err := j.need("kind"); err != nil {
			return "", err
		}
	}
	migJobs, err := qmLoadList[qmJob](filepath.Join(root, qmMigrationRel), "jobs")
	if err != nil {
		return "", err
	}
	for _, j := range migJobs {
		if err := j.need("kind"); err != nil {
			return "", err
		}
	}
	deployment := map[string]qmProcess{}
	for _, p := range processes {
		deployment[p.Name] = p
	}
	registry := map[string]qmJob{}
	for _, j := range regJobs {
		registry[j.Kind] = j
	}
	state := map[string]qmJob{}
	for _, j := range migJobs {
		state[j.Kind] = j
	}

	curated := qmKeys(qmCorrespondence)
	if got := qmKeys(deployment); qmList(got) != qmList(curated) {
		return "", qmFail{"gen-queue-mapping: " + qmDeploymentRel + " process set " + qmList(got) +
			" does not match the curated qmCorrespondence keys " + qmList(curated) +
			" in internal/cigates/docstools/queuemap.go -- update the curated map."}
	}
	if rk, mk := qmKeys(registry), qmKeys(state); qmList(rk) != qmList(mk) {
		return "", qmFail{"gen-queue-mapping: " + qmRegistryRel + " kinds " + qmList(rk) +
			" do not match " + qmMigrationRel + " kinds " + qmList(mk) +
			" -- every registered kind must have a migration-state row."}
	}

	lines := []string{
		qmBegin,
		"| Go process (binary) | Go queue | Job kind(s) | Timeout(s) | Max attempts | Historical Celery queue(s) | Historical Celery consumer(s) | Plane / route status |",
		"| --- | --- | --- | --- | --- | --- | --- | --- |",
	}
	for _, name := range qmKeys(deployment) {
		process := deployment[name]
		if err := process.need("queues"); err != nil {
			return "", err
		}
		entries := qmCorrespondence[name]
		queues := process.Queues
		if len(queues) == 0 {
			queues = []string{"—"}
		}
		for _, goQueue := range queues {
			entry, ok := entries[goQueue]
			if !ok {
				return "", qmFail{"gen-queue-mapping: process '" + name + "' queue '" + goQueue +
					"' from deployment.json has no qmCorrespondence entry."}
			}
			if err := process.need("job_kinds"); err != nil {
				return "", err
			}
			var kinds []string
			for _, k := range process.JobKinds {
				job, ok := registry[k]
				if !ok {
					return "", qmKeyError{k}
				}
				if err := job.need("queue"); err != nil {
					return "", err
				}
				if job.Queue == goQueue {
					kinds = append(kinds, k)
				}
			}
			sort.Strings(kinds)
			kindCell, timeoutCell, attemptsCell := "—", "—", "—"
			if len(kinds) > 0 {
				var timeouts, attempts []int
				ticked := make([]string, len(kinds))
				for _, k := range kinds {
					if err := registry[k].need("timeout_seconds"); err != nil {
						return "", err
					}
				}
				for _, k := range kinds {
					if err := registry[k].need("max_attempts"); err != nil {
						return "", err
					}
				}
				for i, k := range kinds {
					timeouts = append(timeouts, registry[k].TimeoutSeconds)
					attempts = append(attempts, registry[k].MaxAttempts)
					ticked[i] = "`" + k + "`"
				}
				kindCell = strings.Join(ticked, "<br>")
				timeoutCell = qmRange(qmUniqSorted(timeouts))
				attemptsCell = qmRange(qmUniqSorted(attempts))
			}
			plane := entry.plane
			if entry.note != "" {
				plane += "<br>" + entry.note
			}
			for _, k := range kinds {
				st := state[k]
				if err := st.need("state", "route", "rollback_route"); err != nil {
					return "", err
				}
				flag := " ⚠"
				if st.State == "go_default" {
					flag = ""
				}
				plane += "<br>`" + k + "`: state=`" + st.State + "`" + flag + ", route=`" + st.Route +
					"`, rollback_route=`" + st.RollbackRoute + "` (migration-state.json)"
			}
			if err := process.need("binary"); err != nil {
				return "", err
			}
			lines = append(lines, "| `"+name+"` (`"+process.Binary+"`) | `"+goQueue+"` | "+kindCell+" | "+
				timeoutCell+" | "+attemptsCell+" | "+qmTicks(entry.celeryQueues)+" | "+
				qmTicks(entry.celeryConsumers)+" | "+plane+" |")
		}
	}
	served := map[string]bool{}
	for _, p := range processes {
		for _, k := range p.JobKinds {
			served[k] = true
		}
	}
	var orphans []string
	for _, k := range qmKeys(registry) {
		if !served[k] {
			orphans = append(orphans, k)
		}
	}
	if len(orphans) > 0 {
		return "", qmFail{"gen-queue-mapping: " + qmRegistryRel + " kinds " + qmList(orphans) +
			" are in no process job_kinds of " + qmDeploymentRel + " -- every registered kind must be served by a worker."}
	}

	for _, p := range processes {
		for _, k := range p.JobKinds {
			if q := registry[k].Queue; !qmHas(p.Queues, q) {
				return "", qmFail{"gen-queue-mapping: kind '" + k + "' is listed in job_kinds of process '" + p.Name +
					"' but its registry queue '" + q + "' is not among that process's queues " + qmList(p.Queues) + "."}
			}
		}
	}
	lines = append(lines, "",
		"Celery queues carrying no work reachable through a Go queue at all (telemetry, not routed work):",
		"", "| Celery queue | Why it has no Go queue |", "| --- | --- |")
	for _, q := range qmKeys(qmNoGoQueue) {
		lines = append(lines, "| `"+q+"` | "+qmNoGoQueue[q]+" |")
	}
	lines = append(lines, qmEnd)
	return strings.Join(lines, "\n"), nil
}

// qmMarkers returns the offsets of the marker pair, or ok=false.
func qmMarkers(doc string) (start, stop int, ok bool) {
	start = strings.Index(doc, qmBegin)
	stop = strings.Index(doc, qmEnd)
	if start == -1 || stop == -1 || stop < start {
		return 0, 0, false
	}
	return start, stop + len(qmEnd), true
}

func qmGenVerb(_ []string, _, stderr io.Writer) int {
	root, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(stderr, "gen-queue-mapping:", err)
		return 1
	}
	rendered, err := qmRender(root)
	if err != nil {
		return qmReport(stderr, "gen-queue-mapping:", err)
	}
	docPath := filepath.Join(root, qmDocRel)
	doc, err := a1ReadText(docPath)
	if err != nil {
		return qmReport(stderr, "gen-queue-mapping:", err)
	}
	start, stop, ok := qmMarkers(doc)
	if !ok {
		fmt.Fprintf(stderr, "Generated queue-map markers not found in %s\n", docPath)
		return 1
	}
	if err := os.WriteFile(docPath, []byte(doc[:start]+rendered+doc[stop:]), 0o644); err != nil {
		fmt.Fprintln(stderr, "gen-queue-mapping:", err)
		return 1
	}
	return 0
}

func qmDriftVerb(_ []string, stdout, stderr io.Writer) int {
	root, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(stderr, "check-queue-mapping-drift:", err)
		return 1
	}
	expected, err := qmRender(root)
	if err != nil {
		if f, ok := err.(qmFail); ok {
			fmt.Fprintf(stdout, "ERROR: generator failed to render (producers out of sync?): %s\n", f.msg)
			return 1
		}
		return qmReport(stderr, "check-queue-mapping-drift:", err)
	}
	docPath := filepath.Join(root, qmDocRel)
	doc, err := a1ReadText(docPath)
	if err != nil {
		return qmReport(stderr, "check-queue-mapping-drift:", err)
	}
	start, stop, ok := qmMarkers(doc)
	if !ok {
		fmt.Fprintf(stdout, "ERROR: generated queue-map markers missing in %s\n", docPath)
		return 1
	}
	if doc[start:stop] != expected {
		fmt.Fprintf(stdout, "ERROR: generated queue-map block in %s is stale. Run 'cigate docs gen-queue-mapping' and commit the result.\n", qmDocRel)
		return 1
	}
	fmt.Fprintln(stdout, "Queue mapping docs drift check passed")
	return 0
}
