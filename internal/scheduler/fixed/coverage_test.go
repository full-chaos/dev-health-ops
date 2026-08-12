package fixed

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The schedule-coverage test reads the legacy Celery Beat inventory directly
// out of the Python source rather than a copy of it. That is the point: a
// hand-maintained duplicate of the Beat table would drift silently, and the
// TRD requires the cadence contract to be "generated from or compared against
// the existing schedule definitions so a migration cannot silently change
// timing."
//
// The parser understands exactly the forms the checked-in file uses. An
// unrecognized form fails the test rather than being skipped, so introducing a
// new cadence syntax in Python forces a reviewed decision here.

const beatConfigRelativePath = "src/dev_health_ops/workers/config.py"

var (
	beatEntryPattern    = regexp.MustCompile(`^\s{4}"([a-z0-9-]+)":\s*\{\s*$`)
	beatOptionalPattern = regexp.MustCompile(
		`^\s*beat_schedule\["([a-z0-9-]+)"\]\s*=\s*\{\s*$`)
	beatSchedulePattern = regexp.MustCompile(`^\s*"schedule":\s*(.+?),?\s*$`)
	moduleFloatPattern  = regexp.MustCompile(`^([a-z_][a-z0-9_]*)\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*$`)
	crontabPattern      = regexp.MustCompile(`^crontab\((.*)\)$`)
	crontabArgPattern   = regexp.MustCompile(`([a-z_]+)=("?[A-Za-z0-9]+"?)`)
)

type parsedBeatEntry struct {
	Name     string
	Cadence  Cadence
	Optional bool
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatalf("working directory: %v", err)
	}
	for depth := 0; depth < 8; depth++ {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
		directory = parent
	}
	t.Fatal("could not locate the repository root from the test working directory")
	return ""
}

func parseBeatSchedule(t *testing.T) []parsedBeatEntry {
	t.Helper()
	path := filepath.Join(repositoryRoot(t), beatConfigRelativePath)
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", beatConfigRelativePath, err)
	}
	lines := strings.Split(string(source), "\n")

	// Module-level float constants are the only indirection the checked-in
	// file uses for a cadence (the shared stream-consumer interval).
	constants := make(map[string]float64)
	for _, line := range lines {
		if match := moduleFloatPattern.FindStringSubmatch(line); match != nil {
			value, parseErr := strconv.ParseFloat(match[2], 64)
			if parseErr == nil {
				constants[match[1]] = value
			}
		}
	}

	entries := make([]parsedBeatEntry, 0, 20)
	inSchedule := false
	var current *parsedBeatEntry
	currentOptional := false
	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, "beat_schedule = {"):
			inSchedule = true
			continue
		case inSchedule && line == "}":
			inSchedule = false
			continue
		}
		if match := beatOptionalPattern.FindStringSubmatch(line); match != nil {
			entries = append(entries, parsedBeatEntry{Name: match[1], Optional: true})
			current = &entries[len(entries)-1]
			currentOptional = true
			continue
		}
		if inSchedule {
			if match := beatEntryPattern.FindStringSubmatch(line); match != nil {
				entries = append(entries, parsedBeatEntry{Name: match[1]})
				current = &entries[len(entries)-1]
				currentOptional = false
				continue
			}
		}
		if current == nil {
			continue
		}
		match := beatSchedulePattern.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		cadence, parseErr := parseBeatCadence(strings.TrimSpace(match[1]), constants)
		if parseErr != nil {
			t.Fatalf("beat entry %s: %v", current.Name, parseErr)
		}
		current.Cadence = cadence
		current.Optional = currentOptional
		current = nil
	}

	sort.Slice(entries, func(first, second int) bool {
		return entries[first].Name < entries[second].Name
	})
	return entries
}

func parseBeatCadence(expression string, constants map[string]float64) (Cadence, error) {
	if match := crontabPattern.FindStringSubmatch(expression); match != nil {
		return parseCrontab(match[1])
	}
	if seconds, err := strconv.ParseFloat(expression, 64); err == nil {
		return EveryInterval(time.Duration(seconds) * time.Second), nil
	}
	if seconds, ok := constants[expression]; ok {
		return EveryInterval(time.Duration(seconds) * time.Second), nil
	}
	return Cadence{}, fmt.Errorf("unrecognized cadence expression %q", expression)
}

var weekdayNames = map[string]time.Weekday{
	"sunday": time.Sunday, "monday": time.Monday, "tuesday": time.Tuesday,
	"wednesday": time.Wednesday, "thursday": time.Thursday,
	"friday": time.Friday, "saturday": time.Saturday,
}

func parseCrontab(arguments string) (Cadence, error) {
	fields := map[string]string{}
	for _, match := range crontabArgPattern.FindAllStringSubmatch(arguments, -1) {
		fields[match[1]] = strings.Trim(match[2], `"`)
	}
	hourText, hasHour := fields["hour"]
	minuteText, hasMinute := fields["minute"]
	if !hasHour || !hasMinute {
		return Cadence{}, fmt.Errorf("crontab(%s) omits hour or minute", arguments)
	}
	hour, err := strconv.Atoi(hourText)
	if err != nil {
		return Cadence{}, fmt.Errorf("crontab hour %q: %w", hourText, err)
	}
	minute, err := strconv.Atoi(minuteText)
	if err != nil {
		return Cadence{}, fmt.Errorf("crontab minute %q: %w", minuteText, err)
	}
	dayOfWeek, hasDayOfWeek := fields["day_of_week"]
	if !hasDayOfWeek {
		if len(fields) != 2 {
			return Cadence{}, fmt.Errorf("crontab(%s) uses unsupported fields", arguments)
		}
		return DailyAt(hour, minute), nil
	}
	weekday, ok := weekdayNames[strings.ToLower(dayOfWeek)]
	if !ok {
		return Cadence{}, fmt.Errorf("crontab day_of_week %q is unsupported", dayOfWeek)
	}
	if len(fields) != 3 {
		return Cadence{}, fmt.Errorf("crontab(%s) uses unsupported fields", arguments)
	}
	return WeeklyAt(weekday, hour, minute), nil
}

func TestBeatScheduleParserFindsTheCheckedInventory(t *testing.T) {
	entries := parseBeatSchedule(t)
	unconditional, optional := 0, 0
	for _, entry := range entries {
		if entry.Cadence.Kind == "" {
			t.Fatalf("beat entry %s has no parsed cadence", entry.Name)
		}
		if entry.Optional {
			optional++
			continue
		}
		unconditional++
	}
	// The TRD acceptance criteria are stated against exactly these counts. A
	// change here means a Beat entry was added or removed, which must be a
	// reviewed ownership decision rather than an unnoticed coverage change.
	//
	// 19 -> 20 when CHAOS-3404's `ask-dev-retention-sweep` merged from main.
	// The reviewed decision: it is owned by the already-ported Go schedule
	// `prune_ask_dev_conversations` (CHAOS-3209), which stopped being Native
	// in the same change because it now has a Python predecessor. The native
	// sync coverage refresh then retired the Beat entry entirely, so the
	// checked inventory now holds twenty unconditional rows.
	if unconditional != 20 {
		t.Fatalf("parsed %d unconditional beat entries, want 20", unconditional)
	}
	if optional != 1 {
		t.Fatalf("parsed %d optional beat entries, want 1", optional)
	}
}

func TestScheduleCoverageAccountsForEveryBeatEntry(t *testing.T) {
	if err := ValidateInventory(); err != nil {
		t.Fatalf("ValidateInventory() = %v", err)
	}
	parsed := parseBeatSchedule(t)
	inventory := LegacyBeatInventoryIndex()

	for _, entry := range parsed {
		owner, ok := inventory[entry.Name]
		if !ok {
			t.Errorf(
				"beat entry %q has no Go owner; add it to LegacyBeatInventory with an explicit owner",
				entry.Name,
			)
			continue
		}
		if owner.Cadence.Fingerprint() != entry.Cadence.Fingerprint() {
			t.Errorf(
				"beat entry %q cadence %s does not match the checked owner cadence %s",
				entry.Name, entry.Cadence.Fingerprint(), owner.Cadence.Fingerprint(),
			)
		}
		if owner.Optional != entry.Optional {
			t.Errorf(
				"beat entry %q optional=%t but the inventory records optional=%t",
				entry.Name, entry.Optional, owner.Optional,
			)
		}
	}

	names := make(map[string]struct{}, len(parsed))
	for _, entry := range parsed {
		names[entry.Name] = struct{}{}
	}
	for name := range inventory {
		if _, ok := names[name]; !ok {
			t.Errorf(
				"inventory claims beat entry %q which no longer exists in %s",
				name, beatConfigRelativePath,
			)
		}
	}
}

func TestScheduleCoverageBindsEveryFixedScheduleToItsBeatCadence(t *testing.T) {
	parsed := parseBeatSchedule(t)
	byName := make(map[string]parsedBeatEntry, len(parsed))
	for _, entry := range parsed {
		byName[entry.Name] = entry
	}
	schedules, err := Schedules()
	if err != nil {
		t.Fatalf("Schedules() = %v", err)
	}
	if len(schedules) == 0 {
		t.Fatal("no fixed schedules are declared")
	}
	for _, schedule := range schedules {
		if schedule.Native {
			if schedule.LegacyBeatEntry != "" {
				t.Errorf("native schedule %s also claims legacy beat entry %q", schedule.ID, schedule.LegacyBeatEntry)
			}
			continue
		}
		legacy, ok := byName[schedule.LegacyBeatEntry]
		if !ok {
			t.Errorf(
				"schedule %s claims beat entry %q which is absent from %s",
				schedule.ID, schedule.LegacyBeatEntry, beatConfigRelativePath,
			)
			continue
		}
		if legacy.Cadence.Fingerprint() != schedule.Cadence.Fingerprint() {
			t.Errorf(
				"schedule %s cadence %s drifted from beat entry %q cadence %s",
				schedule.ID, schedule.Cadence.Fingerprint(),
				schedule.LegacyBeatEntry, legacy.Cadence.Fingerprint(),
			)
		}
	}
}

func TestLegacyInventoryReplacementsAreNotSilentlyDropped(t *testing.T) {
	// Every non-schedule owner must name a concrete replacement component. A
	// legacy entry whose owner is "removed" is the only case where no runtime
	// component takes over, and it still has to say what makes the removal
	// safe.
	for _, entry := range LegacyBeatInventory() {
		switch entry.Owner {
		case OwnerFixedSchedule:
			continue
		case OwnerRemoved, OwnerProductScheduler, OwnerReconciler,
			OwnerStreamRunner, OwnerRuntimeTelemetry:
			if strings.TrimSpace(entry.Note) == "" {
				t.Errorf("legacy entry %s is replaced by %s without a reason", entry.Name, entry.Owner)
			}
			if strings.TrimSpace(entry.OwnerRef) == "" {
				t.Errorf("legacy entry %s names no replacement component", entry.Name)
			}
		default:
			t.Errorf("legacy entry %s has unknown owner %q", entry.Name, entry.Owner)
		}
	}
}

// Sync coverage is durable product state, not runtime telemetry. Its Python
// Beat entry and task are deleted only after the native schedule and producer
// are constructed, so a Go-only stack can create cold projections and rebuild
// invalidated ones without restoring a second writer.
func TestSyncCoverageRefreshHasAConstructedFixedScheduleOwner(t *testing.T) {
	schedules, err := Schedules()
	if err != nil {
		t.Fatal(err)
	}
	for _, schedule := range schedules {
		if schedule.ID == "sync_coverage_refresh" && schedule.Native &&
			schedule.LegacyBeatEntry == "" &&
			schedule.Cadence.Fingerprint() == EveryInterval(300*time.Second).Fingerprint() {
			producers, producerErr := NewProducerSet(NewSyncCoverageRefreshProducer())
			if producerErr != nil {
				t.Fatal(producerErr)
			}
			if _, ok := producers.Producer(schedule.ProducerID); !ok {
				t.Fatalf("sync coverage producer %q is not constructed", schedule.ProducerID)
			}
			return
		}
	}
	t.Fatal("native sync coverage schedule is not constructed")
}

// The cadence fingerprint alone is not the whole timing contract. Beat resolves
// every crontab against the Celery `timezone` setting, and the missed-run
// policy decides what happens after an outage. A change to either would alter
// when work runs without changing a single cadence value, so both are pinned
// here against the Python source and against a checked policy table.

var pythonSettingPattern = regexp.MustCompile(`^([a-z_]+)\s*=\s*"([^"]*)"\s*$`)

func parseBeatSettings(t *testing.T) map[string]string {
	t.Helper()
	path := filepath.Join(repositoryRoot(t), beatConfigRelativePath)
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", beatConfigRelativePath, err)
	}
	settings := map[string]string{}
	for _, line := range strings.Split(string(source), "\n") {
		if match := pythonSettingPattern.FindStringSubmatch(line); match != nil {
			settings[match[1]] = match[2]
		}
	}
	return settings
}

func TestScheduleCoverageFingerprintsTheBeatTimezone(t *testing.T) {
	settings := parseBeatSettings(t)
	zone, ok := settings["timezone"]
	if !ok {
		t.Fatalf("%s no longer declares a Celery timezone", beatConfigRelativePath)
	}
	if zone != inventoryTimezone {
		t.Fatalf(
			"Beat resolves crontabs in %q but the fixed inventory declares %q; "+
				"every schedule would fire at a different wall-clock time",
			zone, inventoryTimezone,
		)
	}
	schedules, err := Schedules()
	if err != nil {
		t.Fatalf("Schedules() = %v", err)
	}
	for _, schedule := range schedules {
		if schedule.Timezone != zone {
			t.Errorf(
				"schedule %s declares timezone %q but Beat resolves in %q",
				schedule.ID, schedule.Timezone, zone,
			)
		}
	}
}

// The missed-run policy is a reviewed decision per schedule, not a default.
// Pinning it here means flipping a safety net to skip, or making telemetry
// catch up, has to be an explicit edit to this table.
func TestScheduleCoveragePinsTheMissedRunPolicy(t *testing.T) {
	want := map[string]CatchUpPolicy{
		"scheduled_metrics_dispatch":       CatchUpSkip,
		"scheduled_reports_dispatch":       CatchUpSkip,
		"phone_home_heartbeat":             CatchUpSkip,
		"prune_rate_limit_observations":    CatchUpSkip,
		"prune_external_ingest_batches":    CatchUpSkip,
		"prune_ask_dev_conversations":      CatchUpSkip,
		"daily_metrics_fanout":             CatchUpBounded,
		"complexity_daily_fanout":          CatchUpBounded,
		"release_impact_daily_fanout":      CatchUpBounded,
		"recommendations_daily_fanout":     CatchUpBounded,
		"membership_backfill_daily_fanout": CatchUpBounded,
		"capacity_forecast_weekly_fanout":  CatchUpBounded,
		"sync_coverage_refresh":            CatchUpSkip,
	}
	schedules, err := Schedules()
	if err != nil {
		t.Fatalf("Schedules() = %v", err)
	}
	if len(schedules) != len(want) {
		t.Fatalf("%d schedules declared but %d policies pinned", len(schedules), len(want))
	}
	for _, schedule := range schedules {
		expected, ok := want[schedule.ID]
		if !ok {
			t.Errorf("schedule %s has no pinned missed-run policy", schedule.ID)
			continue
		}
		if schedule.CatchUp != expected {
			t.Errorf(
				"schedule %s missed-run policy changed to %q (pinned %q)",
				schedule.ID, schedule.CatchUp, expected,
			)
		}
	}
}
