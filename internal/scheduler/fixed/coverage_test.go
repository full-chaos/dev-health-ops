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
	if unconditional != 19 {
		t.Fatalf("parsed %d unconditional beat entries, want 19", unconditional)
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
