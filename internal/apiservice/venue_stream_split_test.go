package apiservice

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The product-telemetry stream rows of the protected-routes oracle are split by
// the request that wrote each entry: the entries of the requests a ruling retired
// are pinned, every other entry is still compared with the frozen Python rows.

const productTelemetryEventsPath = "/api/v1/product-telemetry/events"

// requestEntries are the stream entries one request added to the Go plane.
type requestEntries struct {
	request string
	entries []string
}

// parseStreamEntries splits venueoracle.StreamEntries text into its entries.
func parseStreamEntries(text string) []string {
	if text == "" {
		return nil
	}
	parts := strings.Split(text, " | product-telemetry:")
	for index := 1; index < len(parts); index++ {
		parts[index] = "product-telemetry:" + parts[index]
	}
	return parts
}

// newStreamEntries are the entries of after that before did not hold (as a multiset).
func newStreamEntries(before, after []string) []string {
	held := map[string]int{}
	for _, entry := range before {
		held[entry]++
	}
	var added []string
	for _, entry := range after {
		if held[entry] > 0 {
			held[entry]--
			continue
		}
		added = append(added, entry)
	}
	return added
}

func streamKeyOfEntry(entry string) string {
	rest := strings.TrimPrefix(entry, "product-telemetry:")
	if end := strings.Index(rest, ":events: "); end >= 0 {
		return rest[:end]
	}
	return rest
}

// acceptedStreamWrites maps each stream key to the requests, in request order, whose answer
// is a 202 that names that stream: the order of the entries in the stream.
func acceptedStreamWrites(requests []venueoracle.Request, responses []venueoracle.Response) (map[string][]string, error) {
	if len(requests) != len(responses) {
		return nil, fmt.Errorf("%d requests but %d responses", len(requests), len(responses))
	}
	writes := map[string][]string{}
	for index, request := range requests {
		if request.Path != productTelemetryEventsPath || responses[index].Status != 202 {
			continue
		}
		var body struct {
			Stream string `json:"stream"`
		}
		if err := json.Unmarshal([]byte(responses[index].Body), &body); err != nil {
			return nil, fmt.Errorf("%s: %v", request.Name, err)
		}
		stream := strings.TrimSuffix(strings.TrimPrefix(body.Stream, "product-telemetry:"), ":events")
		if body.Stream == "disabled" || stream == body.Stream {
			continue
		}
		writes[stream] = append(writes[stream], request.Name)
	}
	return writes, nil
}

// splitProductTelemetryStreams attributes the frozen Python entries to requests (the j-th entry of
// a stream is the j-th accepted request that named it) and the Go entries to the request whose
// answer added them. It returns the Python and Go entries of the requests NOT in retired
// (sorted, joined) and the Go entries of the retired requests (sorted, joined), or an error when
// an entry cannot be attributed.
func splitProductTelemetryStreams(
	pythonText string, writes map[string][]string, goDeltas []requestEntries, goText string, retired map[string]bool,
) (pythonOther, goOther, goRetired string, err error) {
	byKey := map[string][]string{}
	for _, entry := range parseStreamEntries(pythonText) {
		key := streamKeyOfEntry(entry)
		byKey[key] = append(byKey[key], entry)
	}
	var pythonKept []string
	for key, entries := range byKey {
		requests := writes[key]
		if len(requests) != len(entries) {
			return "", "", "", fmt.Errorf("the Python stream %q holds %d entries but %d accepted requests named it: the entries cannot be attributed", key, len(entries), len(requests))
		}
		for index, entry := range entries {
			if !retired[requests[index]] {
				pythonKept = append(pythonKept, entry)
			}
		}
	}
	for key, requests := range writes {
		if len(byKey[key]) != len(requests) {
			return "", "", "", fmt.Errorf("%d accepted requests named the stream %q but the Python plane holds %d entries: the entries cannot be attributed", len(requests), key, len(byKey[key]))
		}
	}
	var goKept, goPinned, goAll []string
	for _, delta := range goDeltas {
		goAll = append(goAll, delta.entries...)
		if retired[delta.request] {
			goPinned = append(goPinned, delta.entries...)
		} else {
			goKept = append(goKept, delta.entries...)
		}
	}
	held := parseStreamEntries(goText)
	if missed := newStreamEntries(goAll, held); len(missed) > 0 {
		return "", "", "", fmt.Errorf("the Go plane holds %d entries no request added: %v", len(missed), missed)
	}
	if missed := newStreamEntries(held, goAll); len(missed) > 0 {
		return "", "", "", fmt.Errorf("the requests added %d entries the Go plane does not hold: %v", len(missed), missed)
	}
	for _, list := range [][]string{pythonKept, goKept, goPinned} {
		sort.Strings(list)
	}
	return strings.Join(pythonKept, " | "), strings.Join(goKept, " | "), strings.Join(goPinned, " | "), nil
}

const streamPlantedEntryA = `product-telemetry:anonymous:events: events=[{"eventId":"a"}] ingestion_id=<blank> org_id_hash= source=dev-health-web`
const streamPlantedEntryB = `product-telemetry:h2:events: events=[{"eventId":"b"}] ingestion_id=<blank> org_id_hash=h2 source=dev-health-web`
const streamPlantedExtra = `product-telemetry:anonymous:events: events=[{"eventId":"x"}] ingestion_id=<blank> org_id_hash= source=dev-health-web`

func TestRetiredStreamEntriesAreSetAsideAndTheRestStillMatchPython(t *testing.T) {
	retired := map[string]bool{"pt: retired a": true, "pt: retired b": true}
	pythonText := streamPlantedEntryA + " | " + streamPlantedEntryB
	writes := map[string][]string{"anonymous": {"pt: retired a"}, "h2": {"pt: retired b"}}
	// Go stores nothing for the two retired requests: the others match (both empty).
	pythonOther, goOther, goRetired, err := splitProductTelemetryStreams(pythonText, writes, []requestEntries{{"pt: retired a", nil}, {"pt: retired b", nil}}, "", retired)
	if err != nil || pythonOther != "" || goOther != "" || goRetired != "" {
		t.Fatalf("got (%q, %q, %q, %v), want everything empty and no error", pythonOther, goOther, goRetired, err)
	}
	// Go's entries of a retired request are pinned, not compared.
	_, goOther, goRetired, err = splitProductTelemetryStreams(pythonText, writes, []requestEntries{{"pt: retired a", []string{streamPlantedExtra}}}, streamPlantedExtra, retired)
	if err != nil || goOther != "" || goRetired != streamPlantedExtra {
		t.Fatalf("got (%q, %q, %v): a retired request's Go entry must be pinned and not compared", goOther, goRetired, err)
	}
}

// The planted controls: an entry of a request that is NOT retired still differs.
func TestAnEntryOfARequestThatIsNotRetiredStillFails(t *testing.T) {
	retired := map[string]bool{"pt: retired a": true}
	t.Run("an extra Go entry from a non-retired request", func(t *testing.T) {
		pythonText := streamPlantedEntryA
		writes := map[string][]string{"anonymous": {"pt: retired a"}}
		pythonOther, goOther, _, err := splitProductTelemetryStreams(pythonText, writes,
			[]requestEntries{{"pt: retired a", nil}, {"pt: kept", []string{streamPlantedExtra}}}, streamPlantedExtra, retired)
		if err != nil || pythonOther == goOther {
			t.Fatalf("python-other %q, go-other %q, err %v: the planted Go entry must make them differ", pythonOther, goOther, err)
		}
	})
	t.Run("a Python entry of a non-retired request that Go does not write", func(t *testing.T) {
		pythonText := streamPlantedEntryA + " | " + streamPlantedEntryB
		writes := map[string][]string{"anonymous": {"pt: retired a"}, "h2": {"pt: kept"}}
		pythonOther, goOther, _, err := splitProductTelemetryStreams(pythonText, writes, []requestEntries{{"pt: retired a", nil}, {"pt: kept", nil}}, "", retired)
		if err != nil || pythonOther == goOther {
			t.Fatalf("python-other %q, go-other %q, err %v: the missing Go entry must make them differ", pythonOther, goOther, err)
		}
	})
	t.Run("an entry that cannot be attributed is an error", func(t *testing.T) {
		if _, _, _, err := splitProductTelemetryStreams(streamPlantedEntryA+" | "+streamPlantedExtra, map[string][]string{"anonymous": {"pt: retired a"}}, nil, "", retired); err == nil {
			t.Fatal("two entries for one accepted request must not be attributed")
		}
		if _, _, _, err := splitProductTelemetryStreams(streamPlantedEntryA, map[string][]string{"anonymous": {"pt: retired a"}}, []requestEntries{{"pt: kept", []string{streamPlantedExtra}}}, "", retired); err == nil {
			t.Fatal("a Go entry the plane does not hold must not be attributed")
		}
	})
}

func TestAcceptedStreamWritesFollowTheFrozenAnswers(t *testing.T) {
	requests := []venueoracle.Request{
		{Name: "a", Path: productTelemetryEventsPath}, {Name: "b", Path: productTelemetryEventsPath},
		{Name: "c", Path: productTelemetryEventsPath}, {Name: "d", Path: "/health"}, {Name: "e", Path: productTelemetryEventsPath},
	}
	responses := []venueoracle.Response{
		{Status: 202, Body: `{"stream":"product-telemetry:anonymous:events"}`},
		{Status: 202, Body: `{"stream":"disabled"}`},
		{Status: 422, Body: `{"detail":[]}`},
		{Status: 202, Body: `{"stream":"product-telemetry:x:events"}`},
		{Status: 202, Body: `{"stream":"product-telemetry:anonymous:events"}`},
	}
	writes, err := acceptedStreamWrites(requests, responses)
	if err != nil || len(writes) != 1 || strings.Join(writes["anonymous"], ",") != "a,e" {
		t.Fatalf("writes = %v, err = %v, want only anonymous: a,e (disabled, 422 and other routes write nothing)", writes, err)
	}
}

func TestNewStreamEntriesIsAMultisetDifference(t *testing.T) {
	before := []string{"x", "x", "y"}
	if added := newStreamEntries(before, []string{"x", "x", "x", "y", "z"}); strings.Join(added, ",") != "x,z" {
		t.Fatalf("added = %v, want x,z", added)
	}
	if got := parseStreamEntries(streamPlantedEntryA + " | " + streamPlantedEntryB); len(got) != 2 || streamKeyOfEntry(got[1]) != "h2" {
		t.Fatalf("parsed %v", got)
	}
}
