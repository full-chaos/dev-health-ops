package teamsidentity

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// storeTouchCounter fails every store call and counts it: a bad import must
// answer before any observation or catalog write, so the count stays zero.
type storeTouchCounter struct {
	driver.Conn
	calls int
}

var errStoreTouched = errors.New("the store must not be touched by a bad import")

func (c *storeTouchCounter) Query(context.Context, string, ...any) (driver.Rows, error) {
	c.calls++
	return nil, errStoreTouched
}

func (c *storeTouchCounter) QueryRow(context.Context, string, ...any) driver.Row {
	c.calls++
	return touchedRow{}
}

type touchedRow struct{}

func (touchedRow) Err() error           { return errStoreTouched }
func (touchedRow) Scan(...any) error    { return errStoreTouched }
func (touchedRow) ScanStruct(any) error { return errStoreTouched }

func (c *storeTouchCounter) Exec(context.Context, string, ...any) error {
	c.calls++
	return errStoreTouched
}

func (c *storeTouchCounter) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.calls++
	return nil, errStoreTouched
}

func (c *storeTouchCounter) Select(context.Context, any, string, ...any) error {
	c.calls++
	return errStoreTouched
}

// TestImportBadAssociationListsAre422WithNoWrite pins CHAOS-9014: an
// association list whose shape ClickHouse cannot store used to answer 500
// after the observation row was written. It now answers a named 422 before
// any write, for every provider type and both list keys.
func TestImportBadAssociationListsAre422WithNoWrite(t *testing.T) {
	shapes := []struct{ id, value, errType string }{
		{"AINT", `[7,"ENG"]`, "string_type"},
		{"ANULL", `["a",null]`, "string_type"},
		{"ABOOL", `[true]`, "string_type"},
		{"AFLOAT", `[1.5]`, "string_type"},
		{"ANESTED", `[["x"]]`, "string_type"},
		{"ANUM", `5`, "list_type"},
		{"ABOOLS", `true`, "list_type"},
		{"AFLOATS", `1.5`, "list_type"},
	}
	for _, provider := range []string{"jira", "github", "gitlab", "linear"} {
		for _, key := range []string{"project_keys", "repo_patterns"} {
			for _, shape := range shapes {
				name := provider + "/" + key + "/" + shape.id
				t.Run(name, func(t *testing.T) {
					counter := &storeTouchCounter{}
					body := `{"teams":[{"provider_type":"` + provider + `","provider_team_id":"T1","name":"T1","associations":{"` + key + `":` + shape.value + `}}],"on_conflict":"merge"}`
					rec := postImportBody(t, Store{Conn: counter}, body)
					if rec.Code != http.StatusUnprocessableEntity {
						t.Fatalf("status = %d, want 422\nbody: %s", rec.Code, rec.Body.String())
					}
					if !strings.Contains(rec.Body.String(), `"type":"`+shape.errType+`"`) ||
						!strings.Contains(rec.Body.String(), `"associations","`+key+`"`) {
						t.Errorf("body does not name the error and the field: %s", rec.Body.String())
					}
					if counter.calls != 0 {
						t.Errorf("store touched %d times by a rejected import, want 0", counter.calls)
					}
				})
			}
		}
	}
}

// TestImportAcceptedAssociationShapesPassValidation guards the other side:
// shapes the catalog stores today are not newly refused.
func TestImportAcceptedAssociationShapesPassValidation(t *testing.T) {
	for _, value := range []string{`["ENG","OPS"]`, `[]`, `null`, `"single"`, `""`, `{"k":[1],"z":null}`} {
		for _, key := range []string{"project_keys", "repo_patterns"} {
			counter := &storeTouchCounter{}
			body := `{"teams":[{"provider_type":"jira","provider_team_id":"T1","name":"T1","associations":{"` + key + `":` + value + `}}]}`
			rec := postImportBody(t, Store{Conn: counter}, body)
			if rec.Code == http.StatusUnprocessableEntity {
				t.Errorf("%s=%s refused with 422: %s", key, value, rec.Body.String())
			}
		}
	}
}

// TestImportReportsEveryFieldErrorInOneResponse pins CHAOS-10593: association
// errors used to be dropped when another field of the same team was also
// invalid.
func TestImportReportsEveryFieldErrorInOneResponse(t *testing.T) {
	counter := &storeTouchCounter{}
	body := `{"teams":[{"provider_type":"jira","provider_team_id":"T1","associations":{"project_keys":[7],"repo_patterns":5}}]}`
	rec := postImportBody(t, Store{Conn: counter}, body)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422\nbody: %s", rec.Code, rec.Body.String())
	}
	const want = `{"detail":[` +
		`{"type":"missing","loc":["body","teams",0,"name"],"msg":"Field required"},` +
		`{"type":"string_type","loc":["body","teams",0,"associations","project_keys",0],"msg":"Input should be a valid string"},` +
		`{"type":"list_type","loc":["body","teams",0,"associations","repo_patterns"],"msg":"Input should be a valid list or string"}]}`
	if got := rec.Body.String(); got != want {
		t.Errorf("body = %s\nwant   %s", got, want)
	}
	if counter.calls != 0 {
		t.Errorf("store touched %d times, want 0", counter.calls)
	}
}
