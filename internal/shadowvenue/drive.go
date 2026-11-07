package shadowvenue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// MinGap is the least time between one answer and the next request: above the shadow's default 2 s decision
// timeout, so a slot's late result line can never fall in the next slot's window.
const MinGap = 2300 * time.Millisecond

const loginPath = "/api/v1/auth/login"

// CheckBase accepts only a plain-http loopback origin with no path, query or credentials: the venue's published
// go-api port. A production or LAN host is refused here, before any request.
func CheckBase(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.Opaque != "" {
		return nil, fmt.Errorf("base %q: want http://127.0.0.1:<port> (loopback, no path, query or credentials)", raw)
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("base %q: the host is not loopback", raw)
	}
	u.Path = ""
	return u, nil
}

// ReadPassword reads one password from an owner-only regular file. The value never reaches an error.
func ReadPassword(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("password file %s: %w", path, errors.Unwrap(err))
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > 1024 {
		return "", fmt.Errorf("password file %s: want an owner-only (0600) regular file of at most 1 KiB", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("password file %s: unreadable", path)
	}
	value := strings.TrimRight(string(raw), "\r\n")
	if value == "" || strings.ContainsAny(value, "\r\n") {
		return "", fmt.Errorf("password file %s: want one non-empty line", path)
	}
	return value, nil
}

// Driver sends a plan's slots to the venue go-api, one at a time.
type Driver struct {
	Base   *url.URL
	Client *http.Client
	// Gap is the wait after each answer; at least MinGap.
	Gap time.Duration
	// Frozen is the frozen throwaway ids: before any request, a destructive slot whose path or body holds another id
	// refuses the whole drive.
	Frozen ThrowawayIDs
	Sleep  func(context.Context, time.Duration) error
	Now    func() time.Time
}

func (d *Driver) now() time.Time {
	if d.Now != nil {
		return d.Now().UTC()
	}
	return time.Now().UTC()
}

func (d *Driver) sleep(ctx context.Context, gap time.Duration) error {
	if d.Sleep != nil {
		return d.Sleep(ctx, gap)
	}
	timer := time.NewTimer(gap)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Login signs p in on go-api's login route (orgID "" asks for no org) and returns the access token. The token and
// the answer's body never reach an error.
func (d *Driver) Login(ctx context.Context, p Principal, orgID string) (string, error) {
	password, err := ReadPassword(p.PasswordFile)
	if err != nil {
		return "", fmt.Errorf("login %s: %w", p.Name, err)
	}
	in := map[string]string{"email": p.Email, "password": password}
	if orgID != "" {
		in["org_id"] = orgID
	}
	body, _ := json.Marshal(in)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.Base.String()+loginPath, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("login %s: build request", p.Name)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.Client.Do(req)
	if err != nil {
		return "", fmt.Errorf("login %s: transport failure", p.Name)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("login %s: http %d", p.Name, resp.StatusCode)
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if json.Unmarshal(raw, &out) != nil || out.AccessToken == "" {
		return "", fmt.Errorf("login %s: http 200 with no access_token (unverified account?)", p.Name)
	}
	return out.AccessToken, nil
}

// Drive sends every slot of plan in order with its principal's token, waits Gap after each answer, and writes the
// slots file to w. A gap slot is written, not sent. Answer bodies are read and dropped.
func (d *Driver) Drive(ctx context.Context, plan Plan, tokens map[string]string, w io.Writer) error {
	if d.Gap < MinGap {
		return fmt.Errorf("drive: gap %s is below %s (the shadow decision timeout plus a margin)", d.Gap, MinGap)
	}
	for _, slot := range plan.Slots {
		if !slot.Gap() && tokens[slot.Principal] == "" {
			return fmt.Errorf("drive: principal %s of row %s has no token", slot.Principal, slot.Row)
		}
		if slot.Destructive && !slot.Gap() {
			if err := d.Frozen.Holds(slot.Path, slot.Body); err != nil {
				return fmt.Errorf("drive: destructive slot %d row %s refused before any request: %w", slot.Seq, slot.Row, err)
			}
		}
	}
	if _, err := io.WriteString(w, slotsHeader+"\n"); err != nil {
		return err
	}
	for _, slot := range plan.Slots {
		if !slot.Gap() {
			if err := d.send(ctx, &slot, tokens[slot.Principal]); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(w, slot.line()+"\n"); err != nil {
			return err
		}
		if !slot.Gap() {
			if err := d.sleep(ctx, d.Gap); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Driver) send(ctx context.Context, slot *Slot, token string) error {
	var body io.Reader
	if slot.Body != none {
		body = strings.NewReader(slot.Body)
	}
	req, err := http.NewRequestWithContext(ctx, slot.Method, d.Base.String()+slot.Path, body)
	if err != nil {
		return fmt.Errorf("drive: slot %d: build request", slot.Seq)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	slot.Start = d.now()
	resp, err := d.Client.Do(req)
	if err != nil {
		slot.End, slot.Status, slot.Note = d.now(), 0, "transport failure"
		return ctx.Err()
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<20))
	_ = resp.Body.Close()
	slot.End, slot.Status = d.now(), resp.StatusCode
	return nil
}

const slotsHeader = "seq\trow\tcaller\tprincipal\tcross_org\texpect\tk\tmethod\troute\tpath\tt_start\tt_end\tstatus\tnote"

func (s Slot) line() string {
	stamp := func(t time.Time) string {
		if t.IsZero() {
			return none
		}
		return t.UTC().Format(time.RFC3339Nano)
	}
	note := s.Note
	if note == "" {
		note = none
	}
	return strings.Join([]string{strconv.Itoa(s.Seq), s.Row, s.Caller, s.Principal, strconv.FormatBool(s.CrossOrg), s.Expect,
		strconv.Itoa(s.K), s.Method, s.Route, s.Path, stamp(s.Start), stamp(s.End), strconv.Itoa(s.Status), note}, "\t")
}

// ParseSlots reads a slots file Drive wrote.
func ParseSlots(raw []byte) ([]Slot, error) {
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) == 0 || lines[0] != slotsHeader {
		return nil, errors.New("slots: the first line is not the slots header")
	}
	var slots []Slot
	for n, line := range lines[1:] {
		f := strings.Split(line, "\t")
		if len(f) != 14 {
			return nil, fmt.Errorf("slots: line %d has %d columns, want 14", n+2, len(f))
		}
		s := Slot{Row: f[1], Caller: f[2], Principal: f[3], Expect: f[5], Method: f[7], Route: f[8], Path: f[9]}
		var err1, err2, err3, err4, err5 error
		s.Seq, err1 = strconv.Atoi(f[0])
		s.CrossOrg, err2 = strconv.ParseBool(f[4])
		s.K, err3 = strconv.Atoi(f[6])
		s.Status, err4 = strconv.Atoi(f[12])
		if f[13] != none {
			s.Note = f[13]
		}
		if !s.Gap() {
			if s.Start, err5 = time.Parse(time.RFC3339Nano, f[10]); err5 == nil {
				s.End, err5 = time.Parse(time.RFC3339Nano, f[11])
			}
		}
		if err := errors.Join(err1, err2, err3, err4, err5); err != nil {
			return nil, fmt.Errorf("slots: line %d: %w", n+2, err)
		}
		slots = append(slots, s)
	}
	return slots, nil
}
