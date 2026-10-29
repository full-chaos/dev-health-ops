package restorefence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// ManifestVersion is the manifest's format version.
const ManifestVersion = 1

// maxRevision is the largest revision the wire allows (2^53-1, migration 0013).
const maxRevision int64 = 9007199254740991

// ErrManifest reports a manifest the fence refuses.
var ErrManifest = errors.New("restorefence: invalid manifest")

// Floor is the highest revision the source reached for one aggregate.
type Floor struct {
	AggregateType string `json:"aggregate_type"`
	AggregateID   string `json:"aggregate_id"`
	Revision      int64  `json:"revision"`
}

// Row is one carried row: the section's column names and their values. A value
// is a string, a number, a boolean or null; the key set is checked against the
// section's declaration, so no other column can ride along.
type Row map[string]any

// Section is the carried rows of one table.
type Section struct {
	Name string `json:"name"`
	Rows []Row  `json:"rows"`
}

// Manifest is what the fence carries from the shadow restore (the source) to
// the restored database. It holds identifiers, timestamps, reason codes and
// counters only.
type Manifest struct {
	Version        int       `json:"version"`
	SourceTimeline int64     `json:"source_timeline"`
	Floors         []Floor   `json:"floors"`
	Sections       []Section `json:"sections"`
	SHA256         string    `json:"sha256"`
}

// Canonical returns the bytes the digest covers: the manifest without its own
// digest, members in a fixed order, rows sorted by key, numbers as written.
func (m Manifest) Canonical() ([]byte, error) {
	body := m
	floors := append([]Floor(nil), body.Floors...)
	sort.Slice(floors, func(i, j int) bool {
		if floors[i].AggregateType != floors[j].AggregateType {
			return floors[i].AggregateType < floors[j].AggregateType
		}
		return floors[i].AggregateID < floors[j].AggregateID
	})
	body.Floors = floors
	secs := make([]Section, 0, len(body.Sections))
	for _, s := range body.Sections {
		rows := append([]Row(nil), s.Rows...)
		def, ok := sectionByName(s.Name)
		if ok {
			key := def.key.name
			sort.SliceStable(rows, func(i, j int) bool { return fmt.Sprint(rows[i][key]) < fmt.Sprint(rows[j][key]) })
		}
		secs = append(secs, Section{Name: s.Name, Rows: rows})
	}
	sort.Slice(secs, func(i, j int) bool { return secs[i].Name < secs[j].Name })
	body.Sections = secs
	// The digest input leaves out the sha256 member.
	raw, err := json.Marshal(struct {
		Version        int       `json:"version"`
		SourceTimeline int64     `json:"source_timeline"`
		Floors         []Floor   `json:"floors"`
		Sections       []Section `json:"sections"`
	}{body.Version, body.SourceTimeline, body.Floors, body.Sections})
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// Digest is the hex SHA-256 of Canonical.
func (m Manifest) Digest() (string, error) {
	raw, err := m.Canonical()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// Seal sets SHA256 to the digest.
func (m *Manifest) Seal() error {
	d, err := m.Digest()
	if err != nil {
		return err
	}
	m.SHA256 = d
	return nil
}

func sectionByName(name string) (section, bool) {
	for _, s := range sections {
		if s.name == name {
			return s, true
		}
	}
	return section{}, false
}

// ParseManifest reads a manifest and refuses anything the fence did not write:
// an unknown member, an unknown section, a row with other columns than the
// section declares, a value of another shape, a floor outside 1..2^53-1, a
// digest that does not match the content, or trailing content.
func ParseManifest(raw []byte) (Manifest, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	var m struct {
		Version        int          `json:"version"`
		SourceTimeline int64        `json:"source_timeline"`
		Floors         []Floor      `json:"floors"`
		Sections       []rawSection `json:"sections"`
		SHA256         string       `json:"sha256"`
	}
	if err := dec.Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("%w: %v", ErrManifest, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return Manifest{}, fmt.Errorf("%w: trailing content", ErrManifest)
	}
	out := Manifest{Version: m.Version, SourceTimeline: m.SourceTimeline, Floors: m.Floors, SHA256: m.SHA256}
	if out.Version != ManifestVersion {
		return Manifest{}, fmt.Errorf("%w: version %d, want %d", ErrManifest, out.Version, ManifestVersion)
	}
	if out.SourceTimeline < 1 {
		return Manifest{}, fmt.Errorf("%w: source_timeline %d", ErrManifest, out.SourceTimeline)
	}
	floorSeen := map[string]bool{}
	for _, f := range out.Floors {
		if f.AggregateType == "" || f.AggregateID == "" || f.Revision < 1 || f.Revision > maxRevision {
			return Manifest{}, fmt.Errorf("%w: a floor is outside 1..2^53-1 or unnamed", ErrManifest)
		}
		key := f.AggregateType + ":" + f.AggregateID
		if floorSeen[key] {
			return Manifest{}, fmt.Errorf("%w: a floor is listed twice", ErrManifest)
		}
		floorSeen[key] = true
	}
	seen := map[string]bool{}
	for _, rs := range m.Sections {
		def, ok := sectionByName(rs.Name)
		if !ok {
			return Manifest{}, fmt.Errorf("%w: unknown section %q", ErrManifest, rs.Name)
		}
		if seen[rs.Name] {
			return Manifest{}, fmt.Errorf("%w: section %q twice", ErrManifest, rs.Name)
		}
		seen[rs.Name] = true
		want := def.columnNames()
		sort.Strings(want)
		for _, row := range rs.Rows {
			keys := make([]string, 0, len(row))
			for k, v := range row {
				keys = append(keys, k)
				switch v.(type) {
				case nil, string, bool, json.Number:
				default:
					return Manifest{}, fmt.Errorf("%w: section %q has a value of another shape", ErrManifest, rs.Name)
				}
			}
			sort.Strings(keys)
			if strings.Join(keys, ",") != strings.Join(want, ",") {
				return Manifest{}, fmt.Errorf("%w: section %q rows must carry exactly %v", ErrManifest, rs.Name, want)
			}
		}
		out.Sections = append(out.Sections, Section{Name: rs.Name, Rows: rs.Rows})
	}
	digest, err := out.Digest()
	if err != nil {
		return Manifest{}, err
	}
	if out.SHA256 != digest {
		return Manifest{}, fmt.Errorf("%w: digest does not match the content", ErrManifest)
	}
	return out, nil
}

type rawSection struct {
	Name string `json:"name"`
	Rows []Row  `json:"rows"`
}

// Marshal returns the manifest as written to a file.
func (m Manifest) Marshal() ([]byte, error) {
	if m.SHA256 == "" {
		return nil, fmt.Errorf("%w: not sealed", ErrManifest)
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}
