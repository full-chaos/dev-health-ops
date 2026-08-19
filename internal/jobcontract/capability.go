package jobcontract

import (
	"crypto/sha256"
	"fmt"
	"os"
	"sort"
	"strconv"
)

type ContractCapability struct {
	Kind          string            `json:"kind"`
	Versions      []int             `json:"versions"`
	SchemaDigests map[string]string `json:"schema_digests"`
}

// CapabilityReport is safe to expose from readiness/operator surfaces: it
// contains only queue/version/schema support, never encoded arguments.
type CapabilityReport struct {
	SchemaVersion int                  `json:"schema_version"`
	Queues        []string             `json:"queues"`
	Contracts     []ContractCapability `json:"contracts"`
}

// CapabilitiesForQueues derives the exact support advertised by this binary.
func CapabilitiesForQueues(root string, registry Registry, queues []string) (CapabilityReport, error) {
	if len(queues) == 0 {
		return CapabilityReport{}, fmt.Errorf("queues are required")
	}
	queueSet := make(map[string]struct{}, len(queues))
	for _, queue := range queues {
		if !matchesBounded(queuePattern, queue, 96) {
			return CapabilityReport{}, fmt.Errorf("invalid queue %q", queue)
		}
		queueSet[queue] = struct{}{}
	}

	reportQueues := make([]string, 0, len(queueSet))
	for queue := range queueSet {
		reportQueues = append(reportQueues, queue)
	}
	sort.Strings(reportQueues)
	report := CapabilityReport{SchemaVersion: 1, Queues: reportQueues}
	registryQueues := make(map[string]struct{})
	envelopeSchema, err := readContractFile(root, registry.EnvelopeSchema)
	if err != nil {
		return CapabilityReport{}, err
	}
	for _, job := range registry.Jobs {
		registryQueues[job.Queue] = struct{}{}
		if _, ok := queueSet[job.Queue]; !ok {
			continue
		}
		digests := make(map[string]string, len(job.SupportedVersions))
		for _, version := range job.SupportedVersions {
			versionKey := strconv.Itoa(version)
			data, err := readContractFile(root, job.SchemaVersions[versionKey])
			if err != nil {
				return CapabilityReport{}, err
			}
			hasher := sha256.New()
			_, _ = hasher.Write(envelopeSchema)
			_, _ = hasher.Write([]byte{0})
			_, _ = hasher.Write(data)
			digests[versionKey] = fmt.Sprintf("sha256:%x", hasher.Sum(nil))
		}
		report.Contracts = append(report.Contracts, ContractCapability{
			Kind:          job.Kind,
			Versions:      append([]int(nil), job.SupportedVersions...),
			SchemaDigests: digests,
		})
	}
	if len(report.Contracts) == 0 {
		return CapabilityReport{}, fmt.Errorf("queues %v have no registered contracts", report.Queues)
	}
	for _, queue := range report.Queues {
		if _, ok := registryQueues[queue]; !ok {
			return CapabilityReport{}, fmt.Errorf("queue %q is not registered", queue)
		}
	}
	return report, nil
}

// LoadCapabilityReport strictly loads an externally collected report.
func LoadCapabilityReport(path string) (CapabilityReport, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return CapabilityReport{}, fmt.Errorf("inspect capability report: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return CapabilityReport{}, fmt.Errorf("capability report must be a regular file")
	}
	if info.Size() > 512*1024 {
		return CapabilityReport{}, fmt.Errorf("capability report exceeds 524288 bytes")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return CapabilityReport{}, fmt.Errorf("read capability report: %w", err)
	}
	var report CapabilityReport
	if err := decodeStrict(data, 512*1024, &report); err != nil {
		return CapabilityReport{}, fmt.Errorf("decode capability report: %w", err)
	}
	if err := report.Validate(); err != nil {
		return CapabilityReport{}, err
	}
	return report, nil
}

func (report CapabilityReport) Validate() error {
	if report.SchemaVersion != 1 || len(report.Queues) == 0 {
		return fmt.Errorf("invalid capability report identity")
	}
	if !sortedUniqueStrings(report.Queues) {
		return fmt.Errorf("capability queues must be sorted unique values")
	}
	for _, queue := range report.Queues {
		if !matchesBounded(queuePattern, queue, 96) {
			return fmt.Errorf("capability report has an invalid queue")
		}
	}
	previous := ""
	for _, contract := range report.Contracts {
		if !kindPattern.MatchString(contract.Kind) || contract.Kind <= previous {
			return fmt.Errorf("capability contracts must have valid, sorted, unique kinds")
		}
		previous = contract.Kind
		if !strictlyIncreasing(contract.Versions) {
			return fmt.Errorf("capability %s versions must be sorted unique positive integers", contract.Kind)
		}
		if len(contract.SchemaDigests) != len(contract.Versions) {
			return fmt.Errorf("capability %s schema digests do not cover versions", contract.Kind)
		}
		for _, version := range contract.Versions {
			digest, ok := contract.SchemaDigests[strconv.Itoa(version)]
			if !ok || !isSHA256Digest(digest) {
				return fmt.Errorf("capability %s has an invalid schema digest", contract.Kind)
			}
		}
	}
	return nil
}

// CheckRollout proves that every supplied live report for every required queue
// can consume the producer version. It fails closed if a queue has no report or
// one old replica still lacks support.
func CheckRollout(root string, registry Registry, state MigrationState, reports []CapabilityReport) error {
	byQueue := make(map[string][]CapabilityReport)
	for _, report := range reports {
		if err := report.Validate(); err != nil {
			return err
		}
		for _, queue := range report.Queues {
			byQueue[queue] = append(byQueue[queue], report)
		}
	}

	var failures []string
	definitionsByKind := make(map[string]JobDefinition, len(registry.Jobs))
	for _, definition := range registry.Jobs {
		definitionsByKind[definition.Kind] = definition
	}
	for _, job := range state.Jobs {
		definition, ok := definitionsByKind[job.Kind]
		if !ok {
			return fmt.Errorf("migration job %s is not registered", job.Kind)
		}
		expected, err := CapabilitiesForQueues(root, registry, []string{definition.Queue})
		if err != nil {
			return err
		}
		expectedDigest, ok := reportDigest(expected, job.Kind, job.ProducerVersion)
		if !ok {
			return fmt.Errorf("registry queue %s lacks %s@%d", definition.Queue, job.Kind, job.ProducerVersion)
		}
		queueReports := byQueue[definition.Queue]
		if len(queueReports) == 0 {
			failures = append(failures, fmt.Sprintf("%s@%d: queue %s has no capability report", job.Kind, job.ProducerVersion, definition.Queue))
			continue
		}
		for index, report := range queueReports {
			if !reportSupports(report, job.Kind, job.ProducerVersion, expectedDigest) {
				failures = append(failures, fmt.Sprintf("%s@%d: queue %s report %d lacks support", job.Kind, job.ProducerVersion, definition.Queue, index+1))
			}
		}
	}
	if len(failures) > 0 {
		sort.Strings(failures)
		return fmt.Errorf("rollout capability check failed: %v", failures)
	}
	return nil
}

func reportSupports(report CapabilityReport, kind string, version int, expectedDigest string) bool {
	digest, ok := reportDigest(report, kind, version)
	return ok && digest == expectedDigest
}

func reportDigest(report CapabilityReport, kind string, version int) (string, bool) {
	for _, contract := range report.Contracts {
		if contract.Kind == kind && containsVersion(contract.Versions, version) {
			digest, ok := contract.SchemaDigests[strconv.Itoa(version)]
			return digest, ok
		}
	}
	return "", false
}

func isSHA256Digest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || value[:len("sha256:")] != "sha256:" {
		return false
	}
	for _, character := range value[len("sha256:"):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
