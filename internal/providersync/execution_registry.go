package providersync

import "sort"

type ExecutionMode string

const (
	ExecutionPythonCompatibility ExecutionMode = "python_compatibility"
)

type ExecutionDescriptor struct {
	Provider             string
	Dataset              string
	Mode                 ExecutionMode
	CompatibilityAdapter string
	NativeShadow         bool
	RouteEnabled         bool
}

func ExecutionDescriptors(provider string) []ExecutionDescriptor {
	capabilities := Capabilities(provider)
	descriptors := make([]ExecutionDescriptor, 0, len(capabilities))
	for _, capability := range capabilities {
		adapter := "dev_health_ops.processors.dataset_adapters.run_dataset_unit"
		descriptors = append(descriptors, ExecutionDescriptor{
			Provider: capability.Provider, Dataset: capability.Dataset,
			Mode: ExecutionPythonCompatibility, CompatibilityAdapter: adapter,
			NativeShadow: isNativeReferenceWorkItemDataset(capability.Dataset),
			RouteEnabled: false,
		})
	}
	sort.Slice(descriptors, func(left, right int) bool {
		return descriptors[left].Dataset < descriptors[right].Dataset
	})
	return descriptors
}

type RouteSwitches struct {
	GitHub bool
	GitLab bool
}

func (switches RouteSwitches) Descriptor(provider, dataset string) (ExecutionDescriptor, bool) {
	for _, descriptor := range ExecutionDescriptors(provider) {
		if descriptor.Dataset != dataset {
			continue
		}
		switch provider {
		case "github":
			descriptor.RouteEnabled = switches.GitHub && descriptor.NativeShadow
		case "gitlab":
			descriptor.RouteEnabled = switches.GitLab && descriptor.NativeShadow
		default:
			return ExecutionDescriptor{}, false
		}
		return descriptor, true
	}
	return ExecutionDescriptor{}, false
}
