from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PagerDutyEnrichmentToggles:
    alerts: bool = True
    log_entries: bool = True
    notes: bool = True

    def enabled(self, dataset_key: str) -> bool:
        return {
            "incident-alerts": self.alerts,
            "incident-log-entries": self.log_entries,
            "incident-notes": self.notes,
        }.get(dataset_key, True)

    @classmethod
    def from_dataset_options(
        cls, dataset_key: str, options: Mapping[str, object]
    ) -> PagerDutyEnrichmentToggles:
        enabled = options.get("enabled")
        if not isinstance(enabled, bool):
            return cls()
        if dataset_key == "incident-alerts":
            return cls(alerts=enabled)
        if dataset_key == "incident-log-entries":
            return cls(log_entries=enabled)
        if dataset_key == "incident-notes":
            return cls(notes=enabled)
        return cls()
