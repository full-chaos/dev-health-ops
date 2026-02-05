# Normalization Helpers

This document describes the canonical locations for normalization and transformation utilities across the codebase.

## Module Locations

### Core Utilities (`utils/normalization.py`)

General-purpose normalization utilities used across multiple layers.

| Function | Purpose |
|----------|---------|
| `work_unit_id(nodes)` | Generate stable SHA256 ID from node tuples |
| `rollup_subcategories_to_themes(...)` | Deterministic theme aggregation from subcategories |
| `ensure_full_subcategory_vector(...)` | Ensure all subcategories present in vector |
| `normalize_scores(scores, keys)` | Normalize dict values to sum to 1.0 |
| `evidence_quality_band(value)` | Map float to quality band (high/moderate/low/very_low) |
| `clamp(value, low, high)` | Constrain value to range |

### API Numeric Transforms (`api/utils/numeric.py`)

Safe numeric conversions for API service layer.

| Function | Purpose |
|----------|---------|
| `safe_float(value, default=0.0)` | Convert to float, return default for invalid/non-finite |
| `safe_optional_float(value)` | Convert to float or None |
| `safe_transform(transform, value)` | Apply transform, ensure valid result |
| `delta_pct(current, previous)` | Calculate percentage change |

### Identity Normalization (`api/utils/identity_aliases.py`)

Identity and alias resolution utilities.

| Function | Purpose |
|----------|---------|
| `build_reverse_alias_map(aliases)` | Build reverse lookup from alias dict |
| `normalize_alias(alias)` | Normalize alias string for comparison |

### Provider Normalization (`providers/normalize_common.py`)

Shared utilities for provider data normalization.

| Function | Purpose |
|----------|---------|
| `parse_datetime(value)` | Parse datetime from various formats |
| `extract_priority(value)` | Extract numeric priority from provider data |

## Usage Guidelines

1. **Layer Boundaries**: Use utilities appropriate to the layer:
   - Core business logic: `utils/normalization.py`
   - API services: `api/utils/numeric.py`
   - Provider ingestion: `providers/normalize_common.py`

2. **No Duplication**: Before creating a local helper, check if one exists in these modules.

3. **Adding New Helpers**: Place new utilities in the appropriate module based on scope:
   - Cross-layer: `utils/normalization.py`
   - API-specific: `api/utils/`
   - Provider-specific: `providers/normalize_common.py`
