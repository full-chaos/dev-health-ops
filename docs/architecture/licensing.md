# License Validation Architecture

> **Status**: DECIDED  
> **Model**: Single-repo with runtime feature gating (GitLab model)  
> **Related**: [ADR-001](adr/001-enterprise-edition.md), [Monetization Strategy](../monetization-strategy.md)

## Overview

Dev Health uses a **single-repository model** with all code visible under the BSL license. Premium features are gated at runtime by license validation, not by separate packages or repositories.

This follows the GitLab model:
- All source code is visible (transparency, community contributions)
- Revenue comes from license keys + support contracts
- Self-hosted and SaaS use the same license validation

## Why Single-Repo (GitLab Model)

| Approach | Pros | Cons |
|----------|------|------|
| **Separate repos** (abandoned) | Code hidden, harder to pirate | Maintenance overhead, trust issues, complex CI |
| **Single repo** (chosen) | Transparency, simpler ops, community can contribute to EE | Code visible (mitigated by BSL) |

**Decision rationale:**
- Users see exactly what they're paying for
- Community can contribute to premium features
- One repo = one CI = one deployment
- Trust > obscurity (determined users can always circumvent)

## License Format

### Ed25519-Signed JWT

Based on research of GitLab, Coder, and tldraw licensing patterns.

```
<base64-payload>.<base64-signature>
```

**Why Ed25519:**
- Smaller keys than RSA (32 bytes vs 2048 bits)
- Faster verification
- Modern standard (used by Coder, tldraw)
- No padding attacks (unlike RSA PKCS#1)

### Payload Schema

```json
{
  "iss": "fullchaos.studio",
  "sub": "org_abc123",
  "iat": 1706745600,
  "exp": 1738281600,
  "tier": "team",
  "features": {
    "sso": true,
    "audit": true,
    "api_access": true,
    "investment_view": false
  },
  "limits": {
    "users": 50,
    "repos": -1,
    "api_rate": 1000
  },
  "deployment_ids": ["self-hosted"],
  "grace_days": 14
}
```

| Field | Type | Description |
|-------|------|-------------|
| `iss` | string | Issuer (always `fullchaos.studio`) |
| `sub` | string | Organization ID |
| `iat` | int | Issued at (Unix timestamp) |
| `exp` | int | Expiration (Unix timestamp) |
| `tier` | string | `community`, `team`, `enterprise` |
| `features` | object | Feature flags (boolean map) |
| `limits` | object | Resource limits (-1 = unlimited) |
| `deployment_ids` | array | Optional instance binding |
| `grace_days` | int | Days after expiry before hard cutoff |

### Signature

```python
import nacl.signing

private_key = nacl.signing.SigningKey(seed)
signature = private_key.sign(payload_bytes).signature
license_key = base64url(payload) + "." + base64url(signature)
```

## Validation Flow

### Self-Hosted

```
┌─────────────────────────────────────────────────────────────┐
│                      Self-Hosted Instance                    │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │ License Key  │───▶│  Validator   │───▶│ Entitlements │  │
│  │ (env var or  │    │              │    │              │  │
│  │  settings)   │    │ • Verify sig │    │ • features   │  │
│  │              │    │ • Check exp  │    │ • limits     │  │
│  │              │    │ • Grace      │    │ • tier       │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│                                                              │
│  Public key HARDCODED in binary (offline validation)        │
└─────────────────────────────────────────────────────────────┘
```

**Critical**: Public key must be embedded in the application binary, not fetched from a server. This enables fully offline validation.

### SaaS

```
┌─────────────────────────────────────────────────────────────┐
│                      SaaS Platform                           │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │ Organization │───▶│ License DB   │───▶│ Entitlements │  │
│  │ (from JWT)   │    │              │    │              │  │
│  │              │    │ • Lookup org │    │ • features   │  │
│  │              │    │ • Check tier │    │ • limits     │  │
│  │              │    │ • Billing    │    │ • tier       │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│                                                              │
│  License managed via admin UI / billing integration         │
└─────────────────────────────────────────────────────────────┘
```

## Implementation

### Validator Service

```python
# src/dev_health_ops/licensing/validator.py

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
import base64
import json
import nacl.signing
import nacl.exceptions

# CRITICAL: Hardcode public key in binary for offline validation
PUBLIC_KEY = bytes.fromhex(
    "a1b2c3d4e5f6..."  # 32-byte Ed25519 public key
)

@dataclass
class Entitlements:
    tier: str
    features: dict[str, bool]
    limits: dict[str, int]
    expires_at: datetime
    grace_expires_at: datetime
    is_valid: bool
    is_grace_period: bool

class LicenseValidator:
    def __init__(self, license_key: Optional[str] = None):
        self.license_key = license_key
        self._entitlements: Optional[Entitlements] = None
    
    def validate(self) -> Entitlements:
        if not self.license_key:
            return self._community_entitlements()
        
        try:
            payload_b64, sig_b64 = self.license_key.split(".")
            payload_bytes = base64.urlsafe_b64decode(payload_b64 + "==")
            signature = base64.urlsafe_b64decode(sig_b64 + "==")
            
            # Verify Ed25519 signature
            verify_key = nacl.signing.VerifyKey(PUBLIC_KEY)
            verify_key.verify(payload_bytes, signature)
            
            payload = json.loads(payload_bytes)
            return self._parse_payload(payload)
            
        except (ValueError, nacl.exceptions.BadSignature, json.JSONDecodeError):
            return self._community_entitlements()
    
    def _parse_payload(self, payload: dict) -> Entitlements:
        expires_at = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
        grace_days = payload.get("grace_days", 14)
        grace_expires_at = expires_at + timedelta(days=grace_days)
        now = datetime.now(timezone.utc)
        
        is_expired = now > expires_at
        is_grace = is_expired and now <= grace_expires_at
        is_hard_expired = now > grace_expires_at
        
        return Entitlements(
            tier=payload["tier"],
            features=payload.get("features", {}),
            limits=payload.get("limits", {}),
            expires_at=expires_at,
            grace_expires_at=grace_expires_at,
            is_valid=not is_hard_expired,
            is_grace_period=is_grace,
        )
    
    def _community_entitlements(self) -> Entitlements:
        return Entitlements(
            tier="community",
            features={
                "basic_metrics": True,
                "github_sync": True,
                "gitlab_sync": True,
                "jira_sync": True,
            },
            limits={
                "users": 3,
                "repos": 5,
                "api_rate": 100,
            },
            expires_at=datetime.max.replace(tzinfo=timezone.utc),
            grace_expires_at=datetime.max.replace(tzinfo=timezone.utc),
            is_valid=True,
            is_grace_period=False,
        )

# Singleton for application use
_validator: Optional[LicenseValidator] = None

def get_entitlements() -> Entitlements:
    global _validator
    if _validator is None:
        from dev_health_ops.settings import get_settings
        _validator = LicenseValidator(get_settings().license_key)
    return _validator.validate()
```

### Feature Gating

```python
# src/dev_health_ops/licensing/gates.py

from functools import wraps
from typing import Callable
from dev_health_ops.licensing.validator import get_entitlements

class FeatureNotLicensed(Exception):
    def __init__(self, feature: str, tier_required: str = "Team"):
        self.feature = feature
        self.tier_required = tier_required
        super().__init__(
            f"Feature '{feature}' requires {tier_required} license. "
            "Visit https://fullchaos.studio/pricing"
        )

def require_feature(feature: str):
    """Decorator to gate API endpoints by feature flag."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            entitlements = get_entitlements()
            if not entitlements.features.get(feature, False):
                raise FeatureNotLicensed(feature)
            return await func(*args, **kwargs)
        return wrapper
    return decorator

def check_limit(limit_name: str, current_value: int) -> bool:
    """Check if a resource limit has been reached."""
    entitlements = get_entitlements()
    limit = entitlements.limits.get(limit_name, 0)
    if limit == -1:  # Unlimited
        return True
    return current_value < limit
```

### Usage Examples

```python
# API endpoint gating
@router.get("/api/investment")
@require_feature("investment_view")
async def get_investment_view():
    # Only available with Team+ license
    ...

# Limit enforcement
async def create_user(org_id: str, user_data: dict):
    current_users = await count_users(org_id)
    if not check_limit("users", current_users):
        raise LimitExceeded("users", get_entitlements().limits["users"])
    ...

# Conditional UI rendering (frontend)
const { features, tier } = useEntitlements();
{features.sso && <SSOSettings />}
{tier === "enterprise" && <AuditLog />}
```

## Grace Periods

Based on Formbricks and Coder patterns:

| State | Behavior |
|-------|----------|
| **Valid** | Full access to licensed features |
| **Grace period** (soft expiry) | Full access + warning banner |
| **Expired** (hard expiry) | Downgrade to community tier |

**Grace period defaults:**
- Team: 14 days
- Enterprise: 30 days

During grace period:
- All features remain accessible
- Warning banner shown in UI
- Email notifications sent (if configured)

## License Generation (Private Service)

> **IMPORTANT**: License generation is NOT in this repository.

License generation requires the Ed25519 **private key** and must be in a separate, private service. Putting it in the public repo would allow anyone to mint valid licenses.

```
┌─────────────────────────────────────────────────────────────┐
│           License Service (PRIVATE - Hosted)                 │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐                       │
│  │  License     │───▶│  Billing     │                       │
│  │  Generator   │    │  Integration │                       │
│  │              │    │              │                       │
│  │  • PRIVATE   │    │  • Stripe    │                       │
│  │    key       │    │  • Portal    │                       │
│  └──────────────┘    └──────────────┘                       │
│                                                              │
│  Private key NEVER in public repos                          │
└─────────────────────────────────────────────────────────────┘
```

**What lives where:**
- **This repo (public)**: Validation with PUBLIC key, feature gating
- **Private service**: Generation with PRIVATE key, billing integration

## Research References

Patterns drawn from:

| Company | Approach | Key Insight |
|---------|----------|-------------|
| **GitLab** | RSA-signed JWT, `gitlab-license` gem | Public key in binary, same format for all deployments |
| **Coder** | Ed25519-signed JWT | `deployment_ids` for instance binding |
| **tldraw** | ECDSA P-256 JWT | `hosts` array for domain binding |
| **Formbricks** | Hybrid phone-home | 24hr cache, 3-day grace on API failure |

## Security Considerations

1. **Public key hardcoded**: Prevents key substitution attacks
2. **Ed25519 vs RSA**: No padding oracle attacks, smaller keys
3. **Signature over payload**: Prevents tampering
4. **No phone-home required**: Works fully offline
5. **Grace periods**: Prevents hard lockout on renewal delays

**What we explicitly don't do:**
- Code obfuscation (security through obscurity)
- License server dependency for validation
- Hardware fingerprinting (deployment_ids are optional)

## Migration from No License

1. **Community users**: Continue with community tier (no change)
2. **Team users**: Receive license key via email after purchase
3. **Enterprise users**: License key + support onboarding

## Related Documents

- [ADR-001: Enterprise Edition Decisions](adr/001-enterprise-edition.md)
- [Monetization Strategy](../monetization-strategy.md)
- [Enterprise Overview](enterprise-overview.md)
