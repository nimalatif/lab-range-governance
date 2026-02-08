# Multi-Tenant Boundary Rules (v1)

## 1) Tenant is a first-class identifier
Every request, ingestion event, and stored record must include:
- tenant_id (required)
- workspace_id (optional but recommended)
- environment (dev/test/prod)

## 2) Default isolation is strict
- No data, logs, mappings, catalogs, or derived outputs can be read across tenants by default.
- Cross-tenant sharing is only allowed via explicit, versioned “global baseline” artifacts.

## 3) Partitioning strategy (applies everywhere)
All persisted artifacts are partitioned by tenant_id, including:
- raw inbound payloads
- canonical lab records
- derived indicators and flags
- mapping tables (local code → canonical concept)
- reference range catalogs (if used)
- audit logs and change logs

## 4) Configuration model
- Runtime scope is provided via environment variables / app settings (never hardcoded).
- Secrets are never stored in repo; use secret stores (later phases).
- Tenant-specific overrides live in `configs/tenants/<TENANT>.yaml` (no PHI).

## 5) Auditing and traceability are tenant-scoped
- Audit trails must record who/what changed mappings/rules within the tenant scope.
- Rule versions and mapping versions must be recorded on every derived output.

## 6) Safety rule: no PHI in git
- No patient identifiers, names, dates of birth, or raw HL7 messages are ever committed.
- Test fixtures must be synthetic or heavily de-identified.
