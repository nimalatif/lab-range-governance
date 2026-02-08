# Authority and Governance Rules (v1)

## 1) Result value & unit are authoritative as received
- Store the reported result value and unit exactly as provided by the source.
- If we store any converted value/unit, it must be explicitly labeled as DERIVED, with the conversion rule/version recorded.

## 2) Reference range is authoritative per-result
- Persist the reference range exactly as reported with that specific result event.
- Never overwrite the reported range with a “standard” or “catalog” range.

## 3) Abnormal flag is authoritative only as a reported flag
- If the source provides an abnormal flag (e.g., High/Low/Normal), store it as REPORTED.
- Any recalculated flag must be stored separately as DERIVED and must record the rule version used.

## 4) Derived interpretation is optional, controlled, and reversible
- Derived indicators (e.g., position within range, value vs ULN/LLN) may be computed only when inputs are sufficient (value + unit + usable reference range).
- If inputs are incomplete or ambiguous, the system must not guess and must instead state “Not enough information” and record what is missing.

## 5) Catalog ranges are optional and must never replace reported ranges
- A range catalog may exist only for validation, drift detection, and analytics context.
- Catalog ranges must be scoped by lab/source, method (if known), unit, and demographic applicability (e.g., age/sex/pregnancy) and must be versioned.

## 6) Provenance is mandatory for every field
- Every canonical record must identify whether each key field is RECEIVED or DERIVED.
- Record source system identifiers, timestamps, and mapping/rule versions (including unit conversions and derived indicator logic).

## 7) Tenant boundaries apply to authority and governance artifacts
- Rules, mappings, catalogs, and logs must be tenant-scoped by default.
- Cross-tenant sharing must be explicit, reviewed, and versioned (e.g., “global baseline” with tenant overrides).
