# Reference Range Context Model (v1)

## Purpose
A lab “normal range” is not a single number pair. It is a function of context (patient + specimen + method + lab policy).  
This model defines the **minimum context dimensions** required to select the correct reference interval and to justify any “H/L/critical” interpretation.

## Problem we’re solving
The same test can have different reference ranges across:
- labs (policy / population)
- instruments and analytic methods
- specimen type and collection conditions
- patient demographics (age/sex/pregnancy)
- units (mg/dL vs mmol/L)
- time and versioning (ranges change)

Without capturing context, downstream analytics and AI will produce false flags and misleading trends.

## Context dimensions (v1)

### A) Patient context
- `patient.age_at_observation_days` (integer)
- `patient.sex` (male | female | other | unknown)
- `patient.pregnancy_status` (pregnant | not_pregnant | unknown)
- `patient.pediatric_flag` (true/false) — derived from age thresholds

### B) Specimen & collection context
- `specimen.type` (serum | plasma | whole_blood | urine | csf | other | unknown)
- `specimen.collection_time` (ISO8601, optional)
- `specimen.fasting_status` (fasting | non_fasting | unknown)
- `specimen.body_site` (optional)
- `specimen.hemolysis_index` (optional)
- `specimen.lipemia_index` (optional)

### C) Method & instrument context
- `method.assay_method` (string; e.g., enzymatic, immunoassay)
- `method.instrument_id` (string, optional)
- `method.instrument_model` (string, optional)
- `method.kit_lot` (string, optional)
- `method.clia_complexity` (optional)

### D) Lab / organization context
- `org.performing_lab_id` (string)
- `org.lab_panel_id` (optional)
- `org.geography` (country/region optional)
- `org.policy_version` (string; reference interval set version)

### E) Result context
- `result.analyte_code` (canonical code; prefer LOINC when available)
- `result.unit` (UCUM recommended)
- `result.value_type` (numeric | ordinal | text)
- `result.observation_time` (ISO8601)

## Reference interval selection key (v1)
To select a reference interval, the minimum key is:

- analyte_code
- unit
- specimen.type
- patient.sex
- age bucket (derived from age_at_observation_days)
- performing_lab_id
- policy_version
- method.assay_method (when applicable)

## Versioning rules
- Reference intervals are **SCD Type 2**: never overwrite.
- Store validity: `valid_from_utc`, `valid_to_utc` (open-ended allowed).
- A “range set” must be reproducible: given an observation timestamp + context key, the selected interval is deterministic.

## Outputs enabled by this model
- Accurate normalization across labs and units
- Comparable longitudinal trends for a patient
- Drift detection when labs change ranges
- Auditability: “why was this flagged?”
