from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class FhirObservation:
    tenant_id: str
    source_system_id: str
    observation_json: Dict[str, Any]  # raw FHIR Observation resource


def to_canonical_stub(obs: FhirObservation, environment: str) -> Dict[str, Any]:
    """
    v1 stub: returns a minimally-valid canonical record skeleton.
    Next step will map:
      - subject (patient)
      - code (test identity + LOINC)
      - effective[x] / issued
      - valueQuantity / valueString
      - referenceRange
      - interpretation
    """
    return {
        "tenant": {"tenant_id": obs.tenant_id, "environment": environment},
        "result": {
            "result_id": "TBD",
            "patient_id": "TBD",
            "effective_time": {"timestamp": "1970-01-01T00:00:00Z", "time_type": "unknown"},
            "value": "TBD",
            "unit": "TBD"
        },
        "test": {"local_code": "TBD", "local_name": "TBD"},
        "source": {"source_type": "FHIR", "source_system_id": obs.source_system_id},
        "provenance": {
            "raw_pointer": f"tenants/{obs.tenant_id}/raw/FHIR/{obs.source_system_id}/TBD.json",
            "field_lineage": {
                "value": "RECEIVED",
                "unit": "RECEIVED",
                "reference_range": "MISSING",
                "reported_flag": "MISSING"
            },
            "mapping_version": "v1",
            "rules_version": "v1"
        }
    }
