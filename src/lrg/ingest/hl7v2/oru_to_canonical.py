from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class Hl7OruMessage:
    tenant_id: str
    source_system_id: str
    raw_hl7: str  # store raw message elsewhere; keep pointer in canonical


def to_canonical_stub(msg: Hl7OruMessage, environment: str) -> Dict[str, Any]:
    """
    v1 stub: returns a minimally-valid canonical record skeleton.
    Next step will parse OBX (value/unit), OBR (test), PID (patient), and reference ranges.
    """
    return {
        "tenant": {"tenant_id": msg.tenant_id, "environment": environment},
        "result": {
            "result_id": "TBD",
            "patient_id": "TBD",
            "effective_time": {"timestamp": "1970-01-01T00:00:00Z", "time_type": "unknown"},
            "value": "TBD",
            "unit": "TBD"
        },
        "test": {"local_code": "TBD", "local_name": "TBD"},
        "source": {"source_type": "HL7V2_ORU", "source_system_id": msg.source_system_id},
        "provenance": {
            "raw_pointer": f"tenants/{msg.tenant_id}/raw/HL7V2_ORU/{msg.source_system_id}/TBD.hl7",
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
