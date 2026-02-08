from pathlib import Path
from lrg.common.contract import load_contract, validate_record


def test_contract_accepts_minimal_valid_record():
    repo_root = Path(__file__).resolve().parents[1]
    v = load_contract(repo_root)

    record = {
        "tenant": {"tenant_id": "TENANT_DEMO", "environment": "dev"},
        "result": {
            "result_id": "R1",
            "patient_id": "P1",
            "effective_time": {"timestamp": "2026-01-01T10:00:00Z", "time_type": "result_reported"},
            "value": 5.1,
            "unit": "mmol/L"
        },
        "test": {"local_code": "GLU", "local_name": "Glucose"},
        "source": {"source_type": "CSV", "source_system_id": "demo"},
        "provenance": {
            "raw_pointer": "tenants/TENANT_DEMO/raw/CSV/demo.json",
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

    validate_record(v, record)
