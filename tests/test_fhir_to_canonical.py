from pathlib import Path

from lrg.common.contract import load_contract, validate_record
from lrg.ingest.fhir.observation_to_canonical import FhirObservation, to_canonical


def test_fhir_observation_to_canonical_validates():
    repo_root = Path(__file__).resolve().parents[1]
    v = load_contract(repo_root)

    obs_json = {
        "resourceType": "Observation",
        "id": "obs-1",
        "status": "final",
        "code": {
            "coding": [{"system": "http://loinc.org", "code": "2345-7", "display": "Glucose [Moles/volume] in Serum or Plasma"}],
            "text": "Glucose"
        },
        "subject": {"reference": "Patient/p1"},
        "effectiveDateTime": "2026-02-01T10:00:00Z",
        "valueQuantity": {"value": 5.6, "unit": "mmol/L", "code": "mmol/L"},
        "referenceRange": [{"low": {"value": 3.9, "unit": "mmol/L"}, "high": {"value": 5.5, "unit": "mmol/L"}}],
        "interpretation": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation", "code": "H"}]}]
    }

    obs = FhirObservation(tenant_id="TENANT_DEMO", source_system_id="demo_fhir", observation_json=obs_json)
    rec = to_canonical(obs, environment="dev")
    validate_record(v, rec)
