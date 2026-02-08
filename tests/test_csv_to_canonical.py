from pathlib import Path

from lrg.common.contract import load_contract, validate_record
from lrg.ingest.csv.row_to_canonical import CsvRow, to_canonical


def test_csv_row_to_canonical_validates_against_contract():
    repo_root = Path(__file__).resolve().parents[1]
    v = load_contract(repo_root)

    row = CsvRow(
        tenant_id="TENANT_DEMO",
        patient_id="P1",
        result_id="R100",
        test_code="GLU",
        test_name="Glucose",
        value=5.6,
        unit="mmol/L",
        timestamp_iso="2026-02-01T10:00:00Z",
        source_system_id="demo_csv",
        performing_lab_id="LAB_A",
        ref_low=3.9,
        ref_high=5.5,
        reported_flag="H"
    )

    rec = to_canonical(row=row, environment="dev", rules_version="v1", mapping_version="v1")
    validate_record(v, rec)
