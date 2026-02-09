from lrg.governance.reference_intervals import parse_jsonl, select_interval


def test_select_interval_picks_matching_context():
    jsonl = "\n".join(
        [
            '{"interval_id":"a","analyte_code":"loinc:2345-7","unit":"mg/dL","specimen_type":"serum","age_bucket":"adult","sex":"male","performing_lab_id":"LAB_A","policy_version":"v1","valid_from_utc":"2026-01-01T00:00:00Z","valid_to_utc":null,"range":{"type":"numeric","low":70,"high":99},"metadata":{}}',
            '{"interval_id":"b","analyte_code":"loinc:2345-7","unit":"mg/dL","specimen_type":"serum","age_bucket":"adult","sex":"female","performing_lab_id":"LAB_A","policy_version":"v1","valid_from_utc":"2026-01-01T00:00:00Z","valid_to_utc":null,"range":{"type":"numeric","low":70,"high":99},"metadata":{}}',
        ]
    )
    intervals = parse_jsonl(jsonl)

    it = select_interval(
        intervals,
        analyte_code="loinc:2345-7",
        unit="mg/dL",
        specimen_type="serum",
        age_bucket="adult",
        sex="female",
        performing_lab_id="LAB_A",
        policy_version="v1",
        observation_time_utc="2026-02-01T10:00:00Z",
    )

    assert it is not None
    assert it.interval_id == "b"
