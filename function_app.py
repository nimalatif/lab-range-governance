import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import azure.functions as func

# Make src/ importable on Azure (repo uses src/ layout)
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


def _utc_path_date() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}/{now.month:02d}/{now.day:02d}"


def _datalake_client(account: str):
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient

    url = f"https://{account}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=url, credential=DefaultAzureCredential())


@app.route(route="ingest/csv", methods=["POST"])
def ingest_csv(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Import inside handler so app can register routes even if something is misconfigured
        from lrg.common.contract import load_contract, validate_record
        from lrg.ingest.csv.row_to_canonical import CsvRow, to_canonical

        payload = req.get_json()
        if not isinstance(payload, dict):
            raise ValueError("Body must be a JSON object")

        env = os.getenv("ENV", "dev")
        tenant_id = os.getenv("TENANT_ID", "").strip()
        if not tenant_id:
            raise ValueError("TENANT_ID app setting is required")

        row = CsvRow(
            tenant_id=tenant_id,
            patient_id=str(payload["patient_id"]),
            result_id=str(payload["result_id"]),
            test_code=str(payload["test_code"]),
            test_name=str(payload["test_name"]),
            value=payload["value"],
            unit=str(payload["unit"]),
            timestamp_iso=str(payload["timestamp_iso"]),
            source_system_id=str(payload.get("source_system_id", "api")),
            performing_lab_id=payload.get("performing_lab_id"),
            ref_low=payload.get("ref_low"),
            ref_high=payload.get("ref_high"),
            ref_text=payload.get("ref_text"),
            reported_flag=payload.get("reported_flag"),
        )

        record: Dict[str, Any] = to_canonical(row=row, environment=env, rules_version="v1", mapping_version="v1")

        validator = load_contract(Path(__file__).resolve().parent)
        validate_record(validator, record)

        account = os.environ["DATALAKE_ACCOUNT"]
        container = os.getenv("STORAGE_CANONICAL_CONTAINER", "canonical")

        dl = _datalake_client(account)
        fs = dl.get_file_system_client(container)

        date_path = _utc_path_date()
        tenant_prefix = f"tenants/{tenant_id}/lab_results/{date_path}"
        file_path = f"{tenant_prefix}/{record['result']['result_id']}.json"

        file_client = fs.get_file_client(file_path)
        body = json.dumps(record, ensure_ascii=False).encode("utf-8")
        file_client.upload_data(body, overwrite=True)

        return func.HttpResponse(
            json.dumps({"status": "ok", "written_to": f"{container}/{file_path}"}),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            status_code=400,
            mimetype="application/json",
        )
@app.route(route="ingest/fhir", methods=["POST"])
def ingest_fhir(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from datetime import datetime, timezone

        from lrg.common.contract import load_contract, validate_record
        from lrg.ingest.fhir.observation_to_canonical import FhirObservation, to_canonical
        from lrg.governance.runbook_loader import load_reference_intervals_from_adls
        from lrg.governance.reference_intervals import select_interval

        payload = req.get_json()
        if not isinstance(payload, dict):
            raise ValueError("Body must be a JSON object")

        env = os.getenv("ENV", "dev")
        tenant_id = os.getenv("TENANT_ID", "").strip()
        if not tenant_id:
            raise ValueError("TENANT_ID app setting is required")

        source_system_id = str(payload.get("source_system_id", "api"))
        obs_json = payload.get("observation")
        if not isinstance(obs_json, dict):
            raise ValueError("Missing 'observation' object (FHIR Observation JSON)")

        # Canonicalize (contract-bound)
        obs = FhirObservation(
            tenant_id=tenant_id,
            source_system_id=source_system_id,
            observation_json=obs_json
        )
        record: Dict[str, Any] = to_canonical(
            obs, environment=env, rules_version="v1", mapping_version="v1"
        )

        validator = load_contract(Path(__file__).resolve().parent)
        validate_record(validator, record)

        # Write canonical
        account = os.environ["DATALAKE_ACCOUNT"]
        canonical_container = os.getenv("STORAGE_CANONICAL_CONTAINER", "canonical")

        dl = _datalake_client(account)
        fs_canon = dl.get_file_system_client(canonical_container)

        date_path = _utc_path_date()
        tenant_prefix = f"tenants/{tenant_id}/lab_results/{date_path}"
        canonical_file_path = f"{tenant_prefix}/{record['result']['result_id']}.json"

        canon_file_client = fs_canon.get_file_client(canonical_file_path)
        canon_body = json.dumps(record, ensure_ascii=False).encode("utf-8")
        canon_file_client.upload_data(canon_body, overwrite=True)

        # --- Derived interpretation (semantic / AI-ready layer) ---
        # Defaults (can be overridden by payload.context.*)
        ctx = payload.get("context") if isinstance(payload.get("context"), dict) else {}
        specimen_type = str(ctx.get("specimen_type", "serum"))
        age_bucket = str(ctx.get("age_bucket", "adult"))
        sex = str(ctx.get("sex", "male"))
        performing_lab_id = str(ctx.get("performing_lab_id", "LAB_A"))
        policy_version = str(ctx.get("policy_version", "v1"))

        # Extract LOINC from Observation.code (best-effort)
        loinc_code = None
        code_obj = obs_json.get("code") if isinstance(obs_json.get("code"), dict) else {}
        codings = code_obj.get("coding") if isinstance(code_obj.get("coding"), list) else []
        for c in codings:
            if not isinstance(c, dict):
                continue
            system = str(c.get("system", "")).strip()
            code = str(c.get("code", "")).strip()
            if code and (system == "http://loinc.org" or system.endswith("/loinc")):
                loinc_code = code
                break
        if not loinc_code:
            # fallback: accept first coding code if present
            for c in codings:
                if isinstance(c, dict) and str(c.get("code", "")).strip():
                    loinc_code = str(c.get("code")).strip()
                    break

        analyte_code = f"loinc:{loinc_code}" if loinc_code else None

        # Observation time
        observation_time_utc = (
            str(obs_json.get("effectiveDateTime")).strip()
            if str(obs_json.get("effectiveDateTime", "")).strip()
            else str(record.get("result", {}).get("timestamp_iso", "")).strip()
        )
        if not observation_time_utc:
            observation_time_utc = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

        # Value & unit from canonical
        value = record.get("result", {}).get("value", None)
        unit = record.get("result", {}).get("unit", None)

        derived_container = os.getenv("STORAGE_DERIVED_CONTAINER", "derived")
        fs_derived = dl.get_file_system_client(derived_container)

        interpretation = {
            "computed_flag": "U",
            "interval_id": None,
            "computed_by": "lrg.governance.v1",
            "computed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }

        if analyte_code and unit is not None and value is not None:
            # Load runbook + resolve interval
            runbooks_container = os.getenv("STORAGE_RUNBOOKS_CONTAINER", "runbooks")
            intervals = load_reference_intervals_from_adls(
                account=account,
                container=runbooks_container,
                tenant_id=tenant_id,
            )

            it = select_interval(
                intervals,
                analyte_code=str(analyte_code),
                unit=str(unit),
                specimen_type=str(specimen_type),
                age_bucket=str(age_bucket),
                sex=str(sex),
                performing_lab_id=str(performing_lab_id),
                policy_version=str(policy_version),
                observation_time_utc=str(observation_time_utc),
            )

            if it and isinstance(it.range, dict) and it.range.get("type") == "numeric":
                low = it.range.get("low")
                high = it.range.get("high")
                if isinstance(low, (int, float)) and isinstance(high, (int, float)) and isinstance(value, (int, float)):
                    if value < low:
                        interpretation["computed_flag"] = "L"
                    elif value > high:
                        interpretation["computed_flag"] = "H"
                    else:
                        interpretation["computed_flag"] = "N"
                    interpretation["interval_id"] = it.interval_id

        derived_record = {
            "tenant_id": tenant_id,
            "result_id": record["result"]["result_id"],
            "analyte_code": analyte_code,
            "unit": unit,
            "value": value,
            "observation_time_utc": observation_time_utc,
            "context": {
                "specimen_type": specimen_type,
                "age_bucket": age_bucket,
                "sex": sex,
                "performing_lab_id": performing_lab_id,
                "policy_version": policy_version,
            },
            "interpretation": interpretation,
            "source": {
                "canonical_path": f"{canonical_container}/{canonical_file_path}",
            },
        }

        derived_prefix = f"tenants/{tenant_id}/lab_results_interpreted/{date_path}"
        derived_file_path = f"{derived_prefix}/{record['result']['result_id']}.json"
        der_file_client = fs_derived.get_file_client(derived_file_path)
        der_body = json.dumps(derived_record, ensure_ascii=False).encode("utf-8")
        der_file_client.upload_data(der_body, overwrite=True)

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "written_to": f"{canonical_container}/{canonical_file_path}",
                    "derived_written_to": f"{derived_container}/{derived_file_path}",
                    "computed_flag": interpretation["computed_flag"],
                    "interval_id": interpretation["interval_id"],
                }
            ),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            status_code=400,
            mimetype="application/json",
        )
@app.route(route="governance/interval/resolve", methods=["POST"])
def resolve_interval(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from lrg.governance.runbook_loader import load_reference_intervals_from_adls
        from lrg.governance.reference_intervals import select_interval

        payload = req.get_json()
        if not isinstance(payload, dict):
            raise ValueError("Body must be a JSON object")

        tenant_id = os.getenv("TENANT_ID", "").strip()
        if not tenant_id:
            raise ValueError("TENANT_ID app setting is required")

        account = os.environ["DATALAKE_ACCOUNT"]
        runbooks_container = os.getenv("STORAGE_RUNBOOKS_CONTAINER", "runbooks")

        intervals = load_reference_intervals_from_adls(
            account=account,
            container=runbooks_container,
            tenant_id=tenant_id,
        )

        it = select_interval(
            intervals,
            analyte_code=str(payload["analyte_code"]),
            unit=str(payload["unit"]),
            specimen_type=str(payload["specimen_type"]),
            age_bucket=str(payload["age_bucket"]),
            sex=str(payload["sex"]),
            performing_lab_id=str(payload["performing_lab_id"]),
            policy_version=str(payload["policy_version"]),
            observation_time_utc=str(payload["observation_time_utc"]),
        )

        if not it:
            return func.HttpResponse(
                json.dumps({"status": "not_found"}),
                status_code=404,
                mimetype="application/json",
            )

        return func.HttpResponse(
            json.dumps({"status": "ok", "interval_id": it.interval_id, "range": it.range}),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            status_code=400,
            mimetype="application/json",
        )
