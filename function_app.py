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
        from lrg.common.contract import load_contract, validate_record
        from lrg.ingest.fhir.observation_to_canonical import FhirObservation, to_canonical

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

        obs = FhirObservation(
            tenant_id=tenant_id,
            source_system_id=source_system_id,
            observation_json=obs_json
        )

        record: Dict[str, Any] = to_canonical(obs, environment=env, rules_version="v1", mapping_version="v1")

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
