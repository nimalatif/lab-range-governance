import os
from typing import List

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from lrg.governance.reference_intervals import ReferenceInterval, parse_jsonl


def load_reference_intervals_from_adls(
    *,
    account: str,
    container: str,
    tenant_id: str,
    path: str = "reference_intervals/v1/reference_intervals.jsonl",
) -> List[ReferenceInterval]:
    url = f"https://{account}.dfs.core.windows.net"
    dl = DataLakeServiceClient(account_url=url, credential=DefaultAzureCredential())
    fs = dl.get_file_system_client(container)

    full_path = f"tenants/{tenant_id}/{path}"
    file_client = fs.get_file_client(full_path)
    download = file_client.download_file()
    text = download.readall().decode("utf-8")
    return parse_jsonl(text)
