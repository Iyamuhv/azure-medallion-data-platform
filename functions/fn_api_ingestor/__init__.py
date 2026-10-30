"""
fn-api-ingestor — Azure Function (HTTP trigger)
Fetches paginated GraphQL data and writes JSON to ADLS Bronze.
Called by ADF pipeline via Azure Function Activity.

Pattern: cursor-based pagination with watermark filtering.
Returns max_last_modified so ADF can store the watermark.
"""

import json
import logging
import os
from datetime import datetime, timezone

import azure.functions as func
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
import requests

# ── Configuration ─────────────────────────────────────────────
KEY_VAULT_URL       = os.environ["KEY_VAULT_URL"]
API_TOKEN_SECRET    = os.environ["API_TOKEN_SECRET_NAME"]
AZURE_CLIENT_ID     = os.environ.get("AZURE_CLIENT_ID")
STORAGE_ACCOUNT     = os.environ["STORAGE_ACCOUNT_NAME"]
CONTAINER_NAME      = "bronze"
API_BASE_URL        = os.environ["API_BASE_URL"]

# Entity configuration — maps entity names to GraphQL fields
ENTITY_CONFIG = {
    "resources": {
        "folder": "resources",
        "gql_entity": "resources",
        "watermark_field": "LastModifiedDate",
        "fields": (
            "UID Name IsActive Status RegionId UserId "
            "CreatedDate LastModifiedDate "
        ),
    },
    "jobs": {
        "folder": "jobs",
        "gql_entity": "jobs",
        "watermark_field": "LastModifiedDate",
        "fields": (
            "UID Name JobStatus JobType "
            "ActualStart ActualEnd EstimatedStart EstimatedEnd "
            "RegionId AccountId "
            "CreatedDate LastModifiedDate "
        ),
    },
    "job_allocations": {
        "folder": "job_allocations",
        "gql_entity": "jobAllocations",
        "watermark_field": "LastModifiedDate",
        "fields": (
            "UID JobId ResourceId Status "
            "Start End Duration "
            "CreatedDate LastModifiedDate "
        ),
    },
}


def get_api_token() -> str:
    credential = ManagedIdentityCredential(client_id=AZURE_CLIENT_ID) if AZURE_CLIENT_ID                  else ManagedIdentityCredential()
    client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
    return client.get_secret(API_TOKEN_SECRET).value


def build_graphql_query(gql_entity: str, fields: str, cursor,
                         page_size: int, watermark_from: str = None,
                         extra_filter: str = None) -> str:
    cursor_arg = f', after: "{cursor}"' if cursor else ""
    field_list = "\n        ".join(fields.split())

    if watermark_from:
        wm_clean   = watermark_from.replace("Z", "").split(".")[0] + ".000Z"
        wm_filter  = f"LastModifiedDate > {wm_clean}"
        filter_str = f"({extra_filter}) AND {wm_filter}" if extra_filter else wm_filter
        filter_arg = f', filter: "{filter_str}"\'
    else:
        filter_arg = ""

    order_arg = ', orderBy: "LastModifiedDate ASC, UID ASC"\'
    return f"""
{{
  {gql_entity}(limit: {page_size}{cursor_arg}{filter_arg}{order_arg}) {{
    pageInfo {{ hasNextPage endCursor }}
    totalCount
    edges {{
      node {{
        {field_list}
      }}
    }}
  }}
}}
"""


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("fn-api-ingestor triggered")

    body           = req.get_json()
    entity         = body["entity"]
    watermark_from = body.get("watermark_from", "1900-01-01T00:00:00Z")
    page_size      = int(body.get("page_size", 100))
    cursor         = body.get("cursor")
    page_num       = int(body.get("page_num", 1))

    if entity not in ENTITY_CONFIG:
        return func.HttpResponse(
            json.dumps({"status": "error",
                        "error_message": f"Unknown entity: {entity}"}),
            status_code=200, mimetype="application/json"
        )

    config   = ENTITY_CONFIG[entity]
    run_ts   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    api_token = get_api_token()

    # Build and execute GraphQL query
    query = build_graphql_query(
        config["gql_entity"], config["fields"],
        cursor, page_size, watermark_from,
        config.get("extra_filter")
    )

    resp = requests.post(
        f"{API_BASE_URL}/graphql/graphql",
        headers={"Authorization": f"Bearer {api_token}",
                 "Content-Type": "application/json"},
        json={"query": query}, timeout=60
    )
    resp.raise_for_status()
    gql_data    = resp.json()
    entity_data = gql_data["data"][config["gql_entity"]]
    edges       = entity_data.get("edges", [])
    page_info   = entity_data.get("pageInfo", {})
    records     = [e["node"] for e in edges]
    has_next    = page_info.get("hasNextPage", False)
    end_cursor  = page_info.get("endCursor")

    # Write to ADLS Bronze
    now        = datetime.now(timezone.utc)
    blob_path  = (f"api/{config['folder']}/"
                  f"{now.year:04d}/{now.month:02d}/{now.day:02d}/"
                  f"{config['folder']}_p{page_num:04d}_{run_ts}.json")

    payload = {
        "metadata": {"entity": entity, "watermark_from": watermark_from,
                     "page_num": page_num, "run_ts": run_ts},
        "record_count": len(records),
        "records": records
    }

    credential   = ManagedIdentityCredential(client_id=AZURE_CLIENT_ID) if AZURE_CLIENT_ID                    else ManagedIdentityCredential()
    blob_service = BlobServiceClient(
        f"https://{STORAGE_ACCOUNT}.blob.core.windows.net",
        credential=credential
    )
    blob_client = blob_service.get_blob_client(container=CONTAINER_NAME, blob=blob_path)
    blob_client.upload_blob(json.dumps(payload, default=str), overwrite=True)

    max_wm = max(
        (r.get(config["watermark_field"], "") for r in records),
        default=watermark_from
    )

    return func.HttpResponse(
        json.dumps({
            "has_next_page":     has_next,
            "end_cursor":        end_cursor,
            "rows_written":      len(records),
            "output_path":       blob_path,
            "max_last_modified": max_wm,
            "status":            "success"
        }),
        status_code=200, mimetype="application/json"
    )
