from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

import time
import csv
import io
from datetime import datetime, timezone, timedelta

import requests

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def _enqueue_job(conf: dict, start_ts: int, end_ts: int, attribute_ids: list[str]) -> str:
    """Submit an Intercom reporting export job and return the job identifier."""
    url = "https://api.intercom.io/export/reporting_data/enqueue"
    headers = {
        "Authorization": f"Bearer {conf['access_token']}",
        "Content-Type": "application/json",
        "Intercom-Version": "Unstable",
    }

    payload = {
        "dataset_id": conf["dataset_id"],
        "attribute_ids": attribute_ids,
        "start_time": start_ts,
        "end_time": end_ts,
    }

    log.info(f"Enqueuing job for window {start_ts} - {end_ts}")
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    job_id = resp.json()["job_identifier"]
    log.info(f"Job enqueued. job_identifier={job_id}")
    return job_id


def _poll_job(conf: dict, job_id: str, poll_interval: int = 10, max_tries: int = 60) -> dict:
    """Poll Intercom until the export job has completed and return job metadata."""
    url = f"https://api.intercom.io/export/reporting_data/{job_id}"
    headers = {
        "Authorization": f"Bearer {conf['access_token']}",
        "Intercom-Version": "Unstable",
    }
    params = {
        "app_id": conf["app_id"],
        "client_id": conf["client_id"],
        "job_identifier": job_id,
    }

    for attempt in range(max_tries):
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        # A 404 can be transient while the job is being created. Retry.
        if resp.status_code == 404:
            time.sleep(poll_interval)
            continue
        resp.raise_for_status()
        info = resp.json()
        status = info.get("status", "")
        log.info(f"Poll attempt {attempt + 1}: status={status}")
        if status == "complete":
            return info
        if status in {"failed", "error"}:
            raise RuntimeError(f"Export job {job_id} failed with status '{status}'")
        time.sleep(poll_interval)

    raise TimeoutError(f"Export job {job_id} did not complete after {max_tries * poll_interval} seconds")


def _download_csv(conf: dict, job_id: str, download_url: str) -> str:
    """Download the CSV export file and return its text content."""
    headers = {
        "Authorization": f"Bearer {conf['access_token']}",
        "Intercom-Version": "Unstable",
        "Accept": "application/octet-stream",
    }
    params = {"app_id": conf["app_id"], "job_identifier": job_id}
    log.info("Downloading exported CSV data")
    resp = requests.get(download_url, headers=headers, params=params, timeout=300)
    resp.raise_for_status()
    return resp.content.decode("utf-8")


# -----------------------------------------------------------------------------
# Main update method
# -----------------------------------------------------------------------------

def update(configuration: dict, state: dict):
    """Main entrypoint called by Fivetran to sync data."""

    # --- Configuration ---------------------------------------------------------------------------------------------
    # Required configuration keys (validated implicitly when accessed):
    #   access_token   : Intercom Personal Access Token or OAuth Bearer token
    #   app_id         : Intercom App ID (for status/download endpoints)
    #   client_id      : Client ID as required by status/download endpoints
    #   dataset_id     : Intercom dataset to export (e.g., "conversation")
    #   attribute_ids  : List of attribute ids for the dataset
    # Optional configuration keys:
    #   window_seconds : Size of each incremental sync window in seconds (default 3600)
    #   initial_start_time : Unix epoch seconds to start the initial historical sync (default now - 24h)

    # Helper to coerce numeric strings from configuration into ints
    def _as_int(val, default):
        try:
            return int(val)
        except (TypeError, ValueError):
            return default

    window_seconds: int = _as_int(configuration.get("window_seconds"), 3600)

    if state:
        # Continue from where the previous sync ended
        start_ts = state["last_end_time"] + 1
    else:
        start_ts = _as_int(configuration.get("initial_start_time"), None)
        if not start_ts:
            # default to 24h ago to avoid massive historical exports unintentionally
            start_ts = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp())

    # Calculate end of window but cap at current time
    end_ts = min(start_ts + window_seconds, int(datetime.now(timezone.utc).timestamp()))

    # If the computed window has no new data (start >= end), nothing to do
    if start_ts >= end_ts:
        log.info("No new time window to sync; skipping")
        return

    # Parse attribute_ids which may come as CSV string or list
    raw_attrs = configuration.get("attribute_ids")
    if isinstance(raw_attrs, str):
        attribute_ids = [s.strip() for s in raw_attrs.split(',') if s.strip()]
    else:
        attribute_ids = raw_attrs or []

    # -------------------------------------------------------------------------
    # Intercom export workflow
    # -------------------------------------------------------------------------
    job_id = _enqueue_job(configuration, start_ts, end_ts, attribute_ids)
    job_info = _poll_job(configuration, job_id)
    csv_text = _download_csv(configuration, job_id, job_info["download_url"])

    csv_reader = csv.DictReader(io.StringIO(csv_text))
    row_count = 0
    for row in csv_reader:
        # Convert empty strings to None for better type inference
        cleaned = {k: (v if v != "" else None) for k, v in row.items()}
        row_count += 1
        yield op.upsert(table=configuration["dataset_id"], data=cleaned)

    log.info(f"Upserted {row_count} rows for window {start_ts}-{end_ts}")

    # -------------------------------------------------------------------------
    # Save state checkpoint
    # -------------------------------------------------------------------------
    new_state = {"last_end_time": end_ts}
    yield op.checkpoint(state=new_state)


# -----------------------------------------------------------------------------
# Connector initialization
# -----------------------------------------------------------------------------
connector = Connector(update=update)

# Allow local debugging with `python connector.py`
if __name__ == "__main__":
    connector.debug()
