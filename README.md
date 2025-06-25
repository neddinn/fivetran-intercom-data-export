# Intercom Reporting Export – Fivetran Connector SDK

This repository contains a **Fivetran Connector SDK** implementation that extracts Intercom "Reporting Data Export" datasets and delivers them directly to your Fivetran destination.

The connector automates the otherwise-manual flow:

1. Enqueue an export job for a given dataset/time-window
2. Poll Intercom until the job is `complete`
3. Download the gzipped CSV
4. Parse each row and `UPSERT` it into the destination table
5. Save a cursor so the next sync continues from the last window

No CSVs are stored locally or uploaded – data streams row-by-row to Fivetran just like any native connector.

---
## Contents
* [`connector.py`](connector.py)  – main connector logic
* [`requirements.txt`](requirements.txt) – runtime dependency list (`requests` only)
* `README.md` – you are here ✨

---
## Prerequisites
* Python 3.9 – 3.12
* A Fivetran account with **Connector SDK** access
* An Intercom Personal Access Token **or** OAuth token that has Reporting Export scope

---
## Local quick-start
```bash
# 1  Create & activate a venv (recommended)
python -m venv myenv
source myenv/bin/activate

# 2  Install the SDK and project deps
pip install fivetran-connector-sdk -r requirements.txt

# 3  Create configuration.json (all values **must** be strings)
cat > configuration.json <<'EOF'
{
  "access_token"      : "sk_live_xxxxxxxxxxxxxxxxxxxxx",
  "app_id"            : "your_app_id",
  "client_id"         : "your_client_id",

  "dataset_id"        : "conversation",
  "attribute_ids"     : "conversation.id,conversation.first_user_conversation_part_created_at",

  "initial_start_time": "1717480000",            # optional – defaults to 24h ago
  "window_seconds"    : "3600"                   # optional – default 1 hour
}
EOF

# 4  Run the connector locally
fivetran debug --configuration configuration.json
```
The Local Tester spins up an in-process DuckDB (`files/warehouse.db`) so you can inspect the actual rows that would arrive in your warehouse.

---
## Deploying to Fivetran
```bash
# Base64-encoded "<api_key>:<api_secret>" from your Fivetran profile
export FIVETRAN_API_KEY="xxxxxxxxxxxxxxxx=="

fivetran deploy \
  --api-key     "$FIVETRAN_API_KEY" \
  --destination "MyWarehouse" \
  --connection  "intercom_reporting_export" \
  --configuration configuration.json
```
* After deployment the connection appears in the Fivetran UI (Paused).  Flip the toggle or press **Sync Now** to start the initial sync.
* All subsequent syncs run automatically on Fivetran's schedule.

---
## Configuration options
| Key | Type (string) | Required | Description |
|-----|---------------|----------|-------------|
| `access_token` | ✓ | Intercom PAT / OAuth token |
| `app_id` | ✓ | Workspace App ID – required by status/download endpoints |
| `client_id` | ✓ | Client ID – required by status/download endpoints |
| `dataset_id` | ✓ | One of Intercom's reporting export datasets (e.g. `conversation`) |
| `attribute_ids` | ✓ | **Either** a comma-separated string **or** JSON array of attribute IDs to include |
| `initial_start_time` | ✕ | Unix epoch (seconds) where the first sync starts. Defaults to "24 h ago". |
| `window_seconds` | ✕ | Size of each incremental window. Default `3600` (1 h) |

All values must be strings because of Connector-SDK validation.

---
## Schema & Primary Keys
The connector currently relies on Fivetran's automatic schema inference.  For strict schemas / primary keys add a `schema()` function and update the initialization:
```python
connector = Connector(update=update, schema=schema)
```
See the SDK docs for details.

---
## Enhancements you may want
* **gzip decompression** – Intercom streams a gzipped CSV.  The current implementation already handles it; adjust buffer sizes for very large exports.
* **Regional hosts** – If your workspace is in EU or AU use `api.eu.intercom.io` or `api.au.intercom.io` (make host configurable).
* **Retry / back-off** – `_poll_job` could switch to exponential back-off.
* **Schema()** – Declare types & primary keys for downstream performance.
