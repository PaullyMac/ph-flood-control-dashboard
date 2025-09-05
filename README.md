Sumbong Sa Pangulo — Flood Control Projects Data Fetch

Overview
- This repo fetches and exports the flood control projects (9,855) shown on the Sumbong Sa Pangulo map.
- Recommended: Query the ArcGIS Feature Service behind the on-page Tabulator table. This returns the full set and all attributes reliably.
- Optional (legacy): Replay WordPress admin‑ajax to fetch listings. Kept for reference.

Files
- `fetch_tabulator_table.py` (recommended): Queries the ArcGIS Feature Service behind the Tabulator table, paginates through all features, and writes:
  - `data/Flood Control Projects Raw.json`
  - `data/Flood Control Projects Raw.csv`
  - `data/Flood Control Projects Full.json` (all ArcGIS attributes + lat/lng)
  - `data/Flood Control Projects Full.csv` (all ArcGIS attributes + lat/lng)
- Archived legacy scripts (now in `archive/`):
  - `archive/replay_with_playwright_seed.py`: Adminajax replay. Writes `data/admin_ajax_pages.json` and the same Raw JSON/CSV.
  - `archive/capture_seed_with_playwright.py`: Headful helper during capture runs.
  - See `archive/README.md` for details.
- `requirements.txt`: Minimal Python dependencies.
- `data/`: Example outputs. The dataset is available on `data/Flood Control Projects Raw.csv`.

Notes
- I created this script to learn more about our province's flood control projects and to use it for analysis and visualization.
- If you want to analyze the data for your own purposes, use the cleaned dataset at `data/Flood Control Projects Raw.csv`.

Fallback dataset
- If the admin-ajax endpoint fails entirely, the script can optionally load the open dataset from https://github.com/rukku/sumbongsapangulo.ph-datasets and still emit JSON/CSV for analysis.
- I used this fallback when I was developing the initial version of the script.

Credits
- Thanks to the author of https://github.com/rukku/sumbongsapangulo.ph-datasets for publishing the open dataset used as a fallback.

ArcGIS table fetch (recommended)
- The flood control map page (of the Sumbong Sa Pangulo) uses an ArcGIS Feature Service to power its Tabulator table.
- This script fetches all 9,855 features, maps key columns, and backfills missing coordinates by querying geometry so all rows have `lat`/`lng`.
- It also writes "Full" exports containing every ArcGIS attribute in addition to `lat`/`lng`.

Column mapping (mapped outputs)
- `project_type` := ArcGIS `TypeofWork`
- `cost` := ArcGIS `ContractCost_String` or `ABC_String` (fallback to numeric `ContractCost`/`ABC`)
- `funding_year` and `report_year` := ArcGIS `FundingYear` (or `InfraYear`/`infra_year` when present)
- `lat`/`lng` := ArcGIS `Latitude`/`Longitude` if present; else backfilled from feature geometry (WGS84)


