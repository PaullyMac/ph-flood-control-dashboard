Legacy (archived) capture scripts

Overview
- These scripts were used to scrape via WordPress admin-ajax and assist with headful capture. The project now uses the ArcGIS Feature Service directly via `fetch_tabulator_table.py`.

Files
- `capture_seed_with_playwright.py`: Headful Playwright capture of listing HTML, cookies, admin-ajax responses, and modals.
- `replay_with_playwright_seed.py`: Replays admin-ajax to build `data/Flood Control Projects Raw.*` from server HTML responses.

Status
- Archived on 2025-09-06. Not maintained. They may break if the site changes.

Running (optional)
1) Install Playwright and browser binaries (Windows PowerShell):
   
   python -m pip install --user playwright
   python -m playwright install chromium

2) Run the scripts:
   
   python archive/capture_seed_with_playwright.py
   python archive/replay_with_playwright_seed.py

Outputs
- data/playwright_cookies.json (cookies)
- data/admin_ajax_pages.json (raw admin-ajax responses)
- data/live_page.html (listing page with injected rows)
- data/project_modals.json (captured modal HTML)

Preferred approach
- Use `fetch_tabulator_table.py` at the repo root to query the ArcGIS service and write Raw/Full outputs with complete lat/lng.
