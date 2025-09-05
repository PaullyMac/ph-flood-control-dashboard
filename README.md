Sumbong Sa Pangulo — Admin‑Ajax Replay Scraper

Overview
- I built this code because I wanted to make a visualization for a personal project exploring flood control projects in the Philippines. I was wondering how I could turn SumbongSaPangulo into a dashboard so I could find more insights about flood control projects in my province; hence I tried to make a script that could replay-scrape the website.
- I replay the site’s admin‑ajax endpoint to fetch project listings.
- I have provided my replay script for educational and documentation purposes; if others test or use it for other purposes that is outside my control — I uploaded it mainly for my own documentation.

Files
- `replay_with_playwright_seed.py`: Main script that paginates the endpoint and writes outputs:
  - `data/Flood Control Projects Raw.json`
  - `data/Flood Control Projects Raw.csv`
- `capture_seed_with_playwright.py`: Headful helper used during capture runs (optional)
- `requirements.txt`: Minimal Python dependencies.
- `data/`: Example outputs. The cleaned dataset is available on `data/Flood Control Projects Cleaned.csv`. Use it if you want to analyze the data for your own purposes.

Note: I used this strictly for learning and analysis. I did not overload or harm the site.

To-Do
- Still trying to figure out how to reliably capture the correct `project_type` from the site; at the moment the replay script uses a rule-based mapping and modal-derived values when available. Improving this extraction is a planned work item.

