"""
Replay the site's admin-ajax endpoint to collect project listings.

Outputs
- data/Flood Control Projects Raw.json
- data/Flood Control Projects Raw.csv
- data/admin_ajax_pages.json (debug: all raw JSON responses from admin-ajax)

Improvements
- Auto-detect ajaxUrl and nonce from the site (fallback to env NONCE if provided).
- Paginate until server says has_more=False (no need to set MAX_PAGES for full capture).
- Use application/x-www-form-urlencoded (simpler and more reliable than manual multipart).
- Larger per_page by default (200) to reduce page count.
- Optional fallback: if admin-ajax fails entirely, download the open GeoJSON dataset
    from https://github.com/rukku/sumbongsapangulo.ph-datasets to still produce outputs.
"""
from __future__ import annotations

from pathlib import Path
import os
import json
import time
import re

import requests
from bs4 import BeautifulSoup
import pandas as pd


OUT_JSON = Path('data/Flood Control Projects Raw.json')
OUT_CSV = Path('data/Flood Control Projects Raw.csv')
OUT_AJAX_DUMP = Path('data/admin_ajax_pages.json')

HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Origin': 'https://sumbongsapangulo.ph',
    'Referer': 'https://sumbongsapangulo.ph/flood-control-map/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
}


def _detect_nonce_and_ajax(session: requests.Session) -> tuple[str | None, str]:
    """Try to detect window.FC.nonce and ajaxUrl from the public pages.
    Returns (nonce, ajax_url). If nonce isn't found, returns (None, default_ajax_url).
    """
    default_ajax = 'https://sumbongsapangulo.ph/wp-admin/admin-ajax.php'
    # Try flood control map page first (more likely to expose FC)
    for url in (
        'https://sumbongsapangulo.ph/flood-control-map/',
        'https://sumbongsapangulo.ph/',
    ):
        try:
            r = session.get(url, timeout=30)
            r.raise_for_status()
            html = r.text
        except Exception:
            continue

        # Look for a serialized object like window.FC = { ajaxUrl: "...", nonce: "..." }
        # Very forgiving regex, handles quotes and whitespace
        m = re.search(
            r"(?:window\.)?FC\s*=\s*\{[^}]*?ajaxUrl\s*:\s*['\"](?P<ajax>[^'\"]+)['\"][^}]*?nonce\s*:\s*['\"](?P<nonce>[^'\"]+)['\"][^}]*?\}",
            html,
            flags=re.I | re.S,
        )
        if m:
            ajax = m.group('ajax') or default_ajax
            nonce = m.group('nonce') or None
            return (nonce, ajax)

        # Look for a nonce attribute in markup
        m2 = re.search(r"data-nonce=\"(?P<nonce>[0-9a-fA-F]+)\"", html)
        if m2:
            return (m2.group('nonce'), default_ajax)

        # As a last resort, sometimes localized scripts print "nonce":"..."
        m3 = re.search(r"\bnonce\b\s*[:=]\s*['\"](?P<nonce>[0-9a-fA-F]+)['\"]", html)
        if m3:
            return (m3.group('nonce'), default_ajax)

    return (None, default_ajax)


def _infer_project_type_from_text(text: str) -> str | None:
    """Lightweight heuristics to infer a project type from surrounding text.
    Returns a short normalized string or None.
    """
    if not text:
        return None
    txt = text.strip()
    # Try explicit labels first
    m = re.search(r"(?:Project Type|Type)[:\s]+([A-Za-z0-9 \-/&]+)", txt, flags=re.I)
    if m:
        return m.group(1).strip()

    # Keyword mapping
    mapping = {
        'riverbank': 'Riverbank Protection',
        'riprap': 'Riverbank Protection',
        'drainage': 'Drainage System',
        'drain': 'Drainage System',
        'dredg': 'Dredging',
        'retaining wall': 'Retaining Wall',
        'slope protection': 'Slope Protection',
        'reforestation': 'Reforestation',
        'spillway': 'Spillway',
        'flood control': 'Flood Control Structure',
        'gabion': 'Gabion/Stone Protection',
        'revetment': 'Revetment',
        'river restoration': 'River Restoration',
    }
    low = txt.lower()
    for k, v in mapping.items():
        if k in low:
            return v
    return None



def parse_rows_html_to_dicts(rows_html: str, seed_soup: BeautifulSoup | None = None, modal_map: dict | None = None) -> list[dict]:
    """Parse table-row HTML into list of dicts.
    If seed_soup is provided, attempt to enrich with details found in
    <template> blocks from a saved listing page.
    """
    soup = BeautifulSoup(rows_html or "", "html.parser")
    out: list[dict] = []
    for tr in soup.select("tr"):
        try:
            desc_a = tr.select_one("a.load-project-card")
            pid = None
            desc = None
            if desc_a:
                pid = desc_a.get("data-id") or desc_a.get("href")
                desc = desc_a.get_text(" ", strip=True)

            tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            location = tds[1] if len(tds) > 1 else None
            contractor = tds[2] if len(tds) > 2 else None
            cost = tds[3] if len(tds) > 3 else None
            completion = tds[4] if len(tds) > 4 else None

            report_btn = tr.select_one("button.open-report-form")
            report_contract_id = report_btn.get("data-contract_id") if report_btn else None

            row = {
                "project_id": pid,
                "description": desc,
                "location": location,
                "contractor": contractor,
                "cost": cost,
                "completion_date": completion,
                "report_contract_id": report_contract_id,
                "start_date": None,
                "project_type": None,
                "funding_year": None,
                "report_year": None,
                "region": None,
                "lat": None,
                "lng": None,
            }
            # Optional enrichment from modal_map (captured modals) or seed_soup templates (robust lookup)
            if pid and modal_map and str(pid) in modal_map:
                # modal_map stores inner HTML of the modal; parse it first
                try:
                    inner = modal_map.get(str(pid)) or ''
                    t_soup = BeautifulSoup(inner, 'html.parser')
                except Exception:
                    t_soup = None
                if t_soup:
                    sd_el = t_soup.select_one('.start-date, .start_date, #start-date')
                    if sd_el and sd_el.get_text(strip=True):
                        row['start_date'] = sd_el.get_text(' ', strip=True)
                    else:
                        m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{4}|\d{4}-\d{2}-\d{2})", inner or "")
                        if m:
                            row['start_date'] = m.group(1)

                    for sel in ('.type-of-work', '#type_of_work', '.project-type', '.type_of_work'):
                        el = t_soup.select_one(sel)
                        if el and el.get_text(strip=True):
                            row['project_type'] = el.get_text(' ', strip=True)
                            break

                    attr_year = re.search(r'data-year="?(?P<y>20\d{2})"?', inner or '')
                    if attr_year:
                        row['funding_year'] = attr_year.group('y')
                    else:
                        m = re.search(r"\b(20\d{2})\b", inner or "")
                        if m:
                            row['funding_year'] = m.group(1)

                    m_ry = re.search(r"\b(report[_\s-]?year|report year|report_year)[:\s]*([0-9]{4})\b", inner or "", re.I)
                    if m_ry:
                        row['report_year'] = m_ry.group(2)
                    else:
                        m = re.search(r"\b(20\d{2})\b", inner or "")
                        if m:
                            cand = m.group(1)
                            if cand != row.get('funding_year'):
                                row['report_year'] = cand

                    m_reg = re.search(r"Region\s+([IVX0-9A-Za-z \-]+)", inner or "", re.I)
                    if m_reg:
                        row['region'] = m_reg.group(1).strip()
                    else:
                        m_reg2 = re.search(r"Region[:\s]+([A-Za-z0-9 \-]+)", inner or "", re.I)
                        if m_reg2:
                            row['region'] = m_reg2.group(1).strip()

                    coord_match = re.search(r"\(?\s*(-?\d{1,3}\.\d+)\s*,\s*(-?\d{1,3}\.\d+)\s*\)?", inner or "")
                    if coord_match:
                        try:
                            lat_f = float(coord_match.group(1)); lng_f = float(coord_match.group(2))
                            if 4.0 <= lat_f <= 22.0 and 116.0 <= lng_f <= 127.0:
                                row['lat'] = lat_f
                                row['lng'] = lng_f
                        except Exception:
                            pass
            elif seed_soup is not None and pid:
                tmpl = None
                # Common template id patterns
                for pattern in (
                    rf"proj-card[-_]?{re.escape(str(pid))}$",
                    rf"project[-_]?{re.escape(str(pid))}$",
                    rf"card[-_]?{re.escape(str(pid))}$",
                ):
                    tmpl = seed_soup.find(id=re.compile(pattern))
                    if tmpl:
                        break
                # Look for <template> blocks containing the id or the id in their text
                if tmpl is None:
                    for t in seed_soup.find_all('template'):
                        if str(pid) in (t.get('id') or '') or str(pid) in (t.get_text() or ''):
                            tmpl = t
                            break
                # Look for elements that carry data-id attributes or data-project-id
                if tmpl is None:
                    sel = seed_soup.select_one(f"[data-id=\"{pid}\"]") or seed_soup.select_one(f"[data-project-id=\"{pid}\"]")
                    if sel is not None:
                        tmpl = sel
                # Look for script/template JSON blobs that mention the id
                if tmpl is None:
                    for s in seed_soup.find_all(['script','textarea']):
                        text = s.get_text() or ''
                        if str(pid) in text:
                            tmpl = s
                            break
                if tmpl:
                    # If we found an element container, prefer its inner HTML/text
                    if getattr(tmpl, 'name', '') == 'template':
                        inner = tmpl.get_text(' ')
                    else:
                        inner = tmpl.decode_contents() if hasattr(tmpl, 'decode_contents') else tmpl.get_text(' ')
                    t_soup = BeautifulSoup(inner, 'html.parser')
                    sd_el = t_soup.select_one('.start-date, .start_date, #start-date')
                    if sd_el and sd_el.get_text(strip=True):
                        row['start_date'] = sd_el.get_text(' ', strip=True)
                    else:
                        m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{4}|\d{4}-\d{2}-\d{2})", tmpl.get_text() or "")
                        if m:
                            row['start_date'] = m.group(1)

                    for sel in ('.type-of-work', '#type_of_work', '.project-type', '.type_of_work'):
                        el = t_soup.select_one(sel)
                        if el and el.get_text(strip=True):
                            row['project_type'] = el.get_text(' ', strip=True)
                            break

                    # If project_type still not found, attempt a lightweight inference
                    # from nearby text (template + listing description).
                    if not row.get('project_type'):
                        nearby = (tmpl.get_text() if tmpl else '') + ' ' + (desc or '')
                        try:
                            inferred = _infer_project_type_from_text(nearby)
                            if inferred:
                                row['project_type'] = inferred
                        except Exception:
                            pass

                    attr_year = re.search(r'data-year="?(?P<y>20\d{2})"?', str(tmpl))
                    if attr_year:
                        row['funding_year'] = attr_year.group('y')
                    else:
                        m = re.search(r"\b(20\d{2})\b", tmpl.get_text() or "")
                        if m:
                            row['funding_year'] = m.group(1)

                    m_ry = re.search(r"\b(report[_\s-]?year|report year|report_year)[:\s]*([0-9]{4})\b", tmpl.get_text() or "", re.I)
                    if m_ry:
                        row['report_year'] = m_ry.group(2)
                    else:
                        m = re.search(r"\b(20\d{2})\b", tmpl.get_text() or "")
                        if m:
                            cand = m.group(1)
                            if cand != row.get('funding_year'):
                                row['report_year'] = cand

                    m_reg = re.search(r"Region\s+([IVX0-9A-Za-z \-]+)", tmpl.get_text() or "", re.I)
                    if m_reg:
                        row['region'] = m_reg.group(1).strip()
                    else:
                        m_reg2 = re.search(r"Region[:\s]+([A-Za-z0-9 \-]+)", tmpl.get_text() or "", re.I)
                        if m_reg2:
                            row['region'] = m_reg2.group(1).strip()

                    coord_match = re.search(r"\(?\s*(-?\d{1,3}\.\d+)\s*,\s*(-?\d{1,3}\.\d+)\s*\)?", tmpl.get_text() or "")
                    if coord_match:
                        try:
                            lat_f = float(coord_match.group(1)); lng_f = float(coord_match.group(2))
                            if 4.0 <= lat_f <= 22.0 and 116.0 <= lng_f <= 127.0:
                                row['lat'] = lat_f
                                row['lng'] = lng_f
                        except Exception:
                            pass
            out.append(row)
        except Exception:
            continue
    return out


def main() -> int:
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = True
    used_insecure = False

    # If Playwright cookies exist, load them into the session (helps if site expects cookies)
    cookie_path = Path('data/playwright_cookies.json')
    if cookie_path.exists():
        try:
            cookies = json.loads(cookie_path.read_text(encoding='utf-8'))
            for c in cookies:
                name = c.get('name'); value = c.get('value')
                if not name:
                    continue
                domain = c.get('domain') or 'sumbongsapangulo.ph'
                path = c.get('path') or '/'
                session.cookies.set(name, value, domain=domain, path=path)
            print('Loaded cookies from', cookie_path)
        except Exception as e:
            print('Could not load cookies:', e)

    # Allow override via env, else auto-detect
    env_nonce = os.environ.get('NONCE')
    nonce: str | None = None
    ajax_url: str = 'https://sumbongsapangulo.ph/wp-admin/admin-ajax.php'
    # Highest priority: capture_meta.json produced by headful capture
    meta_path = Path('data/capture_meta.json')
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding='utf-8'))
            ajax_url = meta.get('ajax_url') or ajax_url
            nonce = meta.get('nonce') or None
            print('Using capture_meta nonce/ajax_url from', meta_path)
        except Exception:
            pass
    if env_nonce:
        nonce = env_nonce
        ajax_url = os.environ.get('AJAX_URL', ajax_url)
        print('Using NONCE from environment and ajax_url', ajax_url)
    if nonce is None:
        n2, a2 = _detect_nonce_and_ajax(session)
        nonce = n2
        ajax_url = a2 or ajax_url
        print('Detected ajax endpoint:', ajax_url, '| nonce:', (nonce or '(none)'))

    # Paging controls
    per_page = int(os.environ.get('PER_PAGE', '200'))  # higher throughput
    max_pages = int(os.environ.get('MAX_PAGES', '1000'))  # safety cap only

    # Load local seed HTML for enrichment automatically if present.
    # To disable automatic loading set environment USE_SEED=0
    seed_soup = None
    seed_path = Path('data/live_page.html')
    if seed_path.exists() and os.environ.get('USE_SEED', '1') != '0':
        try:
            seed_soup = BeautifulSoup(seed_path.read_text(encoding='utf-8'), 'html.parser')
            print('Loaded seed HTML for enrichment (auto-detected data/live_page.html)')
        except Exception as e:
            print('Could not parse seed HTML:', e)
    elif seed_path.exists():
        print('Found seed HTML at data/live_page.html but automatic loading is disabled via USE_SEED=0')

    all_rows: list[dict] = []
    seen: set[str] = set()
    # Load captured modal HTML map if available
    modal_map = {}
    modal_path = Path('data/project_modals.json')
    if modal_path.exists():
        try:
            modal_map = json.loads(modal_path.read_text(encoding='utf-8'))
            print('Loaded project modal map from', modal_path)
        except Exception as e:
            print('Could not read project_modals.json:', e)
    page = 1
    ajax_dump: list[dict] = []

    while page <= max_pages:
        # application/x-www-form-urlencoded payload
        form = {
            'action': 'filter_projects',
            'nonce': nonce or '',  # some servers accept empty/omitted nonce
            'page': str(page),
            'per_page': str(per_page),
            'region': '',
            'year': '',
            'type_of_work': '',
            'municipality': '',
            'search_itm': '',
        }
        try:
            r = session.post(ajax_url, data=form, timeout=60)
            r.raise_for_status()
            try:
                j = r.json()
            except Exception:
                print('Non-JSON response page', page)
                print(r.text[:800])
                break
        except requests.exceptions.SSLError as e:
            print('TLS verification failed:', e)
            if not used_insecure:
                print('Falling back to insecure mode (verify=False) for this run')
                session.verify = False
                used_insecure = True
                continue
            else:
                print('Already tried insecure. Aborting.')
                break
        except Exception as e:
            print('Request failed page', page, e)
            break

        success = j.get('success')
        data = j.get('data') or {}
        rows_html = data.get('rows') or ''
        has_more = bool(data.get('has_more'))

        # Keep a raw dump (without the heavy HTML if you want, but store as-is for traceability)
        try:
            ajax_dump.append(j)
        except Exception:
            pass

        parsed = parse_rows_html_to_dicts(rows_html, seed_soup=seed_soup, modal_map=modal_map or None)
        new = 0
        for row in parsed:
            pid = row.get('project_id')
            rcid = row.get('report_contract_id')
            # Composite key avoids collapsing distinct items that share a contract id
            key = f"pid:{pid}|rcid:{rcid}"
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(row)
            new += 1
        print(f'Page {page}: parsed {len(parsed)} rows, new {new}, has_more={has_more}')

        if not success:
            print('Server reported failure on page', page)
            break
        page += 1
        if not has_more:
            break
        time.sleep(0.2)

    # If nothing came back and fallback is allowed, try open dataset GeoJSON
    if not all_rows and os.environ.get('FALLBACK_GEOJSON', '1') == '1':
        try:
            url = 'https://raw.githubusercontent.com/rukku/sumbongsapangulo.ph-datasets/main/flood_control_projects.geojson'
            print('Admin-ajax returned no data. Attempting GeoJSON fallback from:', url)
            g = session.get(url, timeout=60)
            g.raise_for_status()
            gj = g.json()
            feats = gj.get('features') or []
            for f in feats:
                props = f.get('properties') or {}
                row = {
                    'project_id': props.get('ContractID') or props.get('GlobalID') or None,
                    'description': props.get('ProjectDescription') or None,
                    'location': (props.get('Municipality') or '') + ((', ' + props.get('Province')) if props.get('Province') else ''),
                    'contractor': props.get('Contractor') or None,
                    'cost': props.get('ContractCost') or None,
                    'completion_date': props.get('CompletionDate') or None,
                    'report_contract_id': props.get('ContractID') or None,
                    'start_date': props.get('StartDate') or None,
                    'project_type': props.get('TypeofWork') or None,
                    'funding_year': props.get('InfraYear') or None,
                    'report_year': props.get('InfraYear') or None,
                    'region': props.get('Region') or None,
                    'lat': None,
                    'lng': None,
                }
                geom = f.get('geometry') or {}
                coords = (geom.get('coordinates') or [None, None])
                if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                    row['lng'] = coords[0]
                    row['lat'] = coords[1]
                all_rows.append(row)
            print('Loaded from GeoJSON fallback:', len(all_rows), 'rows')
        except Exception as e:
            print('GeoJSON fallback failed:', e)

    # Dump raw ajax pages for inspection
    try:
        OUT_AJAX_DUMP.parent.mkdir(parents=True, exist_ok=True)
        OUT_AJAX_DUMP.write_text(json.dumps(ajax_dump, ensure_ascii=False, indent=2), encoding='utf-8')
        if ajax_dump:
            print('Saved raw admin-ajax pages ->', OUT_AJAX_DUMP)
    except Exception:
        pass

    # --- START: NORMALIZATION FOR REQUIRED JSON NULLS ---
    REQUIRED_NULL_KEYS = [ "start_date", "project_type", "funding_year", "report_year", "region", "lat", "lng", ]

    # If a cleaned CSV exists (from earlier runs), use it to enrich missing fields.
    cleaned_csv_path = Path('data/Flood Control Projects Cleaned.csv')
    cleaned_map: dict[str, dict] = {}
    if cleaned_csv_path.exists():
        try:
            dfc = pd.read_csv(cleaned_csv_path, dtype=str)
            for _, r in dfc.iterrows():
                pid = None
                rcid = None
                try:
                    pid = str(int(float(r.get('project_id')))) if pd.notna(r.get('project_id')) and str(r.get('project_id')).strip() != '' else None
                except Exception:
                    pid = str(r.get('project_id')).strip() if pd.notna(r.get('project_id')) and str(r.get('project_id')).strip() != '' else None
                if pd.notna(r.get('report_contract_id')):
                    rcid = str(r.get('report_contract_id')).strip()

                entry = {}
                for k in ("start_date", "project_type", "funding_year", "report_year", "region", "lat", "lng"):
                    v = r.get(k)
                    if pd.isna(v):
                        entry[k] = None
                    else:
                        entry[k] = v if v is not None else None

                # Sanitize region that looks like a description or project-type string.
                # If region contains verbs like 'Construction of' it's probably mis-mapped;
                # move it to project_type if project_type is empty and clear region.
                reg_val = entry.get('region')
                if isinstance(reg_val, str) and reg_val.strip():
                    reg_s = reg_val.strip()
                    if re.search(r'\b(construction|installation|rehab|mitigation|flood|drainage|slope|bank|protection|line canal|pump|booster)\b', reg_s, re.I) and not re.search(r'\b(region|ncr|car|caraga|iv-a|iv-b|iv|v|vi|vii|viii|ix|x|xi|xii|xiii|i|ii|iii|national capital)\b', reg_s, re.I):
                        if not entry.get('project_type'):
                            entry['project_type'] = reg_s
                        entry['region'] = None

                if pid:
                    cleaned_map.setdefault(pid, {}).update(entry)
                if rcid:
                    cleaned_map.setdefault(rcid, {}).update(entry)
            print(f'Loaded enrichment map from {cleaned_csv_path} ({len(cleaned_map)} keys)')
        except Exception as e:
            print('Could not read cleaned CSV for enrichment:', e)

    # Apply cleaned CSV enrichment to parsed rows (without overwriting existing non-null values)
    if cleaned_map:
        for row in all_rows:
            key = row.get('project_id') or row.get('report_contract_id')
            if not key:
                continue
            src = cleaned_map.get(str(key))
            if not src:
                continue
            for k in ("start_date", "project_type", "funding_year", "report_year", "region", "lat", "lng"):
                if (row.get(k) is None or (isinstance(row.get(k), str) and row.get(k).strip() == '')) and src.get(k) not in (None, ''):
                    row[k] = src.get(k)

    # Post-enrichment sanitization: if a row's region looks like a descriptive string
    # (e.g., starts with 'Construction of', 'Installation', etc.), move it into
    # project_type (if project_type empty) and clear region so location->region
    # fallback can fill a proper region.
    desc_region_re = re.compile(r"\b(construction|installation|rehab|mitigation|flood|drainage|slope|bank|protection|line canal|pump|booster)\b", re.I)
    region_keywords_re = re.compile(r"\b(region|ncr|caraga|car|iv-a|iv-b|iv|v|vi|vii|viii|ix|x|xi|xii|xiii|i|ii|iii|national capital)\b", re.I)
    for row in all_rows:
        reg = row.get('region')
        if isinstance(reg, str) and reg.strip():
            if desc_region_re.search(reg) and not region_keywords_re.search(reg):
                # Move descriptive region into project_type if empty
                if not row.get('project_type'):
                    row['project_type'] = reg.strip()
                row['region'] = None

    def _normalize_row_for_nulls(row):
        """Ensure required keys exist and convert empty/placeholder strings to None.
        Coerce lat/lng to float when possible, otherwise set to None.
        """
        # Ensure keys exist and normalize placeholder strings to None
        for k in REQUIRED_NULL_KEYS:
            if k not in row:
                row[k] = None
                continue
            v = row.get(k)
            if isinstance(v, str):
                if v.strip() == "" or v.strip().lower() in ("none", "null", "n/a", "-", "—"):
                    row[k] = None

        # Coerce lat/lng to float when possible; otherwise set to None
        for coord in ("lat", "lng"):
            val = row.get(coord)
            if val is None:
                continue
            if isinstance(val, (float, int)):
                continue
            try:
                row[coord] = float(str(val).strip())
            except Exception:
                row[coord] = None

        return row

    # Normalize all rows so the required keys are present and missing/placeholder values are None
    all_rows = [_normalize_row_for_nulls(r) for r in all_rows]
    # Location -> region fallback mapping for obvious cases
    loc_to_region = {
        'CITY OF MANILA': 'National Capital Region',
        'MANILA': 'National Capital Region',
        'QUEZON CITY': 'National Capital Region',
        'CALOOCAN': 'National Capital Region',
        'PASIG': 'National Capital Region',
        'MAKATI': 'National Capital Region',
    }
    for r in all_rows:
        if not r.get('region') and isinstance(r.get('location'), str):
            loc = r.get('location').strip().upper()
            for k, v in loc_to_region.items():
                if k in loc:
                    r['region'] = v
                    break
    # --- END: NORMALIZATION FOR REQUIRED JSON NULLS ---

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(all_rows, f, ensure_ascii=False, indent=2)
    print('Saved JSON ->', OUT_JSON)
    print(f'Total rows collected: {len(all_rows)} | Unique keys: {len(seen)}')
    if all_rows:
        df = pd.json_normalize(all_rows)
        df.to_csv(OUT_CSV, index=False)
        print('Saved CSV ->', OUT_CSV)
    else:
        print('No rows collected')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
