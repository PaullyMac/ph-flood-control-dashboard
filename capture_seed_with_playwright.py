"""
Small, fast headful capture for sumbongsapangulo.ph.

Headful Playwright capture: collect listing HTML, admin-ajax pages, and project modals.
This script favors speed: shorter sleeps, larger per_page, and aggressive "Load more" clicking.
"""

from pathlib import Path
import time
import json
import re

from playwright.sync_api import sync_playwright

OUT_HTML = Path("data/live_page.html")
OUT_COOKIES = Path("data/playwright_cookies.json")
OUT_RESPS = Path("data/admin_ajax_pages.json")
OUT_MODALS = Path("data/project_modals.json")


def capture_all():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context()
        page = ctx.new_page()

        page.goto("https://sumbongsapangulo.ph/", timeout=60_000)
        print("Opened browser - solve captcha if present. Waiting 2s...")
        time.sleep(2)

        # Try to read ajax url + nonce if available via a serializable object
        fc = page.evaluate("() => ({ajaxUrl: window.FC?.ajaxUrl, nonce: window.FC?.nonce})")
        ajax_url = fc.get("ajaxUrl") or "https://sumbongsapangulo.ph/wp-admin/admin-ajax.php"
        nonce = fc.get("nonce") or ""

        # Click "Load more" repeatedly to let the client render all cards
        clicks = 0
        max_clicks = 200
        while clicks < max_clicks:
            btn = page.query_selector(
                "button#load-more-projects, .fcp-loadmore-wrap button.map-btn, .fcp-loadmore-wrap button"
            )
            if not btn:
                break
            try:
                btn.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                btn.click(force=True)
            except Exception:
                try:
                    box = btn.bounding_box()
                    if box:
                        page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                except Exception:
                    break
            # Short pause for client to load additional cards
            time.sleep(0.18)
            # Optional: quick idle wait to settle network
            try:
                page.wait_for_load_state("networkidle", timeout=2_000)
            except Exception:
                pass
            clicks += 1
        print("Load-more clicks:", clicks)

        # Fetch admin-ajax pages via browser context (bigger per_page, shorter waits)
        per_page = 200
        page_no = 1
        has_more = True
        responses = []
        aggregated_rows = ""

        while has_more and page_no <= 1000:
            form = {
                "action": "filter_projects",
                "nonce": nonce,
                "page": str(page_no),
                "per_page": str(per_page),
                "region": "",
                "year": "",
                "type_of_work": "",
                "municipality": "",
                "search_itm": "",
            }
            try:
                resp = page.request.post(ajax_url, data=form, timeout=10_000)
                try:
                    j = resp.json()
                except Exception:
                    txt = resp.text()
                    try:
                        j = json.loads(txt)
                    except Exception:
                        j = {"success": False, "data": {"rows": ""}}
                        m = re.search(r'"rows"\s*:\s*"(.*?)"\s*,\s*"has_more"', txt, re.S)
                        if m:
                            rows_html = m.group(1).encode("utf-8").decode("unicode_escape")
                            j["data"]["rows"] = rows_html
            except Exception as e:
                print("Request failed for page", page_no, e)
                break

            responses.append(j)
            data = j.get("data") or {}
            rows_html = data.get("rows") or ""
            if rows_html:
                aggregated_rows += (
                    f"\n<!-- page {page_no} rows start -->\n{rows_html}\n<!-- page {page_no} rows end -->\n"
                )
            has_more = bool(data.get("has_more"))
            print(f"fetched page {page_no}: rows_len={len(rows_html)} has_more={has_more}")
            page_no += 1
            time.sleep(0.08)

        # Save the current page content and admin-ajax responses
        html = page.content()
        OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
        final_html = html + '\n<!-- injected-ajax-rows -->\n<div id="injected-ajax-rows">' + aggregated_rows + "</div>\n"
        OUT_HTML.write_text(final_html, encoding="utf-8")
        OUT_COOKIES.write_text(json.dumps(ctx.cookies(), indent=2), encoding="utf-8")
        OUT_RESPS.write_text(json.dumps(responses, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Saved HTML, cookies, and admin-ajax responses")

        # Capture modals from rendered cards (prefer client-side cards already on the page)
        modals = {}
        try:
            # Inject aggregated rows as a fallback template source
            page.evaluate(
                "(html) => { const d = document.createElement('div'); d.id='__injected_rows_for_modal_capture'; d.innerHTML = html; document.body.appendChild(d); }",
                aggregated_rows,
            )
        except Exception:
            pass
        time.sleep(0.12)

        locator = page.locator(
            "a.load-project-card, button.load-project-card, .load-project-card, .project-card a, .project-card button"
        )
        try:
            count = locator.count()
        except Exception:
            count = 0
        print("Found", count, "cards")

        for i in range(count):
            try:
                el = locator.nth(i)
                try:
                    el.scroll_into_view_if_needed()
                except Exception:
                    pass

                pid = None
                try:
                    pid = el.get_attribute("data-id") or el.get_attribute("data-project-id") or el.get_attribute("href")
                except Exception:
                    pid = None

                try:
                    el.click(force=True)
                except Exception:
                    try:
                        box = el.bounding_box()
                        if box:
                            page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                    except Exception:
                        pass

                try:
                    page.wait_for_selector(
                        "#project-modal .project-card, .project-modal .project-card, #project-modal-body .project-card",
                        timeout=1_200,
                    )
                except Exception:
                    continue

                modal = (
                    page.query_selector("#project-modal")
                    or page.query_selector(".project-modal")
                    or page.query_selector("#project-modal-body")
                )
                if modal:
                    modals[str(pid) if pid else str(len(modals) + 1)] = modal.inner_html()

                try:
                    close_btn = page.query_selector(".close-project-modal")
                    if close_btn:
                        close_btn.click()
                        time.sleep(0.05)
                except Exception:
                    pass
            except Exception:
                continue

        try:
            OUT_MODALS.write_text(json.dumps(modals, ensure_ascii=False, indent=2), encoding="utf-8")
            print("Saved modal captures ->", OUT_MODALS)
        except Exception:
            pass

        browser.close()


if __name__ == "__main__":
    capture_all()