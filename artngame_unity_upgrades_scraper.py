import asyncio
import json
import csv
import os
import sys
from random import uniform
from itertools import islice
from playwright.async_api import async_playwright

# ------------------ Config ------------------
DEFAULT_PUBLISHER = ("ARTnGAME", "https://assetstore.unity.com/publishers/6503?pageSize=96")
STORAGE_STATE = "storage_state.json"
OUTPUT_TXT = "unity_upgrade_discounts.txt"
OUTPUT_CSV = "unity_upgrade_discounts.csv"
PROGRESS_FILE = "processed_assets.txt"

BATCH_SIZE = 10
DELAY_BETWEEN_BATCHES = 10  # seconds
PAGE_SIZE = 96
# --------------------------------------------

# ------------------ Helpers -----------------
async def human_wait(min_sec=2, max_sec=5, reason=""):
    wait_time = uniform(min_sec, max_sec)
    print(f"[wait] {reason} sleeping for {wait_time:.1f}s")
    await asyncio.sleep(wait_time)

def save_txt_line(text):
    with open(OUTPUT_TXT, "a", encoding="utf-8") as f:
        f.write(text + "\n")

def save_csv_row(row):
    with open(OUTPUT_CSV, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

def mark_processed(link):
    with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
        f.write(link + "\n")

def load_processed():
    if not os.path.exists(PROGRESS_FILE):
        return set()
    with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
        return set(l.strip() for l in f.readlines())

def wrap_hyperlink(url, label=None):
    label = label or url
    return f'=HYPERLINK("{url}","{label}")'

def extract_offer_rating(data, found_prices):
    if isinstance(data, dict):
        for k, v in data.items():
            if k == "offerRating" and isinstance(v, dict):
                orig = v.get("originalPrice")
                final = v.get("finalPrice")
                if orig or final:
                    found_prices.append((orig, final))
            else:
                extract_offer_rating(v, found_prices)
    elif isinstance(data, list):
        for item in data:
            extract_offer_rating(item, found_prices)

def extract_upgrade_from(data, upgrades):
    if isinstance(data, dict):
        for k, v in data.items():
            if k in ("upgradeFrom", "upgradableFrom"):
                if isinstance(v, list):
                    for upg in v:
                        if isinstance(upg, dict):
                            name = upg.get("name") or upg.get("title") or "Unknown"
                            url = upg.get("url")
                            upgrades.append((name, f"https://assetstore.unity.com{url}" if url else ""))
                elif isinstance(v, dict):
                    name = v.get("name") or v.get("title") or "Unknown"
                    url = v.get("url")
                    upgrades.append((name, f"https://assetstore.unity.com{url}" if url else ""))
            else:
                extract_upgrade_from(v, upgrades)
    elif isinstance(data, list):
        for item in data:
            extract_upgrade_from(item, upgrades)

def extract_asset_name(data, names):
    if isinstance(data, dict):
        for k, v in data.items():
            if k == "results" and isinstance(v, list):
                for result in v:
                    if isinstance(result, dict) and "name" in result:
                        names.append(result["name"])
            elif k in ("product", "item") and isinstance(v, dict):
                if "name" in v:
                    names.append(v["name"])
            else:
                extract_asset_name(v, names)
    elif isinstance(data, list):
        for item in data:
            extract_asset_name(item, names)

def batched(iterable, n):
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch

# ------------------ Crawl all publisher pages ------------------
async def collect_all_assets_by_url(page, base_url, page_size=PAGE_SIZE):
    all_links = set()
    page_num = 1

    while True:
        url = f"{base_url}&page={page_num}"
        print(f"[crawl] Visiting {url} ...")
        await page.goto(url, wait_until="domcontentloaded", timeout=120000)
        await page.wait_for_timeout(2000)

        asset_items = await page.query_selector_all("a[href^='/packages/']")
        page_links = set()
        for el in asset_items:
            href = await el.get_attribute("href")
            if href and "/packages/" in href:
                page_links.add("https://assetstore.unity.com" + href)

        num_assets = len(page_links)
        if num_assets == 0:
            print(f"[crawl] No assets found on page {page_num}. Stopping.")
            break

        print(f"[crawl] Found {num_assets} assets on page {page_num}")
        all_links.update(page_links)

        if num_assets < page_size:
            print(f"[crawl] Less than {page_size} assets on page {page_num}. Reached last page.")
            break

        page_num += 1
        await human_wait(1, 2, f"between page {page_num-1} and {page_num}")

    print(f"[crawl] Total assets collected: {len(all_links)}")
    return sorted(all_links)

# ------------------ Asset processing ------------------
async def process_asset(context, link, publisher_name, publisher_page):
    found_prices, upgrade_from, asset_names = [], [], []
    page = await context.new_page()

    async def handle_response(response):
        try:
            headers = await response.all_headers()
            if "application/json" in (headers.get("content-type") or ""):
                data = await response.json()
                text = json.dumps(data)
                if any(k in text for k in ["offerRating", "upgradeFrom", "upgradableFrom", "results", "product"]):
                    extract_offer_rating(data, found_prices)
                    extract_upgrade_from(data, upgrade_from)
                    extract_asset_name(data, asset_names)
        except Exception:
            pass

    page.on("response", handle_response)
    print(f"  [open] {link}")
    goto_task = page.goto(link, wait_until="domcontentloaded", timeout=120000)
    return page, link, found_prices, upgrade_from, asset_names, goto_task

# ------------------ Main Async Routine ------------------
async def main():
    headless_arg = "--no-headless" not in sys.argv
    storage_exists = os.path.exists(STORAGE_STATE)

    # Load publishers
    publishers = []
    if os.path.exists("publishers.txt"):
        with open("publishers.txt", "r", encoding="utf-8") as f:
            for line in f.readlines():
                if line.strip() and "," in line:
                    name, url = line.strip().split(",", 1)
                    publishers.append((name.strip(), url.strip()))
    if not publishers:
        publishers = [DEFAULT_PUBLISHER]

    if not os.path.exists(OUTPUT_CSV):
        save_csv_row([
            "Asset Name", "Original Price", "Final Price",
            "Upgrade From", "Upgrade URL", "Asset URL",
            "Publisher Name", "Publisher Page"
        ])

    processed = load_processed()

    async with async_playwright() as p:
        # Determine first-run headless behavior
        first_headless = False
        if not storage_exists:
            print("[login] storage_state.json not found, forcing headed browser for login...")
            browser = await p.chromium.launch(headless=False, slow_mo=30)
            first_headless = True
        else:
            browser = await p.chromium.launch(headless=headless_arg, slow_mo=30)

        # Create context
        if storage_exists:
            context = await browser.new_context(storage_state=STORAGE_STATE)
            print(f"[login] Using saved login: {STORAGE_STATE}")
        else:
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto("https://assetstore.unity.com/sign-in", wait_until="load")
            input("🔑 Log in manually, then press Enter here to continue...")
            await context.storage_state(path=STORAGE_STATE)
            print(f"[login] Saved login session to {STORAGE_STATE}")
            await page.close()
            await context.close()
            await browser.close()
            # Relaunch headless browser after login if first_headless
            browser = await p.chromium.launch(headless=headless_arg, slow_mo=30)
            context = await browser.new_context(storage_state=STORAGE_STATE)

        # Loop through publishers
        for publisher_name, publisher_url in publishers:
            page = await context.new_page()
            print(f"[start] Navigating to publisher: {publisher_name} ({publisher_url})")
            await page.goto(publisher_url, wait_until="domcontentloaded", timeout=120000)
            await human_wait(2, 3, "initial page load")
            all_assets = await collect_all_assets_by_url(page, publisher_url, page_size=PAGE_SIZE)
            await page.close()

            for batch_num, batch_links in enumerate(batched(all_assets, BATCH_SIZE), start=1):
                batch_links = [l for l in batch_links if l not in processed]
                if not batch_links:
                    continue

                print(f"\n[batch {batch_num}] Processing {len(batch_links)} assets for {publisher_name}...")
                tasks = [process_asset(context, link, publisher_name, publisher_url) for link in batch_links]
                results = await asyncio.gather(*tasks)
                await asyncio.gather(*[r[5] for r in results])
                print(f"[wait] Waiting {DELAY_BETWEEN_BATCHES}s for network responses...")
                await asyncio.sleep(DELAY_BETWEEN_BATCHES)

                for page, link, found_prices, upgrade_from, asset_names, _ in results:
                    asset_name = asset_names[0] if asset_names else link.split("/")[-1]
                    unique_prices = list(set(found_prices)) if found_prices else []

                    asset_url_hl = wrap_hyperlink(link, asset_name)

                    if unique_prices:
                        for (orig, final) in unique_prices:
                            if upgrade_from:
                                for (upg_name, upg_url) in upgrade_from:
                                    upg_url_hl = wrap_hyperlink(upg_url, upg_name) if upg_url else ""
                                    line = f"{asset_name} | Original: {orig or 'N/A'} | Final: {final or 'N/A'} | Upgrade from: {upg_name} ({upg_url}) | {link}"
                                    save_txt_line(line)
                                    save_csv_row([asset_name, orig or "N/A", final or "N/A", upg_name, upg_url_hl, asset_url_hl, publisher_name, wrap_hyperlink(publisher_url, publisher_name)])
                            else:
                                line = f"{asset_name} | Original: {orig or 'N/A'} | Final: {final or 'N/A'} | {link}"
                                save_txt_line(line)
                                save_csv_row([asset_name, orig or "N/A", final or "N/A", "", "", asset_url_hl, publisher_name, wrap_hyperlink(publisher_url, publisher_name)])
                    else:
                        line = f"{asset_name} | No offerRating found | {link}"
                        save_txt_line(line)
                        save_csv_row([asset_name, "", "", "", "", asset_url_hl, publisher_name, wrap_hyperlink(publisher_url, publisher_name)])

                    print("  " + line)
                    mark_processed(link)
                    await page.close()

                print(f"[batch {batch_num}] Done for {publisher_name}.")
                await human_wait(1, 3, "before next batch")

        await context.close()
        await browser.close()
        print("\n✅ All assets processed. Program terminated.")
        print(f"Results saved to:\n  - {OUTPUT_TXT}\n  - {OUTPUT_CSV}\nProgress saved in {PROGRESS_FILE}")

# ------------------ Run ------------------
if __name__ == "__main__":
    asyncio.run(main())
