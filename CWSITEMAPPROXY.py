import os
import csv
import re
import requests
from urllib.parse import urljoin
import time

# === CONSTANTS ===
cwd = os.getcwd()
SITEMAP_LIST = "SITEMAP_LIST.csv"
URL_LIST = "URL_LIST.csv"
FILTERED_URL_LIST = "FILTERED_URL_LIST.csv"
PROXI_LIST = "PROXI_LIST.csv"
PROXI_COUNT = 40
DATAFOLDER = os.path.join(cwd, "Companies")
PROXED = True
#PROXED = False
# Ensure DATAFOLDER exists
os.makedirs(DATAFOLDER, exist_ok=True)

# === PROXY HANDLING ===
proxy_index = 0
proxy_usage = 0
proxies_list = []

def load_proxies():
    global proxies_list
    if not os.path.exists(PROXI_LIST):
        print(f"{PROXI_LIST} not found. Running without proxies.")
        return []
    with open(PROXI_LIST, "r", encoding="utf-8") as f:
        proxies_list = [line.strip() for line in f if line.strip()]
    print(f"Loaded {len(proxies_list)} proxies.")
    return proxies_list

def get_next_proxy():
    global proxy_index, proxy_usage
    if not proxies_list:
        return None
    proxy = proxies_list[proxy_index % len(proxies_list)]
    proxy_index += 1
    proxy_usage += 1
    return {"http": f"http://{proxy}", "https": f"http://{proxy}"}

def reset_proxy_counter():
    global proxy_usage
    proxy_usage = 0

# === FETCH FUNCTIONS ===
def fetch(url, use_proxy=False, timeout=10):
    headers = {
        'Accept-Encoding': 'gzip',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    #print(proxy)
    try:
        if use_proxy and PROXED:
            if proxy_usage >= PROXI_COUNT:
                proxy = get_next_proxy()
                reset_proxy_counter()
                print("  Proxy rotation reset.")
            #proxy = get_next_proxy()
            print(proxy)
            print(url)
            if proxy:

                response = requests.get(url, headers=headers, proxies=proxy, timeout=timeout, stream=True)
                #print(len(response.content))
                # --- 1️⃣ Check if compressed ---
                #encoding = response.headers.get("Content-Encoding")
                #if encoding:
                #    print(f"Response is compressed with: {encoding}")
                #else:
                #    print("Response is not compressed.")

                # --- 2️⃣ Measure compressed size ---
                # The 'raw' stream contains the compressed data
                #raw_bytes = response.raw.read()
                #compressed_size = len(raw_bytes)
                #print(f"Compressed size: {compressed_size} bytes")

                #print(response.headers['Content-length'])
                #print(response.headers)
                #print(len(response.content))
            else:
                response = requests.get(url, headers=headers, timeout=timeout)
        else:
            response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Fetch failed for {url}: {e}")
        return None

def fetch_with_proxy_retry(url, proxy,timeout=10):
    print("Using proxies to fetch:", url)
    headers = {
        'Accept-Encoding': 'gzip',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    max_retries = 5
    print(proxy)
    for _ in range(max_retries):
        #html = fetch(url, use_proxy=True, timeout=10)
        html = requests.get(url, headers=headers, proxies=proxy, timeout=timeout)

        if html is not None:
            return html
        print("Proxy failed, trying next...")
        time.sleep(1)
    return None

# === LEVEL 1: Process SITEMAP_LIST → Extract sub-URLs → Save to URL_LIST.csv ===
def level_1():
    if not os.path.exists(SITEMAP_LIST):
        print(f"{SITEMAP_LIST} not found. Level 1 skipped.")
        return
    proxy = get_next_proxy()
    print("PR",proxy)
    url_list = []
    with open(SITEMAP_LIST, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        sitemap_urls = [row[0].strip() for row in reader if row]
    #print(sitemap_urls)
    #print(type(sitemap_urls))
    print(f"Level 1: Fetching {len(sitemap_urls)} sitemap URLs...")

    for idx, sitemap_url in enumerate(sitemap_urls, 1):
        print(f"  [{idx}] Fetching sitemap: {sitemap_url}")
        html = fetch_with_proxy_retry(sitemap_url,proxy) if PROXED else fetch(sitemap_url)
        if not html:
            continue
        #print(html.text)
        # Extract <loc> URLs using simple regex

        locs = re.findall(r'<loc>(https?://[^<]+)</loc>', html.text, re.IGNORECASE)
        url_list.extend(locs)
        print(f"    Found {len(locs)} URLs in sitemap.")
        print(locs)
        #print(url_list)
        # Reset proxy counter every PROXI_COUNT requests
        #if PROXED and proxy_usage >= PROXI_COUNT:
        #    reset_proxy_counter()
        #    print("  Proxy rotation reset.")

    # Save to URL_LIST.csv
    with open(URL_LIST, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for url in url_list:
            writer.writerow([url])
    print(f"Level 1 complete. {len(url_list)} URLs saved to {URL_LIST}")

# === LEVEL 2: Filter URL_LIST → Save to FILTERED_URL_LIST.csv ===
def level_2():
    if not os.path.exists(URL_LIST):
        print(f"{URL_LIST} not found. Level 2 skipped.")
        return

    filtered = []
    with open(URL_LIST, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        urls = [row[0].strip() for row in reader if row]

    print(f"Level 2: Filtering {len(urls)} URLs...")

    pattern_include = re.compile(r'-kft|-bt|-zrt', re.IGNORECASE)
    #pattern_exclude = re.compile(r'-v-a|-f-a', re.IGNORECASE)
    pattern_exclude = re.compile(r'-xxxxxxxxxxxv-a', re.IGNORECASE)
    for url in urls:
        if pattern_include.search(url) and not pattern_exclude.search(url):
            filtered.append(url)

    # Save to FILTERED_URL_LIST.csv
    with open(FILTERED_URL_LIST, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for url in filtered:
            writer.writerow([url])
    print(f"Level 2 complete. {len(filtered)} URLs saved to {FILTERED_URL_LIST}")

# === LEVEL 3: Fetch filtered URLs → Save HTML to DATAFOLDER as 1.html, 2.html... ===
def level_3():
    if not os.path.exists(FILTERED_URL_LIST):
        print(f"{FILTERED_URL_LIST} not found. Level 3 skipped.")
        return

    with open(FILTERED_URL_LIST, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        urls = [row[0].strip() for row in reader if row]

    print(f"Level 3: Fetching content for {len(urls)} URLs...")

    for idx, url in enumerate(urls, 1):
        filename = os.path.join(DATAFOLDER, f"{idx}.html")
        if os.path.exists(filename):
            print(f"  [{idx}] Already exists: {filename}")
            continue

        print(f"  [{idx}] Fetching: {url}")
        html = fetch_with_proxy_retry(url) if PROXED else fetch(url)
        if html:
            with open(filename, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"    Saved: {filename}")
        else:
            print(f"    Failed: {url}")

        # Rotate proxy every PROXI_COUNT
        if PROXED and proxy_usage >= PROXI_COUNT:
            reset_proxy_counter()
            print("  Proxy rotation reset.")
        time.sleep(0.5)  # Be gentle

    print("Level 3 complete.")

# === MAIN: Run levels independently ===
if __name__ == "__main__":
    # Load proxies once
    if PROXED:
        load_proxies()

    print("=== Web Scraper with Proxy Support ===\n")

    # Run levels one by one (can be commented out individually)
    #level_1()
    #level_2()
    level_3()

    print("\nAll levels completed.")