#!/usr/bin/env python3
"""
CW fetcher
- Loads URLs from URL_LIST1.csv
- Fetches via single PROXY
- Validates title and canonical link
- Saves HTML to Companies_1/<filename>.html
"""

import os
import csv
import re
import gzip
import logging
from urllib.parse import urlparse, unquote
import requests
import sys

# === CONSTANTS ===
cwd = os.getcwd()
N = 0
URL_LIST = "URL_LIST" + str(N) + ".csv"
#PROXY = "36fda789ac44aa4cc19e:b966e984e5922790@gw.dataimpulse.com:10012"
#PROXY = "159.112.235.141:80"
PROXY = "195.56.65.172:8081"
DATAFOLDER = os.path.join(cwd, "Companies_" + str(N))
NOPROXY = True
# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Simple regex patterns
_RE_TITLE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_RE_CANON = re.compile(r'<link\s+[^>]*rel=["\']canonical["\'][^>]*href=["\']([^"\']+)["\']', re.IGNORECASE)

# Requests settings
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CWFetcher/1.0)",
    "Accept-Encoding": "gzip"  # ensure server responds with gzip instead of brotli
}
# Use the single proxy for both http and https (no rotation)
PROXIES = {
    "http": f"http://{PROXY}",
    "https": f"http://{PROXY}"
}
REQUEST_TIMEOUT = 20  # seconds


def check_csv_exists(path):
    if not os.path.isfile(path):
        logging.error("Required CSV missing: %s", path)
        return False
    return True


def load_urls_from_csv(path):
    urls = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            # assume URL is in first column; strip whitespace
            url = row[0].strip()
            if url:
                urls.append(url)
    return urls


def parse_filename_from_url(url):
    """
    Example:
      https://www.companywall.hu/v%C3%A1llalat/horizontplast-kft/MMGJWPVR
    -> horizontplast-kft_MMGJWPVR.html
    """
    parsed = urlparse(url)
    # Get path segments, ignore leading/trailing slashes
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        # fallback: use netloc + path
        safe = (parsed.netloc + parsed.path).replace("/", "_")
        return unquote(safe) + ".html"
    # usually last two segments are company-name and code
    if len(parts) >= 2:
        name = unquote(parts[-2])
        code = unquote(parts[-1])
        filename = f"{name}_{code}.html"
    else:
        filename = unquote(parts[-1]) + ".html"
    # sanitize filename a tiny bit
    filename = filename.replace(" ", "_")
    return filename


def fetch_url(url):
    """
    Fetch URL using global PROXIES and HEADERS.
    Returns tuple (status_code, response_bytes, response_headers)
    or raises requests.RequestException
    """
    print(PROXY)
    if NOPROXY:  resp = requests.get(url, headers=HEADERS,  timeout=REQUEST_TIMEOUT)
    else:  resp = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=REQUEST_TIMEOUT)

    resp.raise_for_status()
    return resp.status_code, resp.content, resp.headers


def compressed_size(data_bytes):
    """Return gzip-compressed size of bytes."""
    gz = gzip.compress(data_bytes)
    return len(gz)


def extract_title(html_bytes):
    try:
        html = html_bytes.decode("utf-8", errors="ignore")
    except Exception:
        html = html_bytes.decode("latin1", errors="ignore")
    m = _RE_TITLE.search(html)
    return m.group(1).strip() if m else ""


def extract_canonical(html_bytes):
    try:
        html = html_bytes.decode("utf-8", errors="ignore")
    except Exception:
        html = html_bytes.decode("latin1", errors="ignore")
    m = _RE_CANON.search(html)
    return m.group(1).strip() if m else None


def save_html(folder, filename, html_bytes):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)
    with open(path, "wb") as f:
        f.write(html_bytes)
    logging.info("Saved: %s", path)
    return path


def main():
    csv_path = os.path.join(cwd, URL_LIST)
    if not check_csv_exists(csv_path):
        sys.exit(1)

    urls = load_urls_from_csv(csv_path)
    if not urls:
        logging.error("No URLs found in %s", csv_path)
        sys.exit(1)

    logging.info("Will process %d URLs. Data folder: %s", len(urls), DATAFOLDER)

    logging.info("Will process %d URLs. Data folder: %s", len(urls), DATAFOLDER)

    for idx, url in enumerate(urls, start=1):
        logging.info("[%d/%d] Processing: %s", idx, len(urls), url)
        filename = parse_filename_from_url(url)
        file_path = os.path.join(DATAFOLDER, filename)

        # modify happend next line:
        if os.path.isfile(file_path):
            logging.info("File already exists, skipping fetch: %s", file_path)
            continue

        # Only attempt network fetch when file does not already exist
        try:
            status, content, headers = fetch_url(url)
        except requests.RequestException as e:
            logging.error("Fetch error: %s", e)
            logging.error("Fetch error")
            sys.exit(1)

        # Check compressed size (gzip)
        size_gz = compressed_size(content)
        logging.info("Gzip-compressed size: %d bytes", size_gz)
        if size_gz > 1024*30:
            logging.error("Compressed size < 50000: stopping. Fetch error")
            logging.error("Fetch error")
            sys.exit(1)

        # Check title
        title = extract_title(content)
        logging.info("Title: %s", title)
        if title == "RegisterOpenUser":
            logging.error("Capcsa error")
            sys.exit(1)

        # Check canonical
        canonical = extract_canonical(content)
        logging.info("Canonical: %s", canonical)
        # Compare canonical to original URL exactly (per requirement)
        if canonical is None:
            logging.error("No canonical tag found: URL error")
            logging.error("URL error")
            sys.exit(1)
        # Normalize trivial trailing slash differences
        norm_canonical = canonical.rstrip("/")
        norm_url = url.rstrip("/")
        if norm_canonical != norm_url:
            logging.error("Canonical href != URL: URL error")
            logging.error("URL error")
            sys.exit(1)

        # Save file
        filename = parse_filename_from_url(url)
        save_html(DATAFOLDER, filename, content)

    logging.info("All done.")


if __name__ == "__main__":
    main()
