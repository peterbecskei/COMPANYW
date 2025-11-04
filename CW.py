import os
import re
import csv
import requests
import sys
from urllib.parse import urlparse

# === CONSTANTS ===2
try:
   # print(sys.argv)
    file =sys.argv[1]
except: file =  "URL_LIST0.csv"
print(f"Input file: {file}")
#file =sys.argv[1]
cwd = os.getcwd()
URL_LIST = "URL_LIST.csv"
FILTERED_URL_LIST = "FILTERED_URL_LIST.csv"
PROXY = "36fda789ac44aa4cc19e:b966e984e5922790@gw.dataimpulse.com:10013"
DATAFOLDER = os.path.join(cwd, "Companies")

# === PROXY SETUP ===
proxy_dict = {
    'http': f'http://{PROXY}',
    'https': f'http://{PROXY}'
}


# === FETCH FUNCTION ===
def fetch(url):
    """Fetch HTML content from URL using proxy"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept-Encoding': 'gzip'
    }
    try:
        #response = requests.get(url, headers=headers, proxies=proxy_dict, timeout=30)

        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


# === URL PARSING ===
def parse_url_tree(url):
    """Extract URL tree components from companywall.hu URLs"""
    parsed = urlparse(url)
    path_parts = parsed.path.strip('/').split('/')
    #print(path_parts)
    if len(path_parts) >= 2 :
        company_part = path_parts[1]  # e.g., "horizontplast-kft"
        #print(company_part)
        return company_part
    return None


def filter_urls():
    """Level 1: Filter URLs based on company type patterns"""
    if not os.path.exists(URL_LIST):
        print(f"❌ {URL_LIST} not found!")
        return

    filtered = []
    with open(URL_LIST, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        urls = [row[0].strip() for row in reader if row]

    print(f"Level 2: Filtering {len(urls)} URLs...")

    pattern_include = re.compile(r'-kft|-bt|-zrt', re.IGNORECASE)
    #pattern_include = re.compile(r'horizon', re.IGNORECASE)

    # pattern_exclude = re.compile(r'-v-a|-f-a', re.IGNORECASE)
    pattern_exclude = re.compile(r'-xxxxxxxxxxxv-a', re.IGNORECASE)
    for url in urls:
        if pattern_include.search(url) and not pattern_exclude.search(url):
            filtered.append(url)

    # Save to FILTERED_URL_LIST.csv
    with open(FILTERED_URL_LIST, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for url in filtered:
            writer.writerow([url])
#    print(f"Level 2 complete. {len(filtered)} URLs saved to {FILTERED_URL_LIST}")

    print(f"✅ Level 1 complete: {len(filtered)} URLs filtered -> {FILTERED_URL_LIST}")


# === CONTENT FETCHING ===
def fetch_content():
    """Level 2: Fetch HTML content for filtered URLs"""
    if not os.path.exists(FILTERED_URL_LIST):
        print(f"❌ {FILTERED_URL_LIST} not found!")
        return

    # Read filtered URLs
    #file =sys.argv[1]

    with open(file , 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        urls = [row[0] for row in reader if row]

    print(f"📥 Fetching content for {len(urls)} URLs...")

    for url in urls:
        url_tree = parse_url_tree(url)
        if not url_tree:
            continue

        # Generate filename and folder
        folder_code = url_tree[:2].upper()
        filename = f"{url_tree}.html"
        folder_path = os.path.join(DATAFOLDER, folder_code)

        # Create folder if needed
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, filename)

        # Skip if already exists
        if os.path.exists(file_path):
            print(f"⏭️  Already exists: {filename}")
            continue

        # Fetch and save content
        html_content = fetch(url)
        if html_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"✅ Saved: {filename}")
        else:
            print(f"❌ Failed: {url_tree}")


# === MAIN EXECUTION ===
def main():
    """Main function with separate level execution"""
    print("🚀 CWSITEMAPROXY - Web Scraping Tool")
    print("=" * 40)

    while True:
        print("\nSelect operation:")
        print("1. Level 1 - Filter URLs")
        print("2. Level 2 - Fetch Content")
        print("3. Run Both Levels")
        print("4. Exit")

        choice = input("\nEnter choice (1-4): ").strip()

        if choice == '1':
            print("\n🎯 Running Level 1 - URL Filtering...")
            filter_urls()

        elif choice == '2':
            print("\n🎯 Running Level 2 - Content Fetching...")
            if os.path.exists(FILTERED_URL_LIST):

               fetch_content()
            else:
                print("❌ Run Level 1 first to create filtered URL list!")

        elif choice == '3':
            print("\n🎯 Running Both Levels...")
            filter_urls()
            if os.path.exists(FILTERED_URL_LIST):
                f=1
                #fetch_content()

        elif choice == '4':
            print("👋 Exiting...")
            break

        else:
            print("❌ Invalid choice!")


if __name__ == "__main__":
    main()